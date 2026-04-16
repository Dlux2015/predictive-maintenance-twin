"""
Custom CloudWatch metric publisher for the Predictive Maintenance pipeline.

Designed to run as the final Databricks job task after Gold aggregation,
querying the Gold Delta tables and publishing KPIs to CloudWatch:

  - BronzeRowCount, SilverRowCount, GoldHourlyRowCount, GoldDailyRowCount
  - AtRiskDeviceCount (devices with is_at_risk = true today)
  - MaxVibrationZScore (highest z-score across all devices today)
  - BronzeWriteLatencyMs, SilverWriteLatencyMs, GoldWriteLatencyMs
  - GoldTableLastUpdate (epoch seconds of most recent Gold daily record)

Can also run standalone against a Databricks SQL endpoint.

Usage
-----
    python observability/custom_metrics.py [--dry-run] [--region us-east-1]

Environment variables
---------------------
AWS_REGION                  AWS region (default: us-east-1)
DATABRICKS_SERVER_HOSTNAME  Databricks SQL warehouse hostname
DATABRICKS_HTTP_PATH        Databricks SQL warehouse HTTP path
DATABRICKS_TOKEN            Databricks personal access token
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Optional

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_REGION = "us-east-1"
DEFAULT_NAMESPACE = "PredictiveMaintenance"
DEFAULT_CATALOG = "main"
DEFAULT_SCHEMA = "predictive_maintenance"

# CloudWatch max metrics per put_metric_data call
_CW_BATCH_LIMIT = 20


class MetricPublisher:
    """
    Publishes custom pipeline health metrics to CloudWatch.

    Batches up to 20 metrics per API call (CloudWatch hard limit) and
    handles errors gracefully so a single failed metric does not block others.
    """

    def __init__(
        self,
        namespace: str = DEFAULT_NAMESPACE,
        region: str = DEFAULT_REGION,
        dry_run: bool = False,
    ) -> None:
        """
        Initialise the publisher.

        Parameters
        ----------
        namespace:
            CloudWatch custom metrics namespace.
        region:
            AWS region.
        dry_run:
            If True, log metrics without publishing to CloudWatch.
        """
        self.namespace = namespace
        self.region = region
        self.dry_run = dry_run
        if not dry_run:
            self._client = boto3.client("cloudwatch", region_name=region)

    def publish(
        self,
        metric_name: str,
        value: float,
        unit: str = "Count",
        dimensions: Optional[list[dict]] = None,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """
        Publish a single metric to CloudWatch.

        Parameters
        ----------
        metric_name:
            CloudWatch metric name.
        value:
            Metric value.
        unit:
            CloudWatch unit (Count, Milliseconds, Bytes, None, etc.).
        dimensions:
            List of {"Name": ..., "Value": ...} dimension dicts.
        timestamp:
            Metric timestamp (defaults to now in UTC).
        """
        self.publish_batch(
            [
                {
                    "MetricName": metric_name,
                    "Value": value,
                    "Unit": unit,
                    "Dimensions": dimensions or [],
                    "Timestamp": timestamp or datetime.now(timezone.utc),
                }
            ]
        )

    def publish_batch(self, metrics: list[dict]) -> None:
        """
        Publish multiple metrics to CloudWatch in batches of 20.

        Parameters
        ----------
        metrics:
            List of metric dicts compatible with CloudWatch MetricData format.
        """
        for i in range(0, len(metrics), _CW_BATCH_LIMIT):
            batch = metrics[i : i + _CW_BATCH_LIMIT]

            if self.dry_run:
                for m in batch:
                    logger.info(
                        "[DRY-RUN] Would publish: %s=%.4f %s",
                        m["MetricName"],
                        m.get("Value", 0),
                        m.get("Unit", ""),
                    )
                continue

            try:
                self._client.put_metric_data(
                    Namespace=self.namespace,
                    MetricData=batch,
                )
                logger.info(
                    "Published %d metrics to CloudWatch (batch %d)",
                    len(batch),
                    i // _CW_BATCH_LIMIT + 1,
                )
            except ClientError as exc:
                logger.error(
                    "Failed to publish metrics batch: %s",
                    exc.response["Error"]["Message"],
                )

    def collect_table_metrics(self, connection: Any) -> list[dict]:
        """
        Query Gold Delta tables and collect row count metrics.

        Parameters
        ----------
        connection:
            Databricks SQL connector connection (or any DB-API 2.0 connection).
            Can also be a SparkSession.

        Returns
        -------
        List of CloudWatch MetricData dicts.
        """
        queries = {
            "BronzeRowCount": f"SELECT COUNT(*) FROM {DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.bronze_sensors",
            "SilverRowCount": f"SELECT COUNT(*) FROM {DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.silver_sensors",
            "GoldHourlyRowCount": f"SELECT COUNT(*) FROM {DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.gold_sensors_hourly",
            "GoldDailyRowCount": f"SELECT COUNT(*) FROM {DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.gold_sensors_daily",
        }
        metrics = []
        now = datetime.now(timezone.utc)

        for metric_name, query in queries.items():
            try:
                result = self._execute_query(connection, query)
                value = float(result[0][0]) if result else 0.0
                metrics.append(
                    {
                        "MetricName": metric_name,
                        "Value": value,
                        "Unit": "Count",
                        "Dimensions": [],
                        "Timestamp": now,
                    }
                )
                logger.info("%s = %.0f", metric_name, value)
            except Exception as exc:
                logger.error("Failed to collect %s: %s", metric_name, exc)

        # At-risk device count
        try:
            result = self._execute_query(
                connection,
                f"""SELECT COUNT(DISTINCT device_id)
                    FROM {DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.gold_sensors_daily
                    WHERE is_at_risk = TRUE AND date = CURRENT_DATE()""",
            )
            at_risk = float(result[0][0]) if result else 0.0
            metrics.append(
                {
                    "MetricName": "AtRiskDeviceCount",
                    "Value": at_risk,
                    "Unit": "Count",
                    "Dimensions": [],
                    "Timestamp": now,
                }
            )
            logger.info("AtRiskDeviceCount = %.0f", at_risk)
        except Exception as exc:
            logger.error("Failed to collect AtRiskDeviceCount: %s", exc)

        # Max vibration z-score today
        try:
            result = self._execute_query(
                connection,
                f"""SELECT MAX(vibration_zscore)
                    FROM {DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.gold_sensors_daily
                    WHERE date = CURRENT_DATE()""",
            )
            max_z = float(result[0][0]) if result and result[0][0] is not None else 0.0
            metrics.append(
                {
                    "MetricName": "MaxVibrationZScore",
                    "Value": max_z,
                    "Unit": "None",
                    "Dimensions": [],
                    "Timestamp": now,
                }
            )
            logger.info("MaxVibrationZScore = %.3f", max_z)
        except Exception as exc:
            logger.error("Failed to collect MaxVibrationZScore: %s", exc)

        return metrics

    def collect_pipeline_latency(
        self, stage: str, latency_ms: float, device_id: str = "ALL"
    ) -> None:
        """
        Publish a pipeline stage write latency metric.

        Parameters
        ----------
        stage:
            Pipeline stage: 'Bronze', 'Silver', or 'Gold'.
        latency_ms:
            Write latency in milliseconds.
        device_id:
            Device identifier or 'ALL' for aggregate.
        """
        self.publish(
            metric_name=f"{stage}WriteLatencyMs",
            value=latency_ms,
            unit="Milliseconds",
            dimensions=[{"Name": "DeviceId", "Value": device_id}],
        )

    def collect_freshness_metrics(self, connection: Any) -> None:
        """
        Publish GoldTableLastUpdate metric (epoch seconds of latest Gold record).

        A staleness alarm compares this value against the current epoch time.
        If the difference exceeds 2 hours, the pmt-gold-stale alarm triggers.

        Parameters
        ----------
        connection:
            Databricks SQL connector connection.
        """
        try:
            result = self._execute_query(
                connection,
                f"""SELECT UNIX_TIMESTAMP(MAX(CAST(date AS TIMESTAMP)))
                    FROM {DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.gold_sensors_daily""",
            )
            epoch = float(result[0][0]) if result and result[0][0] is not None else 0.0
            self.publish(
                metric_name="GoldTableLastUpdate",
                value=epoch,
                unit="None",
            )
            logger.info("GoldTableLastUpdate = %.0f (epoch)", epoch)
        except Exception as exc:
            logger.error("Failed to collect GoldTableLastUpdate: %s", exc)

    def _execute_query(self, connection: Any, query: str) -> list:
        """
        Execute a SQL query using either a DB-API connection or SparkSession.

        Parameters
        ----------
        connection:
            DB-API 2.0 connection or PySpark SparkSession.
        query:
            SQL query string.

        Returns
        -------
        List of result rows.
        """
        # PySpark SparkSession
        if hasattr(connection, "sql"):
            rows = connection.sql(query).collect()
            return [list(row) for row in rows]

        # DB-API 2.0 (databricks-sql-connector)
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def run_all(self, connection: Any) -> None:
        """
        Collect and publish all pipeline metrics.

        Parameters
        ----------
        connection:
            Databricks SQL connection or SparkSession.
        """
        logger.info("Collecting pipeline metrics...")
        metrics = self.collect_table_metrics(connection)
        self.publish_batch(metrics)
        self.collect_freshness_metrics(connection)
        logger.info(
            "All pipeline metrics published to CloudWatch namespace: %s", self.namespace
        )


def _get_databricks_connection(hostname: str, http_path: str, token: str):
    """Create a Databricks SQL connector connection."""
    try:
        from databricks import sql

        return sql.connect(
            server_hostname=hostname,
            http_path=http_path,
            access_token=token,
        )
    except ImportError:
        logger.error(
            "databricks-sql-connector not installed. Run: pip install databricks-sql-connector"
        )
        raise


def main() -> None:
    """Parse arguments and run the metric publisher."""
    parser = argparse.ArgumentParser(
        description="Publish custom pipeline metrics to CloudWatch"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=os.environ.get("DRY_RUN", "").lower() == "true",
    )
    parser.add_argument(
        "--region", default=os.environ.get("AWS_REGION", DEFAULT_REGION)
    )
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    parser.add_argument(
        "--databricks-server-hostname",
        default=os.environ.get("DATABRICKS_SERVER_HOSTNAME", ""),
    )
    parser.add_argument(
        "--databricks-http-path", default=os.environ.get("DATABRICKS_HTTP_PATH", "")
    )
    parser.add_argument(
        "--databricks-token", default=os.environ.get("DATABRICKS_TOKEN", "")
    )
    args = parser.parse_args()

    publisher = MetricPublisher(
        namespace=args.namespace,
        region=args.region,
        dry_run=args.dry_run,
    )

    # Try to use active SparkSession (when running inside Databricks)
    connection = None
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is not None:
            logger.info("Using active SparkSession for metric queries")
            connection = spark
    except Exception:
        pass

    # Fall back to Databricks SQL connector
    if connection is None:
        if not (
            args.databricks_server_hostname
            and args.databricks_http_path
            and args.databricks_token
        ):
            logger.error(
                "No SparkSession available and DATABRICKS_SERVER_HOSTNAME / "
                "DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN not set. Cannot query tables."
            )
            return
        logger.info("Using Databricks SQL connector for metric queries")
        connection = _get_databricks_connection(
            hostname=args.databricks_server_hostname,
            http_path=args.databricks_http_path,
            token=args.databricks_token,
        )

    publisher.run_all(connection)


if __name__ == "__main__":
    main()
