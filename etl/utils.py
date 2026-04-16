"""
Shared PySpark utilities and data quality helpers for the predictive maintenance ETL pipeline.

This module provides:
  - SparkSession factory configured for Unity Catalog + Delta Lake
  - DataQualityChecker class for schema and range validation
  - Delta table write helpers
  - CloudWatch metric publishing from Spark jobs
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

CLOUDWATCH_NAMESPACE = "PredictiveMaintenance"
DEFAULT_REGION = "us-east-1"


# ---------------------------------------------------------------------------
# SparkSession factory
# ---------------------------------------------------------------------------


def get_spark_session(
    app_name: str = "pmt-etl",
    catalog: str = "workspace",
    enable_delta_extensions: bool = True,
) -> SparkSession:
    """
    Build and return a SparkSession configured for Unity Catalog and Delta Lake.

    On Databricks, an active session already exists and this function
    returns it. Locally, it creates a new session with the Delta Lake
    extension jar on the classpath.

    Parameters
    ----------
    app_name:
        Spark application name (visible in the Spark UI).
    catalog:
        Default Unity Catalog catalog to use.
    enable_delta_extensions:
        Whether to add Delta Lake extensions configuration (needed locally).

    Returns
    -------
    Configured SparkSession.
    """
    try:
        # On Databricks, SparkSession is pre-configured — just get it
        session = SparkSession.getActiveSession()
        if session is not None:
            logger.info("Using active Databricks SparkSession")
            session.catalog.setCurrentCatalog(catalog)
            return session
    except Exception:
        pass

    builder = SparkSession.builder.appName(app_name)

    if enable_delta_extensions:
        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )

    spark = builder.getOrCreate()
    spark.catalog.setCurrentCatalog(catalog)
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    logger.info("Created local SparkSession with app_name=%s catalog=%s", app_name, catalog)
    return spark


# ---------------------------------------------------------------------------
# Data quality
# ---------------------------------------------------------------------------


class DataQualityChecker:
    """
    Runs data quality checks on a Spark DataFrame and logs results.

    Each check returns True if the data meets the threshold and False if it
    does not. Results are accumulated and can be summarised via log_summary().
    """

    def __init__(self, df: DataFrame, table_name: str) -> None:
        """
        Initialise the checker.

        Parameters
        ----------
        df:
            The DataFrame to validate.
        table_name:
            Name used in log messages for context.
        """
        self._df = df
        self.table_name = table_name
        self._results: dict[str, bool] = {}
        self._total_rows: Optional[int] = None

    def _row_count(self) -> int:
        """Cache and return the DataFrame row count."""
        if self._total_rows is None:
            self._total_rows = self._df.count()
        return self._total_rows

    def check_null_rate(self, column: str, threshold: float = 0.05) -> bool:
        """
        Check that the null rate in a column does not exceed the threshold.

        Parameters
        ----------
        column:
            Column to inspect.
        threshold:
            Maximum acceptable null rate (0.05 = 5 %).

        Returns
        -------
        True if null rate ≤ threshold.
        """
        from pyspark.sql import functions as F

        total = self._row_count()
        if total == 0:
            self._results[f"null_rate_{column}"] = True
            return True

        null_count = self._df.filter(F.col(column).isNull()).count()
        rate = null_count / total
        passed = rate <= threshold
        self._results[f"null_rate_{column}"] = passed
        level = logging.INFO if passed else logging.WARNING
        logger.log(
            level,
            "[DQ] %s | null_rate(%s)=%.2f%% threshold=%.2f%% passed=%s",
            self.table_name, column, rate * 100, threshold * 100, passed,
        )
        return passed

    def check_row_count(self, min_rows: int = 1) -> bool:
        """
        Check that the DataFrame contains at least min_rows rows.

        Parameters
        ----------
        min_rows:
            Minimum acceptable row count.

        Returns
        -------
        True if row count ≥ min_rows.
        """
        count = self._row_count()
        passed = count >= min_rows
        self._results["min_row_count"] = passed
        level = logging.INFO if passed else logging.WARNING
        logger.log(
            level,
            "[DQ] %s | row_count=%d min_rows=%d passed=%s",
            self.table_name, count, min_rows, passed,
        )
        return passed

    def check_value_range(self, column: str, min_val: float, max_val: float) -> bool:
        """
        Check that all non-null values in a column fall within [min_val, max_val].

        Parameters
        ----------
        column:
            Column to inspect (must be numeric).
        min_val:
            Minimum acceptable value (inclusive).
        max_val:
            Maximum acceptable value (inclusive).

        Returns
        -------
        True if all values are within range.
        """
        from pyspark.sql import functions as F

        out_of_range = self._df.filter(
            F.col(column).isNotNull()
            & ((F.col(column) < min_val) | (F.col(column) > max_val))
        ).count()
        passed = out_of_range == 0
        self._results[f"value_range_{column}"] = passed
        level = logging.INFO if passed else logging.WARNING
        logger.log(
            level,
            "[DQ] %s | value_range(%s) [%.2f, %.2f] out_of_range=%d passed=%s",
            self.table_name, column, min_val, max_val, out_of_range, passed,
        )
        return passed

    def run_all_checks(self) -> dict[str, bool]:
        """
        Run a standard battery of checks for sensor data.

        Returns
        -------
        Dict mapping check name to pass/fail boolean.
        """
        self.check_row_count(min_rows=1)
        self.check_null_rate("device_id", threshold=0.0)   # device_id must never be null
        self.check_null_rate("entry_id", threshold=0.0)
        self.check_null_rate("recorded_at", threshold=0.0)
        self.check_null_rate("vibration_rms", threshold=0.5)
        self.check_null_rate("temperature_celsius", threshold=0.5)
        self.check_null_rate("pressure_bar", threshold=0.5)
        return self._results

    def log_summary(self) -> None:
        """Log a pass/fail summary of all checks run so far."""
        passed = sum(1 for v in self._results.values() if v)
        failed = len(self._results) - passed
        level = logging.INFO if failed == 0 else logging.WARNING
        logger.log(
            level,
            "[DQ SUMMARY] %s | checks=%d passed=%d failed=%d",
            self.table_name, len(self._results), passed, failed,
        )


# ---------------------------------------------------------------------------
# Delta table helpers
# ---------------------------------------------------------------------------


def log_table_stats(df: DataFrame, table_name: str, logger: logging.Logger) -> None:
    """
    Log row count, distinct device count, and recorded_at range for a DataFrame.

    Parameters
    ----------
    df:
        DataFrame to inspect.
    table_name:
        Label used in log messages.
    logger:
        Logger instance to write to.
    """
    from pyspark.sql import functions as F

    stats = df.agg(
        F.count("*").alias("row_count"),
        F.countDistinct("device_id").alias("distinct_devices"),
        F.min("recorded_at").alias("min_recorded_at"),
        F.max("recorded_at").alias("max_recorded_at"),
    ).collect()[0]

    logger.info(
        "[STATS] %s | rows=%d devices=%d recorded_at=[%s → %s]",
        table_name,
        stats["row_count"],
        stats["distinct_devices"],
        stats["min_recorded_at"],
        stats["max_recorded_at"],
    )


def write_delta_table(
    df: DataFrame,
    full_table_name: str,
    mode: str = "append",
    partition_cols: Optional[list[str]] = None,
) -> None:
    """
    Write a DataFrame to a Delta table.

    Parameters
    ----------
    df:
        DataFrame to persist.
    full_table_name:
        Three-part table name: ``catalog.schema.table``.
    mode:
        Write mode: 'append', 'overwrite', or 'overwritePartition'.
    partition_cols:
        Columns to partition the Delta table by.
    """
    writer = df.write.format("delta").mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(full_table_name)
    logger.info("Wrote %s (mode=%s)", full_table_name, mode)


def get_table_row_count(spark: SparkSession, table_name: str) -> int:
    """
    Return the row count of a Delta table.

    Parameters
    ----------
    spark:
        Active SparkSession.
    table_name:
        Three-part table name.

    Returns
    -------
    Row count as an integer, or 0 if the table does not exist.
    """
    try:
        return spark.table(table_name).count()
    except Exception as exc:
        logger.warning("Could not count rows in %s: %s", table_name, exc)
        return 0


# ---------------------------------------------------------------------------
# CloudWatch metric publisher
# ---------------------------------------------------------------------------


def publish_pipeline_metric(
    metric_name: str,
    value: float,
    unit: str = "Count",
    dimensions: Optional[dict] = None,
    region: str = DEFAULT_REGION,
) -> None:
    """
    Publish a single custom metric to CloudWatch (AWS secondary backend only).

    This is a no-op when boto3 is not installed or AWS credentials are not
    configured — the pipeline continues normally and logs a debug message.

    Parameters
    ----------
    metric_name:
        CloudWatch metric name (e.g. 'BronzeRowCount').
    value:
        Metric value.
    unit:
        CloudWatch unit string (e.g. 'Count', 'Milliseconds', 'None').
    dimensions:
        Dict of dimension Name → Value pairs.
    region:
        AWS region.
    """
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError:
        logger.debug("boto3 not installed — skipping CloudWatch metric '%s'", metric_name)
        return

    metric_data: dict = {
        "MetricName": metric_name,
        "Value": value,
        "Unit": unit,
        "Timestamp": datetime.now(timezone.utc),
    }
    if dimensions:
        metric_data["Dimensions"] = [{"Name": k, "Value": v} for k, v in dimensions.items()]

    try:
        cw = boto3.client("cloudwatch", region_name=region)
        cw.put_metric_data(Namespace=CLOUDWATCH_NAMESPACE, MetricData=[metric_data])
        logger.debug("Published CloudWatch metric: %s=%.2f %s", metric_name, value, unit)
    except (ClientError, BotoCoreError) as exc:
        # Covers NoCredentialsError, EndpointResolutionError, etc.
        logger.warning(
            "Could not publish CloudWatch metric '%s' (AWS unavailable): %s", metric_name, exc
        )
