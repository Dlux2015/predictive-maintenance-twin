"""
CloudWatch alarms and SNS topic setup for the Predictive Maintenance pipeline.

Creates alerting infrastructure that monitors pipeline health:
  - Ingestion freshness (no records in 5 min → STALE alarm)
  - Pipeline errors (any error → immediate alarm)
  - Kinesis iterator lag (> 60s → consumer falling behind)
  - S3 put errors (5xx errors on the raw bucket)
  - Gold table freshness (no Gold update in 2 hours)

All operations are idempotent. Run with --dry-run to preview.

Usage
-----
    python infra/cloudwatch_alarms.py [--dry-run] [--alert-email EMAIL]

Environment variables
---------------------
AWS_REGION      AWS region (default: us-east-1)
ALERT_EMAIL     Email address for SNS alarm notifications
"""

from __future__ import annotations

import argparse
import logging
import os

import boto3
from botocore.exceptions import ClientError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_REGION = "us-east-1"
DEFAULT_NAMESPACE = "PredictiveMaintenance"
KINESIS_STREAM = "pmt-sensor-stream"
S3_BUCKET = "predictive-maintenance-twin-raw"


class AlarmSetup:
    """
    Idempotent CloudWatch alarm and SNS topic provisioner.

    Creates all alarms needed to monitor the predictive maintenance pipeline
    health end-to-end, from S3 ingestion through to Gold table freshness.
    """

    def __init__(
        self,
        region: str = DEFAULT_REGION,
        namespace: str = DEFAULT_NAMESPACE,
        dry_run: bool = False,
    ) -> None:
        """
        Initialise the alarm setup helper.

        Parameters
        ----------
        region:
            AWS region.
        namespace:
            CloudWatch custom metrics namespace.
        dry_run:
            If True, log actions without executing.
        """
        self.region = region
        self.namespace = namespace
        self.dry_run = dry_run
        self._cw = boto3.client("cloudwatch", region_name=region)
        self._sns = boto3.client("sns", region_name=region)

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=32),
        reraise=True,
    )
    def create_sns_topic(self, alert_email: str) -> str:
        """
        Create the pmt-pipeline-alerts SNS topic and subscribe the alert email.

        SNS topic creation is idempotent — calling CreateTopic with the same
        name returns the existing topic ARN.

        Parameters
        ----------
        alert_email:
            Email address to subscribe to the topic.

        Returns
        -------
        SNS topic ARN.
        """
        topic_name = "pmt-pipeline-alerts"

        if self.dry_run:
            fake_arn = f"arn:aws:sns:{self.region}:123456789012:{topic_name}"
            logger.info("[DRY-RUN] Would create SNS topic: %s", topic_name)
            logger.info("[DRY-RUN] Would subscribe: %s", alert_email)
            return fake_arn

        try:
            resp = self._sns.create_topic(
                Name=topic_name,
                Tags=[{"Key": "Project", "Value": "predictive-maintenance-twin"}],
            )
            arn = resp["TopicArn"]
            logger.info("SNS topic ready: %s", arn)

            # Subscribe email (idempotent — duplicate subscriptions are filtered)
            if alert_email:
                self._sns.subscribe(TopicArn=arn, Protocol="email", Endpoint=alert_email)
                logger.info("Subscribed %s to %s (confirm via email)", alert_email, arn)

            return arn
        except ClientError as exc:
            logger.error("Failed to create SNS topic: %s", exc.response["Error"]["Message"])
            raise

    def _put_alarm(
        self,
        alarm_name: str,
        description: str,
        metric_name: str,
        namespace: str,
        statistic: str,
        period: int,
        evaluation_periods: int,
        threshold: float,
        comparison: str,
        treat_missing: str,
        dimensions: list[dict],
        sns_arn: str,
    ) -> None:
        """
        Create or update a CloudWatch alarm (idempotent via put_metric_alarm).

        Parameters
        ----------
        alarm_name:
            Unique alarm name.
        description:
            Human-readable alarm description.
        metric_name:
            CloudWatch metric to alarm on.
        namespace:
            CloudWatch namespace.
        statistic:
            Statistic to apply (Sum, Average, Maximum, etc.).
        period:
            Evaluation period in seconds.
        evaluation_periods:
            Number of periods to evaluate.
        threshold:
            Alarm threshold value.
        comparison:
            Comparison operator (GreaterThanThreshold, etc.).
        treat_missing:
            How to treat missing data: notBreaching, breaching, ignore, missing.
        dimensions:
            List of {"Name": ..., "Value": ...} dimension dicts.
        sns_arn:
            SNS topic ARN to notify on alarm.
        """
        if self.dry_run:
            logger.info("[DRY-RUN] Would create alarm: %s (threshold=%s %s %s)", alarm_name, statistic, comparison, threshold)
            return

        try:
            self._cw.put_metric_alarm(
                AlarmName=alarm_name,
                AlarmDescription=description,
                MetricName=metric_name,
                Namespace=namespace,
                Statistic=statistic,
                Period=period,
                EvaluationPeriods=evaluation_periods,
                Threshold=threshold,
                ComparisonOperator=comparison,
                TreatMissingData=treat_missing,
                Dimensions=dimensions,
                AlarmActions=[sns_arn],
                OKActions=[sns_arn],
                ActionsEnabled=True,
            )
            logger.info("Created/updated alarm: %s", alarm_name)
        except ClientError as exc:
            logger.error("Failed to create alarm '%s': %s", alarm_name, exc.response["Error"]["Message"])
            raise

    def create_ingestion_freshness_alarm(self, sns_arn: str) -> None:
        """
        Alarm if no records have been ingested in the last 5 minutes.

        A sum of 0 for RecordsIngested over a 5-minute period means the
        producer is down or ThingSpeak is unreachable.

        Parameters
        ----------
        sns_arn:
            SNS topic ARN for notifications.
        """
        self._put_alarm(
            alarm_name="pmt-ingestion-stale",
            description="No sensor records ingested in the last 5 minutes. Check the api_producer.py process and ThingSpeak connectivity.",
            metric_name="RecordsIngested",
            namespace=self.namespace,
            statistic="Sum",
            period=300,
            evaluation_periods=1,
            threshold=1.0,
            comparison="LessThanThreshold",
            treat_missing="breaching",
            dimensions=[],
            sns_arn=sns_arn,
        )

    def create_pipeline_error_alarm(self, sns_arn: str) -> None:
        """
        Alarm if any pipeline errors are recorded.

        Even a single PipelineError in 5 minutes warrants investigation.

        Parameters
        ----------
        sns_arn:
            SNS topic ARN for notifications.
        """
        self._put_alarm(
            alarm_name="pmt-pipeline-errors",
            description="One or more pipeline errors detected. Check CloudWatch logs for the ETL job.",
            metric_name="PipelineErrors",
            namespace=self.namespace,
            statistic="Sum",
            period=300,
            evaluation_periods=1,
            threshold=1.0,
            comparison="GreaterThanOrEqualToThreshold",
            treat_missing="notBreaching",
            dimensions=[],
            sns_arn=sns_arn,
        )

    def create_kinesis_lag_alarm(self, sns_arn: str) -> None:
        """
        Alarm if the Kinesis consumer falls more than 60 seconds behind.

        IteratorAgeMilliseconds > 60000ms means the downstream consumer
        (Firehose or Databricks) is not keeping up with the producer.

        Parameters
        ----------
        sns_arn:
            SNS topic ARN for notifications.
        """
        self._put_alarm(
            alarm_name="pmt-kinesis-lag",
            description="Kinesis consumer lag exceeds 60 seconds. Firehose or downstream consumer may be throttled.",
            metric_name="GetRecords.IteratorAgeMilliseconds",
            namespace="AWS/Kinesis",
            statistic="Maximum",
            period=300,
            evaluation_periods=1,
            threshold=60000.0,
            comparison="GreaterThanThreshold",
            treat_missing="notBreaching",
            dimensions=[{"Name": "StreamName", "Value": KINESIS_STREAM}],
            sns_arn=sns_arn,
        )

    def create_s3_put_errors_alarm(self, sns_arn: str) -> None:
        """
        Alarm on S3 5xx errors for the raw data bucket.

        5xx errors on PutObject indicate IAM permission issues or S3 service
        problems affecting ingestion.

        Parameters
        ----------
        sns_arn:
            SNS topic ARN for notifications.
        """
        self._put_alarm(
            alarm_name="pmt-s3-errors",
            description=f"S3 5xx errors detected on s3://{S3_BUCKET}. Check IAM permissions and S3 service health.",
            metric_name="5xxErrors",
            namespace="AWS/S3",
            statistic="Sum",
            period=300,
            evaluation_periods=1,
            threshold=1.0,
            comparison="GreaterThanOrEqualToThreshold",
            treat_missing="notBreaching",
            dimensions=[
                {"Name": "BucketName", "Value": S3_BUCKET},
                {"Name": "FilterId", "Value": "EntireBucket"},
            ],
            sns_arn=sns_arn,
        )

    def create_gold_freshness_alarm(self, sns_arn: str) -> None:
        """
        Alarm if the Gold table has not been updated in the last 2 hours.

        GoldTableLastUpdate is a custom metric (epoch seconds) published
        by the Gold ETL task. An old timestamp means the Databricks job
        failed or was not triggered.

        Parameters
        ----------
        sns_arn:
            SNS topic ARN for notifications.
        """
        import time
        two_hours_ago = time.time() - 7200

        self._put_alarm(
            alarm_name="pmt-gold-stale",
            description="Gold table has not been updated in the last 2 hours. Check the Databricks job run history.",
            metric_name="GoldTableLastUpdate",
            namespace=self.namespace,
            statistic="Maximum",
            period=3600,
            evaluation_periods=2,
            threshold=two_hours_ago,
            comparison="LessThanThreshold",
            treat_missing="breaching",
            dimensions=[],
            sns_arn=sns_arn,
        )

    def setup_all(self, alert_email: str) -> None:
        """
        Create all CloudWatch alarms and the SNS notification topic.

        Parameters
        ----------
        alert_email:
            Email address to receive alarm notifications.
        """
        logger.info("Starting CloudWatch alarm setup (dry_run=%s)", self.dry_run)
        sns_arn = self.create_sns_topic(alert_email=alert_email)
        self.create_ingestion_freshness_alarm(sns_arn)
        self.create_pipeline_error_alarm(sns_arn)
        self.create_kinesis_lag_alarm(sns_arn)
        self.create_s3_put_errors_alarm(sns_arn)
        self.create_gold_freshness_alarm(sns_arn)
        logger.info("CloudWatch alarm setup complete — 5 alarms configured")


def main() -> None:
    """Parse arguments and set up CloudWatch alarms."""
    parser = argparse.ArgumentParser(description="Set up CloudWatch alarms for predictive maintenance")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--alert-email", default=os.environ.get("ALERT_EMAIL", ""))
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", DEFAULT_REGION))
    args = parser.parse_args()

    if not args.alert_email and not args.dry_run:
        logger.warning("No ALERT_EMAIL set — SNS topic will be created without subscriptions")

    setup = AlarmSetup(region=args.region, dry_run=args.dry_run)
    setup.setup_all(alert_email=args.alert_email)


if __name__ == "__main__":
    main()
