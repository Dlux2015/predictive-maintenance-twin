"""
Kinesis Data Stream + Kinesis Data Firehose setup for the Predictive Maintenance pipeline.

Creates the pmt-sensor-stream Kinesis stream and a Firehose delivery stream
that delivers to S3. All operations are idempotent.

Usage
-----
    python infra/kinesis_setup.py [--dry-run] [--stream-name NAME] [--shard-count N]

Environment variables
---------------------
AWS_REGION          AWS region (default: us-east-1)
PMT_S3_BUCKET       Target S3 bucket for Firehose delivery
PMT_FIREHOSE_ROLE   IAM role ARN for Firehose → S3 permissions
"""

from __future__ import annotations

import argparse
import logging
import os
import time

import boto3
from botocore.exceptions import ClientError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_STREAM = "pmt-sensor-stream"
DEFAULT_REGION = "us-east-1"
DEFAULT_SHARDS = 1


class KinesisSetup:
    """Idempotent Kinesis stream and Firehose provisioner."""

    def __init__(self, region: str, dry_run: bool = False) -> None:
        """
        Initialise the setup helper.

        Parameters
        ----------
        region:
            AWS region.
        dry_run:
            If True, log actions without executing them.
        """
        self.region = region
        self.dry_run = dry_run
        self._kinesis = boto3.client("kinesis", region_name=region)
        self._firehose = boto3.client("firehose", region_name=region)

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=32),
        reraise=True,
    )
    def create_kinesis_stream(
        self, stream_name: str = DEFAULT_STREAM, shard_count: int = DEFAULT_SHARDS
    ) -> None:
        """
        Create a Kinesis Data Stream if it does not already exist.

        Parameters
        ----------
        stream_name:
            Name of the Kinesis stream.
        shard_count:
            Number of shards (each shard handles ~1 MB/s ingestion).
        """
        if self.dry_run:
            logger.info(
                "[DRY-RUN] Would create Kinesis stream '%s' with %d shard(s)",
                stream_name,
                shard_count,
            )
            return

        try:
            self._kinesis.create_stream(StreamName=stream_name, ShardCount=shard_count)
            logger.info(
                "Created Kinesis stream '%s' with %d shard(s)", stream_name, shard_count
            )
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code == "ResourceInUseException":
                logger.info(
                    "Kinesis stream '%s' already exists — skipping create", stream_name
                )
            else:
                logger.error(
                    "Failed to create stream '%s': %s",
                    stream_name,
                    exc.response["Error"]["Message"],
                )
                raise

    def wait_for_stream_active(
        self, stream_name: str = DEFAULT_STREAM, timeout: int = 60
    ) -> None:
        """
        Poll until the Kinesis stream reaches ACTIVE status.

        Parameters
        ----------
        stream_name:
            Stream to wait on.
        timeout:
            Maximum seconds to wait before raising TimeoutError.
        """
        if self.dry_run:
            logger.info(
                "[DRY-RUN] Would wait for stream '%s' to become ACTIVE", stream_name
            )
            return

        logger.info("Waiting for stream '%s' to become ACTIVE...", stream_name)
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                resp = self._kinesis.describe_stream_summary(StreamName=stream_name)
                status = resp["StreamDescriptionSummary"]["StreamStatus"]
                if status == "ACTIVE":
                    logger.info("Stream '%s' is ACTIVE", stream_name)
                    return
                logger.debug("Stream status: %s — waiting...", status)
            except ClientError as exc:
                logger.warning(
                    "describe_stream_summary failed: %s",
                    exc.response["Error"]["Message"],
                )
            time.sleep(5)
        raise TimeoutError(
            f"Stream '{stream_name}' did not become ACTIVE within {timeout}s"
        )

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=32),
        reraise=True,
    )
    def create_firehose_delivery_stream(
        self,
        stream_name: str,
        s3_bucket: str,
        s3_prefix: str,
        iam_role_arn: str,
    ) -> None:
        """
        Create a Kinesis Data Firehose delivery stream that writes to S3.

        The Firehose stream name is derived from the Kinesis stream name
        (pmt-sensor-stream → pmt-sensor-firehose).

        Parameters
        ----------
        stream_name:
            Source Kinesis Data Stream name.
        s3_bucket:
            Target S3 bucket for delivery.
        s3_prefix:
            S3 key prefix for Firehose records.
        iam_role_arn:
            IAM role ARN granting Firehose access to S3 and Kinesis.
        """
        firehose_name = stream_name.replace("stream", "firehose")

        if self.dry_run:
            logger.info(
                "[DRY-RUN] Would create Firehose '%s' → s3://%s/%s",
                firehose_name,
                s3_bucket,
                s3_prefix,
            )
            return

        try:
            self._firehose.create_delivery_stream(
                DeliveryStreamName=firehose_name,
                DeliveryStreamType="KinesisStreamAsSource",
                KinesisStreamSourceConfiguration={
                    "KinesisStreamARN": f"arn:aws:kinesis:{self.region}:*:stream/{stream_name}",
                    "RoleARN": iam_role_arn,
                },
                ExtendedS3DestinationConfiguration={
                    "RoleARN": iam_role_arn,
                    "BucketARN": f"arn:aws:s3:::{s3_bucket}",
                    "Prefix": f"{s3_prefix}/year=!{{timestamp:yyyy}}/month=!{{timestamp:MM}}/day=!{{timestamp:dd}}/hour=!{{timestamp:HH}}/",
                    "ErrorOutputPrefix": "firehose-errors/",
                    "BufferingHints": {"SizeInMBs": 5, "IntervalInSeconds": 60},
                    "CompressionFormat": "UNCOMPRESSED",
                },
            )
            logger.info("Created Firehose delivery stream '%s'", firehose_name)
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code == "ResourceInUseException":
                logger.info(
                    "Firehose '%s' already exists — skipping create", firehose_name
                )
            else:
                logger.error(
                    "Failed to create Firehose '%s': %s",
                    firehose_name,
                    exc.response["Error"]["Message"],
                )
                raise


def main() -> None:
    """Parse arguments and set up Kinesis resources."""
    parser = argparse.ArgumentParser(
        description="Set up Kinesis stream + Firehose for predictive maintenance"
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--stream-name", default=os.environ.get("KINESIS_STREAM", DEFAULT_STREAM)
    )
    parser.add_argument("--shard-count", type=int, default=DEFAULT_SHARDS)
    parser.add_argument(
        "--s3-bucket",
        default=os.environ.get("PMT_S3_BUCKET", "predictive-maintenance-twin-raw"),
    )
    parser.add_argument(
        "--iam-role-arn", default=os.environ.get("PMT_FIREHOSE_ROLE", "")
    )
    parser.add_argument(
        "--region", default=os.environ.get("AWS_REGION", DEFAULT_REGION)
    )
    args = parser.parse_args()

    setup = KinesisSetup(region=args.region, dry_run=args.dry_run)
    setup.create_kinesis_stream(
        stream_name=args.stream_name, shard_count=args.shard_count
    )
    setup.wait_for_stream_active(stream_name=args.stream_name)

    if args.iam_role_arn:
        setup.create_firehose_delivery_stream(
            stream_name=args.stream_name,
            s3_bucket=args.s3_bucket,
            s3_prefix="firehose",
            iam_role_arn=args.iam_role_arn,
        )
    else:
        logger.warning("--iam-role-arn not provided — skipping Firehose setup")

    logger.info("Kinesis setup complete")


if __name__ == "__main__":
    main()
