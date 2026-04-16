"""
S3 bucket setup for the Predictive Maintenance Digital Twin.

Creates and configures the raw landing zone bucket with versioning,
lifecycle policies, folder structure, and a deny-HTTP bucket policy.
All operations are idempotent — safe to run repeatedly.

Usage
-----
    python infra/s3_setup.py [--dry-run] [--bucket BUCKET] [--region REGION]

Environment variables
---------------------
AWS_REGION      AWS region (default: us-east-1)
PMT_S3_BUCKET   Bucket name (default: predictive-maintenance-twin-raw)
"""

from __future__ import annotations

import argparse
import json
import logging
import os

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

DEFAULT_BUCKET = "predictive-maintenance-twin-raw"
DEFAULT_REGION = "us-east-1"


class S3Setup:
    """Idempotent S3 bucket provisioner for the predictive maintenance pipeline."""

    def __init__(self, bucket: str, region: str, dry_run: bool = False) -> None:
        """
        Initialise the setup helper.

        Parameters
        ----------
        bucket:
            S3 bucket name to create/configure.
        region:
            AWS region.
        dry_run:
            If True, log actions without executing them.
        """
        self.bucket = bucket
        self.region = region
        self.dry_run = dry_run
        self._s3 = boto3.client("s3", region_name=region)

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=32),
        reraise=True,
    )
    def create_bucket(self) -> None:
        """
        Create the S3 bucket if it does not already exist.

        Handles BucketAlreadyOwnedByYou gracefully (idempotent).
        us-east-1 does not accept a LocationConstraint — handled automatically.
        """
        if self.dry_run:
            logger.info(
                "[DRY-RUN] Would create bucket: s3://%s (region=%s)",
                self.bucket,
                self.region,
            )
            return

        try:
            if self.region == "us-east-1":
                self._s3.create_bucket(Bucket=self.bucket)
            else:
                self._s3.create_bucket(
                    Bucket=self.bucket,
                    CreateBucketConfiguration={"LocationConstraint": self.region},
                )
            logger.info("Created bucket s3://%s in %s", self.bucket, self.region)
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                logger.info(
                    "Bucket s3://%s already exists — skipping create", self.bucket
                )
            else:
                logger.error(
                    "Failed to create bucket s3://%s: %s",
                    self.bucket,
                    exc.response["Error"]["Message"],
                )
                raise

    def enable_versioning(self) -> None:
        """Enable S3 versioning on the bucket for data lineage and recovery."""
        if self.dry_run:
            logger.info("[DRY-RUN] Would enable versioning on s3://%s", self.bucket)
            return
        try:
            self._s3.put_bucket_versioning(
                Bucket=self.bucket,
                VersioningConfiguration={"Status": "Enabled"},
            )
            logger.info("Enabled versioning on s3://%s", self.bucket)
        except ClientError as exc:
            logger.error(
                "Failed to enable versioning: %s", exc.response["Error"]["Message"]
            )
            raise

    def set_lifecycle_policy(self) -> None:
        """
        Apply a lifecycle policy to the bucket.

        - raw/ objects: transition to Glacier after 30 days, expire after 90 days.
        - processed/ objects: expire after 365 days.
        """
        if self.dry_run:
            logger.info("[DRY-RUN] Would set lifecycle policy on s3://%s", self.bucket)
            return

        policy = {
            "Rules": [
                {
                    "ID": "pmt-raw-lifecycle",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "raw/"},
                    "Transitions": [
                        {"Days": 30, "StorageClass": "GLACIER"},
                    ],
                    "Expiration": {"Days": 90},
                },
                {
                    "ID": "pmt-processed-lifecycle",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "bronze/"},
                    "Expiration": {"Days": 365},
                },
            ]
        }
        try:
            self._s3.put_bucket_lifecycle_configuration(
                Bucket=self.bucket,
                LifecycleConfiguration=policy,
            )
            logger.info("Applied lifecycle policy to s3://%s", self.bucket)
        except ClientError as exc:
            logger.error(
                "Failed to set lifecycle policy: %s", exc.response["Error"]["Message"]
            )
            raise

    def create_folder_structure(self) -> None:
        """
        Create placeholder objects to establish the folder structure.

        Folders: raw/, bronze/, silver/, gold/
        """
        prefixes = ["raw/", "bronze/", "silver/", "gold/", "firehose/", "checkpoints/"]
        for prefix in prefixes:
            if self.dry_run:
                logger.info(
                    "[DRY-RUN] Would create folder s3://%s/%s", self.bucket, prefix
                )
                continue
            try:
                self._s3.put_object(Bucket=self.bucket, Key=prefix, Body=b"")
                logger.info("Created folder s3://%s/%s", self.bucket, prefix)
            except ClientError as exc:
                logger.error(
                    "Failed to create folder %s: %s",
                    prefix,
                    exc.response["Error"]["Message"],
                )
                raise

    def set_bucket_policy(self) -> None:
        """
        Apply a bucket policy that denies all non-HTTPS (HTTP) access.

        This enforces encryption in transit for all data movement.
        """
        if self.dry_run:
            logger.info(
                "[DRY-RUN] Would set deny-HTTP bucket policy on s3://%s", self.bucket
            )
            return

        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "DenyNonHTTPS",
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:*",
                    "Resource": [
                        f"arn:aws:s3:::{self.bucket}",
                        f"arn:aws:s3:::{self.bucket}/*",
                    ],
                    "Condition": {"Bool": {"aws:SecureTransport": "false"}},
                }
            ],
        }
        try:
            self._s3.put_bucket_policy(
                Bucket=self.bucket,
                Policy=json.dumps(policy),
            )
            logger.info("Applied deny-HTTP bucket policy to s3://%s", self.bucket)
        except ClientError as exc:
            logger.error(
                "Failed to set bucket policy: %s", exc.response["Error"]["Message"]
            )
            raise

    def setup_all(self) -> None:
        """Run the full bucket setup sequence."""
        logger.info(
            "Starting S3 setup for bucket: %s (region=%s, dry_run=%s)",
            self.bucket,
            self.region,
            self.dry_run,
        )
        self.create_bucket()
        self.enable_versioning()
        self.set_lifecycle_policy()
        self.create_folder_structure()
        self.set_bucket_policy()
        logger.info("S3 setup complete for s3://%s", self.bucket)


def main() -> None:
    """Parse arguments and run S3 bucket setup."""
    parser = argparse.ArgumentParser(
        description="Set up S3 bucket for predictive maintenance pipeline"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Log actions without executing"
    )
    parser.add_argument(
        "--bucket", default=os.environ.get("PMT_S3_BUCKET", DEFAULT_BUCKET)
    )
    parser.add_argument(
        "--region", default=os.environ.get("AWS_REGION", DEFAULT_REGION)
    )
    args = parser.parse_args()

    setup = S3Setup(bucket=args.bucket, region=args.region, dry_run=args.dry_run)
    setup.setup_all()


if __name__ == "__main__":
    main()
