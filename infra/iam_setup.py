"""
IAM roles and policies for the Predictive Maintenance Digital Twin.

Creates three IAM roles:
  - pmt-producer-role      : ingestion producer (S3 write + Kinesis put + CloudWatch)
  - pmt-databricks-role    : Databricks (S3 read/write for Delta Lake + Unity Catalog)
  - pmt-firehose-role      : Kinesis Firehose → S3 delivery

All operations are idempotent. Run with --dry-run to preview.

Usage
-----
    python infra/iam_setup.py [--dry-run] [--output-arns]

Environment variables
---------------------
AWS_REGION      AWS region (default: us-east-1)
PMT_S3_BUCKET   S3 bucket name for policy scoping
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

DEFAULT_REGION = "us-east-1"
DEFAULT_BUCKET = "predictive-maintenance-twin-raw"


class IAMSetup:
    """Idempotent IAM role and policy provisioner."""

    def __init__(self, region: str, bucket: str, dry_run: bool = False) -> None:
        """
        Initialise the IAM setup helper.

        Parameters
        ----------
        region:
            AWS region (used for ARN construction).
        bucket:
            S3 bucket name to scope policies.
        dry_run:
            If True, log actions without executing them.
        """
        self.region = region
        self.bucket = bucket
        self.dry_run = dry_run
        self._iam = boto3.client("iam")
        self._sts = boto3.client("sts")
        self._account_id = self._get_account_id()

    def _get_account_id(self) -> str:
        """Return the current AWS account ID."""
        try:
            return self._sts.get_caller_identity()["Account"]
        except ClientError:
            return "123456789012"  # fallback for dry-run without credentials

    def _role_exists(self, role_name: str) -> bool:
        """Check whether an IAM role already exists."""
        try:
            self._iam.get_role(RoleName=role_name)
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "NoSuchEntity":
                return False
            raise

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=32),
        reraise=True,
    )
    def _create_role(self, role_name: str, trust_policy: dict, description: str) -> str:
        """
        Create an IAM role if it does not already exist.

        Parameters
        ----------
        role_name:
            IAM role name.
        trust_policy:
            JSON-serialisable trust relationship policy document.
        description:
            Human-readable role description.

        Returns
        -------
        Role ARN.
        """
        if self.dry_run:
            arn = f"arn:aws:iam::{self._account_id}:role/{role_name}"
            logger.info("[DRY-RUN] Would create IAM role: %s", arn)
            return arn

        if self._role_exists(role_name):
            role = self._iam.get_role(RoleName=role_name)
            arn = role["Role"]["Arn"]
            logger.info(
                "IAM role '%s' already exists — skipping create: %s", role_name, arn
            )
            return arn

        try:
            resp = self._iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description=description,
                Tags=[{"Key": "Project", "Value": "predictive-maintenance-twin"}],
            )
            arn = resp["Role"]["Arn"]
            logger.info("Created IAM role: %s", arn)
            return arn
        except ClientError as exc:
            logger.error(
                "Failed to create role '%s': %s",
                role_name,
                exc.response["Error"]["Message"],
            )
            raise

    def _put_inline_policy(
        self, role_name: str, policy_name: str, policy_doc: dict
    ) -> None:
        """Attach an inline policy to an IAM role (idempotent)."""
        if self.dry_run:
            logger.info(
                "[DRY-RUN] Would attach inline policy '%s' to role '%s'",
                policy_name,
                role_name,
            )
            return
        try:
            self._iam.put_role_policy(
                RoleName=role_name,
                PolicyName=policy_name,
                PolicyDocument=json.dumps(policy_doc),
            )
            logger.info(
                "Attached inline policy '%s' to role '%s'", policy_name, role_name
            )
        except ClientError as exc:
            logger.error(
                "Failed to attach policy '%s': %s",
                policy_name,
                exc.response["Error"]["Message"],
            )
            raise

    def attach_policy(self, role_name: str, policy_arn: str) -> None:
        """
        Attach a managed policy to a role (idempotent).

        Parameters
        ----------
        role_name:
            IAM role name.
        policy_arn:
            ARN of the managed policy to attach.
        """
        if self.dry_run:
            logger.info(
                "[DRY-RUN] Would attach managed policy %s to %s", policy_arn, role_name
            )
            return
        try:
            self._iam.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
            logger.info("Attached %s to %s", policy_arn, role_name)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "EntityAlreadyExists":
                logger.info("Policy %s already attached to %s", policy_arn, role_name)
            else:
                logger.error(
                    "Failed to attach policy: %s", exc.response["Error"]["Message"]
                )
                raise

    # ------------------------------------------------------------------
    # Role creators
    # ------------------------------------------------------------------

    def create_producer_role(self) -> str:
        """
        Create the ingestion producer IAM role.

        Permissions: S3 PutObject, Kinesis PutRecord, CloudWatch PutMetricData.
        Trust: EC2 + Lambda (covers both deployment targets).

        Returns
        -------
        Role ARN.
        """
        trust = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": ["ec2.amazonaws.com", "lambda.amazonaws.com"]
                    },
                    "Action": "sts:AssumeRole",
                }
            ],
        }
        arn = self._create_role(
            role_name="pmt-producer-role",
            trust_policy=trust,
            description="Predictive maintenance ingestion producer — S3 + Kinesis + CloudWatch",
        )

        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "S3Write",
                    "Effect": "Allow",
                    "Action": ["s3:PutObject", "s3:GetObject"],
                    "Resource": f"arn:aws:s3:::{self.bucket}/*",
                },
                {
                    "Sid": "KinesisPut",
                    "Effect": "Allow",
                    "Action": [
                        "kinesis:PutRecord",
                        "kinesis:PutRecords",
                        "kinesis:DescribeStream",
                    ],
                    "Resource": f"arn:aws:kinesis:{self.region}:{self._account_id}:stream/pmt-*",
                },
                {
                    "Sid": "CloudWatchMetrics",
                    "Effect": "Allow",
                    "Action": "cloudwatch:PutMetricData",
                    "Resource": "*",
                },
            ],
        }
        self._put_inline_policy("pmt-producer-role", "pmt-producer-policy", policy)
        return arn

    def create_databricks_role(self) -> str:
        """
        Create the Databricks IAM role for Unity Catalog external location access.

        Permissions: S3 full access on the pipeline bucket + Glue for catalog.
        Trust: Databricks (cross-account — update account ID for production).

        Returns
        -------
        Role ARN.
        """
        trust = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": f"arn:aws:iam::{self._account_id}:root"},
                    "Action": "sts:AssumeRole",
                    "Condition": {
                        "StringEquals": {"sts:ExternalId": "databricks-unity-catalog"}
                    },
                }
            ],
        }
        arn = self._create_role(
            role_name="pmt-databricks-role",
            trust_policy=trust,
            description="Predictive maintenance Databricks role — S3 Delta Lake + Unity Catalog",
        )

        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "S3FullAccess",
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket",
                        "s3:GetBucketLocation",
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{self.bucket}",
                        f"arn:aws:s3:::{self.bucket}/*",
                    ],
                },
                {
                    "Sid": "GlueForUnityCatalog",
                    "Effect": "Allow",
                    "Action": "glue:*",
                    "Resource": "*",
                },
            ],
        }
        self._put_inline_policy("pmt-databricks-role", "pmt-databricks-policy", policy)
        return arn

    def create_firehose_role(self) -> str:
        """
        Create the Kinesis Firehose → S3 delivery IAM role.

        Returns
        -------
        Role ARN.
        """
        trust = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "firehose.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
        arn = self._create_role(
            role_name="pmt-firehose-role",
            trust_policy=trust,
            description="Predictive maintenance Kinesis Firehose → S3 delivery role",
        )

        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "S3Delivery",
                    "Effect": "Allow",
                    "Action": [
                        "s3:PutObject",
                        "s3:GetObject",
                        "s3:ListBucket",
                        "s3:AbortMultipartUpload",
                        "s3:ListBucketMultipartUploads",
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{self.bucket}",
                        f"arn:aws:s3:::{self.bucket}/*",
                    ],
                },
                {
                    "Sid": "KinesisSource",
                    "Effect": "Allow",
                    "Action": [
                        "kinesis:GetRecords",
                        "kinesis:GetShardIterator",
                        "kinesis:DescribeStream",
                        "kinesis:ListShards",
                    ],
                    "Resource": f"arn:aws:kinesis:{self.region}:{self._account_id}:stream/pmt-*",
                },
            ],
        }
        self._put_inline_policy("pmt-firehose-role", "pmt-firehose-policy", policy)
        return arn

    def setup_all(self) -> dict[str, str]:
        """
        Create all IAM roles and return their ARNs.

        Returns
        -------
        Dict mapping role name to ARN.
        """
        logger.info("Starting IAM setup (dry_run=%s)", self.dry_run)
        arns = {
            "pmt-producer-role": self.create_producer_role(),
            "pmt-databricks-role": self.create_databricks_role(),
            "pmt-firehose-role": self.create_firehose_role(),
        }
        logger.info("IAM setup complete")
        return arns


def main() -> None:
    """Parse arguments and run IAM setup."""
    parser = argparse.ArgumentParser(
        description="Set up IAM roles for predictive maintenance pipeline"
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--output-arns",
        action="store_true",
        help="Print all role ARNs as JSON on completion",
    )
    parser.add_argument(
        "--region", default=os.environ.get("AWS_REGION", DEFAULT_REGION)
    )
    parser.add_argument(
        "--bucket", default=os.environ.get("PMT_S3_BUCKET", DEFAULT_BUCKET)
    )
    args = parser.parse_args()

    setup = IAMSetup(region=args.region, bucket=args.bucket, dry_run=args.dry_run)
    arns = setup.setup_all()

    if args.output_arns:
        print(json.dumps(arns, indent=2))


if __name__ == "__main__":
    main()
