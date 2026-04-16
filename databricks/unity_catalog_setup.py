"""
Unity Catalog setup for the Predictive Maintenance Digital Twin.

Creates and configures Unity Catalog objects:
  - Storage credential (IAM role → S3 access)
  - External location (S3 bucket → Unity Catalog)
  - Catalog: main
  - Schema: predictive_maintenance
  - Grants for the pipeline service principal

All operations are idempotent. Run with --dry-run to preview.

Usage
-----
    python databricks/unity_catalog_setup.py [--dry-run] [--workspace-url URL] [--iam-role-arn ARN]

Environment variables
---------------------
DATABRICKS_HOST         Databricks workspace URL (e.g. https://dbc-xxx.azuredatabricks.net)
DATABRICKS_TOKEN        Databricks personal access token
AWS_IAM_ROLE_ARN        IAM role ARN for Unity Catalog external location
PMT_S3_BUCKET           S3 bucket name
"""

from __future__ import annotations

import argparse
import logging
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import AlreadyExists, BadRequest
from databricks.sdk.service.catalog import (
    SecurableType,
)

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_CATALOG = "workspace"
DEFAULT_SCHEMA = "predictive_maintenance"
DEFAULT_BUCKET = os.environ.get("PMT_S3_BUCKET", "predictive-maintenance-twin-raw")
DEFAULT_CREDENTIAL = "pmt-storage-credential"
DEFAULT_EXT_LOCATION = "pmt-external-location"


class UnityCatalogSetup:
    """
    Idempotent Unity Catalog provisioner using the Databricks SDK.

    Establishes the full object hierarchy required by the predictive
    maintenance pipeline: credential → external location → catalog → schema → grants.
    """

    def __init__(self, workspace_client: WorkspaceClient) -> None:
        """
        Initialise the setup helper.

        Parameters
        ----------
        workspace_client:
            Authenticated Databricks WorkspaceClient.
        """
        self._client = workspace_client

    def create_storage_credential(
        self,
        name: str = DEFAULT_CREDENTIAL,
        iam_role_arn: str = "",
        dry_run: bool = False,
    ) -> None:
        """
        Create a Unity Catalog storage credential backed by an IAM role.

        The storage credential allows Unity Catalog to access S3 on behalf
        of the Databricks workspace using the specified IAM role.

        Parameters
        ----------
        name:
            Storage credential name.
        iam_role_arn:
            ARN of the IAM role to use for S3 access.
        dry_run:
            If True, log actions without executing.
        """
        if not iam_role_arn:
            logger.warning(
                "No IAM role ARN provided — skipping storage credential creation"
            )
            return

        if dry_run:
            logger.info(
                "[DRY-RUN] Would create storage credential '%s' with role %s",
                name,
                iam_role_arn,
            )
            return

        try:
            self._client.credentials.create(
                name=name,
                aws_iam_role={"role_arn": iam_role_arn},
                comment="Predictive maintenance pipeline S3 access",
                read_only=False,
            )
            logger.info("Created storage credential: %s", name)
        except AlreadyExists:
            logger.info("Storage credential '%s' already exists — skipping", name)
        except Exception as exc:
            logger.error("Failed to create storage credential '%s': %s", name, exc)
            raise

    def create_external_location(
        self,
        name: str = DEFAULT_EXT_LOCATION,
        s3_url: str = f"s3://{DEFAULT_BUCKET}/",
        credential_name: str = DEFAULT_CREDENTIAL,
        dry_run: bool = False,
    ) -> None:
        """
        Create a Unity Catalog external location pointing to the S3 bucket.

        The external location enables managed Delta tables to be stored in
        S3 and accessed via Unity Catalog governance.

        Parameters
        ----------
        name:
            External location name.
        s3_url:
            S3 URL prefix (e.g. s3://my-bucket/).
        credential_name:
            Name of the storage credential to use.
        dry_run:
            If True, log actions without executing.
        """
        if dry_run:
            logger.info(
                "[DRY-RUN] Would create external location '%s' → %s", name, s3_url
            )
            return

        try:
            self._client.external_locations.create(
                name=name,
                url=s3_url,
                credential_name=credential_name,
                comment="Predictive maintenance S3 external location",
                read_only=False,
                skip_validation=False,
            )
            logger.info("Created external location: %s → %s", name, s3_url)
        except AlreadyExists:
            logger.info("External location '%s' already exists — skipping", name)
        except Exception as exc:
            logger.error("Failed to create external location '%s': %s", name, exc)
            raise

    def create_catalog(
        self,
        catalog_name: str = DEFAULT_CATALOG,
        dry_run: bool = False,
    ) -> None:
        """
        Create the Unity Catalog catalog if it does not already exist.

        The 'main' catalog typically exists by default in Databricks workspaces.
        This call is a no-op if the catalog already exists.

        Parameters
        ----------
        catalog_name:
            Name of the catalog to create.
        dry_run:
            If True, log actions without executing.
        """
        if dry_run:
            logger.info("[DRY-RUN] Would create catalog '%s'", catalog_name)
            return

        try:
            self._client.catalogs.get(catalog_name)
            logger.info("Catalog '%s' already exists — skipping", catalog_name)
            return
        except Exception:
            pass  # catalog not found or permissions issue — attempt creation

        try:
            self._client.catalogs.create(
                name=catalog_name, comment="Predictive maintenance digital twin"
            )
            logger.info("Created catalog: %s", catalog_name)
        except Exception as exc:
            # On free-tier workspaces the 'main' catalog exists but cannot be re-created
            # without a managed storage location. Log a warning and continue — if the
            # schema creation succeeds, the catalog is accessible.
            logger.warning(
                "Could not create catalog '%s' (it may already exist in the UI): %s",
                catalog_name,
                exc,
            )

    def create_schema(
        self,
        catalog: str = DEFAULT_CATALOG,
        schema: str = DEFAULT_SCHEMA,
        dry_run: bool = False,
    ) -> None:
        """
        Create the Unity Catalog schema (database) if it does not exist.

        Parameters
        ----------
        catalog:
            Parent catalog name.
        schema:
            Schema name to create.
        dry_run:
            If True, log actions without executing.
        """
        if dry_run:
            logger.info("[DRY-RUN] Would create schema '%s.%s'", catalog, schema)
            return

        try:
            self._client.schemas.create(
                name=schema,
                catalog_name=catalog,
                comment="Predictive maintenance sensor data — Bronze, Silver, Gold tables",
            )
            logger.info("Created schema: %s.%s", catalog, schema)
        except (AlreadyExists, BadRequest) as exc:
            if "already exists" in str(exc).lower():
                logger.info("Schema '%s.%s' already exists — skipping", catalog, schema)
            else:
                logger.error(
                    "Failed to create schema '%s.%s': %s", catalog, schema, exc
                )
                raise
        except Exception as exc:
            logger.error("Failed to create schema '%s.%s': %s", catalog, schema, exc)
            raise

    def grant_permissions(
        self,
        principal: str,
        catalog: str = DEFAULT_CATALOG,
        schema: str = DEFAULT_SCHEMA,
        dry_run: bool = False,
    ) -> None:
        """
        Grant pipeline permissions to a principal (user, group, or service principal).

        Grants: USE CATALOG, USE SCHEMA, SELECT, MODIFY on the catalog and schema.

        Parameters
        ----------
        principal:
            User email, group name, or service principal application ID.
        catalog:
            Catalog to grant USE CATALOG on.
        schema:
            Schema to grant USE SCHEMA, SELECT, MODIFY on.
        dry_run:
            If True, log actions without executing.
        """
        if dry_run:
            logger.info(
                "[DRY-RUN] Would grant permissions to '%s' on %s.%s",
                principal,
                catalog,
                schema,
            )
            return

        try:
            self._client.grants.update(
                securable_type=SecurableType.CATALOG,
                full_name=catalog,
                changes=[{"principal": principal, "add": ["USE CATALOG"]}],
            )
            self._client.grants.update(
                securable_type=SecurableType.SCHEMA,
                full_name=f"{catalog}.{schema}",
                changes=[
                    {
                        "principal": principal,
                        "add": ["USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE"],
                    }
                ],
            )
            logger.info(
                "Granted permissions to '%s' on %s.%s", principal, catalog, schema
            )
        except Exception as exc:
            logger.error("Failed to grant permissions to '%s': %s", principal, exc)
            raise

    def setup_all(
        self,
        iam_role_arn: str = "",
        principal: str = "",
        catalog: str = DEFAULT_CATALOG,
        schema: str = DEFAULT_SCHEMA,
        dry_run: bool = False,
    ) -> None:
        """
        Run the full Unity Catalog setup sequence.

        Steps:
        1. Create storage credential (if IAM role ARN provided)
        2. Create external location
        3. Create/verify catalog
        4. Create schema
        5. Grant permissions (if principal provided)

        Parameters
        ----------
        iam_role_arn:
            IAM role ARN for S3 access (optional — skip credential if not provided).
        principal:
            Principal to grant permissions to (optional).
        catalog:
            Catalog name to create/use.
        schema:
            Schema name to create inside the catalog.
        dry_run:
            If True, log actions without executing.
        """
        logger.info("Starting Unity Catalog setup (dry_run=%s)", dry_run)
        self.create_storage_credential(iam_role_arn=iam_role_arn, dry_run=dry_run)
        if iam_role_arn:
            self.create_external_location(dry_run=dry_run)
        else:
            logger.warning(
                "No IAM role ARN provided — skipping external location creation"
            )
        self.create_catalog(catalog_name=catalog, dry_run=dry_run)
        self.create_schema(catalog=catalog, schema=schema, dry_run=dry_run)
        if principal:
            self.grant_permissions(principal=principal, dry_run=dry_run)
        logger.info("Unity Catalog setup complete")


def main() -> None:
    """Parse arguments and run Unity Catalog setup."""
    parser = argparse.ArgumentParser(
        description="Unity Catalog setup for predictive maintenance"
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--workspace-url", default=os.environ.get("DATABRICKS_HOST", "")
    )
    parser.add_argument(
        "--iam-role-arn", default=os.environ.get("AWS_IAM_ROLE_ARN", "")
    )
    parser.add_argument("--credential-name", default=DEFAULT_CREDENTIAL)
    parser.add_argument(
        "--catalog",
        default=os.environ.get("DATABRICKS_CATALOG", DEFAULT_CATALOG),
        help="Unity Catalog catalog name (default: workspace)",
    )
    parser.add_argument(
        "--schema",
        default=os.environ.get("DATABRICKS_SCHEMA", DEFAULT_SCHEMA),
        help="Unity Catalog schema name (default: predictive_maintenance)",
    )
    parser.add_argument(
        "--principal",
        default=os.environ.get("DATABRICKS_PRINCIPAL", ""),
        help="User/group/SP to grant permissions to",
    )
    args = parser.parse_args()

    client = WorkspaceClient(
        host=args.workspace_url or os.environ.get("DATABRICKS_HOST"),
        token=os.environ.get("DATABRICKS_TOKEN"),
    )

    setup = UnityCatalogSetup(workspace_client=client)
    setup.setup_all(
        iam_role_arn=args.iam_role_arn,
        principal=args.principal,
        catalog=args.catalog,
        schema=args.schema,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
