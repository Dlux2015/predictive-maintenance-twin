"""
Bronze layer ingestion: Raw JSON → Bronze Delta table using Databricks Auto Loader.

Reads raw sensor JSON files using Auto Loader (cloudFiles format), adds audit
columns, runs data quality checks, and appends to the bronze_sensors Delta table
(append-only, no deduplication at this layer).

Backend selection (PMT_BACKEND env var or --backend flag):
  databricks (default)
      Reads from DBFS: dbfs:/pmt/raw/
      Checkpoints stored in DBFS: dbfs:/pmt/checkpoints/bronze_sensors/
      No AWS credentials required.
  aws (secondary)
      Reads from S3 via Auto Loader: s3://<PMT_S3_BUCKET>/raw/
      Checkpoints stored in S3: s3://<PMT_S3_BUCKET>/checkpoints/bronze_sensors/

Usage
-----
    python etl/bronze_ingest.py [--trigger-once] [--source-path PATH] [--checkpoint-path PATH]

Environment variables
---------------------
PMT_BACKEND             'databricks' (default) or 'aws'
DATABRICKS_CATALOG      Unity Catalog catalog name (default: main)
DATABRICKS_SCHEMA       Schema name (default: predictive_maintenance)
DBFS_RAW_PATH           DBFS source path  [databricks backend]
DBFS_CHECKPOINT_PATH    DBFS checkpoint path  [databricks backend]
PMT_S3_BUCKET           S3 bucket name  [aws backend]
"""

from __future__ import annotations

import argparse
import logging
import os
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from etl.utils import DataQualityChecker, get_spark_session, log_table_stats, publish_pipeline_metric

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_CATALOG = os.environ.get("DATABRICKS_CATALOG", "workspace")
DEFAULT_SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "predictive_maintenance")
DEFAULT_TABLE = f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.bronze_sensors"

# ── Backend-aware path defaults ───────────────────────────────────────────────
_BACKEND = os.environ.get("PMT_BACKEND", "databricks")

if _BACKEND == "aws":
    _S3_BUCKET = os.environ.get("PMT_S3_BUCKET", "predictive-maintenance-twin-raw")
    DEFAULT_SOURCE_PATH = f"s3://{_S3_BUCKET}/raw/"
    DEFAULT_CHECKPOINT = f"s3://{_S3_BUCKET}/checkpoints/bronze_sensors/"
else:
    # Databricks primary: read from UC Volume (no AWS credentials required)
    DEFAULT_SOURCE_PATH = os.environ.get(
        "DBFS_RAW_PATH", "/Volumes/workspace/predictive_maintenance/raw/"
    )
    DEFAULT_CHECKPOINT = os.environ.get(
        "DBFS_CHECKPOINT_PATH",
        "/Volumes/workspace/predictive_maintenance/raw/_checkpoints/bronze_sensors/",
    )


class BronzeIngestor:
    """
    Ingests raw JSON sensor files from S3 into the Bronze Delta table.

    Uses Databricks Auto Loader (cloudFiles) for incremental, scalable
    ingestion with exactly-once delivery guarantees via checkpointing.
    """

    def __init__(
        self,
        spark: SparkSession,
        source_path: str = DEFAULT_SOURCE_PATH,
        table_name: str = DEFAULT_TABLE,
    ) -> None:
        """
        Initialise the ingestor.

        Parameters
        ----------
        spark:
            Active SparkSession.
        source_path:
            Path containing raw JSON files. Accepts DBFS (``dbfs:/pmt/raw/``)
            or S3 (``s3://bucket/raw/``) — Auto Loader handles both.
        table_name:
            Three-part Delta table name to write to.
        """
        self.spark = spark
        self.source_path = source_path
        self.table_name = table_name

    def get_schema(self) -> StructType:
        """
        Return the explicit schema for raw sensor JSON files.

        Using an explicit schema (not schema inference) is required for
        Auto Loader in production to avoid schema evolution issues.

        Returns
        -------
        StructType matching ingestion/schema.json.
        """
        return StructType([
            StructField("device_id", StringType(), nullable=True),
            StructField("entry_id", LongType(), nullable=True),
            StructField("recorded_at", StringType(), nullable=True),
            StructField("vibration_rms", DoubleType(), nullable=True),
            StructField("temperature_celsius", DoubleType(), nullable=True),
            StructField("pressure_bar", DoubleType(), nullable=True),
            StructField("source_channel", IntegerType(), nullable=True),
            StructField("ingested_at", StringType(), nullable=True),
        ])

    def read_with_autoloader(
        self, checkpoint_path: str = DEFAULT_CHECKPOINT
    ) -> DataFrame:
        """
        Read incremental raw JSON files using Databricks Auto Loader.

        Auto Loader uses directory listing to discover new files incrementally
        without re-scanning the full prefix. Works with both DBFS (primary)
        and S3 (secondary) source paths.

        Parameters
        ----------
        checkpoint_path:
            DBFS or S3 path for storing Auto Loader checkpoints.
            Must be stable between job runs.

        Returns
        -------
        Streaming DataFrame of raw sensor records.
        """
        logger.info(
            "Configuring Auto Loader: source=%s checkpoint=%s", self.source_path, checkpoint_path
        )
        return (
            self.spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", checkpoint_path + "/_schema")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("cloudFiles.useNotifications", "false")  # directory listing — works for DBFS and S3
            .schema(self.get_schema())
            .load(self.source_path)
        )

    def add_audit_columns(self, df: DataFrame) -> DataFrame:
        """
        Add pipeline audit columns to the DataFrame.

        Audit columns added:
          _ingested_at  — when this batch was processed by the ETL
          _source_file  — the S3 file path this record came from
          _batch_id     — UUID unique to this pipeline run

        Parameters
        ----------
        df:
            Input DataFrame (streaming or static).

        Returns
        -------
        DataFrame with audit columns appended.
        """
        batch_id = str(uuid.uuid4())
        return (
            df
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_batch_id", F.lit(batch_id))
        )

    def write_bronze(
        self,
        df: DataFrame,
        checkpoint_path: str = DEFAULT_CHECKPOINT,
        trigger_once: bool = False,
    ) -> None:
        """
        Write the streaming DataFrame to the Bronze Delta table (append-only).

        Partitioned by the date portion of recorded_at for efficient
        downstream reads by the Silver transformer.

        Parameters
        ----------
        df:
            Streaming DataFrame from Auto Loader.
        checkpoint_path:
            Checkpoint path for the streaming write.
        trigger_once:
            If True, process all available files once and stop (useful for
            scheduled jobs). If False, run as a continuous stream.
        """
        trigger_config = {"once": True} if trigger_once else {"processingTime": "30 seconds"}

        logger.info("Writing to Bronze table: %s (trigger_once=%s)", self.table_name, trigger_once)

        query = (
            df.withColumn("_partition_date", F.to_date(F.col("recorded_at")))
            .writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .partitionBy("_partition_date")
            .trigger(**trigger_config)
            .toTable(self.table_name)
        )

        if trigger_once:
            query.awaitTermination()
            logger.info("Bronze write complete (trigger_once)")

    def run(
        self,
        checkpoint_path: str = DEFAULT_CHECKPOINT,
        trigger_once: bool = False,
    ) -> None:
        """
        Execute the full Bronze ingestion pipeline.

        Steps:
        1. Read raw JSON from S3 via Auto Loader
        2. Add audit columns
        3. Run data quality checks on a sample
        4. Write to Bronze Delta table
        5. Log table statistics
        6. Publish CloudWatch metrics

        Parameters
        ----------
        checkpoint_path:
            Auto Loader checkpoint path.
        trigger_once:
            Run once and stop (for scheduled execution) vs. continuous stream.
        """
        logger.info("Starting Bronze ingestion pipeline")

        raw_df = self.read_with_autoloader(checkpoint_path=checkpoint_path)
        enriched_df = self.add_audit_columns(raw_df)

        # For static DQ checks, take a sample from the existing table if it exists
        try:
            sample = self.spark.table(self.table_name).limit(1000)
            checker = DataQualityChecker(sample, self.table_name)
            checker.run_all_checks()
            checker.log_summary()
        except Exception as exc:
            logger.info("Skipping DQ pre-check (table may not exist yet): %s", exc)

        self.write_bronze(enriched_df, checkpoint_path=checkpoint_path, trigger_once=trigger_once)

        # Post-write stats
        try:
            from etl.utils import get_table_row_count
            row_count = get_table_row_count(self.spark, self.table_name)
            publish_pipeline_metric("BronzeRowCount", float(row_count), "Count")
            logger.info("Bronze table row count: %d", row_count)
        except Exception as exc:
            logger.warning("Could not publish Bronze metrics: %s", exc)

        logger.info("Bronze ingestion pipeline complete")


def main() -> None:
    """Parse arguments and run Bronze ingestion."""
    parser = argparse.ArgumentParser(
        description="Bronze layer ingestion: raw JSON (DBFS or S3) → Delta"
    )
    parser.add_argument("--trigger-once", action="store_true", default=True,
                        help="Process all available files once and stop")
    parser.add_argument(
        "--source-path",
        default=DEFAULT_SOURCE_PATH,
        help="Source path for raw JSON files (dbfs:/... or s3://...)",
    )
    parser.add_argument("--checkpoint-path", default=DEFAULT_CHECKPOINT)
    parser.add_argument("--table-name", default=DEFAULT_TABLE)
    args = parser.parse_args()

    spark = get_spark_session(app_name="pmt-bronze-ingest")
    ingestor = BronzeIngestor(
        spark=spark, source_path=args.source_path, table_name=args.table_name
    )
    ingestor.run(checkpoint_path=args.checkpoint_path, trigger_once=args.trigger_once)


if __name__ == "__main__":
    main()
