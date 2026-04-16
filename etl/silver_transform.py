"""
Silver layer transformation: Clean, deduplicate, and type-cast sensor data.

Reads from Bronze Delta table, applies type casts, parses timestamps to UTC,
fills null metrics with sentinel value -1.0, deduplicates on (device_id, entry_id),
and MERGE-upserts into the Silver Delta table.

Usage
-----
    python etl/silver_transform.py [--catalog CATALOG] [--schema SCHEMA]

Environment variables
---------------------
DATABRICKS_CATALOG      Unity Catalog catalog name (default: main)
DATABRICKS_SCHEMA       Schema name (default: predictive_maintenance)
AWS_REGION              AWS region (default: us-east-1)
"""

from __future__ import annotations

import argparse
import logging
import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, TimestampType
from pyspark.sql.window import Window

from etl.utils import (
    DataQualityChecker,
    get_spark_session,
    get_table_row_count,
    log_table_stats,
    publish_pipeline_metric,
)

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_CATALOG = os.environ.get("DATABRICKS_CATALOG", "workspace")
DEFAULT_SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "predictive_maintenance")
BRONZE_TABLE = f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.bronze_sensors"
SILVER_TABLE = f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.silver_sensors"

# Sentinel value for missing sensor reads (allows downstream numerics without null handling)
NULL_SENTINEL = -1.0


class SilverTransformer:
    """
    Transforms Bronze sensor data into the clean Silver Delta table.

    Applies the following transformations in order:
    1. Type casts (metrics → DoubleType, entry_id → LongType)
    2. Timestamp parsing (recorded_at string → UTC TimestampType)
    3. Null filling with -1.0 sentinel (missing metrics)
    4. Row filtering (drop rows where device_id is null)
    5. Deduplication (device_id + entry_id, keep latest ingested_at)
    6. MERGE upsert into Silver Delta table
    """

    def __init__(self, spark: SparkSession) -> None:
        """
        Initialise the transformer.

        Parameters
        ----------
        spark:
            Active SparkSession.
        """
        self.spark = spark
        self.bronze_table = BRONZE_TABLE
        self.silver_table = SILVER_TABLE

    def read_bronze(self) -> DataFrame:
        """
        Read all records from the Bronze Delta table.

        Returns
        -------
        DataFrame of raw Bronze records.
        """
        logger.info("Reading from Bronze table: %s", self.bronze_table)
        df = self.spark.table(self.bronze_table)
        logger.info("Bronze row count: %d", df.count())
        return df

    def cast_metrics(self, df: DataFrame) -> DataFrame:
        """
        Cast metric columns to DoubleType and entry_id to LongType.

        Parameters
        ----------
        df:
            Input DataFrame from Bronze.

        Returns
        -------
        DataFrame with correct numeric types.
        """
        return (
            df.withColumn("vibration_rms", F.col("vibration_rms").cast(DoubleType()))
            .withColumn(
                "temperature_celsius", F.col("temperature_celsius").cast(DoubleType())
            )
            .withColumn("pressure_bar", F.col("pressure_bar").cast(DoubleType()))
            .withColumn("entry_id", F.col("entry_id").cast(LongType()))
        )

    def parse_timestamps(self, df: DataFrame) -> DataFrame:
        """
        Parse recorded_at from ISO-8601 string to UTC TimestampType.

        ThingSpeak returns ISO-8601 format: "2024-01-15T14:30:00Z".
        The resulting column is renamed to recorded_at (overwriting string version).

        Parameters
        ----------
        df:
            Input DataFrame with recorded_at as StringType.

        Returns
        -------
        DataFrame with recorded_at as TimestampType in UTC.
        """
        return df.withColumn(
            "recorded_at",
            F.to_utc_timestamp(F.to_timestamp(F.col("recorded_at")), "UTC"),
        )

    def handle_nulls(self, df: DataFrame) -> DataFrame:
        """
        Fill null metrics with -1.0 sentinel and drop rows with null device_id.

        The -1.0 sentinel indicates a missing sensor read at the Silver layer.
        Downstream Gold aggregations filter out sentinels where appropriate.

        Parameters
        ----------
        df:
            Input DataFrame.

        Returns
        -------
        DataFrame with no null metrics and no null device_id rows.
        """
        # Drop rows with no device_id — cannot associate with any device
        df = df.filter(F.col("device_id").isNotNull())

        # Fill null sensor readings with the -1.0 sentinel
        df = df.fillna(
            NULL_SENTINEL,
            subset=["vibration_rms", "temperature_celsius", "pressure_bar"],
        )

        return df

    def deduplicate(self, df: DataFrame) -> DataFrame:
        """
        Deduplicate on (device_id, entry_id), keeping the row with the latest _ingested_at.

        In normal operation each (device_id, entry_id) combination is unique,
        but re-ingestion or duplicate file delivery can create duplicates.

        Parameters
        ----------
        df:
            Input DataFrame (may contain duplicates).

        Returns
        -------
        Deduplicated DataFrame.
        """
        window = Window.partitionBy("device_id", "entry_id").orderBy(
            F.col("_ingested_at").desc()
        )
        return (
            df.withColumn("_row_num", F.row_number().over(window))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Run data quality checks and log warnings. Does not filter rows.

        Parameters
        ----------
        df:
            DataFrame to validate.

        Returns
        -------
        The same DataFrame (checks are non-blocking).
        """
        checker = DataQualityChecker(df, self.silver_table)
        checker.run_all_checks()
        # Physical value range checks (sentinels are -1.0 so we skip negatives)
        checker.check_value_range("temperature_celsius", min_val=-1.0, max_val=200.0)
        checker.check_value_range("pressure_bar", min_val=-1.0, max_val=1000.0)
        checker.check_value_range("vibration_rms", min_val=-1.0, max_val=500.0)
        checker.log_summary()
        return df

    def merge_upsert(self, df: DataFrame) -> None:
        """
        MERGE (upsert) the transformed DataFrame into the Silver Delta table.

        Match condition: device_id + entry_id.
        - If matched: update all metric and audit columns.
        - If not matched: insert the new row.

        Parameters
        ----------
        df:
            Cleaned and deduplicated DataFrame to upsert.
        """
        logger.info(
            "Merging %d rows into Silver table: %s", df.count(), self.silver_table
        )

        # Add silver audit timestamp
        df = df.withColumn("_silver_updated_at", F.current_timestamp())

        table_exists = self.spark.catalog.tableExists(self.silver_table)

        if not table_exists:
            # First run — create the managed Delta table via a plain write.
            # Unity Catalog manages the storage location; no LOCATION clause needed.
            logger.info("Silver table does not exist — creating via initial write")
            (df.write.format("delta").mode("overwrite").saveAsTable(self.silver_table))
            logger.info("Silver table created: %s", self.silver_table)
        else:
            target = DeltaTable.forName(self.spark, self.silver_table)
            (
                target.alias("target")
                .merge(
                    df.alias("source"),
                    "target.device_id = source.device_id AND target.entry_id = source.entry_id",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            logger.info("MERGE complete for %s", self.silver_table)

    def run(self) -> None:
        """
        Execute the full Silver transformation pipeline.

        Pipeline: Bronze read → cast → parse timestamps → handle nulls →
                  deduplicate → validate → MERGE upsert → publish metrics.
        """
        logger.info("Starting Silver transformation pipeline")

        df = self.read_bronze()
        df = self.cast_metrics(df)
        df = self.parse_timestamps(df)
        df = self.handle_nulls(df)
        df = self.deduplicate(df)
        df = self.validate(df)
        self.merge_upsert(df)

        # Post-merge metrics
        try:
            row_count = get_table_row_count(self.spark, self.silver_table)
            publish_pipeline_metric("SilverRowCount", float(row_count), "Count")
            logger.info("Silver table row count: %d", row_count)
        except Exception as exc:
            logger.warning("Could not publish Silver metrics: %s", exc)

        logger.info("Silver transformation pipeline complete")


def main() -> None:
    """Parse arguments and run Silver transformation."""
    parser = argparse.ArgumentParser(
        description="Silver transformation: Bronze → Silver Delta"
    )
    parser.add_argument("--catalog", default=DEFAULT_CATALOG)
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    args = parser.parse_args()

    spark = get_spark_session(app_name="pmt-silver-transform", catalog=args.catalog)
    transformer = SilverTransformer(spark=spark)
    transformer.run()


if __name__ == "__main__":
    main()
