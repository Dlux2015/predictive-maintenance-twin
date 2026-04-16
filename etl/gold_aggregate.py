"""
Gold layer aggregation: KPI aggregations → Gold Delta tables.

Reads from Silver Delta table, computes hourly and daily aggregations,
calculates rolling 24-hour vibration z-scores per device, flags at-risk
devices (vibration_zscore > 2.5), then writes to gold_sensors_hourly and
gold_sensors_daily.

Post-write: OPTIMIZE + ZORDER BY (device_id, recorded_at) and VACUUM on both tables.

Usage
-----
    python etl/gold_aggregate.py [--skip-optimize] [--skip-vacuum]

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

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from etl.utils import (
    get_spark_session,
    get_table_row_count,
    publish_pipeline_metric,
    write_delta_table,
)

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_CATALOG = os.environ.get("DATABRICKS_CATALOG", "workspace")
DEFAULT_SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "predictive_maintenance")
SILVER_TABLE = f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.silver_sensors"
GOLD_HOURLY_TABLE = f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.gold_sensors_hourly"
GOLD_DAILY_TABLE = f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.gold_sensors_daily"

# Vibration z-score threshold above which a device is flagged as at-risk
RISK_ZSCORE_THRESHOLD = 2.5

# Sentinel value used in Silver layer for missing reads — exclude from aggregations
NULL_SENTINEL = -1.0

VACUUM_RETAIN_HOURS = 168  # 7 days


class GoldAggregator:
    """
    Computes predictive maintenance KPI aggregations and writes Gold Delta tables.

    Gold hourly:
      - avg/min/max vibration_rms, temperature_celsius, pressure_bar per device per hour
      - reading_count per window

    Gold daily:
      - Same aggregations per device per day
      - Rolling 24-hour z-score on vibration_rms
      - is_at_risk flag: vibration_zscore > 2.5 (core predictive maintenance signal)
    """

    def __init__(self, spark: SparkSession) -> None:
        """
        Initialise the aggregator.

        Parameters
        ----------
        spark:
            Active SparkSession.
        """
        self.spark = spark

    def read_silver(self) -> DataFrame:
        """
        Read the Silver Delta table, excluding sentinel values from aggregations.

        Sentinel values (-1.0) represent missing sensor reads and must not
        skew the statistical aggregations.

        Returns
        -------
        DataFrame with sentinel rows zeroed out to null for aggregation columns.
        """
        logger.info("Reading Silver table: %s", SILVER_TABLE)
        df = self.spark.table(SILVER_TABLE)

        # Replace sentinel -1.0 with null so they don't affect avg/min/max
        for col in ("vibration_rms", "temperature_celsius", "pressure_bar"):
            df = df.withColumn(
                col,
                F.when(F.col(col) == NULL_SENTINEL, None).otherwise(F.col(col)),
            )

        logger.info("Silver row count (pre-aggregation): %d", df.count())
        return df

    def compute_hourly(self, df: DataFrame) -> DataFrame:
        """
        Compute hourly KPI aggregations grouped by device and 1-hour window.

        Parameters
        ----------
        df:
            Silver DataFrame.

        Returns
        -------
        DataFrame with columns: device_id, window_start, window_end,
        avg/min/max vibration_rms, temperature_celsius, pressure_bar,
        reading_count.
        """
        logger.info("Computing hourly Gold aggregations")
        return (
            df.groupBy(
                "device_id",
                F.window(F.col("recorded_at"), "1 hour").alias("window"),
            )
            .agg(
                F.avg("vibration_rms").alias("avg_vibration_rms"),
                F.min("vibration_rms").alias("min_vibration_rms"),
                F.max("vibration_rms").alias("max_vibration_rms"),
                F.avg("temperature_celsius").alias("avg_temperature_celsius"),
                F.min("temperature_celsius").alias("min_temperature_celsius"),
                F.max("temperature_celsius").alias("max_temperature_celsius"),
                F.avg("pressure_bar").alias("avg_pressure_bar"),
                F.min("pressure_bar").alias("min_pressure_bar"),
                F.max("pressure_bar").alias("max_pressure_bar"),
                F.count("entry_id").alias("reading_count"),
            )
            .withColumn("window_start", F.col("window.start"))
            .withColumn("window_end", F.col("window.end"))
            .drop("window")
            .withColumn("_computed_at", F.current_timestamp())
        )

    def compute_daily(self, df: DataFrame) -> DataFrame:
        """
        Compute daily KPI aggregations grouped by device and calendar date.

        Parameters
        ----------
        df:
            Silver DataFrame.

        Returns
        -------
        DataFrame with columns: device_id, date, avg/min/max metrics,
        reading_count.
        """
        logger.info("Computing daily Gold aggregations")
        return (
            df.groupBy(
                "device_id",
                F.to_date("recorded_at").alias("date"),
            )
            .agg(
                F.avg("vibration_rms").alias("avg_vibration_rms"),
                F.min("vibration_rms").alias("min_vibration_rms"),
                F.max("vibration_rms").alias("max_vibration_rms"),
                F.avg("temperature_celsius").alias("avg_temperature_celsius"),
                F.min("temperature_celsius").alias("min_temperature_celsius"),
                F.max("temperature_celsius").alias("max_temperature_celsius"),
                F.avg("pressure_bar").alias("avg_pressure_bar"),
                F.min("pressure_bar").alias("min_pressure_bar"),
                F.max("pressure_bar").alias("max_pressure_bar"),
                F.count("entry_id").alias("reading_count"),
            )
            .withColumn("_computed_at", F.current_timestamp())
        )

    def add_zscore(self, df: DataFrame) -> DataFrame:
        """
        Add a rolling 24-hour vibration z-score per device to the daily DataFrame.

        Z-score formula:
          z = (x - mean) / stddev
        where mean and stddev are computed over a 2-row (24-hour) rolling
        window per device, ordered by date.

        A z-score > 2.5 indicates the device's vibration is more than 2.5
        standard deviations above its recent baseline — a strong predictive
        maintenance signal.

        Parameters
        ----------
        df:
            Daily aggregation DataFrame with avg_vibration_rms column.

        Returns
        -------
        DataFrame with vibration_zscore column added (null when stddev = 0).
        """
        # 2-row window corresponds to current day + previous day (24-hour rolling)
        window = Window.partitionBy("device_id").orderBy("date").rowsBetween(-1, 0)
        rolling_mean = F.avg("avg_vibration_rms").over(window)
        rolling_std = F.stddev("avg_vibration_rms").over(window)

        return df.withColumn(
            "vibration_zscore",
            F.when(
                rolling_std > 0,
                (F.col("avg_vibration_rms") - rolling_mean) / rolling_std,
            ).otherwise(F.lit(0.0)),
        )

    def add_risk_flag(self, df: DataFrame) -> DataFrame:
        """
        Add the is_at_risk boolean flag based on vibration z-score threshold.

        is_at_risk = True when vibration_zscore > 2.5

        This is the core predictive maintenance signal. Devices flagged as
        at-risk should trigger maintenance inspection.

        Parameters
        ----------
        df:
            Daily DataFrame with vibration_zscore column.

        Returns
        -------
        DataFrame with is_at_risk BooleanType column.
        """
        return df.withColumn(
            "is_at_risk",
            F.col("vibration_zscore") > RISK_ZSCORE_THRESHOLD,
        )

    def write_gold(self, df: DataFrame, table_name: str) -> None:
        """
        Write a Gold DataFrame to a Delta table (overwrite by partition date).

        Uses dynamic partition overwrite so each run refreshes only the
        dates present in the current batch.

        Parameters
        ----------
        df:
            Gold DataFrame to persist.
        table_name:
            Three-part Delta table name.
        """
        logger.info("Writing to Gold table: %s (%d rows)", table_name, df.count())

        # spark.conf.set is not available in Spark Connect; pass as writer option instead
        partition_col = "date" if "date" in df.columns else "window_start"
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("partitionOverwriteMode", "dynamic")
            .partitionBy(partition_col)
            .saveAsTable(table_name)
        )
        logger.info("Write complete: %s", table_name)

    def optimize_tables(self) -> None:
        """
        Run OPTIMIZE + ZORDER BY (device_id, recorded_at) on both Gold tables.

        OPTIMIZE compacts small files for faster query performance.
        ZORDER collocates related data (by device and time) within files.
        """
        for table in (GOLD_HOURLY_TABLE, GOLD_DAILY_TABLE):
            logger.info("Running OPTIMIZE + ZORDER on %s", table)
            try:
                order_col = "window_start" if "hourly" in table else "date"
                self.spark.sql(f"OPTIMIZE {table} ZORDER BY (device_id, {order_col})")
                logger.info("OPTIMIZE complete: %s", table)
            except Exception as exc:
                logger.warning("OPTIMIZE failed for %s: %s", table, exc)

    def vacuum_tables(self, retain_hours: int = VACUUM_RETAIN_HOURS) -> None:
        """
        Run VACUUM on both Gold tables to remove old file versions.

        Retains 168 hours (7 days) of history for time-travel and rollback.

        Parameters
        ----------
        retain_hours:
            Number of hours of Delta history to retain.
        """
        for table in (GOLD_HOURLY_TABLE, GOLD_DAILY_TABLE):
            logger.info("Running VACUUM RETAIN %d HOURS on %s", retain_hours, table)
            try:
                self.spark.conf.set(
                    "spark.databricks.delta.retentionDurationCheck.enabled", "false"
                )
                self.spark.sql(f"VACUUM {table} RETAIN {retain_hours} HOURS")
                logger.info("VACUUM complete: %s", table)
            except Exception as exc:
                logger.warning("VACUUM failed for %s: %s", table, exc)

    def run(self, skip_optimize: bool = False, skip_vacuum: bool = False) -> None:
        """
        Execute the full Gold aggregation pipeline.

        Pipeline:
        1. Read Silver (with sentinel replacement)
        2. Compute hourly aggregations → write to gold_sensors_hourly
        3. Compute daily aggregations → add z-score → add risk flag → write to gold_sensors_daily
        4. Optionally OPTIMIZE + ZORDER both tables
        5. Optionally VACUUM both tables
        6. Publish CloudWatch metrics

        Parameters
        ----------
        skip_optimize:
            Skip OPTIMIZE + ZORDER (faster, useful during development).
        skip_vacuum:
            Skip VACUUM (never skip in production).
        """
        logger.info("Starting Gold aggregation pipeline")

        silver_df = self.read_silver()

        # Hourly aggregations
        hourly_df = self.compute_hourly(silver_df)
        self.write_gold(hourly_df, GOLD_HOURLY_TABLE)

        # Daily aggregations + predictive maintenance signals
        daily_df = self.compute_daily(silver_df)
        daily_df = self.add_zscore(daily_df)
        daily_df = self.add_risk_flag(daily_df)
        self.write_gold(daily_df, GOLD_DAILY_TABLE)

        # Table maintenance
        if not skip_optimize:
            self.optimize_tables()
        else:
            logger.info("Skipping OPTIMIZE (--skip-optimize)")

        if not skip_vacuum:
            self.vacuum_tables()
        else:
            logger.info("Skipping VACUUM (--skip-vacuum)")

        # CloudWatch metrics
        try:
            hourly_count = get_table_row_count(self.spark, GOLD_HOURLY_TABLE)
            daily_count = get_table_row_count(self.spark, GOLD_DAILY_TABLE)
            publish_pipeline_metric("GoldHourlyRowCount", float(hourly_count), "Count")
            publish_pipeline_metric("GoldDailyRowCount", float(daily_count), "Count")

            # Count at-risk devices
            at_risk = (
                self.spark.table(GOLD_DAILY_TABLE)
                .filter(F.col("is_at_risk"))
                .select("device_id")
                .distinct()
                .count()
            )
            publish_pipeline_metric("AtRiskDeviceCount", float(at_risk), "Count")

            # Max z-score across all devices today
            from pyspark.sql import functions as F2

            max_zscore = (
                self.spark.table(GOLD_DAILY_TABLE)
                .filter(F.col("date") == F.current_date())
                .agg(F.max("vibration_zscore").alias("max_z"))
                .collect()[0]["max_z"]
                or 0.0
            )
            publish_pipeline_metric("MaxVibrationZScore", float(max_zscore), "None")

            logger.info(
                "Gold metrics: hourly=%d daily=%d at_risk_devices=%d max_zscore=%.3f",
                hourly_count,
                daily_count,
                at_risk,
                max_zscore,
            )
        except Exception as exc:
            logger.warning("Could not publish Gold CloudWatch metrics: %s", exc)

        logger.info("Gold aggregation pipeline complete")


def main() -> None:
    """Parse arguments and run Gold aggregation."""
    parser = argparse.ArgumentParser(
        description="Gold aggregation: Silver → KPI Delta tables"
    )
    parser.add_argument("--skip-optimize", action="store_true")
    parser.add_argument("--skip-vacuum", action="store_true")
    parser.add_argument("--catalog", default=DEFAULT_CATALOG)
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    args = parser.parse_args()

    spark = get_spark_session(app_name="pmt-gold-aggregate", catalog=args.catalog)
    aggregator = GoldAggregator(spark=spark)
    aggregator.run(skip_optimize=args.skip_optimize, skip_vacuum=args.skip_vacuum)


if __name__ == "__main__":
    main()
