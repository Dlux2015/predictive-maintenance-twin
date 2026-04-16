"""
Unit tests for etl/gold_aggregate.py — GoldAggregator pure transformations.

Tests cover:
  - Sentinel (-1.0) → null replacement in read_silver logic
  - compute_hourly: output columns, row count, aggregation correctness
  - compute_daily:  output columns, row count, aggregation correctness
  - add_zscore:     z-score column present, type correct, 0.0 for single-row window
  - add_risk_flag:  True when zscore > 2.5, False otherwise

All tests use plain DataFrames — no Delta Lake required.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from etl.gold_aggregate import GoldAggregator, NULL_SENTINEL, RISK_ZSCORE_THRESHOLD


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _silver_schema() -> StructType:
    return StructType(
        [
            StructField("device_id", StringType(), nullable=False),
            StructField("entry_id", LongType(), nullable=True),
            StructField("recorded_at", TimestampType(), nullable=True),
            StructField("vibration_rms", DoubleType(), nullable=True),
            StructField("temperature_celsius", DoubleType(), nullable=True),
            StructField("pressure_bar", DoubleType(), nullable=True),
        ]
    )


def _ts(year: int, month: int, day: int, hour: int = 0) -> datetime:
    return datetime(year, month, day, hour, 0, 0, tzinfo=timezone.utc)


def _make_aggregator(spark: SparkSession) -> GoldAggregator:
    return GoldAggregator(spark=spark)


# ---------------------------------------------------------------------------
# Sentinel replacement (logic from read_silver)
# ---------------------------------------------------------------------------


class TestSentinelReplacement:
    """
    read_silver() replaces -1.0 sentinel with null before aggregations.
    We test this logic by replicating it on a plain DataFrame.
    """

    def test_sentinel_replaced_with_null(self, spark):
        schema = _silver_schema()
        rows = [
            Row(
                device_id="d1",
                entry_id=1,
                recorded_at=_ts(2024, 1, 1),
                vibration_rms=NULL_SENTINEL,
                temperature_celsius=NULL_SENTINEL,
                pressure_bar=NULL_SENTINEL,
            ),
        ]
        df = spark.createDataFrame(rows, schema=schema)

        # Replicate the sentinel → null replacement from GoldAggregator.read_silver
        for col in ("vibration_rms", "temperature_celsius", "pressure_bar"):
            df = df.withColumn(
                col,
                F.when(F.col(col) == NULL_SENTINEL, None).otherwise(F.col(col)),
            )

        row = df.collect()[0]
        assert row["vibration_rms"] is None
        assert row["temperature_celsius"] is None
        assert row["pressure_bar"] is None

    def test_real_values_not_replaced(self, spark):
        schema = _silver_schema()
        rows = [
            Row(
                device_id="d1",
                entry_id=1,
                recorded_at=_ts(2024, 1, 1),
                vibration_rms=5.5,
                temperature_celsius=25.0,
                pressure_bar=1.2,
            ),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        for col in ("vibration_rms", "temperature_celsius", "pressure_bar"):
            df = df.withColumn(
                col,
                F.when(F.col(col) == NULL_SENTINEL, None).otherwise(F.col(col)),
            )
        row = df.collect()[0]
        assert abs(row["vibration_rms"] - 5.5) < 1e-9
        assert abs(row["temperature_celsius"] - 25.0) < 1e-9
        assert abs(row["pressure_bar"] - 1.2) < 1e-9


# ---------------------------------------------------------------------------
# compute_hourly
# ---------------------------------------------------------------------------


class TestComputeHourly:
    def _silver_df(self, spark: SparkSession):
        schema = _silver_schema()
        rows = [
            # device d1: 2 readings in the same hour
            Row(
                device_id="d1",
                entry_id=1,
                recorded_at=_ts(2024, 1, 1, 10),
                vibration_rms=2.0,
                temperature_celsius=20.0,
                pressure_bar=1.0,
            ),
            Row(
                device_id="d1",
                entry_id=2,
                recorded_at=_ts(2024, 1, 1, 10),
                vibration_rms=4.0,
                temperature_celsius=24.0,
                pressure_bar=1.4,
            ),
            # device d2: 1 reading in a different hour
            Row(
                device_id="d2",
                entry_id=3,
                recorded_at=_ts(2024, 1, 1, 11),
                vibration_rms=6.0,
                temperature_celsius=30.0,
                pressure_bar=2.0,
            ),
        ]
        return spark.createDataFrame(rows, schema=schema)

    def test_output_has_expected_columns(self, spark):
        agg = _make_aggregator(spark)
        result = agg.compute_hourly(self._silver_df(spark))
        cols = set(result.columns)
        for expected in (
            "device_id",
            "window_start",
            "window_end",
            "avg_vibration_rms",
            "min_vibration_rms",
            "max_vibration_rms",
            "avg_temperature_celsius",
            "reading_count",
        ):
            assert expected in cols, f"Missing column: {expected}"

    def test_row_count_equals_device_hour_combinations(self, spark):
        agg = _make_aggregator(spark)
        result = agg.compute_hourly(self._silver_df(spark))
        # d1 in hour 10 = 1 group; d2 in hour 11 = 1 group → 2 rows total
        assert result.count() == 2

    def test_avg_vibration_correct(self, spark):
        agg = _make_aggregator(spark)
        result = agg.compute_hourly(self._silver_df(spark))
        d1_row = result.filter(F.col("device_id") == "d1").collect()[0]
        # avg of [2.0, 4.0] = 3.0
        assert abs(d1_row["avg_vibration_rms"] - 3.0) < 1e-6

    def test_min_max_vibration_correct(self, spark):
        agg = _make_aggregator(spark)
        result = agg.compute_hourly(self._silver_df(spark))
        d1_row = result.filter(F.col("device_id") == "d1").collect()[0]
        assert abs(d1_row["min_vibration_rms"] - 2.0) < 1e-6
        assert abs(d1_row["max_vibration_rms"] - 4.0) < 1e-6

    def test_reading_count_correct(self, spark):
        agg = _make_aggregator(spark)
        result = agg.compute_hourly(self._silver_df(spark))
        d1_row = result.filter(F.col("device_id") == "d1").collect()[0]
        d2_row = result.filter(F.col("device_id") == "d2").collect()[0]
        assert d1_row["reading_count"] == 2
        assert d2_row["reading_count"] == 1

    def test_nulls_excluded_from_avg(self, spark):
        """Null values (sentinel-replaced) must not drag down avg."""
        schema = _silver_schema()
        rows = [
            Row(
                device_id="d1",
                entry_id=1,
                recorded_at=_ts(2024, 1, 1, 10),
                vibration_rms=None,
                temperature_celsius=20.0,
                pressure_bar=1.0,
            ),
            Row(
                device_id="d1",
                entry_id=2,
                recorded_at=_ts(2024, 1, 1, 10),
                vibration_rms=6.0,
                temperature_celsius=20.0,
                pressure_bar=1.0,
            ),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        agg = _make_aggregator(spark)
        result = agg.compute_hourly(df).collect()[0]
        # avg([null, 6.0]) with Spark ignores nulls → 6.0
        assert abs(result["avg_vibration_rms"] - 6.0) < 1e-6


# ---------------------------------------------------------------------------
# compute_daily
# ---------------------------------------------------------------------------


class TestComputeDaily:
    def _silver_df(self, spark: SparkSession):
        schema = _silver_schema()
        rows = [
            # device d1: 3 readings on 2024-01-01
            Row(
                device_id="d1",
                entry_id=1,
                recorded_at=_ts(2024, 1, 1, 8),
                vibration_rms=1.0,
                temperature_celsius=20.0,
                pressure_bar=1.0,
            ),
            Row(
                device_id="d1",
                entry_id=2,
                recorded_at=_ts(2024, 1, 1, 12),
                vibration_rms=3.0,
                temperature_celsius=22.0,
                pressure_bar=1.2,
            ),
            Row(
                device_id="d1",
                entry_id=3,
                recorded_at=_ts(2024, 1, 1, 16),
                vibration_rms=5.0,
                temperature_celsius=24.0,
                pressure_bar=1.4,
            ),
        ]
        return spark.createDataFrame(rows, schema=schema)

    def test_output_has_expected_columns(self, spark):
        agg = _make_aggregator(spark)
        result = agg.compute_daily(self._silver_df(spark))
        cols = set(result.columns)
        for expected in (
            "device_id",
            "date",
            "avg_vibration_rms",
            "min_vibration_rms",
            "max_vibration_rms",
            "avg_temperature_celsius",
            "reading_count",
        ):
            assert expected in cols, f"Missing column: {expected}"

    def test_one_row_per_device_per_day(self, spark):
        agg = _make_aggregator(spark)
        result = agg.compute_daily(self._silver_df(spark))
        assert result.count() == 1  # one device, one day

    def test_avg_vibration_correct(self, spark):
        agg = _make_aggregator(spark)
        result = agg.compute_daily(self._silver_df(spark)).collect()[0]
        # avg([1.0, 3.0, 5.0]) = 3.0
        assert abs(result["avg_vibration_rms"] - 3.0) < 1e-6

    def test_reading_count_correct(self, spark):
        agg = _make_aggregator(spark)
        result = agg.compute_daily(self._silver_df(spark)).collect()[0]
        assert result["reading_count"] == 3


# ---------------------------------------------------------------------------
# add_zscore
# ---------------------------------------------------------------------------


class TestAddZscore:
    def _daily_schema(self) -> StructType:
        return StructType(
            [
                StructField("device_id", StringType(), nullable=False),
                StructField("date", DateType(), nullable=False),
                StructField("avg_vibration_rms", DoubleType(), nullable=True),
                StructField("reading_count", LongType(), nullable=True),
            ]
        )

    def test_zscore_column_present(self, spark):
        from datetime import date

        schema = self._daily_schema()
        rows = [
            Row(
                device_id="d1",
                date=date(2024, 1, 1),
                avg_vibration_rms=2.0,
                reading_count=3,
            ),
            Row(
                device_id="d1",
                date=date(2024, 1, 2),
                avg_vibration_rms=4.0,
                reading_count=3,
            ),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        result = _make_aggregator(spark).add_zscore(df)
        assert "vibration_zscore" in result.columns

    def test_single_row_device_gets_zero_zscore(self, spark):
        """With only one row in the window, stddev is null → z-score defaults to 0.0."""
        from datetime import date

        schema = self._daily_schema()
        rows = [
            Row(
                device_id="d1",
                date=date(2024, 1, 1),
                avg_vibration_rms=5.0,
                reading_count=2,
            ),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        result = _make_aggregator(spark).add_zscore(df).collect()[0]
        assert result["vibration_zscore"] == 0.0

    def test_zscore_is_zero_for_equal_values(self, spark):
        """If both rows in the window have the same value, stddev = 0 → z-score = 0.0."""
        from datetime import date

        schema = self._daily_schema()
        rows = [
            Row(
                device_id="d1",
                date=date(2024, 1, 1),
                avg_vibration_rms=3.0,
                reading_count=2,
            ),
            Row(
                device_id="d1",
                date=date(2024, 1, 2),
                avg_vibration_rms=3.0,
                reading_count=2,
            ),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        result = _make_aggregator(spark).add_zscore(df)
        for row in result.collect():
            assert (
                row["vibration_zscore"] == 0.0
            ), f"Expected 0.0, got {row['vibration_zscore']}"

    def test_zscore_magnitude_for_two_row_window(self, spark):
        """
        In a 2-row rolling window the z-score of the last row is always ±1/sqrt(2) ≈ 0.707.
        This is a mathematical property of sample std over 2 points.
        """
        from datetime import date

        schema = self._daily_schema()
        rows = [
            Row(
                device_id="d1",
                date=date(2024, 1, 1),
                avg_vibration_rms=1.0,
                reading_count=2,
            ),
            Row(
                device_id="d1",
                date=date(2024, 1, 2),
                avg_vibration_rms=3.0,
                reading_count=2,
            ),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        result = (
            _make_aggregator(spark)
            .add_zscore(df)
            .filter(F.col("date") == date(2024, 1, 2))
            .collect()[0]
        )
        expected = 1.0 / math.sqrt(2)  # ≈ 0.7071
        assert abs(abs(result["vibration_zscore"]) - expected) < 0.01

    def test_zscore_partitioned_by_device(self, spark):
        """Devices must be z-scored independently — one device's data must not bleed into another."""
        from datetime import date

        schema = self._daily_schema()
        rows = [
            Row(
                device_id="d1",
                date=date(2024, 1, 1),
                avg_vibration_rms=10.0,
                reading_count=2,
            ),
            Row(
                device_id="d2",
                date=date(2024, 1, 1),
                avg_vibration_rms=0.1,
                reading_count=2,
            ),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        result = _make_aggregator(spark).add_zscore(df)
        # Both devices have only 1 row → z-score must be 0.0 regardless of values
        for row in result.collect():
            assert row["vibration_zscore"] == 0.0


# ---------------------------------------------------------------------------
# add_risk_flag
# ---------------------------------------------------------------------------


class TestAddRiskFlag:
    def _zscore_schema(self) -> StructType:
        from datetime import date

        return StructType(
            [
                StructField("device_id", StringType(), nullable=False),
                StructField("vibration_zscore", DoubleType(), nullable=True),
            ]
        )

    def test_is_at_risk_true_when_zscore_above_threshold(self, spark):
        schema = self._zscore_schema()
        rows = [Row(device_id="d1", vibration_zscore=RISK_ZSCORE_THRESHOLD + 0.1)]
        df = spark.createDataFrame(rows, schema=schema)
        result = _make_aggregator(spark).add_risk_flag(df).collect()[0]
        assert result["is_at_risk"] is True

    def test_is_at_risk_false_when_zscore_at_threshold(self, spark):
        """Threshold is strictly greater-than — exactly 2.5 is NOT at risk."""
        schema = self._zscore_schema()
        rows = [Row(device_id="d1", vibration_zscore=RISK_ZSCORE_THRESHOLD)]
        df = spark.createDataFrame(rows, schema=schema)
        result = _make_aggregator(spark).add_risk_flag(df).collect()[0]
        assert result["is_at_risk"] is False

    def test_is_at_risk_false_when_zscore_below_threshold(self, spark):
        schema = self._zscore_schema()
        rows = [Row(device_id="d1", vibration_zscore=0.5)]
        df = spark.createDataFrame(rows, schema=schema)
        result = _make_aggregator(spark).add_risk_flag(df).collect()[0]
        assert result["is_at_risk"] is False

    def test_is_at_risk_false_for_zero_zscore(self, spark):
        schema = self._zscore_schema()
        rows = [Row(device_id="d1", vibration_zscore=0.0)]
        df = spark.createDataFrame(rows, schema=schema)
        result = _make_aggregator(spark).add_risk_flag(df).collect()[0]
        assert result["is_at_risk"] is False

    def test_is_at_risk_column_present(self, spark):
        schema = self._zscore_schema()
        rows = [Row(device_id="d1", vibration_zscore=1.0)]
        df = spark.createDataFrame(rows, schema=schema)
        result = _make_aggregator(spark).add_risk_flag(df)
        assert "is_at_risk" in result.columns

    def test_multiple_devices_flagged_independently(self, spark):
        schema = self._zscore_schema()
        rows = [
            Row(device_id="safe", vibration_zscore=1.0),
            Row(device_id="risky", vibration_zscore=3.5),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        result = {
            r["device_id"]: r["is_at_risk"]
            for r in _make_aggregator(spark).add_risk_flag(df).collect()
        }
        assert result["safe"] is False
        assert result["risky"] is True
