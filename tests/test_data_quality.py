"""
Unit tests for etl/utils.py — DataQualityChecker.

All tests use the session-scoped SparkSession from conftest.py and work on
plain DataFrames (no Delta Lake required).
"""

from __future__ import annotations

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from etl.utils import DataQualityChecker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sensor_schema() -> StructType:
    return StructType(
        [
            StructField("device_id", StringType(), nullable=True),
            StructField("entry_id", LongType(), nullable=True),
            StructField("recorded_at", StringType(), nullable=True),
            StructField("vibration_rms", DoubleType(), nullable=True),
            StructField("temperature_celsius", DoubleType(), nullable=True),
            StructField("pressure_bar", DoubleType(), nullable=True),
        ]
    )


def _make_df(spark: SparkSession, rows: list[Row]):
    return spark.createDataFrame(rows, schema=_sensor_schema())


# ---------------------------------------------------------------------------
# check_row_count
# ---------------------------------------------------------------------------


class TestCheckRowCount:
    def test_passes_when_rows_meet_minimum(self, spark):
        df = _make_df(
            spark,
            [
                Row(
                    device_id="d1",
                    entry_id=1,
                    recorded_at="2024-01-01T00:00:00Z",
                    vibration_rms=1.0,
                    temperature_celsius=20.0,
                    pressure_bar=1.0,
                )
            ],
        )
        checker = DataQualityChecker(df, "test_table")
        assert checker.check_row_count(min_rows=1) is True

    def test_fails_when_empty(self, spark):
        df = spark.createDataFrame([], schema=_sensor_schema())
        checker = DataQualityChecker(df, "test_table")
        assert checker.check_row_count(min_rows=1) is False

    def test_passes_with_multiple_rows(self, spark):
        rows = [
            Row(
                device_id="d1",
                entry_id=i,
                recorded_at="2024-01-01T00:00:00Z",
                vibration_rms=1.0,
                temperature_celsius=20.0,
                pressure_bar=1.0,
            )
            for i in range(5)
        ]
        df = _make_df(spark, rows)
        checker = DataQualityChecker(df, "test_table")
        assert checker.check_row_count(min_rows=5) is True

    def test_fails_when_below_minimum(self, spark):
        rows = [
            Row(
                device_id="d1",
                entry_id=1,
                recorded_at="2024-01-01T00:00:00Z",
                vibration_rms=1.0,
                temperature_celsius=20.0,
                pressure_bar=1.0,
            )
        ]
        df = _make_df(spark, rows)
        checker = DataQualityChecker(df, "test_table")
        assert checker.check_row_count(min_rows=10) is False


# ---------------------------------------------------------------------------
# check_null_rate
# ---------------------------------------------------------------------------


class TestCheckNullRate:
    def test_passes_when_no_nulls(self, spark):
        rows = [
            Row(
                device_id="d1",
                entry_id=1,
                recorded_at="2024-01-01T00:00:00Z",
                vibration_rms=1.0,
                temperature_celsius=20.0,
                pressure_bar=1.0,
            ),
            Row(
                device_id="d2",
                entry_id=2,
                recorded_at="2024-01-01T00:00:00Z",
                vibration_rms=2.0,
                temperature_celsius=21.0,
                pressure_bar=1.1,
            ),
        ]
        df = _make_df(spark, rows)
        checker = DataQualityChecker(df, "test_table")
        assert checker.check_null_rate("device_id", threshold=0.0) is True

    def test_fails_when_threshold_zero_and_any_null(self, spark):
        rows = [
            Row(
                device_id=None,
                entry_id=1,
                recorded_at="2024-01-01T00:00:00Z",
                vibration_rms=1.0,
                temperature_celsius=20.0,
                pressure_bar=1.0,
            ),
            Row(
                device_id="d2",
                entry_id=2,
                recorded_at="2024-01-01T00:00:00Z",
                vibration_rms=2.0,
                temperature_celsius=21.0,
                pressure_bar=1.1,
            ),
        ]
        df = _make_df(spark, rows)
        checker = DataQualityChecker(df, "test_table")
        assert checker.check_null_rate("device_id", threshold=0.0) is False

    def test_passes_when_null_rate_below_threshold(self, spark):
        # 1 null out of 10 rows = 10 % — threshold is 20 %
        rows = [
            Row(
                device_id=None,
                entry_id=0,
                recorded_at="t",
                vibration_rms=None,
                temperature_celsius=None,
                pressure_bar=None,
            )
        ]
        rows += [
            Row(
                device_id="d1",
                entry_id=i,
                recorded_at="t",
                vibration_rms=1.0,
                temperature_celsius=1.0,
                pressure_bar=1.0,
            )
            for i in range(1, 10)
        ]
        df = _make_df(spark, rows)
        checker = DataQualityChecker(df, "test_table")
        assert checker.check_null_rate("device_id", threshold=0.20) is True

    def test_fails_when_null_rate_exceeds_threshold(self, spark):
        # 6 nulls out of 10 = 60 % — threshold is 5 %
        rows = [
            Row(
                device_id=None,
                entry_id=i,
                recorded_at="t",
                vibration_rms=None,
                temperature_celsius=None,
                pressure_bar=None,
            )
            for i in range(6)
        ]
        rows += [
            Row(
                device_id="d1",
                entry_id=i,
                recorded_at="t",
                vibration_rms=1.0,
                temperature_celsius=1.0,
                pressure_bar=1.0,
            )
            for i in range(6, 10)
        ]
        df = _make_df(spark, rows)
        checker = DataQualityChecker(df, "test_table")
        assert checker.check_null_rate("device_id", threshold=0.05) is False

    def test_empty_dataframe_always_passes(self, spark):
        df = spark.createDataFrame([], schema=_sensor_schema())
        checker = DataQualityChecker(df, "test_table")
        # No rows → no nulls → passes regardless of threshold
        assert checker.check_null_rate("device_id", threshold=0.0) is True


# ---------------------------------------------------------------------------
# check_value_range
# ---------------------------------------------------------------------------


class TestCheckValueRange:
    def test_passes_when_all_values_in_range(self, spark):
        rows = [
            Row(
                device_id="d1",
                entry_id=1,
                recorded_at="t",
                vibration_rms=5.0,
                temperature_celsius=25.0,
                pressure_bar=1.0,
            ),
            Row(
                device_id="d1",
                entry_id=2,
                recorded_at="t",
                vibration_rms=10.0,
                temperature_celsius=30.0,
                pressure_bar=2.0,
            ),
        ]
        df = _make_df(spark, rows)
        checker = DataQualityChecker(df, "test_table")
        assert (
            checker.check_value_range("vibration_rms", min_val=0.0, max_val=500.0)
            is True
        )

    def test_fails_when_value_above_max(self, spark):
        rows = [
            Row(
                device_id="d1",
                entry_id=1,
                recorded_at="t",
                vibration_rms=999.0,
                temperature_celsius=25.0,
                pressure_bar=1.0,
            ),
        ]
        df = _make_df(spark, rows)
        checker = DataQualityChecker(df, "test_table")
        assert (
            checker.check_value_range("vibration_rms", min_val=0.0, max_val=500.0)
            is False
        )

    def test_fails_when_value_below_min(self, spark):
        rows = [
            Row(
                device_id="d1",
                entry_id=1,
                recorded_at="t",
                vibration_rms=-5.0,
                temperature_celsius=25.0,
                pressure_bar=1.0,
            ),
        ]
        df = _make_df(spark, rows)
        checker = DataQualityChecker(df, "test_table")
        # Sentinel (-1.0) is the floor, so -5.0 is out of range
        assert (
            checker.check_value_range("vibration_rms", min_val=-1.0, max_val=500.0)
            is False
        )

    def test_passes_with_sentinel_value(self, spark):
        """Sentinel value -1.0 should be within the Silver-layer range [-1.0, 500.0]."""
        rows = [
            Row(
                device_id="d1",
                entry_id=1,
                recorded_at="t",
                vibration_rms=-1.0,
                temperature_celsius=-1.0,
                pressure_bar=-1.0,
            ),
        ]
        df = _make_df(spark, rows)
        checker = DataQualityChecker(df, "test_table")
        assert (
            checker.check_value_range("vibration_rms", min_val=-1.0, max_val=500.0)
            is True
        )

    def test_nulls_are_ignored(self, spark):
        """Null values must not be counted as out-of-range."""
        rows = [
            Row(
                device_id="d1",
                entry_id=1,
                recorded_at="t",
                vibration_rms=None,
                temperature_celsius=25.0,
                pressure_bar=1.0,
            ),
        ]
        df = _make_df(spark, rows)
        checker = DataQualityChecker(df, "test_table")
        assert (
            checker.check_value_range("vibration_rms", min_val=0.0, max_val=500.0)
            is True
        )


# ---------------------------------------------------------------------------
# run_all_checks + log_summary
# ---------------------------------------------------------------------------


class TestRunAllChecks:
    def test_returns_dict_of_results(self, spark):
        rows = [
            Row(
                device_id="d1",
                entry_id=1,
                recorded_at="2024-01-01T00:00:00Z",
                vibration_rms=5.0,
                temperature_celsius=25.0,
                pressure_bar=1.0,
            ),
        ]
        df = _make_df(spark, rows)
        checker = DataQualityChecker(df, "test_table")
        results = checker.run_all_checks()
        assert isinstance(results, dict)
        assert "min_row_count" in results
        assert "null_rate_device_id" in results
        assert "null_rate_entry_id" in results

    def test_all_pass_on_clean_data(self, spark):
        rows = [
            Row(
                device_id="d1",
                entry_id=1,
                recorded_at="2024-01-01T00:00:00Z",
                vibration_rms=5.0,
                temperature_celsius=25.0,
                pressure_bar=1.0,
            ),
        ]
        df = _make_df(spark, rows)
        checker = DataQualityChecker(df, "test_table")
        results = checker.run_all_checks()
        assert all(results.values()), f"Expected all checks to pass, got: {results}"

    def test_log_summary_does_not_raise(self, spark):
        rows = [
            Row(
                device_id="d1",
                entry_id=1,
                recorded_at="2024-01-01T00:00:00Z",
                vibration_rms=5.0,
                temperature_celsius=25.0,
                pressure_bar=1.0,
            ),
        ]
        df = _make_df(spark, rows)
        checker = DataQualityChecker(df, "test_table")
        checker.run_all_checks()
        checker.log_summary()  # Should not raise
