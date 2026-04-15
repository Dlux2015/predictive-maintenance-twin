"""
Unit tests for etl/silver_transform.py — SilverTransformer pure transformations.

Only the DataFrame-in / DataFrame-out methods are tested here (cast_metrics,
parse_timestamps, handle_nulls, deduplicate, validate). The I/O methods
(read_bronze, merge_upsert) require a live Delta Lake and are excluded.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

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

from etl.silver_transform import NULL_SENTINEL, SilverTransformer


# ---------------------------------------------------------------------------
# Shared schema & helper
# ---------------------------------------------------------------------------


def _bronze_schema() -> StructType:
    """Schema matching the raw Bronze table output."""
    return StructType(
        [
            StructField("device_id", StringType(), nullable=True),
            StructField("entry_id", StringType(), nullable=True),   # Bronze stores as String
            StructField("recorded_at", StringType(), nullable=True),
            StructField("vibration_rms", StringType(), nullable=True),
            StructField("temperature_celsius", StringType(), nullable=True),
            StructField("pressure_bar", StringType(), nullable=True),
            StructField("source_channel", StringType(), nullable=True),
            StructField("ingested_at", StringType(), nullable=True),
            StructField("_ingested_at", TimestampType(), nullable=True),
            StructField("_source_file", StringType(), nullable=True),
            StructField("_batch_id", StringType(), nullable=True),
        ]
    )


def _make_transformer(spark: SparkSession) -> SilverTransformer:
    """Return a SilverTransformer with a real SparkSession."""
    return SilverTransformer(spark=spark)


def _ts(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


# ---------------------------------------------------------------------------
# cast_metrics
# ---------------------------------------------------------------------------


class TestCastMetrics:
    def test_metrics_cast_to_double(self, spark):
        schema = _bronze_schema()
        rows = [
            Row(
                device_id="d1", entry_id="42", recorded_at="2024-01-01T00:00:00Z",
                vibration_rms="3.14", temperature_celsius="22.5", pressure_bar="1.01",
                source_channel="9", ingested_at="2024-01-01T00:00:01Z",
                _ingested_at=None, _source_file="file.json", _batch_id="b1",
            )
        ]
        df = spark.createDataFrame(rows, schema=schema)
        transformer = _make_transformer(spark)
        result = transformer.cast_metrics(df)

        vib_type = dict(result.dtypes)["vibration_rms"]
        temp_type = dict(result.dtypes)["temperature_celsius"]
        pres_type = dict(result.dtypes)["pressure_bar"]
        entry_type = dict(result.dtypes)["entry_id"]

        assert vib_type == "double", f"Expected double, got {vib_type}"
        assert temp_type == "double", f"Expected double, got {temp_type}"
        assert pres_type == "double", f"Expected double, got {pres_type}"
        assert entry_type == "bigint", f"Expected bigint, got {entry_type}"

    def test_numeric_values_preserved_after_cast(self, spark):
        schema = _bronze_schema()
        rows = [
            Row(
                device_id="d1", entry_id="100", recorded_at="t",
                vibration_rms="5.55", temperature_celsius="30.0", pressure_bar="2.22",
                source_channel="9", ingested_at="t",
                _ingested_at=None, _source_file="f", _batch_id="b",
            )
        ]
        df = spark.createDataFrame(rows, schema=schema)
        result = _make_transformer(spark).cast_metrics(df).collect()[0]
        assert abs(result["vibration_rms"] - 5.55) < 1e-6
        assert abs(result["temperature_celsius"] - 30.0) < 1e-6
        assert result["entry_id"] == 100

    def test_null_string_becomes_null_double(self, spark):
        schema = _bronze_schema()
        rows = [
            Row(
                device_id="d1", entry_id="1", recorded_at="t",
                vibration_rms=None, temperature_celsius=None, pressure_bar=None,
                source_channel="9", ingested_at="t",
                _ingested_at=None, _source_file="f", _batch_id="b",
            )
        ]
        df = spark.createDataFrame(rows, schema=schema)
        result = _make_transformer(spark).cast_metrics(df).collect()[0]
        assert result["vibration_rms"] is None
        assert result["temperature_celsius"] is None
        assert result["pressure_bar"] is None


# ---------------------------------------------------------------------------
# parse_timestamps
# ---------------------------------------------------------------------------


class TestParseTimestamps:
    def _typed_schema(self) -> StructType:
        """Schema after cast_metrics — recorded_at is still a string here."""
        return StructType(
            [
                StructField("device_id", StringType(), nullable=True),
                StructField("entry_id", LongType(), nullable=True),
                StructField("recorded_at", StringType(), nullable=True),
                StructField("vibration_rms", DoubleType(), nullable=True),
                StructField("temperature_celsius", DoubleType(), nullable=True),
                StructField("pressure_bar", DoubleType(), nullable=True),
                StructField("_ingested_at", TimestampType(), nullable=True),
            ]
        )

    def test_recorded_at_becomes_timestamp(self, spark):
        schema = self._typed_schema()
        rows = [
            Row(device_id="d1", entry_id=1, recorded_at="2024-01-15T14:30:00Z",
                vibration_rms=1.0, temperature_celsius=20.0, pressure_bar=1.0,
                _ingested_at=None)
        ]
        df = spark.createDataFrame(rows, schema=schema)
        result = _make_transformer(spark).parse_timestamps(df)
        col_type = dict(result.dtypes)["recorded_at"]
        assert col_type == "timestamp", f"Expected timestamp, got {col_type}"

    def test_iso8601_parsed_correctly(self, spark):
        schema = self._typed_schema()
        rows = [
            Row(device_id="d1", entry_id=1, recorded_at="2024-06-01T12:00:00Z",
                vibration_rms=1.0, temperature_celsius=20.0, pressure_bar=1.0,
                _ingested_at=None)
        ]
        df = spark.createDataFrame(rows, schema=schema)
        result = _make_transformer(spark).parse_timestamps(df).collect()[0]
        ts = result["recorded_at"]
        assert ts is not None
        # Year and month should be preserved
        assert ts.year == 2024
        assert ts.month == 6
        assert ts.day == 1


# ---------------------------------------------------------------------------
# handle_nulls
# ---------------------------------------------------------------------------


class TestHandleNulls:
    def _clean_schema(self) -> StructType:
        return StructType(
            [
                StructField("device_id", StringType(), nullable=True),
                StructField("entry_id", LongType(), nullable=True),
                StructField("recorded_at", StringType(), nullable=True),
                StructField("vibration_rms", DoubleType(), nullable=True),
                StructField("temperature_celsius", DoubleType(), nullable=True),
                StructField("pressure_bar", DoubleType(), nullable=True),
                StructField("_ingested_at", TimestampType(), nullable=True),
            ]
        )

    def test_rows_with_null_device_id_dropped(self, spark):
        schema = self._clean_schema()
        rows = [
            Row(device_id=None, entry_id=1, recorded_at="t",
                vibration_rms=1.0, temperature_celsius=20.0, pressure_bar=1.0,
                _ingested_at=None),
            Row(device_id="d1", entry_id=2, recorded_at="t",
                vibration_rms=2.0, temperature_celsius=21.0, pressure_bar=1.1,
                _ingested_at=None),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        result = _make_transformer(spark).handle_nulls(df)
        assert result.count() == 1
        assert result.collect()[0]["device_id"] == "d1"

    def test_null_metrics_filled_with_sentinel(self, spark):
        schema = self._clean_schema()
        rows = [
            Row(device_id="d1", entry_id=1, recorded_at="t",
                vibration_rms=None, temperature_celsius=None, pressure_bar=None,
                _ingested_at=None),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        result = _make_transformer(spark).handle_nulls(df).collect()[0]
        assert result["vibration_rms"] == NULL_SENTINEL
        assert result["temperature_celsius"] == NULL_SENTINEL
        assert result["pressure_bar"] == NULL_SENTINEL

    def test_non_null_metrics_unchanged(self, spark):
        schema = self._clean_schema()
        rows = [
            Row(device_id="d1", entry_id=1, recorded_at="t",
                vibration_rms=5.5, temperature_celsius=25.0, pressure_bar=1.2,
                _ingested_at=None),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        result = _make_transformer(spark).handle_nulls(df).collect()[0]
        assert abs(result["vibration_rms"] - 5.5) < 1e-9
        assert abs(result["temperature_celsius"] - 25.0) < 1e-9
        assert abs(result["pressure_bar"] - 1.2) < 1e-9

    def test_empty_dataframe_after_all_null_device_ids(self, spark):
        schema = self._clean_schema()
        rows = [
            Row(device_id=None, entry_id=i, recorded_at="t",
                vibration_rms=1.0, temperature_celsius=20.0, pressure_bar=1.0,
                _ingested_at=None)
            for i in range(5)
        ]
        df = spark.createDataFrame(rows, schema=schema)
        result = _make_transformer(spark).handle_nulls(df)
        assert result.count() == 0


# ---------------------------------------------------------------------------
# deduplicate
# ---------------------------------------------------------------------------


class TestDeduplicate:
    def _dedup_schema(self) -> StructType:
        return StructType(
            [
                StructField("device_id", StringType(), nullable=False),
                StructField("entry_id", LongType(), nullable=False),
                StructField("recorded_at", StringType(), nullable=True),
                StructField("vibration_rms", DoubleType(), nullable=True),
                StructField("temperature_celsius", DoubleType(), nullable=True),
                StructField("pressure_bar", DoubleType(), nullable=True),
                StructField("_ingested_at", TimestampType(), nullable=True),
            ]
        )

    def test_unique_rows_unchanged(self, spark):
        schema = self._dedup_schema()
        ts1 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2024, 1, 1, 0, 1, 0, tzinfo=timezone.utc)
        rows = [
            Row(device_id="d1", entry_id=1, recorded_at="t",
                vibration_rms=1.0, temperature_celsius=20.0, pressure_bar=1.0,
                _ingested_at=ts1),
            Row(device_id="d1", entry_id=2, recorded_at="t",
                vibration_rms=2.0, temperature_celsius=21.0, pressure_bar=1.1,
                _ingested_at=ts2),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        result = _make_transformer(spark).deduplicate(df)
        assert result.count() == 2

    def test_duplicate_device_entry_keeps_latest_ingested_at(self, spark):
        schema = self._dedup_schema()
        older = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        newer = datetime(2024, 1, 1, 0, 5, 0, tzinfo=timezone.utc)
        rows = [
            Row(device_id="d1", entry_id=42, recorded_at="t",
                vibration_rms=1.0, temperature_celsius=20.0, pressure_bar=1.0,
                _ingested_at=older),
            Row(device_id="d1", entry_id=42, recorded_at="t",
                vibration_rms=9.9, temperature_celsius=99.0, pressure_bar=9.9,
                _ingested_at=newer),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        result = _make_transformer(spark).deduplicate(df)
        assert result.count() == 1
        row = result.collect()[0]
        # The newer _ingested_at row should be kept (vibration_rms=9.9)
        assert abs(row["vibration_rms"] - 9.9) < 1e-9

    def test_dedup_across_multiple_devices(self, spark):
        schema = self._dedup_schema()
        ts = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ts_new = datetime(2024, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
        rows = [
            # device d1: entry 1 duplicated
            Row(device_id="d1", entry_id=1, recorded_at="t", vibration_rms=1.0,
                temperature_celsius=20.0, pressure_bar=1.0, _ingested_at=ts),
            Row(device_id="d1", entry_id=1, recorded_at="t", vibration_rms=7.0,
                temperature_celsius=20.0, pressure_bar=1.0, _ingested_at=ts_new),
            # device d2: unique
            Row(device_id="d2", entry_id=1, recorded_at="t", vibration_rms=2.0,
                temperature_celsius=21.0, pressure_bar=1.1, _ingested_at=ts),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        result = _make_transformer(spark).deduplicate(df)
        assert result.count() == 2
