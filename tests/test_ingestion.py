"""
Unit tests for ingestion/api_producer.py — pure-Python logic only.

Covered:
  - SensorReading.to_dict / to_json
  - ThingSpeakPoller._parse_response (field mapping, null handling, empty feeds)
  - DBFSWriter._build_path (partition scheme, uniqueness)
  - S3Writer._build_key (partition scheme, uniqueness)
  - load_config (YAML parsing, missing file fallback, malformed YAML)
  - build_channels (config channels, default fallback)

No AWS or Databricks API calls are made — network clients are never
instantiated in these tests.
"""

from __future__ import annotations

import json
import os
import tempfile

import pytest

from ingestion.api_producer import (
    ChannelConfig,
    DBFSWriter,
    S3Writer,
    SensorReading,
    ThingSpeakPoller,
    build_channels,
    load_config,
)

# ---------------------------------------------------------------------------
# SensorReading
# ---------------------------------------------------------------------------


class TestSensorReading:
    def _sample(self) -> SensorReading:
        return SensorReading(
            device_id="device-001",
            entry_id=42,
            recorded_at="2024-06-01T12:00:00Z",
            vibration_rms=3.14,
            temperature_celsius=22.5,
            pressure_bar=1.01,
            source_channel=9,
            ingested_at="2024-06-01T12:00:01Z",
        )

    def test_to_dict_has_all_fields(self):
        d = self._sample().to_dict()
        assert d["device_id"] == "device-001"
        assert d["entry_id"] == 42
        assert d["recorded_at"] == "2024-06-01T12:00:00Z"
        assert abs(d["vibration_rms"] - 3.14) < 1e-9
        assert abs(d["temperature_celsius"] - 22.5) < 1e-9
        assert abs(d["pressure_bar"] - 1.01) < 1e-9
        assert d["source_channel"] == 9
        assert d["ingested_at"] == "2024-06-01T12:00:01Z"

    def test_to_json_is_valid_json(self):
        payload = self._sample().to_json()
        parsed = json.loads(payload)
        assert parsed["device_id"] == "device-001"

    def test_to_json_round_trips(self):
        reading = self._sample()
        parsed = json.loads(reading.to_json())
        assert parsed == reading.to_dict()

    def test_to_json_compact_no_spaces(self):
        """JSON must be compact (no spaces after separators) for S3 efficiency."""
        payload = self._sample().to_json()
        assert " " not in payload

    def test_none_fields_serialise_as_null(self):
        reading = SensorReading(
            device_id="d1",
            entry_id=1,
            recorded_at="t",
            vibration_rms=None,
            temperature_celsius=None,
            pressure_bar=None,
            source_channel=9,
            ingested_at="t",
        )
        parsed = json.loads(reading.to_json())
        assert parsed["vibration_rms"] is None
        assert parsed["temperature_celsius"] is None
        assert parsed["pressure_bar"] is None


# ---------------------------------------------------------------------------
# ThingSpeakPoller._parse_response
# ---------------------------------------------------------------------------


class TestThingSpeakParseResponse:
    def _channel(self) -> ChannelConfig:
        return ChannelConfig(channel_id=9, device_id="device-001", name="sensor_01")

    def _poller(self) -> ThingSpeakPoller:
        return ThingSpeakPoller(channels=[self._channel()], config={})

    def _api_response(
        self,
        field1: str | None = "3.14",
        field2: str | None = "22.5",
        field3: str | None = "1.01",
        entry_id: int = 42,
        created_at: str = "2024-06-01T12:00:00Z",
    ) -> dict:
        return {
            "channel": {"id": 9, "name": "Test"},
            "feeds": [
                {
                    "entry_id": entry_id,
                    "created_at": created_at,
                    "field1": field1,
                    "field2": field2,
                    "field3": field3,
                }
            ],
        }

    def test_parses_valid_reading(self):
        poller = self._poller()
        reading = poller._parse_response(self._api_response(), self._channel())
        assert reading is not None
        assert reading.device_id == "device-001"
        assert reading.entry_id == 42
        assert reading.recorded_at == "2024-06-01T12:00:00Z"
        assert abs(reading.vibration_rms - 3.14) < 1e-9
        assert abs(reading.temperature_celsius - 22.5) < 1e-9
        assert abs(reading.pressure_bar - 1.01) < 1e-9
        assert reading.source_channel == 9

    def test_returns_none_on_empty_feeds(self):
        poller = self._poller()
        reading = poller._parse_response({"channel": {}, "feeds": []}, self._channel())
        assert reading is None

    def test_null_field1_becomes_none(self):
        poller = self._poller()
        reading = poller._parse_response(
            self._api_response(field1=None), self._channel()
        )
        assert reading is not None
        assert reading.vibration_rms is None

    def test_empty_string_field_becomes_none(self):
        poller = self._poller()
        reading = poller._parse_response(self._api_response(field2=""), self._channel())
        assert reading is not None
        assert reading.temperature_celsius is None

    def test_non_numeric_field_becomes_none(self):
        poller = self._poller()
        reading = poller._parse_response(
            self._api_response(field3="N/A"), self._channel()
        )
        assert reading is not None
        assert reading.pressure_bar is None

    def test_device_id_from_channel_config(self):
        channel = ChannelConfig(
            channel_id=276330, device_id="device-002", name="vib_02"
        )
        poller = ThingSpeakPoller(channels=[channel], config={})
        reading = poller._parse_response(self._api_response(), channel)
        assert reading.device_id == "device-002"
        assert reading.source_channel == 276330

    def test_entry_id_cast_to_int(self):
        poller = self._poller()
        reading = poller._parse_response(
            self._api_response(entry_id=99), self._channel()
        )
        assert reading.entry_id == 99
        assert isinstance(reading.entry_id, int)

    def test_ingested_at_is_set(self):
        poller = self._poller()
        reading = poller._parse_response(self._api_response(), self._channel())
        assert reading is not None
        assert reading.ingested_at != ""


# ---------------------------------------------------------------------------
# DBFSWriter._build_path
# ---------------------------------------------------------------------------


class TestDBFSWriter:
    def _writer(self) -> DBFSWriter:
        return DBFSWriter(
            volume_path="/Volumes/workspace/predictive_maintenance/raw",
            host="https://dbc-test.cloud.databricks.com",
            token="dapi-test-token",
        )

    def _reading(self) -> SensorReading:
        return SensorReading(
            device_id="device-001",
            entry_id=1,
            recorded_at="2024-06-01T12:00:00Z",
            vibration_rms=3.14,
            temperature_celsius=22.5,
            pressure_bar=1.01,
            source_channel=9,
            ingested_at="2024-06-01T12:00:01Z",
        )

    def test_path_starts_with_volume_path(self):
        writer = self._writer()
        path = writer._build_path(self._reading())
        assert path.startswith("/Volumes/workspace/predictive_maintenance/raw/")

    def test_path_has_partitioned_structure(self):
        writer = self._writer()
        path = writer._build_path(self._reading())
        assert "year=" in path
        assert "month=" in path
        assert "day=" in path
        assert "hour=" in path

    def test_path_ends_with_json(self):
        writer = self._writer()
        path = writer._build_path(self._reading())
        assert path.endswith(".json")

    def test_paths_are_unique(self):
        """Each call must produce a different UUID-based path."""
        writer = self._writer()
        reading = self._reading()
        paths = {writer._build_path(reading) for _ in range(10)}
        assert len(paths) == 10, "Expected unique paths for each call"

    def test_trailing_slash_stripped_from_volume_path(self):
        """Constructor should strip trailing slash so paths don't double-slash."""
        writer = DBFSWriter(
            volume_path="/Volumes/workspace/predictive_maintenance/raw/",
            host="https://dbc-test.cloud.databricks.com",
            token="token",
        )
        path = writer._build_path(self._reading())
        assert "raw//year=" not in path
        assert path.startswith("/Volumes/workspace/predictive_maintenance/raw/year=")

    def test_dry_run_does_not_make_http_call(self):
        """write() in dry-run mode must return a path without calling requests."""
        import unittest.mock as mock

        writer = self._writer()
        with mock.patch("requests.post") as mock_post:
            result = writer.write(self._reading(), dry_run=True)
            mock_post.assert_not_called()
        assert result.endswith(".json")


# ---------------------------------------------------------------------------
# S3Writer._build_key
# ---------------------------------------------------------------------------


class TestS3BuildKey:
    def _writer(self) -> S3Writer:
        # Passing a nonsense region — _build_key doesn't call boto3
        import unittest.mock as mock

        with mock.patch("boto3.client"):
            return S3Writer(bucket="test-bucket", prefix="raw", region="us-east-1")

    def test_key_starts_with_prefix(self):
        writer = self._writer()
        reading = SensorReading(
            device_id="d1",
            entry_id=1,
            recorded_at="t",
            vibration_rms=1.0,
            temperature_celsius=20.0,
            pressure_bar=1.0,
            source_channel=9,
            ingested_at="t",
        )
        key = writer._build_key(reading)
        assert key.startswith("raw/")

    def test_key_has_partitioned_structure(self):
        writer = self._writer()
        reading = SensorReading(
            device_id="d1",
            entry_id=1,
            recorded_at="t",
            vibration_rms=1.0,
            temperature_celsius=20.0,
            pressure_bar=1.0,
            source_channel=9,
            ingested_at="t",
        )
        key = writer._build_key(reading)
        assert "year=" in key
        assert "month=" in key
        assert "day=" in key
        assert "hour=" in key

    def test_key_ends_with_json(self):
        writer = self._writer()
        reading = SensorReading(
            device_id="d1",
            entry_id=1,
            recorded_at="t",
            vibration_rms=1.0,
            temperature_celsius=20.0,
            pressure_bar=1.0,
            source_channel=9,
            ingested_at="t",
        )
        key = writer._build_key(reading)
        assert key.endswith(".json")

    def test_keys_are_unique(self):
        """Each call must produce a different UUID-based key."""
        writer = self._writer()
        reading = SensorReading(
            device_id="d1",
            entry_id=1,
            recorded_at="t",
            vibration_rms=1.0,
            temperature_celsius=20.0,
            pressure_bar=1.0,
            source_channel=9,
            ingested_at="t",
        )
        keys = {writer._build_key(reading) for _ in range(10)}
        assert len(keys) == 10, "Expected unique keys for each call"


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------


class TestLoadConfig:
    def test_loads_valid_yaml(self):
        yaml_content = """
thingspeak:
  poll_interval_seconds: 30
  results_per_poll: 1
  channels:
    - id: 9
      device_id: device-001
      name: sensor_01
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, encoding="utf-8"
        ) as f:
            f.write(yaml_content)
            path = f.name

        try:
            config = load_config(path)
            assert config["thingspeak"]["poll_interval_seconds"] == 30
            assert len(config["thingspeak"]["channels"]) == 1
        finally:
            os.unlink(path)

    def test_returns_empty_dict_on_missing_file(self):
        config = load_config("/nonexistent/path/config.yaml")
        assert config == {}

    def test_returns_empty_dict_on_malformed_yaml(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, encoding="utf-8"
        ) as f:
            f.write("key: [unclosed bracket\n")
            path = f.name
        try:
            config = load_config(path)
            assert config == {}
        finally:
            os.unlink(path)

    def test_returns_empty_dict_on_empty_file(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, encoding="utf-8"
        ) as f:
            f.write("")
            path = f.name
        try:
            config = load_config(path)
            assert config == {}
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# build_channels
# ---------------------------------------------------------------------------


class TestBuildChannels:
    def test_builds_from_config(self):
        config = {
            "thingspeak": {
                "channels": [
                    {"id": 9, "device_id": "device-001", "name": "sensor_01"},
                    {"id": 276330, "device_id": "device-002", "name": "sensor_02"},
                ]
            }
        }
        channels = build_channels(config)
        assert len(channels) == 2
        assert channels[0].channel_id == 9
        assert channels[0].device_id == "device-001"
        assert channels[1].channel_id == 276330
        assert channels[1].device_id == "device-002"

    def test_falls_back_to_defaults_when_no_channels(self):
        channels = build_channels({})
        assert len(channels) == 3  # Built-in defaults: device-001, -002, -003
        device_ids = {c.device_id for c in channels}
        assert "device-001" in device_ids
        assert "device-002" in device_ids
        assert "device-003" in device_ids

    def test_falls_back_to_defaults_on_empty_channels_list(self):
        config = {"thingspeak": {"channels": []}}
        channels = build_channels(config)
        assert len(channels) == 3

    def test_name_defaults_to_channel_id_when_missing(self):
        config = {"thingspeak": {"channels": [{"id": 123, "device_id": "dev-x"}]}}
        channels = build_channels(config)
        assert channels[0].name == "channel_123"
