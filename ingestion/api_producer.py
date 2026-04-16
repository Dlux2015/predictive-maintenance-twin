"""
ThingSpeak ingestion producer — Databricks-primary, AWS-secondary.

Polls ThingSpeak public IoT channels every 60 seconds, transforms readings
into the canonical SensorReading schema, then writes each record to one of
two backends:

  databricks (default)
      Writes JSON directly to Databricks DBFS via the REST API.
      Requires: DATABRICKS_HOST, DATABRICKS_TOKEN.
      No AWS credentials needed.

  aws (secondary)
      Writes JSON to S3 (partitioned by date/hour) and optionally puts records
      to a Kinesis Data Stream and publishes metrics to CloudWatch.
      Requires: AWS credentials, PMT_S3_BUCKET.

Environment variables
---------------------
PMT_BACKEND         Storage backend: 'databricks' (default) or 'aws'
DATABRICKS_HOST     Databricks workspace URL (required for databricks backend)
DATABRICKS_TOKEN    Databricks personal access token (required for databricks backend)
DBFS_RAW_PATH       DBFS path for raw JSON files (default: /pmt/raw)
AWS_REGION          AWS region (default: us-east-1)  [aws backend only]
PMT_S3_BUCKET       Target S3 bucket  [aws backend only]
S3_PREFIX           S3 key prefix (default: raw)  [aws backend only]
KINESIS_STREAM      Kinesis stream name. Empty string disables Kinesis.  [aws backend only]
CONFIG_PATH         Path to YAML config file (default: ingestion/config.example.yaml)
LOG_LEVEL           Logging verbosity (default: INFO)
DRY_RUN             Set to 'true' to skip all writes (default: false)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

import boto3
import requests
import yaml
from botocore.exceptions import ClientError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class ChannelConfig:
    """Maps a ThingSpeak channel to a logical factory device."""

    channel_id: int
    device_id: str
    name: str


@dataclass
class SensorReading:
    """Canonical sensor reading after ThingSpeak field mapping."""

    device_id: str
    entry_id: int
    recorded_at: str  # ISO-8601 UTC
    vibration_rms: float | None
    temperature_celsius: float | None
    pressure_bar: float | None
    source_channel: int
    ingested_at: str  # ISO-8601 UTC, set at write time

    def to_dict(self) -> dict:
        """Return a JSON-serialisable dictionary of this reading."""
        return asdict(self)

    def to_json(self) -> str:
        """Serialise this reading to a compact JSON string."""
        return json.dumps(self.to_dict(), separators=(",", ":"))


# ---------------------------------------------------------------------------
# ThingSpeak poller
# ---------------------------------------------------------------------------


class ThingSpeakPoller:
    """Fetches sensor readings from ThingSpeak public channels."""

    BASE_URL = "https://api.thingspeak.com"

    def __init__(self, channels: list[ChannelConfig], config: dict) -> None:
        """
        Initialise the poller.

        Parameters
        ----------
        channels:
            List of channel configurations to poll.
        config:
            Full application config dict (loaded from YAML).
        """
        self.channels = channels
        self.config = config
        self.base_url = config.get("thingspeak", {}).get("base_url", self.BASE_URL)
        self.results_per_poll = config.get("thingspeak", {}).get("results_per_poll", 1)

    @retry(
        retry=retry_if_exception_type((requests.RequestException, ValueError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=32),
        reraise=True,
    )
    def fetch_reading(self, channel: ChannelConfig) -> SensorReading | None:
        """
        Fetch the latest reading from a ThingSpeak channel.

        Retries up to 5 times with exponential backoff on network errors.

        Parameters
        ----------
        channel:
            Channel configuration to fetch.

        Returns
        -------
        SensorReading or None if the channel returned no feeds.
        """
        url = (
            f"{self.base_url}/channels/{channel.channel_id}/feeds.json"
            f"?results={self.results_per_poll}"
        )
        logger.info(
            "Fetching channel %d (%s) from %s", channel.channel_id, channel.name, url
        )
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            return self._parse_response(data, channel)
        except requests.HTTPError as exc:
            logger.error("HTTP error fetching channel %d: %s", channel.channel_id, exc)
            raise
        except requests.RequestException as exc:
            logger.error(
                "Network error fetching channel %d: %s", channel.channel_id, exc
            )
            raise

    def _parse_response(
        self, data: dict, channel: ChannelConfig
    ) -> SensorReading | None:
        """
        Parse a raw ThingSpeak API response into a SensorReading.

        ThingSpeak returns ``{"channel": {...}, "feeds": [...]}``. We take
        the first (most recent) feed entry and map its fields.

        Parameters
        ----------
        data:
            Parsed JSON response from ThingSpeak.
        channel:
            Channel configuration for field mapping.

        Returns
        -------
        SensorReading or None if feeds list is empty.
        """
        feeds = data.get("feeds", [])
        if not feeds:
            logger.warning("No feeds returned for channel %d", channel.channel_id)
            return None

        feed = feeds[0]

        def _safe_float(val: str | None) -> float | None:
            """Parse a string to float, returning None on failure."""
            if val is None or str(val).strip() == "":
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        return SensorReading(
            device_id=channel.device_id,
            entry_id=int(feed.get("entry_id", 0)),
            recorded_at=feed.get("created_at", ""),
            vibration_rms=_safe_float(feed.get("field1")),
            temperature_celsius=_safe_float(feed.get("field2")),
            pressure_bar=_safe_float(feed.get("field3")),
            source_channel=channel.channel_id,
            ingested_at=datetime.now(UTC).isoformat(),
        )


# ---------------------------------------------------------------------------
# S3 writer
# ---------------------------------------------------------------------------


class S3Writer:
    """Writes SensorReading records to S3 partitioned by date and hour."""

    def __init__(self, bucket: str, prefix: str, region: str) -> None:
        """
        Initialise the S3 writer.

        Parameters
        ----------
        bucket:
            Target S3 bucket name.
        prefix:
            Key prefix (e.g. 'raw').
        region:
            AWS region.
        """
        self.bucket = bucket
        self.prefix = prefix
        self._client = boto3.client("s3", region_name=region)

    def _build_key(self, reading: SensorReading) -> str:
        """
        Build the partitioned S3 key for a reading.

        Partition scheme:
        ``<prefix>/year=YYYY/month=MM/day=DD/hour=HH/<uuid>.json``
        """
        now = datetime.now(UTC)
        return (
            f"{self.prefix}/"
            f"year={now.year:04d}/"
            f"month={now.month:02d}/"
            f"day={now.day:02d}/"
            f"hour={now.hour:02d}/"
            f"{uuid.uuid4()}.json"
        )

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=32),
        reraise=True,
    )
    def write(self, reading: SensorReading, dry_run: bool = False) -> str:
        """
        Write a single reading to S3 as a JSON object.

        Parameters
        ----------
        reading:
            The sensor reading to persist.
        dry_run:
            If True, log the intended write but skip the actual PutObject call.

        Returns
        -------
        The S3 key that was (or would have been) written.
        """
        key = self._build_key(reading)
        body = reading.to_json().encode("utf-8")

        if dry_run:
            logger.info(
                "[DRY-RUN] Would write s3://%s/%s (%d bytes)",
                self.bucket,
                key,
                len(body),
            )
            return key

        try:
            self._client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
            )
            logger.info("Wrote s3://%s/%s (%d bytes)", self.bucket, key, len(body))
            return key
        except ClientError as exc:
            logger.error(
                "Failed to write s3://%s/%s: %s",
                self.bucket,
                key,
                exc.response["Error"]["Message"],
            )
            raise


# ---------------------------------------------------------------------------
# Kinesis producer
# ---------------------------------------------------------------------------


class KinesisProducer:
    """Puts SensorReading records onto a Kinesis Data Stream."""

    def __init__(self, stream_name: str, region: str) -> None:
        """
        Initialise the Kinesis producer.

        Parameters
        ----------
        stream_name:
            Target Kinesis stream name. Empty string disables Kinesis.
        region:
            AWS region.
        """
        self.stream_name = stream_name
        self._enabled = bool(stream_name)
        if self._enabled:
            self._client = boto3.client("kinesis", region_name=region)

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=32),
        reraise=True,
    )
    def put_record(self, reading: SensorReading, dry_run: bool = False) -> None:
        """
        Put a single reading onto the Kinesis stream.

        Uses device_id as the partition key so all readings from the same
        device land on the same shard (preserving order).

        Parameters
        ----------
        reading:
            The sensor reading to stream.
        dry_run:
            If True, log the intended put but skip the actual API call.
        """
        if not self._enabled:
            logger.debug(
                "Kinesis disabled — skipping put_record for %s", reading.device_id
            )
            return

        if dry_run:
            logger.info(
                "[DRY-RUN] Would put record to Kinesis stream '%s' for device %s",
                self.stream_name,
                reading.device_id,
            )
            return

        try:
            self._client.put_record(
                StreamName=self.stream_name,
                Data=reading.to_json().encode("utf-8"),
                PartitionKey=reading.device_id,
            )
            logger.info(
                "Kinesis put_record: stream=%s device=%s entry_id=%d",
                self.stream_name,
                reading.device_id,
                reading.entry_id,
            )
        except ClientError as exc:
            logger.error(
                "Kinesis put_record failed for device %s: %s",
                reading.device_id,
                exc.response["Error"]["Message"],
            )
            raise


# ---------------------------------------------------------------------------
# DBFS writer (Databricks primary backend)
# ---------------------------------------------------------------------------


class DBFSWriter:
    """Writes SensorReading records to a Unity Catalog Volume via the Files API.

    Uses ``PUT /api/2.0/fs/files{path}`` with raw bytes — works on Unity
    Catalog-enabled workspaces where DBFS root write access is restricted.
    The volume path must start with ``/Volumes/<catalog>/<schema>/<volume>``.
    """

    def __init__(self, volume_path: str, host: str, token: str) -> None:
        """
        Initialise the volume writer.

        Parameters
        ----------
        volume_path:
            UC Volume root path for raw JSON files.
            Must be in the form ``/Volumes/<catalog>/<schema>/<volume>``
            (e.g. ``/Volumes/workspace/predictive_maintenance/raw``).
        host:
            Databricks workspace URL (e.g. https://dbc-xxxx.cloud.databricks.com).
        token:
            Databricks personal access token or OAuth token.
        """
        self.volume_path = volume_path.rstrip("/")
        self._host = host.rstrip("/")
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream",
        }

    def _build_path(self, reading: SensorReading) -> str:
        """
        Build the partitioned volume path for a reading.

        Partition scheme mirrors the S3 layout so Auto Loader config is
        interchangeable between backends:
        ``<volume_path>/year=YYYY/month=MM/day=DD/hour=HH/<uuid>.json``
        """
        now = datetime.now(UTC)
        return (
            f"{self.volume_path}/"
            f"year={now.year:04d}/"
            f"month={now.month:02d}/"
            f"day={now.day:02d}/"
            f"hour={now.hour:02d}/"
            f"{uuid.uuid4()}.json"
        )

    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=32),
        reraise=True,
    )
    def write(self, reading: SensorReading, dry_run: bool = False) -> str:
        """
        Write a single reading to the UC Volume as a JSON file.

        Parameters
        ----------
        reading:
            The sensor reading to persist.
        dry_run:
            If True, log the intended write but skip the actual API call.

        Returns
        -------
        The volume path that was (or would have been) written.
        """
        path = self._build_path(reading)
        body = reading.to_json().encode("utf-8")

        if dry_run:
            logger.info("[DRY-RUN] Would write %s (%d bytes)", path, len(body))
            return path

        try:
            response = requests.put(
                f"{self._host}/api/2.0/fs/files{path}",
                headers=self._headers,
                data=body,
                timeout=30,
            )
            response.raise_for_status()
            logger.info("Wrote %s (%d bytes)", path, len(body))
            return path
        except requests.HTTPError as exc:
            logger.error("Volume write failed for %s: %s", path, exc)
            raise
        except requests.RequestException as exc:
            logger.error("Network error writing to %s: %s", path, exc)
            raise


# ---------------------------------------------------------------------------
# CloudWatch reporter (AWS secondary backend)
# ---------------------------------------------------------------------------


class CloudWatchReporter:
    """Publishes ingestion metrics to CloudWatch."""

    NAMESPACE = "PredictiveMaintenance"

    def __init__(self, region: str) -> None:
        """
        Initialise the CloudWatch reporter.

        Parameters
        ----------
        region:
            AWS region.
        """
        self._client = boto3.client("cloudwatch", region_name=region)

    def record_ingestion(self, count: int, device_id: str) -> None:
        """
        Publish a RecordsIngested metric for a specific device.

        Parameters
        ----------
        count:
            Number of records ingested in this batch.
        device_id:
            The device that generated the readings.
        """
        try:
            self._client.put_metric_data(
                Namespace=self.NAMESPACE,
                MetricData=[
                    {
                        "MetricName": "RecordsIngested",
                        "Dimensions": [{"Name": "DeviceId", "Value": device_id}],
                        "Value": float(count),
                        "Unit": "Count",
                        "Timestamp": datetime.now(UTC),
                    }
                ],
            )
            logger.debug("Published RecordsIngested=%d for device %s", count, device_id)
        except ClientError as exc:
            logger.error(
                "CloudWatch put_metric_data failed for device %s: %s",
                device_id,
                exc.response["Error"]["Message"],
            )


# ---------------------------------------------------------------------------
# Configuration loader
# ---------------------------------------------------------------------------


def load_config(config_path: str) -> dict:
    """
    Load and return the YAML application config.

    Parameters
    ----------
    config_path:
        Path to the YAML config file.

    Returns
    -------
    Parsed config dictionary.
    """
    try:
        with open(config_path, encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}
    except FileNotFoundError:
        logger.warning("Config file not found at %s — using defaults", config_path)
        return {}
    except yaml.YAMLError as exc:
        logger.error("Failed to parse config file %s: %s", config_path, exc)
        return {}


def build_channels(config: dict) -> list[ChannelConfig]:
    """
    Build the channel list from config, falling back to sensible defaults.

    Parameters
    ----------
    config:
        Parsed application config.

    Returns
    -------
    List of ChannelConfig objects.
    """
    raw = config.get("thingspeak", {}).get("channels", [])
    if not raw:
        logger.warning("No channels in config — using built-in defaults")
        return [
            ChannelConfig(
                channel_id=9, device_id="device-001", name="temperature_sensor_01"
            ),
            ChannelConfig(
                channel_id=276330, device_id="device-002", name="vibration_sensor_01"
            ),
            ChannelConfig(
                channel_id=9, device_id="device-003", name="pressure_sensor_01"
            ),
        ]
    return [
        ChannelConfig(
            channel_id=ch["id"],
            device_id=ch["device_id"],
            name=ch.get("name", f"channel_{ch['id']}"),
        )
        for ch in raw
    ]


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Parse arguments and run the ingestion loop."""
    import time

    parser = argparse.ArgumentParser(
        description="ThingSpeak ingestion producer — Databricks-primary, AWS-secondary"
    )
    parser.add_argument(
        "--backend",
        choices=["databricks", "aws"],
        default=os.environ.get("PMT_BACKEND", "databricks"),
        help=(
            "Storage backend: 'databricks' (default, writes to DBFS) "
            "or 'aws' (writes to S3 + Kinesis + CloudWatch)"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=os.environ.get("DRY_RUN", "").lower() == "true",
        help="Log intended writes without executing them",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Poll each channel once then exit (useful for testing)",
    )
    parser.add_argument(
        "--config",
        default=os.environ.get("CONFIG_PATH", "ingestion/config.example.yaml"),
        help="Path to YAML config file",
    )
    args = parser.parse_args()

    if args.dry_run:
        logger.info("=== DRY-RUN MODE — no writes will be performed ===")

    config = load_config(args.config)
    channels = build_channels(config)
    poll_interval = config.get("thingspeak", {}).get("poll_interval_seconds", 60)

    poller = ThingSpeakPoller(channels=channels, config=config)

    # ── Build backend writer ───────────────────────────────────────────────────
    if args.backend == "databricks":
        db_host = os.environ.get("DATABRICKS_HOST", "")
        db_token = os.environ.get("DATABRICKS_TOKEN", "")
        volume_path = os.environ.get(
            "DBFS_RAW_PATH", "/Volumes/workspace/predictive_maintenance/raw"
        )
        if not db_host and not args.dry_run:
            logger.error(
                "DATABRICKS_HOST is not set. Use --dry-run or set the env var."
            )
            sys.exit(1)
        writer: DBFSWriter | S3Writer = DBFSWriter(
            volume_path=volume_path, host=db_host, token=db_token
        )
        logger.info(
            "Backend: Databricks Volumes | host=%s | path=%s",
            db_host or "<dry-run>",
            volume_path,
        )
    else:
        region = os.environ.get("AWS_REGION", "us-east-1")
        bucket = os.environ.get("PMT_S3_BUCKET", "predictive-maintenance-twin-raw")
        s3_prefix = os.environ.get("S3_PREFIX", "raw")
        kinesis_stream = os.environ.get("KINESIS_STREAM", "pmt-sensor-stream")
        writer = S3Writer(bucket=bucket, prefix=s3_prefix, region=region)
        kinesis_producer = KinesisProducer(stream_name=kinesis_stream, region=region)
        cw_reporter = CloudWatchReporter(region=region)
        logger.info(
            "Backend: AWS | bucket=%s | kinesis=%s | region=%s",
            bucket,
            kinesis_stream,
            region,
        )

    logger.info(
        "Starting ingestion producer | channels=%d | poll_interval=%ds | dry_run=%s | backend=%s",
        len(channels),
        poll_interval,
        args.dry_run,
        args.backend,
    )

    while True:
        for channel in channels:
            try:
                reading = poller.fetch_reading(channel)
                if reading is None:
                    continue

                writer.write(reading, dry_run=args.dry_run)

                if args.backend == "aws":
                    kinesis_producer.put_record(reading, dry_run=args.dry_run)  # type: ignore[union-attr]
                    if not args.dry_run:
                        cw_reporter.record_ingestion(count=1, device_id=reading.device_id)  # type: ignore[union-attr]

                logger.info(
                    "Ingested: device=%s entry_id=%d vibration=%.3f temp=%.2f pressure=%.3f",
                    reading.device_id,
                    reading.entry_id,
                    reading.vibration_rms or -1.0,
                    reading.temperature_celsius or -1.0,
                    reading.pressure_bar or -1.0,
                )
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Failed to ingest channel %d (%s): %s",
                    channel.channel_id,
                    channel.name,
                    exc,
                )

        if args.once:
            logger.info("--once flag set — exiting after single poll")
            break

        logger.debug("Sleeping %d seconds until next poll", poll_interval)
        time.sleep(poll_interval)


if __name__ == "__main__":
    main()
