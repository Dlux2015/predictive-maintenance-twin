# API Ingestion Agent

## Role

Owns the ingestion layer: polling ThingSpeak, transforming raw readings into the canonical SensorReading schema, writing to S3 (partitioned), and optionally streaming to Kinesis.

## Files Owned

| File | Purpose |
|---|---|
| `ingestion/api_producer.py` | Main ingestion producer — ThingSpeak → S3 + Kinesis |
| `ingestion/schema.json` | JSON Schema draft-07 for one raw sensor reading |
| `ingestion/config.example.yaml` | Config template for channels, AWS, retry, logging |

## Key Dependencies

- `requests` — HTTP polling of ThingSpeak API (no auth required)
- `boto3` — S3 PutObject, Kinesis PutRecord, CloudWatch PutMetricData
- `tenacity` — exponential backoff on all network/AWS calls
- `pyyaml` — config file loading

## Architecture Decisions

- **One JSON file per reading** at `raw/year=YYYY/month=MM/day=DD/hour=HH/<uuid>.json`
- **device_id as Kinesis partition key** — preserves per-device ordering on shards
- **CloudWatch metric published per device** — `RecordsIngested` with `DeviceId` dimension
- **Sentinel mapping** — null/empty ThingSpeak fields become `None` in Python (Silver fills with -1.0)

## When to Use This Agent

- Adding a new ThingSpeak channel → add to `config.example.yaml` + test with `--once --dry-run`
- Changing poll interval → update `poll_interval_seconds` in config
- Adding a new raw field (field4, field5, etc.) → update `_parse_response()` + `SensorReading` dataclass + `schema.json`
- Handling ThingSpeak downtime → tenacity already retries; check `retry.max_attempts` in config

## Testing

```bash
# Single poll, no AWS writes
python ingestion/api_producer.py --once --dry-run

# Single poll, real AWS writes (requires env vars)
python ingestion/api_producer.py --once

# Continuous polling
python ingestion/api_producer.py
```

## Code Patterns

All methods use the pattern:
1. `@retry(...)` decorator from tenacity for network resilience
2. `try/except ClientError` with `exc.response["Error"]["Message"]` logging
3. `if dry_run: logger.info("[DRY-RUN] ..."); return` guard at top of write methods
