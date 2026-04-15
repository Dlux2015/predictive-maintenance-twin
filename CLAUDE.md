# CLAUDE.md — Predictive Maintenance Digital Twin

Project context for future Claude Code sessions. Read this before making any changes.

---

## Project Overview

**What:** Manufacturing Predictive Maintenance Digital Twin — a portfolio-quality data engineering project.

**Why:** Demonstrating a production-grade AWS + Databricks data engineering stack. Every technical decision reinforces real-world engineering quality.

**Target audience:** Data engineering hiring teams. They care about: PySpark, Delta Lake, Unity Catalog, Databricks Auto Loader, AWS (S3/Kinesis/CloudWatch), Medallion Architecture, and production code quality.

---

## Stack & Technology

| Layer | Technology | Version |
|---|---|---|
| IoT data source | ThingSpeak public API | free, no key |
| Ingestion runtime | Python | 3.9+ |
| Streaming | AWS Kinesis Data Streams + Firehose | — |
| Raw storage | AWS S3 | — |
| ETL engine | PySpark | 3.5.x |
| Table format | Delta Lake | 3.2.x |
| Catalog | Unity Catalog (Databricks) | — |
| Databricks runtime | DBR 14.3 LTS | Photon enabled |
| Orchestration | Databricks Jobs API v2.1 | — |
| Observability | AWS CloudWatch | — |
| Retry | tenacity | 8.3.x |
| Config | PyYAML | 6.0.x |

---

## Key Configuration Values

| Key | Value |
|---|---|
| AWS Region | `us-east-1` |
| S3 Bucket | `predictive-maintenance-twin-raw` |
| S3 Prefix | `raw` |
| Kinesis Stream | `pmt-sensor-stream` |
| Resource prefix | `pmt-` |
| Databricks Catalog | `workspace` |
| Databricks Schema | `predictive_maintenance` |
| Bronze table | `workspace.predictive_maintenance.bronze_sensors` |
| Silver table | `workspace.predictive_maintenance.silver_sensors` |
| Gold hourly table | `workspace.predictive_maintenance.gold_sensors_hourly` |
| Gold daily table | `workspace.predictive_maintenance.gold_sensors_daily` |
| CloudWatch Namespace | `PredictiveMaintenance` |
| Dedup key | `device_id + entry_id` |
| Null sentinel | `-1.0` (missing sensor reads) |
| Risk threshold | `vibration_zscore > 2.5` |
| VACUUM retention | `168 hours (7 days)` |

---

## Architecture Overview

```
ThingSpeak → api_producer.py → S3 (raw/) + Kinesis
                                    │
                              Auto Loader
                                    │
                              Bronze (append-only)
                                    │
                         cast + parse + MERGE
                                    │
                              Silver (clean)
                                    │
                      hourly/daily aggregations
                         z-score + is_at_risk
                                    │
                     Gold (hourly) + Gold (daily)
                                    │
              ┌─────────────────────┴─────────────────────┐
              │                                           │
      Databricks SQL Dashboard              CloudWatch Metrics + Alarms
```

---

## Code Standards

Enforce these throughout **every file in the project**:

1. **Python 3.9+ type hints** on all functions and classes
2. **Docstrings** on every class and every public method
3. **Secrets via environment variables** — never hardcoded, never in config files committed to git
4. **All boto3 calls wrapped in `try/except ClientError`** with descriptive error messages
5. **Idempotent AWS resource creation** — always check before create; never fail if resource exists
6. **S3 key partitioning**: `raw/year=YYYY/month=MM/day=DD/hour=HH/<uuid>.json`
7. **Logging via `logging` module** at INFO and ERROR levels (never `print()`)
8. **Exponential backoff via tenacity** — max 5 attempts, base 1s, cap 32s
9. **`--dry-run` flag** on all infra scripts

---

## Domain Knowledge

**Sensor fields:**
- `vibration_rms` — root-mean-square vibration in mm/s (field1 from ThingSpeak)
- `temperature_celsius` — temperature in °C (field2)
- `pressure_bar` — pressure in bar (field3)

**Null handling:**
- Raw nulls (missing sensor reads) become `-1.0` sentinel in Silver layer
- Downstream aggregations replace `-1.0` with SQL NULL before computing avg/min/max
- `-1.0` is the deliberate signal for "sensor did not report"

**Predictive maintenance signal:**
- `vibration_zscore`: rolling 24h z-score per device on `avg_vibration_rms`
- `is_at_risk = vibration_zscore > 2.5` — device is vibrating >2.5 std devs above its baseline
- This flag drives the "At-Risk Devices" dashboard widget and is the portfolio's core ML feature

**Device simulation:**
- Three logical devices (device-001, device-002, device-003)
- All polling from ThingSpeak public channels (channels 9 and 276330)
- The same channel can serve as multiple "devices" for demo purposes

---

## File Map

| File | Purpose |
|---|---|
| `ingestion/api_producer.py` | ThingSpeak poller → S3 + Kinesis + CloudWatch |
| `ingestion/schema.json` | JSON Schema draft-07 for one sensor reading |
| `ingestion/config.example.yaml` | Config template (copy to config.yaml, never commit) |
| `infra/s3_setup.py` | S3 bucket creation, versioning, lifecycle, policy |
| `infra/kinesis_stub.py` | Local Kinesis mock — drop-in replacement for boto3 client |
| `infra/kinesis_setup.py` | Real Kinesis stream + Firehose delivery stream |
| `infra/iam_setup.py` | IAM roles: producer, Databricks, Firehose |
| `infra/cloudwatch_alarms.py` | CloudWatch alarms + SNS topic setup |
| `etl/utils.py` | SparkSession factory, DataQualityChecker, Delta helpers |
| `etl/bronze_ingest.py` | Auto Loader → Bronze Delta (append-only streaming write) |
| `etl/silver_transform.py` | Bronze → Silver: cast, parse, fill nulls, dedupe, MERGE |
| `etl/gold_aggregate.py` | Silver → Gold: hourly/daily KPIs, z-score, risk flag, OPTIMIZE |
| `databricks/unity_catalog_setup.py` | Storage credential, external location, catalog, schema, grants |
| `databricks/workflow_config.json` | Jobs API v2.1 payload (4-task DAG, hourly schedule) |
| `databricks/dashboard_query.sql` | 5 Databricks SQL dashboard queries |
| `databricks/deploy.sh` | Idempotent CLI deploy script (Unity Catalog + job + trigger) |
| `observability/cloudwatch_dashboard.json` | 12-widget CloudWatch dashboard JSON |
| `observability/custom_metrics.py` | Publishes Gold KPIs to CloudWatch (runs as Databricks task) |

---

## Common Tasks

### Add a new ThingSpeak channel
1. Add an entry to `ingestion/config.example.yaml` under `thingspeak.channels`
2. Assign a unique `device_id`
3. Restart `api_producer.py` — no code changes needed

### Add a new Gold KPI metric
1. Edit `etl/gold_aggregate.py` → add to the `.agg()` call in `compute_hourly()` and/or `compute_daily()`
2. Add the metric to `observability/custom_metrics.py` → `collect_table_metrics()`
3. Add a CloudWatch widget to `observability/cloudwatch_dashboard.json`
4. Add a dashboard query to `databricks/dashboard_query.sql`

### Add a new CloudWatch alarm
1. Add a new method to `AlarmSetup` in `infra/cloudwatch_alarms.py` (follow the `_put_alarm` pattern)
2. Call it from `setup_all()`
3. Re-run `python infra/cloudwatch_alarms.py` (idempotent)

### Run the full pipeline locally (dry-run)
```bash
python ingestion/api_producer.py --once --dry-run
python infra/s3_setup.py --dry-run
python infra/iam_setup.py --dry-run
python infra/kinesis_setup.py --dry-run
python infra/cloudwatch_alarms.py --dry-run
./databricks/deploy.sh --dry-run
```

---

## Environment Variables Reference

| Variable | Required | Default | Used By |
|---|---|---|---|
| `AWS_REGION` | No | `us-east-1` | All AWS scripts |
| `PMT_S3_BUCKET` | No | `predictive-maintenance-twin-raw` | infra, ingestion, etl |
| `KINESIS_STREAM` | No | `pmt-sensor-stream` | api_producer, kinesis_setup |
| `DATABRICKS_HOST` | Yes (prod) | — | unity_catalog_setup, deploy.sh |
| `DATABRICKS_TOKEN` | Yes (prod) | — | unity_catalog_setup, deploy.sh, custom_metrics |
| `DATABRICKS_CATALOG` | No | `main` | etl scripts |
| `DATABRICKS_SCHEMA` | No | `predictive_maintenance` | etl scripts |
| `AWS_IAM_ROLE_ARN` | Yes (prod) | — | unity_catalog_setup, kinesis_setup |
| `ALERT_EMAIL` | Yes (prod) | — | cloudwatch_alarms, deploy.sh |
| `CONFIG_PATH` | No | `ingestion/config.example.yaml` | api_producer |
| `LOG_LEVEL` | No | `INFO` | All Python scripts |
| `DRY_RUN` | No | `false` | All infra + ingestion scripts |

---

## Known Limitations & Future Work

- **ThingSpeak public channels** are shared; data may not always represent realistic manufacturing sensor readings. For production, replace with a private ThingSpeak account or a real MQTT broker.
- **Kinesis Firehose IAM** requires a real account ID in the trust policy — update `infra/iam_setup.py` with your Databricks account ID for cross-account access.
- **Unity Catalog external location validation** may fail if the IAM role trust relationship doesn't include the Databricks account ID — update the trust policy in `iam_setup.py`.
- **No unit tests yet** — add pytest tests for each ETL transformation function using local Spark sessions.
- **Future: ML model** — the `is_at_risk` flag is the label; a classification model could be trained on Gold features in Databricks ML.
