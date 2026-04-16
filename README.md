# Predictive Maintenance Digital Twin

[![CI](https://github.com/Dlux2015/predictive-maintenance-twin/actions/workflows/ci.yml/badge.svg)](https://github.com/Dlux2015/predictive-maintenance-twin/actions/workflows/ci.yml)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

> **Unplanned downtime costs manufacturers an average of $260,000 per hour.** This project shows how real-time IoT sensor data, cloud streaming, and machine learning signals can identify at-risk equipment *before* it fails — turning reactive maintenance into a predictive, data-driven operation.

---

## What This Project Does

Manufacturing equipment (motors, pumps, compressors) emits continuous sensor signals — vibration, temperature, pressure. When those signals drift outside their normal range, failure is coming. The problem is that most factories either ignore the data or look at it too late.

This pipeline:

1. **Ingests** live sensor readings from IoT devices every 60 seconds
2. **Cleans and stores** the data in a governed Delta Lake (Bronze → Silver → Gold)
3. **Computes a risk score** — a rolling 24-hour vibration z-score per device
4. **Flags at-risk devices** (`is_at_risk = true`) before failure occurs
5. **Alerts operations teams** via a live dashboard and CloudWatch alarms

The result: maintenance teams get notified about the right equipment at the right time, not after a line has gone down.

---

## Technology Stack

| Layer | Technology | Why |
|---|---|---|
| IoT Data Source | ThingSpeak Public API | Real sensor data, no infrastructure required |
| Ingestion | Python + tenacity | Resilient polling with exponential backoff |
| Raw Storage | UC Volumes (partitioned by date/hour) | Databricks-native landing zone, no AWS needed |
| ETL Engine | PySpark 3.5 on Databricks | Distributed processing at any scale |
| Table Format | Delta Lake 3.2 | ACID transactions, time travel, schema enforcement |
| Data Catalog | Unity Catalog (Databricks) | Governance, access control, lineage |
| Orchestration | Databricks Jobs API v2.1 | Hourly scheduled pipeline with dependency DAG |
| Dashboard | Databricks SQL | Live 5-widget pipeline + device health dashboard |
| Runtime | Databricks Runtime 14.3 LTS (Photon) | Production-grade, Photon-accelerated compute |
| Streaming *(optional)* | AWS Kinesis Data Streams + Firehose | Real-time path alongside batch landing |
| Observability *(optional)* | AWS CloudWatch | Pipeline health metrics, alarms, SNS alerting |

---

## Key Capabilities Demonstrated

| Capability | Implementation |
|---|---|
| **Medallion Architecture** | Bronze (raw) → Silver (clean) → Gold (KPIs) with clear layer contracts |
| **Incremental ingestion** | Databricks Auto Loader with checkpoint-based exactly-once delivery |
| **Data quality** | `DataQualityChecker` runs null rate, row count, and range checks at every layer |
| **Deduplication** | MERGE upsert on `(device_id, entry_id)` — duplicates never accumulate |
| **Predictive signal** | Rolling 24h vibration z-score; `is_at_risk` flag when z > 2.5 |
| **Governance** | Unity Catalog Volumes, managed Delta tables, schema-level grants |
| **Resilience** | Exponential backoff (tenacity) on all SDK and HTTP calls |
| **Idempotent infra** | All setup scripts are safe to re-run; check-before-create everywhere |
| **Observability** | Databricks SQL 5-widget dashboard; optional CloudWatch metrics + alarms |
| **Tested** | pytest suite covering ETL transforms, data quality checks, ingestion logic |
| **CI/CD** | GitHub Actions runs tests and linting on every push |
| **Streaming *(optional)*** | Kinesis real-time path runs in parallel with Auto Loader batch ingestion |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES (IoT Layer)                          │
│                                                                          │
│   ThingSpeak Public API (free, no key required)                          │
│   ├── Channel 9      → device-001  (temperature / vibration / pressure)  │
│   ├── Channel 276330 → device-002  (vibration sensor simulation)         │
│   └── Channel 9      → device-003  (multi-device simulation)             │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │ HTTP poll every 60s
                                ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                      INGESTION LAYER (Python)                            │
│                                                                          │
│  api_producer.py  [PMT_BACKEND=databricks]                               │
│  ├── ThingSpeakPoller   — fetches latest feed, maps fields               │
│  ├── DBFSWriter         — writes JSON to UC Volume via Files API  ◄ primary
│  ├── S3Writer           — writes JSON to partitioned S3 key       ◄ optional
│  ├── KinesisProducer    — puts record to Kinesis stream            ◄ optional
│  └── CloudWatchReporter — publishes RecordsIngested metric         ◄ optional
│                                                                          │
│  Retry: tenacity exponential backoff (max 5 attempts, cap 32s)           │
│  Volume path: /Volumes/workspace/predictive_maintenance/raw/             │
│               year=YYYY/month=MM/day=DD/hour=HH/<uuid>.json              │
└──────────┬─────────────────────────────────┬─────────────────────────────┘
           │ Files API (UC Volumes)           │ PutRecord [optional]
           ▼                                  ▼
┌──────────────────────────┐    ┌─────────────────────────────────────────┐
│  UC Volume (raw landing) │    │   AWS Kinesis Data Stream [optional]    │
│  workspace.predictive_   │    │   pmt-sensor-stream                     │
│  maintenance.raw         │    │         │ Firehose delivery              │
│                          │    │         └──→ s3://bucket/firehose/       │
└──────────┬───────────────┘    └─────────────────────────────────────────┘
           │ Auto Loader (cloudFiles)
           ▼
┌──────────────────────────────────────────────────────────────────────────┐
│              DATABRICKS MEDALLION ETL (PySpark + Delta Lake)             │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  BRONZE  (bronze_sensors)                                        │    │
│  │  • Append-only raw JSON → Delta                                  │    │
│  │  • Auto Loader incremental ingestion with checkpoint             │    │
│  │  • Audit cols: _ingested_at, _source_file, _batch_id            │    │
│  └────────────────────────────┬────────────────────────────────────┘    │
│                               │ cast + parse + fill nulls + MERGE        │
│  ┌─────────────────────────────▼──────────────────────────────────┐     │
│  │  SILVER  (silver_sensors)                                        │    │
│  │  • Metrics cast to DoubleType                                    │    │
│  │  • recorded_at → UTC TimestampType                               │    │
│  │  • Null fills: -1.0 sentinel for missing reads                   │    │
│  │  • Dedup on (device_id, entry_id) via MERGE upsert              │    │
│  └────────────────────────────┬────────────────────────────────────┘    │
│                               │ aggregate + z-score + risk flag           │
│  ┌─────────────────────────────▼──────────────────────────────────┐     │
│  │  GOLD   (gold_sensors_hourly + gold_sensors_daily)               │    │
│  │  • Hourly: avg/min/max vibration, temp, pressure per device      │    │
│  │  • Daily: same + rolling 24h z-score on vibration_rms           │    │
│  │  • is_at_risk = vibration_zscore > 2.5  ← core PM signal        │    │
│  │  • OPTIMIZE + ZORDER BY (device_id, date) + VACUUM 7 days       │    │
│  └────────────────────────────┬────────────────────────────────────┘    │
│                               │                                          │
│  Unity Catalog: workspace.predictive_maintenance.*                       │
└──────────────┬────────────────┴──────────────────────────────────────────┘
               │
    ┌──────────┴────────────────┐
    ▼                            ▼
┌────────────────┐   ┌──────────────────────────────────────────────────────┐
│ Databricks SQL │   │   CloudWatch Monitoring  [optional — aws backend]    │
│ Dashboard      │   │                                                        │
│                │   │   Alarms:                                              │
│ • Twin state   │   │   • pmt-ingestion-stale (no records > 5 min)          │
│ • At-risk      │   │   • pmt-pipeline-errors (any error)                   │
│   devices      │   │   • pmt-gold-stale (Gold not updated > 2h)            │
│ • 24h vibr.    │   │                                                        │
│   trend        │   │   Custom Metrics (namespace: PredictiveMaintenance):  │
│ • Pipeline     │   │   • BronzeRowCount, SilverRowCount, GoldRowCount      │
│   health       │   │   • AtRiskDeviceCount, MaxVibrationZScore             │
│ • Daily KPI    │   │   SNS → pmt-pipeline-alerts → email                  │
│   summary      │   │                                                        │
└────────────────┘   └──────────────────────────────────────────────────────┘
```

---

## Data Model

### Bronze — `bronze_sensors`
Raw JSON as received from IoT devices. **Append-only** — the immutable source of truth. Nothing is ever modified or deleted here.

| Column | Type | Description |
|---|---|---|
| `device_id` | STRING | Logical device identifier |
| `entry_id` | BIGINT | Source system sequence number |
| `recorded_at` | STRING | ISO-8601 timestamp from sensor |
| `vibration_rms` | DOUBLE | Root-mean-square vibration (mm/s) |
| `temperature_celsius` | DOUBLE | Temperature reading (°C) |
| `pressure_bar` | DOUBLE | Pressure reading (bar) |
| `_ingested_at` | TIMESTAMP | When this record hit the pipeline |
| `_source_file` | STRING | Source file path (UC Volume or S3) |

### Silver — `silver_sensors`
Clean, typed, deduplicated. Ready for analytics and feature engineering.

**Transformations:** ISO timestamp → UTC `TimestampType` · all metrics → `DoubleType` · null reads → `-1.0` sentinel · rows with null `device_id` dropped · MERGE upsert deduplication on `(device_id, entry_id)`

### Gold — `gold_sensors_hourly` + `gold_sensors_daily`
Pre-aggregated KPIs optimised for dashboard query performance.

| Column | Description |
|---|---|
| `avg/min/max_vibration_rms` | Vibration statistics per device per window |
| `avg/min/max_temperature_celsius` | Temperature statistics |
| `avg/min/max_pressure_bar` | Pressure statistics |
| `reading_count` | Number of sensor readings in the window |
| `vibration_zscore` | Rolling 24h z-score on avg vibration per device |
| `is_at_risk` | `true` when `vibration_zscore > 2.5` — the core maintenance alert |

---

## Project Structure

```
predictive-maintenance-twin/
├── ingestion/
│   ├── api_producer.py         # ThingSpeak poller → S3 + Kinesis
│   ├── schema.json             # JSON Schema for one sensor reading
│   └── config.example.yaml    # Config template (copy to config.yaml, never commit)
├── infra/
│   ├── s3_setup.py             # S3 bucket + versioning + lifecycle
│   ├── kinesis_setup.py        # Kinesis stream + Firehose delivery stream
│   ├── iam_setup.py            # IAM roles: producer, Databricks, Firehose
│   ├── kinesis_stub.py         # Local Kinesis mock for dev/testing
│   └── cloudwatch_alarms.py   # Pipeline health alarms + SNS topic
├── etl/
│   ├── utils.py                # SparkSession factory, DataQualityChecker, helpers
│   ├── bronze_ingest.py        # Auto Loader: S3 JSON → Bronze Delta (append-only)
│   ├── silver_transform.py     # Cast, dedupe, null-fill → Silver (MERGE upsert)
│   └── gold_aggregate.py      # KPI aggregations, z-score, risk flag → Gold
├── databricks/
│   ├── unity_catalog_setup.py  # Catalog, schema, external location, grants
│   ├── workflow_config.json    # Databricks Jobs API v2.1 — 4-task DAG
│   ├── dashboard_query.sql     # 5 Databricks SQL dashboard queries
│   └── deploy.sh               # Idempotent CLI deploy script
├── observability/
│   ├── cloudwatch_dashboard.json  # 12-widget CloudWatch dashboard
│   └── custom_metrics.py          # Publishes Gold KPIs to CloudWatch
├── tests/
│   ├── conftest.py             # Session-scoped SparkSession fixture
│   ├── test_data_quality.py   # DataQualityChecker unit tests
│   ├── test_silver.py         # SilverTransformer transformation tests
│   ├── test_gold.py           # GoldAggregator transformation tests
│   └── test_ingestion.py      # Pure-Python ingestion logic tests
├── requirements.txt            # Pinned Python dependencies
└── pytest.ini                  # Test discovery configuration
```

---

## Quick Start

The primary path runs entirely on Databricks — no AWS account required.

### Prerequisites

| Requirement | Notes |
|---|---|
| Python 3.9+ | 3.11 recommended |
| Databricks Workspace | Free trial or existing workspace with Unity Catalog enabled |
| Databricks CLI | 0.18+ (`pip install databricks-cli`) |

### 1. Clone and install

```bash
git clone https://github.com/Dlux2015/predictive-maintenance-twin.git
cd predictive-maintenance-twin
pip install -r requirements.txt
```

### 2. Configure environment variables

```bash
export DATABRICKS_HOST="https://dbc-xxxxxxxx.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
# Optional overrides (defaults shown)
export DATABRICKS_CATALOG="workspace"
export DATABRICKS_SCHEMA="predictive_maintenance"
```

### 3. Set up Unity Catalog schema

```bash
# Preview changes first
python databricks/unity_catalog_setup.py --dry-run

# Apply (creates catalog, schema — idempotent)
python databricks/unity_catalog_setup.py
```

### 4. Deploy the Databricks workflow

```bash
# Creates the 3-task Bronze → Silver → Gold job and triggers a first run
./databricks/deploy.sh --env prod
```

The script connects the Databricks Repo to this GitHub repository, creates the
`workspace.predictive_maintenance` schema if needed, deploys the job, and
triggers an initial run. Monitor progress in the Databricks Jobs UI.

### 5. Push sensor data (ingestion)

```bash
# Single poll, dry-run — verify ThingSpeak connectivity
python ingestion/api_producer.py --once --dry-run --backend databricks

# Single poll, write one JSON file to the UC Volume
python ingestion/api_producer.py --once --backend databricks

# Continuous production polling (every 60s)
python ingestion/api_producer.py --backend databricks
```

### 6. Run tests

```bash
pytest                               # full suite (requires PySpark)
pytest tests/test_ingestion.py       # pure-Python tests only, no Spark needed
pytest --cov=etl --cov=ingestion tests/
```

### Optional: AWS backend

To use S3 + Kinesis as the ingestion path instead of UC Volumes, set:

```bash
export PMT_BACKEND="aws"
export PMT_S3_BUCKET="predictive-maintenance-twin-raw"
export KINESIS_STREAM="pmt-sensor-stream"
export AWS_REGION="us-east-1"
export AWS_IAM_ROLE_ARN="arn:aws:iam::123456789012:role/pmt-databricks-role"

# Provision AWS resources
python infra/s3_setup.py
python infra/iam_setup.py
python infra/kinesis_setup.py
python infra/cloudwatch_alarms.py

# Then deploy with IAM role for UC external location
python databricks/unity_catalog_setup.py --iam-role-arn $AWS_IAM_ROLE_ARN
```

---

## Observability

### Databricks SQL Dashboard (primary)

`databricks/dashboard_query.sql` contains five queries for a live pipeline + device health dashboard:

| Widget | Query | Purpose |
|---|---|---|
| Current Twin State | Query 1 | Latest reading per device with data freshness |
| At-Risk Devices | Query 2 | Devices with `vibration_zscore > 2.5` in the last 7 days |
| 24h Vibration Trend | Query 3 | Hourly vibration line chart per device |
| Pipeline Health | Query 4 | Row counts + data lag across Bronze / Silver / Gold |
| Daily KPI Summary | Query 5 | 30-day aggregation table with `is_at_risk` flag |

**To create the dashboard in Databricks SQL:**
1. Open **Databricks SQL → Queries → Create query**
2. Paste each query from `dashboard_query.sql` as a separate saved query
3. Open **Dashboards → Create dashboard**, add a widget for each query, and select the appropriate visualisation type (table or line chart as noted in the query comments)

### CloudWatch Monitoring (optional — AWS backend)

When running with `PMT_BACKEND=aws`, the pipeline publishes custom metrics to the
`PredictiveMaintenance` CloudWatch namespace and creates these alarms:

| Alarm | Triggers When | Action |
|---|---|---|
| `pmt-ingestion-stale` | No records ingested in 5 min | SNS email |
| `pmt-pipeline-errors` | Any pipeline error logged | SNS email |
| `pmt-gold-stale` | Gold table not updated in 2h | SNS email |

Import `observability/cloudwatch_dashboard.json` into the CloudWatch console for a 12-widget live view of pipeline health, ingestion rates, and device risk status.

---

## Environment Variables Reference

### Required (Databricks primary backend)

| Variable | Description |
|---|---|
| `DATABRICKS_HOST` | Databricks workspace URL (e.g. `https://dbc-xxx.cloud.databricks.com`) |
| `DATABRICKS_TOKEN` | Databricks PAT or OAuth token |

### Optional / overrides

| Variable | Default | Description |
|---|---|---|
| `PMT_BACKEND` | `databricks` | `databricks` (UC Volumes) or `aws` (S3 + Kinesis) |
| `DATABRICKS_CATALOG` | `workspace` | Unity Catalog catalog name |
| `DATABRICKS_SCHEMA` | `predictive_maintenance` | Unity Catalog schema name |
| `DBFS_RAW_PATH` | `/Volumes/workspace/predictive_maintenance/raw` | UC Volume path for raw JSON |
| `LOG_LEVEL` | `INFO` | Python logging verbosity |

### AWS backend only (`PMT_BACKEND=aws`)

| Variable | Default | Description |
|---|---|---|
| `AWS_REGION` | `us-east-1` | AWS region for all services |
| `PMT_S3_BUCKET` | `predictive-maintenance-twin-raw` | Raw landing zone S3 bucket |
| `KINESIS_STREAM` | `pmt-sensor-stream` | Kinesis stream name |
| `AWS_IAM_ROLE_ARN` | — | IAM role ARN for Databricks → S3 access |
| `ALERT_EMAIL` | — | Email address for SNS alarm notifications |
