# Predictive Maintenance Digital Twin

End-to-end manufacturing IoT pipeline demonstrating AWS + Databricks data engineering — ingestion, Medallion ETL, Delta Lake, Unity Catalog, and CloudWatch observability — built as a portfolio project targeting **Wavicle Data Solutions**.

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
│  api_producer.py                                                         │
│  ├── ThingSpeakPoller   — fetches latest feed, maps fields               │
│  ├── S3Writer           — writes JSON to partitioned S3 key              │
│  ├── KinesisProducer    — puts record to Kinesis stream                  │
│  └── CloudWatchReporter — publishes RecordsIngested metric               │
│                                                                          │
│  Retry: tenacity exponential backoff (max 5 attempts, cap 32s)           │
│  S3 key: raw/year=YYYY/month=MM/day=DD/hour=HH/<uuid>.json               │
└──────────┬─────────────────────────────────┬─────────────────────────────┘
           │ PutObject                        │ PutRecord
           ▼                                  ▼
┌──────────────────────────┐    ┌─────────────────────────────────────────┐
│   AWS S3 (raw landing)   │    │   Kinesis Data Stream                   │
│                          │    │   pmt-sensor-stream (1 shard)            │
│  predictive-maintenance- │    │              │                           │
│  twin-raw                │    │              │ Firehose delivery          │
│  ├── raw/          ←─────│────│──────────────┘                           │
│  ├── checkpoints/        │    │   → s3://bucket/firehose/ (partitioned)  │
│  ├── bronze/             │    └─────────────────────────────────────────┘
│  ├── silver/             │
│  └── gold/               │
└──────────┬───────────────┘
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
│  │  • Partitioned by date(recorded_at)                              │    │
│  └────────────────────────────┬────────────────────────────────────┘    │
│                               │ cast + parse + fill nulls + MERGE        │
│  ┌─────────────────────────────▼──────────────────────────────────┐     │
│  │  SILVER  (silver_sensors)                                        │    │
│  │  • DoubleType casts for all metrics                              │    │
│  │  • recorded_at → UTC TimestampType                               │    │
│  │  • Null fills: -1.0 sentinel for missing reads                   │    │
│  │  • Dedup on (device_id, entry_id) — keep latest ingested_at     │    │
│  │  • MERGE upsert (no duplicates ever accumulate)                  │    │
│  └────────────────────────────┬────────────────────────────────────┘    │
│                               │ aggregate + z-score + risk flag           │
│  ┌─────────────────────────────▼──────────────────────────────────┐     │
│  │  GOLD   (gold_sensors_hourly + gold_sensors_daily)               │    │
│  │  • Hourly: avg/min/max vibration, temp, pressure per device      │    │
│  │  • Daily: same + rolling 24h z-score on vibration_rms           │    │
│  │  • is_at_risk = vibration_zscore > 2.5  ← core PM signal        │    │
│  │  • OPTIMIZE + ZORDER BY (device_id, date)                       │    │
│  │  • VACUUM RETAIN 168 HOURS (7 days)                              │    │
│  └────────────────────────────┬────────────────────────────────────┘    │
│                               │                                          │
│  Unity Catalog: main.predictive_maintenance.*                            │
└──────────────┬────────────────┴──────────────────────────────────────────┘
               │
    ┌──────────┴──────────┐
    ▼                      ▼
┌────────────────┐   ┌──────────────────────────────────────────────────────┐
│ Databricks SQL │   │   CloudWatch Monitoring                               │
│ Dashboard      │   │                                                        │
│                │   │   Alarms:                                              │
│ Q1: Twin state │   │   • pmt-ingestion-stale (no records > 5 min)          │
│ Q2: At-risk    │   │   • pmt-pipeline-errors (any error)                   │
│    devices     │   │   • pmt-kinesis-lag (iterator age > 60s)              │
│ Q3: 24h vibr.  │   │   • pmt-s3-errors (5xx on raw bucket)                │
│    trend       │   │   • pmt-gold-stale (Gold not updated > 2h)            │
│ Q4: Pipeline   │   │                                                        │
│    health      │   │   Custom Metrics (namespace: PredictiveMaintenance):  │
│ Q5: Daily KPI  │   │   • RecordsIngested, PipelineErrors                   │
│    summary     │   │   • BronzeRowCount, SilverRowCount, GoldRowCount      │
└────────────────┘   │   • AtRiskDeviceCount, MaxVibrationZScore             │
                     │   • Bronze/Silver/GoldWriteLatencyMs                  │
                     │   • GoldTableLastUpdate                                │
                     │                                                        │
                     │   SNS → pmt-pipeline-alerts → email (ALERT_EMAIL)     │
                     └──────────────────────────────────────────────────────┘
```

---

## Table of Contents

1. [Features](#features)
2. [Prerequisites](#prerequisites)
3. [Quick Start](#quick-start)
4. [Configuration](#configuration)
5. [Project Structure](#project-structure)
6. [Data Flow](#data-flow)
7. [KPI Dashboard](#kpi-dashboard)
8. [Observability](#observability)
9. [Running Tests](#running-tests)
10. [Portfolio Context](#portfolio-context)

---

## Features

- **Real IoT data ingestion** from ThingSpeak public channels (no API key required)
- **Medallion Architecture** — Bronze (raw) → Silver (clean) → Gold (KPIs) on Delta Lake
- **Auto Loader** incremental ingestion with checkpoint-based exactly-once delivery
- **MERGE upsert** deduplication in Silver layer using Delta Lake DML
- **Predictive maintenance signal**: rolling 24h vibration z-score with `is_at_risk` flag
- **Unity Catalog** governance — catalog, schema, external location, row-level grants
- **Kinesis Data Streams + Firehose** real-time streaming path alongside S3 batch
- **CloudWatch alarms** for pipeline freshness, lag, and error detection
- **Custom CloudWatch metrics** published from Databricks tasks
- **Databricks Jobs API v2.1** workflow with 4-task DAG and email alerting
- **Idempotent infrastructure** — all AWS and Databricks setup scripts are re-runnable
- **Exponential backoff** (tenacity) on all external API and AWS SDK calls
- **Full type hints** and docstrings throughout

---

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Python | 3.9+ | 3.11 recommended |
| AWS Account | — | IAM permissions for S3, Kinesis, CloudWatch, IAM |
| Databricks Workspace | — | Unity Catalog enabled |
| Databricks CLI | 0.18+ | `pip install databricks-cli` |
| AWS CLI | 2.x | Optional but recommended |

---

## Quick Start

### 1. Clone and install

```bash
git clone <your-repo-url>
cd predictive-maintenance-twin
pip install -r requirements.txt
```

### 2. Configure environment variables

```bash
export AWS_REGION="us-east-1"
export PMT_S3_BUCKET="predictive-maintenance-twin-raw"
export KINESIS_STREAM="pmt-sensor-stream"
export DATABRICKS_HOST="https://dbc-xxxxxxxx.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
export ALERT_EMAIL="you@example.com"
export AWS_IAM_ROLE_ARN="arn:aws:iam::123456789012:role/pmt-databricks-role"
```

### 3. Set up AWS infrastructure

```bash
# Create S3 bucket (idempotent)
python infra/s3_setup.py --dry-run   # preview
python infra/s3_setup.py

# Create IAM roles
python infra/iam_setup.py --dry-run
python infra/iam_setup.py --output-arns

# Create Kinesis stream
python infra/kinesis_setup.py --dry-run
python infra/kinesis_setup.py --iam-role-arn $AWS_IAM_ROLE_ARN

# Create CloudWatch alarms
python infra/cloudwatch_alarms.py --dry-run
python infra/cloudwatch_alarms.py
```

### 4. Set up Databricks Unity Catalog

```bash
python databricks/unity_catalog_setup.py --dry-run
python databricks/unity_catalog_setup.py --iam-role-arn $AWS_IAM_ROLE_ARN
```

### 5. Start the ingestion producer

```bash
# Test with a single poll (--once) and dry-run
python ingestion/api_producer.py --once --dry-run

# Run continuously (polls every 60s, writes to S3 + Kinesis)
python ingestion/api_producer.py
```

### 6. Run the ETL pipeline manually

```bash
# In Databricks notebook or cluster
python etl/bronze_ingest.py --trigger-once
python etl/silver_transform.py
python etl/gold_aggregate.py
```

### 7. Deploy the Databricks workflow

```bash
# Preview deployment
./databricks/deploy.sh --dry-run

# Deploy to production
./databricks/deploy.sh --env prod
```

### 8. Open the Databricks SQL Dashboard

Import `databricks/dashboard_query.sql` into a new Databricks SQL Dashboard with 5 widgets. The job runs hourly and refreshes all Gold tables automatically.

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `AWS_REGION` | `us-east-1` | AWS region for all services |
| `PMT_S3_BUCKET` | `predictive-maintenance-twin-raw` | Raw landing zone S3 bucket |
| `S3_PREFIX` | `raw` | S3 key prefix for raw files |
| `KINESIS_STREAM` | `pmt-sensor-stream` | Kinesis stream name (empty = disable) |
| `DATABRICKS_HOST` | — | Databricks workspace URL |
| `DATABRICKS_TOKEN` | — | Databricks PAT or OAuth token |
| `DATABRICKS_CATALOG` | `main` | Unity Catalog catalog |
| `DATABRICKS_SCHEMA` | `predictive_maintenance` | Unity Catalog schema |
| `AWS_IAM_ROLE_ARN` | — | IAM role ARN for Databricks → S3 |
| `ALERT_EMAIL` | — | Email for SNS alarm notifications |
| `CONFIG_PATH` | `ingestion/config.example.yaml` | Ingestion config file path |
| `LOG_LEVEL` | `INFO` | Python logging level |
| `DRY_RUN` | `false` | Skip all writes when `true` |

### Config File

Copy `ingestion/config.example.yaml` to `ingestion/config.yaml` and update the channel list and AWS settings. The file has detailed inline comments for every option.

---

## Project Structure

```
predictive-maintenance-twin/
├── requirements.txt                  # Pinned Python dependencies
├── ingestion/
│   ├── api_producer.py               # ThingSpeak poller → S3 + Kinesis
│   ├── schema.json                   # JSON Schema draft-07 for sensor readings
│   └── config.example.yaml          # Config template (copy to config.yaml)
├── infra/
│   ├── s3_setup.py                   # S3 bucket + lifecycle + folder structure
│   ├── kinesis_stub.py               # Local Kinesis mock for dev/testing
│   ├── kinesis_setup.py              # Kinesis stream + Firehose setup
│   ├── iam_setup.py                  # IAM roles for producer + Databricks + Firehose
│   └── cloudwatch_alarms.py         # Pipeline health alarms + SNS topic
├── etl/
│   ├── utils.py                      # Spark helpers, DQ checks, metric publisher
│   ├── bronze_ingest.py              # Auto Loader: S3 JSON → Bronze Delta (append)
│   ├── silver_transform.py           # Clean/dedupe/type-cast → Silver (MERGE)
│   └── gold_aggregate.py            # KPI aggregations → Gold + OPTIMIZE/ZORDER
├── databricks/
│   ├── unity_catalog_setup.py        # Catalog, schema, external location, grants
│   ├── workflow_config.json          # Databricks Jobs API v2.1 workflow payload
│   ├── dashboard_query.sql           # 5 Databricks SQL dashboard queries
│   └── deploy.sh                     # Idempotent CLI deploy script
├── observability/
│   ├── cloudwatch_dashboard.json     # CloudWatch dashboard with 12 widgets
│   ├── cloudwatch_alarms.py         # (see infra/cloudwatch_alarms.py)
│   └── custom_metrics.py            # Publishes pipeline KPIs to CloudWatch
└── skill/
    ├── SKILL.md                      # Orchestrator skill definition
    └── agents/
        ├── api-ingestion-agent.md
        ├── aws-infra-agent.md
        ├── pyspark-etl-agent.md
        └── databricks-agent.md
```

---

## Data Flow

### Bronze Layer (`bronze_sensors`)
Raw JSON exactly as received from ThingSpeak, enriched with audit columns. **Append-only** — no data is ever modified or deleted at this layer. Serves as the immutable source of truth.

| Column | Type | Source |
|---|---|---|
| device_id | STRING | Channel config mapping |
| entry_id | BIGINT | ThingSpeak entry_id |
| recorded_at | STRING | ThingSpeak created_at (raw string) |
| vibration_rms | DOUBLE | field1 |
| temperature_celsius | DOUBLE | field2 |
| pressure_bar | DOUBLE | field3 |
| source_channel | INT | ThingSpeak channel ID |
| _ingested_at | TIMESTAMP | ETL processing time |
| _source_file | STRING | S3 file path |

### Silver Layer (`silver_sensors`)
Clean, typed, deduplicated data. Ready for analytics and ML features.

**Transformations applied:**
- `recorded_at` string → UTC `TimestampType`
- All metrics cast to `DoubleType`
- Null metric values filled with `-1.0` sentinel
- Rows with null `device_id` dropped
- Deduplication on `(device_id, entry_id)` via MERGE upsert

### Gold Layer (`gold_sensors_hourly`, `gold_sensors_daily`)
Pre-aggregated KPIs. Optimised for dashboard query performance.

**Predictive maintenance features:**
- `avg/min/max vibration_rms` per device per hour/day
- `vibration_zscore`: rolling 24h z-score per device
- `is_at_risk`: `vibration_zscore > 2.5` — the core maintenance signal

---

## KPI Dashboard

Five Databricks SQL queries power the dashboard:

| Widget | Query | Purpose |
|---|---|---|
| 1 | Current Twin State | Latest reading per device with data freshness indicator |
| 2 | At-Risk Devices | Devices with `vibration_zscore > 2.5` in last 7 days |
| 3 | 24h Vibration Trend | Hourly vibration time series per device |
| 4 | Pipeline Health | Row counts Bronze → Silver → Gold with data lag |
| 5 | Daily Summary | 30-day daily KPI table with all metrics and risk flags |

---

## Observability

### CloudWatch Alarms

| Alarm | Metric | Threshold | Action |
|---|---|---|---|
| `pmt-ingestion-stale` | RecordsIngested sum | < 1 in 5 min | SNS email |
| `pmt-pipeline-errors` | PipelineErrors sum | ≥ 1 in 5 min | SNS email |
| `pmt-kinesis-lag` | IteratorAgeMilliseconds | > 60,000 ms | SNS email |
| `pmt-s3-errors` | S3 5xxErrors | ≥ 1 in 5 min | SNS email |
| `pmt-gold-stale` | GoldTableLastUpdate | > 2 hours old | SNS email |

### CloudWatch Dashboard
Import `observability/cloudwatch_dashboard.json` via the CloudWatch console (Dashboards → Create dashboard → Source JSON). Provides 12 widgets across 4 rows.

---

## Running Tests

```bash
# Run all tests
pytest

# With coverage report
pytest --cov=. --cov-report=html

# Test the ingestion producer locally (no AWS writes)
python ingestion/api_producer.py --once --dry-run

# Test the Kinesis stub
python infra/kinesis_stub.py

# Test infra scripts in dry-run mode
python infra/s3_setup.py --dry-run
python infra/iam_setup.py --dry-run
python infra/kinesis_setup.py --dry-run
python infra/cloudwatch_alarms.py --dry-run
```

---

## Portfolio Context

This project demonstrates the **Wavicle Data Solutions core stack** end-to-end:

| Skill | Demonstrated By |
|---|---|
| **PySpark** | Medallion ETL with window functions, MERGE, OPTIMIZE |
| **Delta Lake** | Bronze/Silver/Gold tables, time travel, VACUUM |
| **Unity Catalog** | External location, storage credential, schema grants |
| **Databricks Auto Loader** | Incremental S3 ingestion with schema enforcement |
| **Databricks Jobs API** | Multi-task workflow with dependency DAG |
| **AWS S3** | Partitioned raw landing zone, lifecycle policies |
| **AWS Kinesis** | Real-time streaming path + Firehose → S3 |
| **AWS IAM** | Least-privilege roles for producer, Databricks, Firehose |
| **AWS CloudWatch** | Custom metrics, alarms, dashboard, SNS alerting |
| **Medallion Architecture** | Bronze → Silver → Gold with clear layer contracts |
| **Predictive Maintenance** | Vibration z-score, rolling statistics, risk flagging |
| **Production Code Quality** | Type hints, docstrings, retry, idempotency, DQ checks |
