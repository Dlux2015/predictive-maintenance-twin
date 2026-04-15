# Data Engineering Portfolio Project — Bootstrap Skill

## Purpose

Use this skill to scaffold a **new data engineering portfolio project** that follows the same architecture patterns, code standards, and agent structure as the Predictive Maintenance Digital Twin. When invoked, gather the parameters below, then generate the full project directory tree with all files populated.

This skill is intentionally **stack-agnostic within the AWS + Databricks ecosystem** — swap parameters to produce projects targeting IoT pipelines, financial data, logistics, healthcare, or any domain that fits the Medallion Architecture.

---

## Step 0 — Gather Parameters

Before generating any files, ask the user (or extract from their prompt) the following. Everything has a sensible default.

| Parameter | Description | Default |
|---|---|---|
| `PROJECT_NAME` | Kebab-case project name (used for S3 bucket prefix, resource names, directory) | `my-data-twin` |
| `DISPLAY_NAME` | Human-readable title used in README and CLAUDE.md | `My Data Engineering Twin` |
| `DOMAIN` | Business domain (e.g. `manufacturing`, `logistics`, `finance`, `healthcare`) | `manufacturing` |
| `DATA_SOURCE` | Where raw data comes from (e.g. `ThingSpeak API`, `Kafka`, `REST API`, `CSV files`) | `ThingSpeak API` |
| `SENSOR_FIELDS` | List of metric field names and descriptions | `vibration_rms (mm/s), temperature_celsius (°C), pressure_bar (bar)` |
| `DEDUP_KEY` | Natural unique key for MERGE upsert | `device_id + entry_id` |
| `RISK_SIGNAL` | The KPI that drives the predictive alert (name + threshold) | `vibration_zscore > 2.5` |
| `AWS_REGION` | AWS region for all resources | `us-east-1` |
| `RESOURCE_PREFIX` | Short prefix for all AWS/Databricks resource names | `pmt-` |
| `DATABRICKS_CATALOG` | Unity Catalog catalog name | `main` |
| `DATABRICKS_SCHEMA` | Unity Catalog schema name | `predictive_maintenance` |
| `TARGET_AUDIENCE` | Who will review this project (shapes README tone) | `hiring team` |
| `COMPANY_STACK` | Technologies to highlight (shapes README and code comments) | `AWS + Databricks (PySpark, Delta Lake, Unity Catalog)` |

---

## Step 1 — Generate Directory Structure

Create the following directory tree. Replace `{PROJECT_NAME}` with the actual value.

```
{PROJECT_NAME}/
├── CLAUDE.md                          ← project context for future Claude sessions
├── README.md                          ← portfolio-quality README
├── requirements.txt                   ← pinned Python dependencies
├── pytest.ini                         ← test discovery config
│
├── ingestion/
│   ├── __init__.py
│   ├── producer.py                    ← data source poller → S3 + Kinesis
│   ├── schema.json                    ← JSON Schema draft-07 for one raw record
│   └── config.example.yaml            ← config template (never commit real config)
│
├── infra/
│   ├── __init__.py
│   ├── s3_setup.py                    ← S3 bucket + versioning + lifecycle
│   ├── kinesis_setup.py               ← Kinesis stream + Firehose
│   ├── iam_setup.py                   ← IAM roles: producer, Databricks, Firehose
│   └── cloudwatch_alarms.py           ← CloudWatch alarms + SNS
│
├── etl/
│   ├── __init__.py
│   ├── utils.py                       ← SparkSession factory, DQ checker, helpers
│   ├── bronze_ingest.py               ← Auto Loader → Bronze Delta (append-only)
│   ├── silver_transform.py            ← Bronze → Silver: cast, dedupe, MERGE
│   └── gold_aggregate.py              ← Silver → Gold: KPIs, risk flag
│
├── databricks/
│   ├── unity_catalog_setup.py         ← storage credential, external location, grants
│   ├── workflow_config.json           ← Jobs API v2.1 DAG (4-task pipeline)
│   ├── dashboard_query.sql            ← 5 Databricks SQL dashboard queries
│   └── deploy.sh                      ← idempotent CLI deploy script
│
├── observability/
│   ├── cloudwatch_dashboard.json      ← 12-widget CloudWatch dashboard
│   └── custom_metrics.py             ← publishes Gold KPIs to CloudWatch
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py                    ← session-scoped SparkSession fixture
│   ├── test_data_quality.py           ← DataQualityChecker unit tests
│   ├── test_silver.py                 ← SilverTransformer transform tests
│   ├── test_gold.py                   ← GoldAggregator transform tests
│   └── test_ingestion.py              ← pure-Python ingestion tests
│
└── skill/
    ├── SKILL.md                       ← orchestrator skill for this project
    └── agents/
        ├── ingestion-agent.md
        ├── infra-agent.md
        ├── etl-agent.md
        ├── databricks-agent.md
        └── observability-agent.md
```

---

## Step 2 — Generate CLAUDE.md

Populate `CLAUDE.md` using this template. Fill every `{PLACEHOLDER}` with the gathered parameters.

````markdown
# CLAUDE.md — {DISPLAY_NAME}

Project context for future Claude Code sessions. Read this before making any changes.

---

## Project Overview

**What:** {DISPLAY_NAME} — a portfolio-quality data engineering project.

**Why:** Demonstrating the {COMPANY_STACK} stack. Every technical decision should reinforce this positioning.

**Target audience:** {TARGET_AUDIENCE}. They care about: PySpark, Delta Lake, Unity Catalog, Databricks Auto Loader, AWS (S3/Kinesis/CloudWatch), Medallion Architecture, and production code quality.

---

## Stack & Technology

| Layer | Technology |
|---|---|
| Data source | {DATA_SOURCE} |
| Ingestion runtime | Python 3.9+ |
| Streaming | AWS Kinesis Data Streams + Firehose |
| Raw storage | AWS S3 |
| ETL engine | PySpark 3.5.x |
| Table format | Delta Lake 3.2.x |
| Catalog | Unity Catalog (Databricks) |
| Databricks runtime | DBR 14.3 LTS (Photon enabled) |
| Orchestration | Databricks Jobs API v2.1 |
| Observability | AWS CloudWatch |
| Retry | tenacity 8.3.x |
| Config | PyYAML 6.0.x |

---

## Key Configuration Values

| Key | Value |
|---|---|
| AWS Region | `{AWS_REGION}` |
| S3 Bucket | `{RESOURCE_PREFIX}raw` |
| Kinesis Stream | `{RESOURCE_PREFIX}stream` |
| Resource prefix | `{RESOURCE_PREFIX}` |
| Databricks Catalog | `{DATABRICKS_CATALOG}` |
| Databricks Schema | `{DATABRICKS_SCHEMA}` |
| Bronze table | `{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.bronze_raw` |
| Silver table | `{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.silver_clean` |
| Gold hourly table | `{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.gold_hourly` |
| Gold daily table | `{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.gold_daily` |
| CloudWatch Namespace | `{DISPLAY_NAME}` |
| Dedup key | `{DEDUP_KEY}` |
| Null sentinel | `-1.0` (missing sensor reads) |
| Risk threshold | `{RISK_SIGNAL}` |
| VACUUM retention | `168 hours (7 days)` |

---

## Architecture Overview

```
{DATA_SOURCE} → ingestion/producer.py → S3 (raw/) + Kinesis
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
                    ┌─────────────────────────┴──────────────────────┐
                    │                                                 │
            Databricks SQL Dashboard              CloudWatch Metrics + Alarms
```

---

## Code Standards

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

**Sensor/data fields:**
{SENSOR_FIELDS}

**Null handling:**
- Raw nulls become `-1.0` sentinel in Silver layer
- Downstream aggregations replace `-1.0` with SQL NULL before computing avg/min/max
- `-1.0` is the deliberate signal for "sensor did not report"

**Risk signal:**
- `{RISK_SIGNAL}` — drives the dashboard alert widget

---

## Known Limitations & Future Work

- No unit tests for I/O methods (read_bronze, merge_upsert) — requires live Delta Lake
- IAM role trust policy requires a real Databricks account ID for cross-account access
- No ML model yet — the risk flag is the label; a classifier could be trained on Gold features
````

---

## Step 3 — Generate requirements.txt

Use this standard pinned dependency list (identical across projects):

```
# AWS SDK
boto3==1.34.*

# Configuration & HTTP
pyyaml==6.0.*
requests==2.32.*

# Spark / Delta Lake
pyspark==3.5.*
delta-spark==3.2.*

# Databricks
databricks-sdk==0.28.*
databricks-sql-connector==3.3.*

# Resilience
tenacity==8.3.*

# Environment / secrets
python-dotenv==1.0.*

# Testing
pytest==8.2.*
pytest-cov==5.0.*

# Code quality
black==24.4.*
mypy==1.10.*
types-requests==2.32.*
types-PyYAML==6.0.*
```

---

## Step 4 — Generate Core Code Files

For each file, follow the canonical patterns established in the Predictive Maintenance Digital Twin. Adapt field names, table names, and domain language to the new project. The structural patterns must stay identical:

### ingestion/producer.py
- `@dataclass` for the raw record schema (named after the domain, e.g. `LogisticsEvent`, `FinancialTick`)
- `SourcePoller` class: fetch from `{DATA_SOURCE}`, map fields, return typed dataclass
- `S3Writer` class: `_build_key` → `raw/year=/month=/day=/hour=/<uuid>.json`
- `KinesisProducer` class: uses `device_id` (or domain equivalent) as partition key
- `CloudWatchReporter` class: publishes `RecordsIngested` metric
- All network calls decorated with `@retry(stop=stop_after_attempt(5), wait=wait_exponential(...))`
- `--dry-run` and `--once` CLI flags

### etl/utils.py
- `get_spark_session()`: handles Databricks active session + local fallback
- `DataQualityChecker`: `check_null_rate`, `check_row_count`, `check_value_range`, `run_all_checks`, `log_summary`
- `publish_pipeline_metric()`: CloudWatch metric publisher
- `write_delta_table()`, `get_table_row_count()`, `log_table_stats()`

### etl/silver_transform.py
- `SilverTransformer` class with methods: `cast_metrics`, `parse_timestamps`, `handle_nulls`, `deduplicate`, `validate`, `merge_upsert`, `run`
- MERGE key: `{DEDUP_KEY}`
- Null sentinel: `-1.0`

### etl/gold_aggregate.py
- `GoldAggregator` class with methods: `read_silver`, `compute_hourly`, `compute_daily`, `add_zscore`, `add_risk_flag`, `write_gold`, `optimize_tables`, `vacuum_tables`, `run`
- Z-score window: `Window.partitionBy("device_id").orderBy("date").rowsBetween(-1, 0)`
- Risk flag: `{RISK_SIGNAL}`
- VACUUM retention: 168 hours

### infra/s3_setup.py, kinesis_setup.py, iam_setup.py, cloudwatch_alarms.py
- All idempotent (check-before-create)
- All support `--dry-run`
- All wrapped in `try/except ClientError`

---

## Step 5 — Generate Test Files

Copy the test structure from the Predictive Maintenance Digital Twin, adapting:
- Field names to match `{SENSOR_FIELDS}`
- Table names to match the new project's Gold/Silver tables
- Domain-specific sentinel and threshold values

The test structure is canonical and must not change:
- `tests/conftest.py` — session-scoped SparkSession, `local[1]`, no Delta
- `tests/test_data_quality.py` — `DataQualityChecker` full coverage
- `tests/test_silver.py` — `cast_metrics`, `parse_timestamps`, `handle_nulls`, `deduplicate`
- `tests/test_gold.py` — sentinel replacement, `compute_hourly`, `compute_daily`, `add_zscore`, `add_risk_flag`
- `tests/test_ingestion.py` — dataclass serialisation, field parsing, S3 key structure, config loading

---

## Step 6 — Generate skill/SKILL.md and Agent Files

Create the orchestrator skill and one agent file per layer, following the exact pattern from `skill/SKILL.md` and `skill/agents/` in the Predictive Maintenance Digital Twin.

**Agents to generate:**
| Agent file | Responsibility |
|---|---|
| `ingestion-agent.md` | Data source poller, S3 writer, Kinesis producer |
| `infra-agent.md` | S3, Kinesis, IAM, CloudWatch setup |
| `etl-agent.md` | Bronze → Silver → Gold pipeline |
| `databricks-agent.md` | Unity Catalog, workflow, dashboard, deploy |
| `observability-agent.md` | CloudWatch metrics, dashboard, alarms |

Each agent file must include:
1. **Role** — one-sentence description
2. **Files Owned** — table of file → purpose
3. **Key Dependencies** — Python packages used
4. **Architecture Decisions** — 3-5 bullet points on non-obvious choices
5. **When to Use This Agent** — routing guide for the orchestrator
6. **Code Patterns** — the 3 canonical patterns (retry, ClientError, dry-run)

---

## Step 7 — Generate README.md

The README is the portfolio piece. Structure:

```markdown
# {DISPLAY_NAME}

> One-paragraph elevator pitch: what problem this solves, what stack it uses, why it demonstrates production-quality data engineering.

## Architecture

[ASCII diagram of the pipeline]

## Stack

[Same table as CLAUDE.md Stack section]

## Project Structure

[Directory tree with one-line descriptions]

## Quick Start

### Prerequisites
- Python 3.9+
- AWS CLI configured (`aws configure`)
- Databricks CLI installed and configured

### Installation
\`\`\`bash
pip install -r requirements.txt
\`\`\`

### Dry-Run (no AWS required)
\`\`\`bash
python ingestion/producer.py --once --dry-run
python infra/s3_setup.py --dry-run
python infra/kinesis_setup.py --dry-run
\`\`\`

### Deploy to AWS + Databricks
\`\`\`bash
# Set required env vars
export DATABRICKS_HOST=...
export DATABRICKS_TOKEN=...
export AWS_IAM_ROLE_ARN=...
export ALERT_EMAIL=...

./databricks/deploy.sh
\`\`\`

## Data Model

### Bronze (`bronze_raw`)
[Table of columns with types]

### Silver (`silver_clean`)
[Table of columns with types, key transforms]

### Gold (`gold_daily`)
[Table of columns, highlight the risk signal]

## Running Tests

\`\`\`bash
pytest                      # full suite (requires PySpark)
pytest -m "not spark"       # pure-Python tests only
pytest --cov=etl --cov=ingestion tests/
\`\`\`

## Environment Variables

[Table from CLAUDE.md environment variables section]
```

---

## Routing — Which Sub-Agent to Call

After scaffolding, route future work to the correct agent:

| Area | Agent |
|---|---|
| `ingestion/` files | ingestion-agent |
| `infra/` files | infra-agent |
| `etl/` files | etl-agent |
| `databricks/` files | databricks-agent |
| `observability/` files | observability-agent |
| `tests/` files | etl-agent (ETL tests) or ingestion-agent (ingestion tests) |

---

## Quality Checklist

Before declaring the project scaffolded, verify:

- [ ] `CLAUDE.md` has no unfilled `{PLACEHOLDER}` tokens
- [ ] All Python files have module-level docstrings
- [ ] All classes and public methods have docstrings
- [ ] No secrets or credentials in any committed file
- [ ] All infra scripts accept `--dry-run`
- [ ] All boto3 calls are wrapped in `try/except ClientError`
- [ ] `pytest.ini` has `pythonpath = .` so imports resolve without install
- [ ] `tests/conftest.py` uses `pytest.importorskip("pyspark")` so tests are skippable
- [ ] `README.md` has a dry-run quick-start section
- [ ] `databricks/deploy.sh` is marked executable (`chmod +x`)

---

## Reuse This Skill

To bootstrap another new project, invoke this skill again with new parameters. Every parameter in Step 0 can be changed independently — the architecture, code structure, and agent framework remain constant across all projects.
