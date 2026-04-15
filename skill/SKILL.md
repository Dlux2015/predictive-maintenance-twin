# Predictive Maintenance Digital Twin — Orchestrator Skill

## Purpose

This skill orchestrates the full Predictive Maintenance Digital Twin pipeline across five specialised sub-agents. Use it to build, extend, debug, or explain any part of the project.

## Available Sub-Agents

| Agent | File | Responsibility |
|---|---|---|
| **api-ingestion-agent** | `skill/agents/api-ingestion-agent.md` | ThingSpeak poller, S3 writer, Kinesis producer |
| **aws-infra-agent** | `skill/agents/aws-infra-agent.md` | S3, Kinesis, IAM, CloudWatch alarm setup |
| **pyspark-etl-agent** | `skill/agents/pyspark-etl-agent.md` | Bronze → Silver → Gold ETL pipeline |
| **databricks-agent** | `skill/agents/databricks-agent.md` | Unity Catalog, workflow, dashboard, deploy |
| **observability-agent** | `skill/agents/observability-agent.md` | CloudWatch metrics, dashboard, alarms |

## Invocation

When asked to work on this project, select the appropriate sub-agent(s) based on which files are involved. Multiple agents can work in parallel on non-overlapping files.

**Routing guide:**
- `ingestion/` files → api-ingestion-agent
- `infra/` files → aws-infra-agent
- `etl/` files → pyspark-etl-agent
- `databricks/` files → databricks-agent
- `observability/` files → observability-agent
- `tests/` files → pyspark-etl-agent (ETL tests) or api-ingestion-agent (ingestion tests)

## Bootstrapping a New Project

To spin up a **new** data engineering project using the same architecture, use the project bootstrap skill:

```
skill/project-bootstrap.md
```

That skill walks through parameter collection (project name, domain, data source, fields, risk signal) and then generates the full project tree — including CLAUDE.md, all code files, tests, infra scripts, and the agent skill framework — ready to use immediately.

**Quick bootstrap:**
1. Open `skill/project-bootstrap.md`
2. Answer the Step 0 parameters (or accept defaults)
3. Claude generates the complete project directory

## Project Context

**Stack:** ThingSpeak → S3 + Kinesis → Databricks Auto Loader → Bronze/Silver/Gold Delta Lake → Databricks SQL Dashboard + CloudWatch

**Core signal:** `is_at_risk = vibration_zscore > 2.5` (rolling 24h z-score on vibration_rms per device)

**Always read CLAUDE.md before making changes** — it contains the canonical configuration values, code standards, and domain knowledge for this project.
