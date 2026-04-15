# Databricks Agent

## Role

Owns the Databricks layer: Unity Catalog governance, workflow orchestration, SQL dashboard, and the deployment script.

## Files Owned

| File | Purpose |
|---|---|
| `databricks/unity_catalog_setup.py` | Storage credential, external location, catalog, schema, grants |
| `databricks/workflow_config.json` | Jobs API v2.1 payload (4-task DAG, hourly schedule) |
| `databricks/dashboard_query.sql` | 5 Databricks SQL dashboard queries |
| `databricks/deploy.sh` | Idempotent CLI deploy script |

## Unity Catalog Object Hierarchy

```
main (catalog)
└── predictive_maintenance (schema)
    ├── bronze_sensors        (Delta table, append-only)
    ├── silver_sensors        (Delta table, MERGE upsert)
    ├── gold_sensors_hourly   (Delta table, overwrite by partition)
    └── gold_sensors_daily    (Delta table, overwrite by partition)

External Resources:
├── pmt-storage-credential    (IAM role → S3 access)
└── pmt-external-location     (s3://predictive-maintenance-twin-raw/)
```

## Workflow DAG

```
bronze_ingest
      │
silver_transform
      │
gold_aggregate
      │
publish_metrics
```

Schedule: `0 0 * * * ?` (every hour, Quartz cron, UTC)

## Adding a New Dashboard Query

1. Open `databricks/dashboard_query.sql`
2. Add a new section with a clear header comment
3. Write the SQL against `main.predictive_maintenance.*` tables
4. In the Databricks SQL UI, add a new widget to the dashboard and paste the query

## Updating the Workflow Job

1. Edit `databricks/workflow_config.json`
2. Run `./databricks/deploy.sh` — the script uses `databricks jobs create-or-update` which is idempotent
3. If changing task parameters, check that the Python script accepts those CLI args

## Deploying Safely

```bash
# Always preview first
./databricks/deploy.sh --dry-run

# Deploy without triggering a run
./databricks/deploy.sh --no-run

# Full deploy with validation run
./databricks/deploy.sh --env prod
```

## Required Environment Variables for deploy.sh

```bash
export DATABRICKS_HOST="https://dbc-xxxxxxxx.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
export ALERT_EMAIL="oncall@yourcompany.com"
```

## Databricks SDK Pattern

All `unity_catalog_setup.py` operations use the Databricks SDK and follow this pattern:
```python
try:
    self._client.catalogs.create(name=catalog_name, ...)
    logger.info("Created catalog: %s", catalog_name)
except AlreadyExists:
    logger.info("Catalog '%s' already exists — skipping", catalog_name)
except Exception as exc:
    logger.error("Failed: %s", exc)
    raise
```
