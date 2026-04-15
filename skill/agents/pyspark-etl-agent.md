# PySpark ETL Agent

## Role

Owns the Medallion ETL pipeline: Bronze ingestion via Auto Loader, Silver transformation with MERGE upsert, and Gold aggregation with predictive maintenance KPIs.

## Files Owned

| File | Purpose |
|---|---|
| `etl/utils.py` | SparkSession factory, DataQualityChecker, Delta helpers, metric publisher |
| `etl/bronze_ingest.py` | Auto Loader: S3 JSON → Bronze Delta (append-only streaming) |
| `etl/silver_transform.py` | Bronze → Silver: cast, parse, fill nulls, dedupe, MERGE |
| `etl/gold_aggregate.py` | Silver → Gold: hourly/daily KPIs, z-score, risk flag, OPTIMIZE/VACUUM |

## Medallion Architecture — Layer Contracts

### Bronze (`bronze_sensors`)
- **Source:** S3 `raw/` prefix via Auto Loader
- **Mode:** Append-only — records are NEVER modified or deleted
- **Contains:** Raw ThingSpeak data with audit columns
- **Deduplicated?** No — that happens in Silver
- **Schema enforcement:** Explicit StructType (no schema inference)

### Silver (`silver_sensors`)
- **Source:** Bronze table (full read, not incremental — simple for demo)
- **Mode:** MERGE upsert on `(device_id, entry_id)`
- **Contains:** Clean, typed, deduplicated sensor readings
- **Key transforms:**
  - `recorded_at` string → UTC TimestampType
  - All metrics → DoubleType
  - Null metrics → `-1.0` sentinel
  - Rows with null `device_id` → dropped

### Gold (`gold_sensors_hourly`, `gold_sensors_daily`)
- **Source:** Silver table
- **Mode:** Overwrite by partition (dynamic partition overwrite)
- **Contains:** Pre-aggregated KPIs ready for dashboard queries
- **Key features:**
  - Hourly: avg/min/max per device per hour
  - Daily: same + rolling 24h z-score + `is_at_risk` flag

## Adding a New Metric Column

1. In `etl/gold_aggregate.py`, add the aggregation to `compute_hourly()` and/or `compute_daily()`
2. If it's a new Silver column, add the transformation in `etl/silver_transform.py`
3. Add the Bronze StructType field in `etl/bronze_ingest.py → get_schema()`
4. Update `ingestion/schema.json` with the new field

## DQ Check Patterns

```python
checker = DataQualityChecker(df, "table_name")
checker.check_row_count(min_rows=1)
checker.check_null_rate("device_id", threshold=0.0)   # must never be null
checker.check_value_range("vibration_rms", min_val=-1.0, max_val=500.0)
checker.log_summary()
```

## MERGE Upsert Pattern

```python
target = DeltaTable.forName(spark, "main.predictive_maintenance.silver_sensors")
(
    target.alias("target")
    .merge(source_df.alias("source"),
           "target.device_id = source.device_id AND target.entry_id = source.entry_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

## Running Locally

```bash
# Requires a local Spark installation with Delta Lake jar
python etl/bronze_ingest.py --trigger-once
python etl/silver_transform.py
python etl/gold_aggregate.py --skip-optimize --skip-vacuum
```
