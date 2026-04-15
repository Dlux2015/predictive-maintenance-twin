-- =============================================================================
-- Predictive Maintenance Digital Twin — Databricks SQL Dashboard Queries
-- Catalog: main | Schema: predictive_maintenance
--
-- These queries power the 5-widget Databricks SQL Dashboard.
-- Each query section is separated by a header and a semicolon.
-- Run each query as a separate Dashboard widget.
-- =============================================================================


-- =============================================================================
-- QUERY 1: Current Twin State — Latest Reading Per Device
-- Purpose:  Shows the most recent sensor reading for each device.
--           This represents the "current state" of each device's digital twin.
-- Widget:   Table with conditional formatting on data_freshness column.
-- =============================================================================

SELECT
    device_id,
    recorded_at                                                       AS last_seen,
    ROUND(vibration_rms, 3)                                           AS vibration_rms_mm_s,
    ROUND(temperature_celsius, 2)                                     AS temperature_celsius,
    ROUND(pressure_bar, 3)                                            AS pressure_bar,
    TIMESTAMPDIFF(MINUTE, recorded_at, CURRENT_TIMESTAMP())           AS minutes_since_last_reading,
    CASE
        WHEN TIMESTAMPDIFF(MINUTE, recorded_at, CURRENT_TIMESTAMP()) < 5  THEN 'LIVE'
        WHEN TIMESTAMPDIFF(MINUTE, recorded_at, CURRENT_TIMESTAMP()) < 15 THEN 'RECENT'
        ELSE 'STALE'
    END                                                               AS data_freshness,
    source_channel,
    entry_id
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY device_id
            ORDER BY recorded_at DESC
        ) AS rn
    FROM main.predictive_maintenance.silver_sensors
    WHERE device_id IS NOT NULL
)
WHERE rn = 1
ORDER BY device_id;


-- =============================================================================
-- QUERY 2: At-Risk Devices (Vibration Z-Score > 2.5)
-- Purpose:  Core predictive maintenance signal — devices whose vibration
--           exceeds 2.5 standard deviations above their rolling 24h baseline.
--           These devices should be flagged for maintenance inspection.
-- Widget:   Table with row-level alert formatting on is_at_risk = true.
-- =============================================================================

SELECT
    device_id,
    date,
    ROUND(avg_vibration_rms, 3)     AS avg_vibration_rms_mm_s,
    ROUND(max_vibration_rms, 3)     AS max_vibration_rms_mm_s,
    ROUND(vibration_zscore, 3)      AS vibration_zscore,
    is_at_risk,
    reading_count,
    ROUND(avg_temperature_celsius, 2) AS avg_temperature_celsius,
    ROUND(avg_pressure_bar, 3)      AS avg_pressure_bar,
    _computed_at
FROM main.predictive_maintenance.gold_sensors_daily
WHERE
    is_at_risk = TRUE
    AND date >= DATE_SUB(CURRENT_DATE(), 7)
ORDER BY
    vibration_zscore DESC,
    date DESC;


-- =============================================================================
-- QUERY 3: 24-Hour Vibration Trend Per Device
-- Purpose:  Hourly vibration trend for the last 24 hours, per device.
--           Used to visualise how vibration changes through the day and
--           identify sustained high-vibration periods before failure.
-- Widget:   Line chart: x=window_start, y=avg_vibration_rms, series=device_id
-- =============================================================================

SELECT
    device_id,
    window_start,
    window_end,
    ROUND(avg_vibration_rms, 3)     AS avg_vibration_rms_mm_s,
    ROUND(max_vibration_rms, 3)     AS max_vibration_rms_mm_s,
    ROUND(min_vibration_rms, 3)     AS min_vibration_rms_mm_s,
    reading_count,
    CASE
        WHEN avg_vibration_rms > 10.0 THEN TRUE
        ELSE FALSE
    END                             AS threshold_exceeded,
    ROUND(avg_temperature_celsius, 2) AS avg_temperature_celsius,
    ROUND(avg_pressure_bar, 3)      AS avg_pressure_bar
FROM main.predictive_maintenance.gold_sensors_hourly
WHERE
    window_start >= TIMESTAMPADD(HOUR, -24, CURRENT_TIMESTAMP())
    AND device_id IS NOT NULL
ORDER BY
    device_id,
    window_start ASC;


-- =============================================================================
-- QUERY 4: Pipeline Health — Row Counts Bronze vs Silver vs Gold
-- Purpose:  Shows the volume of records at each pipeline stage plus
--           the latest record timestamp and data lag per layer.
--           Used to detect pipeline stalls or ingestion failures.
-- Widget:   Table / bar chart comparing row counts across layers.
-- =============================================================================

SELECT
    'bronze_sensors'           AS table_name,
    'bronze'                   AS layer,
    COUNT(*)                   AS row_count,
    MAX(recorded_at)           AS latest_record_at,
    TIMESTAMPDIFF(
        MINUTE,
        MAX(recorded_at),
        CURRENT_TIMESTAMP()
    )                          AS data_lag_minutes
FROM main.predictive_maintenance.bronze_sensors

UNION ALL

SELECT
    'silver_sensors'           AS table_name,
    'silver'                   AS layer,
    COUNT(*)                   AS row_count,
    MAX(recorded_at)           AS latest_record_at,
    TIMESTAMPDIFF(
        MINUTE,
        MAX(recorded_at),
        CURRENT_TIMESTAMP()
    )                          AS data_lag_minutes
FROM main.predictive_maintenance.silver_sensors

UNION ALL

SELECT
    'gold_sensors_hourly'      AS table_name,
    'gold'                     AS layer,
    COUNT(*)                   AS row_count,
    MAX(window_start)          AS latest_record_at,
    TIMESTAMPDIFF(
        MINUTE,
        MAX(window_start),
        CURRENT_TIMESTAMP()
    )                          AS data_lag_minutes
FROM main.predictive_maintenance.gold_sensors_hourly

UNION ALL

SELECT
    'gold_sensors_daily'       AS table_name,
    'gold'                     AS layer,
    COUNT(*)                   AS row_count,
    MAX(CAST(date AS TIMESTAMP)) AS latest_record_at,
    TIMESTAMPDIFF(
        MINUTE,
        MAX(CAST(date AS TIMESTAMP)),
        CURRENT_TIMESTAMP()
    )                          AS data_lag_minutes
FROM main.predictive_maintenance.gold_sensors_daily

ORDER BY
    CASE layer
        WHEN 'bronze' THEN 1
        WHEN 'silver' THEN 2
        WHEN 'gold'   THEN 3
        ELSE 4
    END;


-- =============================================================================
-- QUERY 5: Daily Summary Table
-- Purpose:  Full daily aggregation summary for the last 30 days.
--           Provides a complete view of device performance over time
--           including the is_at_risk predictive maintenance flag.
-- Widget:   Table with 30-day lookback, sortable columns.
-- =============================================================================

SELECT
    device_id,
    date,
    reading_count,

    -- Vibration metrics (core predictive maintenance feature)
    ROUND(avg_vibration_rms, 3)   AS avg_vibration_rms_mm_s,
    ROUND(min_vibration_rms, 3)   AS min_vibration_rms_mm_s,
    ROUND(max_vibration_rms, 3)   AS max_vibration_rms_mm_s,
    ROUND(vibration_zscore, 3)    AS vibration_zscore,
    is_at_risk,

    -- Temperature metrics
    ROUND(avg_temperature_celsius, 2) AS avg_temperature_celsius,
    ROUND(min_temperature_celsius, 2) AS min_temperature_celsius,
    ROUND(max_temperature_celsius, 2) AS max_temperature_celsius,

    -- Pressure metrics
    ROUND(avg_pressure_bar, 3)    AS avg_pressure_bar,
    ROUND(min_pressure_bar, 3)    AS min_pressure_bar,
    ROUND(max_pressure_bar, 3)    AS max_pressure_bar,

    _computed_at
FROM main.predictive_maintenance.gold_sensors_daily
WHERE
    date >= DATE_SUB(CURRENT_DATE(), 30)
ORDER BY
    device_id ASC,
    date DESC;
