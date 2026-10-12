# Databricks notebook source
# MAGIC %md
# MAGIC # 🚌 Klang Valley Transit & Weather Data Pipeline
# MAGIC ## 🥈 Step 2: Silver Layer (Data Cleaning & Validation)
# MAGIC
# MAGIC **Objective:** Clean, validate, and align raw Bronze data before analytical joins.
# MAGIC
# MAGIC | Task | Weather | Transit |
# MAGIC |------|---------|---------|
# MAGIC | Deduplication | On `weather_timestamp` | On `(trip_id, stop_id, stop_sequence)` |
# MAGIC | Bounds Validation | Meteorological thresholds | Klang Valley GPS bounding box |
# MAGIC | Quarantine | Invalid met. records logged | Bad rows → `silver_quarantine_transit` |
# MAGIC | Timestamp Alignment | Already hourly | GTFS times rounded to nearest hour |
# MAGIC
# MAGIC **Input Bronze Tables:**
# MAGIC - `bronze_kl_weather`
# MAGIC - `bronze_rapidkl_transit`
# MAGIC
# MAGIC **Output Silver Tables:**
# MAGIC - `silver_kl_weather` — clean hourly weather observations
# MAGIC - `silver_rapidkl_transit` — clean, validated, aligned transit schedules
# MAGIC - `silver_quarantine_transit` — quarantined rows with failure reason tags
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Environment Setup & `src` Package Import

# COMMAND ----------

import os
import sys

# ---------------------------------------------------------------------------
# Make the repository root importable.
# Update _REPO_ROOT to match your Databricks Git Folder path.
# ---------------------------------------------------------------------------
_REPO_ROOT = "/Workspace/Repos/<your-username>/Klang-Valley-Transit-Weather-Pipeline"

if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

# ---------------------------------------------------------------------------
# Import silver layer transformation modules from src/
# ---------------------------------------------------------------------------
from src.config import STORAGE_CONFIG, VALIDATION_CONFIG
from src.transformation.silver_weather import run_silver_weather_pipeline
from src.transformation.silver_transit import run_silver_transit_pipeline
from src.utils.logger import get_logger

logger = get_logger("SilverNotebook")
logger.info("Silver layer src/ modules loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Pipeline Configuration & Widgets

# COMMAND ----------

# ---------------------------------------------------------------------------
# Databricks Widgets — override defaults from Databricks UI or Workflow Jobs
# ---------------------------------------------------------------------------
dbutils.widgets.text("bronze_weather_table", STORAGE_CONFIG.BRONZE_WEATHER_TABLE, "Bronze Weather Table")
dbutils.widgets.text("bronze_transit_table", STORAGE_CONFIG.BRONZE_TRANSIT_TABLE, "Bronze Transit Table")
dbutils.widgets.text("silver_weather_table", STORAGE_CONFIG.SILVER_WEATHER_TABLE, "Silver Weather Table")
dbutils.widgets.text("silver_transit_table", STORAGE_CONFIG.SILVER_TRANSIT_TABLE, "Silver Transit Table")
dbutils.widgets.text("silver_quarantine_table", STORAGE_CONFIG.SILVER_QUARANTINE_TABLE, "Quarantine Table")

BRONZE_WEATHER_TABLE   = dbutils.widgets.get("bronze_weather_table")
BRONZE_TRANSIT_TABLE   = dbutils.widgets.get("bronze_transit_table")
SILVER_WEATHER_TABLE   = dbutils.widgets.get("silver_weather_table")
SILVER_TRANSIT_TABLE   = dbutils.widgets.get("silver_transit_table")
SILVER_QUARANTINE_TABLE = dbutils.widgets.get("silver_quarantine_table")

# ---------------------------------------------------------------------------
# Print active validation bounds from ValidationConfig
# ---------------------------------------------------------------------------
v = VALIDATION_CONFIG
print("=" * 60)
print(f"Bronze Weather Source  : {BRONZE_WEATHER_TABLE}")
print(f"Bronze Transit Source  : {BRONZE_TRANSIT_TABLE}")
print(f"Silver Weather Target  : {SILVER_WEATHER_TABLE}")
print(f"Silver Transit Target  : {SILVER_TRANSIT_TABLE}")
print(f"Quarantine Target      : {SILVER_QUARANTINE_TABLE}")
print("-" * 60)
print(f"KV Bounding Box        : Lat [{v.LAT_MIN}, {v.LAT_MAX}] × Lon [{v.LON_MIN}, {v.LON_MAX}]")
print(f"Temperature Bounds     : {v.TEMP_MIN_C}°C to {v.TEMP_MAX_C}°C")
print(f"Precipitation Min      : {v.PRECIP_MIN_MM} mm (no negatives)")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Silver Weather — Cleaning & Validation
# MAGIC
# MAGIC Steps applied by `run_silver_weather_pipeline()`:
# MAGIC 1. **Deduplication** on `weather_timestamp` (keeps most recent ingest)
# MAGIC 2. **Meteorological bounds** validation (temperature, precipitation, humidity)
# MAGIC 3. **Enrichment** with `is_rainy_hour` flag and `_silver_processed_at` audit column
# MAGIC 4. **Write** to `silver_kl_weather` managed Delta table

# COMMAND ----------

logger.info("Starting Silver Weather transformation...")

df_silver_weather = run_silver_weather_pipeline(
    spark=spark,
    bronze_table=BRONZE_WEATHER_TABLE,
    silver_table=SILVER_WEATHER_TABLE,
)

display(df_silver_weather.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Weather Quality Summary

# COMMAND ----------

from pyspark.sql.functions import count, sum as spark_sum, col

weather_summary = df_silver_weather.agg(
    count("*").alias("total_records"),
    spark_sum(col("is_rainy_hour").cast("int")).alias("rainy_hours"),
    spark_sum((~col("is_rainy_hour")).cast("int")).alias("dry_hours"),
)
display(weather_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Silver Transit — Cleaning, Quarantine & Timestamp Alignment
# MAGIC
# MAGIC Steps applied by `run_silver_transit_pipeline()`:
# MAGIC 1. **Deduplication** on `(trip_id, stop_id, stop_sequence)`
# MAGIC 2. **Bounding Box Validation** — filters GPS outside Klang Valley bounds
# MAGIC 3. **Quarantine Routing** — rows with missing `route_id` or invalid GPS → `silver_quarantine_transit`
# MAGIC 4. **Timestamp Alignment** — GTFS `departure_time` rounded to nearest hour → `scheduled_hour_aligned`
# MAGIC 5. **Write** clean rows to `silver_rapidkl_transit` and quarantine to `silver_quarantine_transit`

# COMMAND ----------

logger.info("Starting Silver Transit transformation...")

df_silver_transit = run_silver_transit_pipeline(
    spark=spark,
    bronze_table=BRONZE_TRANSIT_TABLE,
    silver_table=SILVER_TRANSIT_TABLE,
    quarantine_table=SILVER_QUARANTINE_TABLE,
)

display(df_silver_transit.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transit Quarantine Inspection

# COMMAND ----------

# Inspect quarantine table — understand why rows were rejected
df_quarantine = spark.table(SILVER_QUARANTINE_TABLE)
print(f"Total quarantined rows: {df_quarantine.count():,}")
display(
    df_quarantine
    .groupBy("_quarantine_reason")
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transit Timestamp Distribution

# COMMAND ----------

# Validate scheduled_hour_aligned distribution — should follow a realistic bus schedule curve
display(
    df_silver_transit
    .groupBy("scheduled_hour_aligned")
    .count()
    .orderBy("scheduled_hour_aligned")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Verification & Validation Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver Weather: Confirm deduplication and rainy hour distribution
# MAGIC SELECT
# MAGIC     DATE(weather_timestamp)  AS obs_date,
# MAGIC     COUNT(*)                 AS hourly_records,
# MAGIC     SUM(CAST(is_rainy_hour AS INT)) AS rainy_hours,
# MAGIC     ROUND(AVG(precipitation), 3)    AS avg_precip_mm,
# MAGIC     ROUND(MAX(precipitation), 3)    AS max_precip_mm,
# MAGIC     _layer
# MAGIC FROM silver_kl_weather
# MAGIC GROUP BY obs_date, _layer
# MAGIC ORDER BY obs_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver Transit: Confirm bounding box and hour alignment
# MAGIC SELECT
# MAGIC     route_short_name,
# MAGIC     COUNT(DISTINCT trip_id)          AS unique_trips,
# MAGIC     COUNT(*)                         AS total_stops,
# MAGIC     ROUND(MIN(stop_lat), 4)          AS min_lat,
# MAGIC     ROUND(MAX(stop_lat), 4)          AS max_lat,
# MAGIC     ROUND(MIN(stop_lon), 4)          AS min_lon,
# MAGIC     ROUND(MAX(stop_lon), 4)          AS max_lon,
# MAGIC     MIN(scheduled_hour_aligned)      AS earliest_hour,
# MAGIC     MAX(scheduled_hour_aligned)      AS latest_hour
# MAGIC FROM silver_rapidkl_transit
# MAGIC GROUP BY route_short_name
# MAGIC ORDER BY total_stops DESC
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quarantine: Review failure reasons
# MAGIC SELECT
# MAGIC     _quarantine_reason,
# MAGIC     COUNT(*) AS quarantine_count
# MAGIC FROM silver_quarantine_transit
# MAGIC GROUP BY _quarantine_reason
# MAGIC ORDER BY quarantine_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🥈 Silver Layer Complete!
# MAGIC
# MAGIC Clean, validated data is now available in:
# MAGIC 1. `silver_kl_weather` — deduplicated hourly observations with `is_rainy_hour` flag
# MAGIC 2. `silver_rapidkl_transit` — validated stops within Klang Valley with `scheduled_hour_aligned`
# MAGIC 3. `silver_quarantine_transit` — rejected rows tagged with failure reason for auditing
# MAGIC
# MAGIC **Next Phase (Step 3 - Gold Layer):**
# MAGIC - Temporal join: `silver_kl_weather` × `silver_rapidkl_transit` on `scheduled_hour_aligned` = `HOUR(weather_timestamp)`
# MAGIC - Compute per-route rain impact metrics (avg delay by precipitation intensity band)
# MAGIC - Aggregate into `gold_route_rain_impact_metrics` for BI dashboards and reporting
