# Databricks notebook source
# MAGIC %md
# MAGIC # Klang Valley Transit & Weather Data Pipeline
# MAGIC ## Step 3: Gold Layer (Business Aggregations)
# MAGIC
# MAGIC **Objective:** Build analytics-ready data products from cleaned Silver tables:
# MAGIC 1. **Temporal Join:** Join `silver_rapidkl_transit` and `silver_kl_weather` on hourly timestamp (`scheduled_hour_aligned == HOUR(weather_timestamp)`).
# MAGIC 2. **Transit Physics & Speeds:** Compute hop-by-hop travel distance (Haversine formula) and scheduled operating speeds between stops.
# MAGIC 3. **Rainfall Delay Index:** Calculate average speed and speed drop per route during heavy rain (> 5 mm/hr) vs. clear weather baselines.
# MAGIC 4. **Peak Hour Reliability:** Evaluate average speeds during morning peak (7–9 AM) and evening peak (5–7 PM) vs. off-peak baselines.
# MAGIC 5. **Gold Delta Storage:** Save aggregated metrics to `gold_route_weather_performance`.
# MAGIC
# MAGIC **Input Silver Tables:**
# MAGIC - `silver_rapidkl_transit`
# MAGIC - `silver_kl_weather`
# MAGIC
# MAGIC **Output Gold Table:**
# MAGIC - `gold_route_weather_performance` — Route-level business intelligence data product
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
# Import gold layer aggregation modules from src/
# ---------------------------------------------------------------------------
from src.config import STORAGE_CONFIG, ANALYTICS_CONFIG
from src.transformation.gold_aggregations import (
    compute_transit_speeds,
    join_transit_with_weather,
    aggregate_route_weather_performance,
    save_gold_table,
    run_gold_aggregations_pipeline,
)
from src.utils.logger import get_logger

logger = get_logger("GoldNotebook")
logger.info("Gold layer src/ modules loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Pipeline Configuration & Widgets

# COMMAND ----------

# ---------------------------------------------------------------------------
# Databricks Widgets — override defaults from Databricks UI or Workflow Jobs
# ---------------------------------------------------------------------------
dbutils.widgets.text("silver_transit_table", STORAGE_CONFIG.SILVER_TRANSIT_TABLE, "Silver Transit Table")
dbutils.widgets.text("silver_weather_table", STORAGE_CONFIG.SILVER_WEATHER_TABLE, "Silver Weather Table")
dbutils.widgets.text("gold_table", STORAGE_CONFIG.GOLD_ROUTE_PERFORMANCE_TABLE, "Gold Route Table")

SILVER_TRANSIT_TABLE = dbutils.widgets.get("silver_transit_table")
SILVER_WEATHER_TABLE = dbutils.widgets.get("silver_weather_table")
GOLD_TABLE = dbutils.widgets.get("gold_table")

print(f"Source Silver Transit  : {SILVER_TRANSIT_TABLE}")
print(f"Source Silver Weather  : {SILVER_WEATHER_TABLE}")
print(f"Target Gold Performance: {GOLD_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Load Silver Layer Data

# COMMAND ----------

df_silver_transit = spark.table(SILVER_TRANSIT_TABLE)
df_silver_weather = spark.table(SILVER_WEATHER_TABLE)

transit_count = df_silver_transit.count()
weather_count = df_silver_weather.count()

print(f"Loaded {transit_count:,} silver transit records.")
print(f"Loaded {weather_count:,} silver weather records.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Compute Transit Hop Distances & Scheduled Speeds
# MAGIC
# MAGIC Calculates spherical distance (Haversine formula) and scheduled duration
# MAGIC between consecutive stops partitioned by `trip_id`.

# COMMAND ----------

logger.info("Computing transit segment metrics and speeds...")
df_transit_speed = compute_transit_speeds(df_silver_transit)

display(
    df_transit_speed.select(
        "trip_id",
        "route_short_name",
        "stop_sequence",
        "stop_name",
        "departure_time",
        "scheduled_hour_aligned",
        "segment_distance_km",
        "segment_duration_sec",
        "segment_speed_kmh",
    ).limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Join Transit with Hourly Weather Telemetry
# MAGIC
# MAGIC Joins transit hop records with Open-Meteo hourly observations where:
# MAGIC `scheduled_hour_aligned == HOUR(weather_timestamp)`.
# MAGIC
# MAGIC Enriches records with:
# MAGIC - Rainfall flags (`is_heavy_rain` for > 5 mm/hr, `is_clear_weather` for <= 0.5 mm/hr)
# MAGIC - Peak hour windows (Morning 7–9 AM, Evening 5–7 PM)
# MAGIC - `effective_speed_kmh` reflecting rain-induced traffic friction

# COMMAND ----------

logger.info("Performing hourly temporal join between transit and weather...")
df_joined = join_transit_with_weather(df_transit_speed, df_silver_weather)

display(
    df_joined.select(
        "route_short_name",
        "trip_id",
        "scheduled_hour_aligned",
        "weather_timestamp",
        "temperature_2m",
        "precipitation",
        "weather_condition_category",
        "peak_period",
        "segment_speed_kmh",
        "effective_speed_kmh",
    ).limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Calculate Route Performance & Business Aggregations
# MAGIC
# MAGIC Aggregates per route:
# MAGIC - **Rainfall Delay Index**: `avg_speed_clear_kmh / avg_speed_heavy_rain_kmh`
# MAGIC - **Speed Drop**: Absolute drop (`km/h`) and percentage drop (`%`)
# MAGIC - **Peak Hour Reliability**: Morning and Evening peak speeds vs. off-peak baselines

# COMMAND ----------

logger.info("Aggregating route weather performance metrics...")
df_gold = aggregate_route_weather_performance(df_joined)

display(
    df_gold.select(
        "route_short_name",
        "route_long_name",
        "avg_speed_clear_kmh",
        "avg_speed_heavy_rain_kmh",
        "speed_drop_heavy_rain_pct",
        "rainfall_delay_index",
        "avg_speed_morning_peak_kmh",
        "avg_speed_evening_peak_kmh",
        "avg_speed_off_peak_kmh",
        "overall_peak_reliability_score",
    ).orderBy("rainfall_delay_index", ascending=False).limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Save to Delta Lake (`gold_route_weather_performance`)

# COMMAND ----------

logger.info(f"Persisting Gold table: {GOLD_TABLE}")
save_gold_table(df_gold, table_name=GOLD_TABLE, mode="overwrite")

print(f"Successfully saved {df_gold.count():,} route summaries to Delta table: {GOLD_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Analytical Queries & Route Insights

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Top 10 routes most vulnerable to heavy rainfall (> 5 mm/hr)
# MAGIC SELECT
# MAGIC     route_short_name,
# MAGIC     route_long_name,
# MAGIC     avg_speed_clear_kmh,
# MAGIC     avg_speed_heavy_rain_kmh,
# MAGIC     speed_drop_heavy_rain_kmh,
# MAGIC     speed_drop_heavy_rain_pct,
# MAGIC     rainfall_delay_index
# MAGIC FROM gold_route_weather_performance
# MAGIC WHERE avg_speed_heavy_rain_kmh IS NOT NULL
# MAGIC ORDER BY rainfall_delay_index DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2. Peak hour reliability comparison (Morning 7-9 AM vs. Evening 5-7 PM vs. Off-Peak)
# MAGIC SELECT
# MAGIC     route_short_name,
# MAGIC     avg_speed_off_peak_kmh,
# MAGIC     avg_speed_morning_peak_kmh,
# MAGIC     morning_peak_reliability_pct,
# MAGIC     avg_speed_evening_peak_kmh,
# MAGIC     evening_peak_reliability_pct,
# MAGIC     overall_peak_reliability_score
# MAGIC FROM gold_route_weather_performance
# MAGIC WHERE avg_speed_off_peak_kmh IS NOT NULL
# MAGIC ORDER BY overall_peak_reliability_score ASC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. Overall network reliability summary
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_routes_analyzed,
# MAGIC     ROUND(AVG(avg_speed_clear_kmh), 2) AS network_avg_clear_speed_kmh,
# MAGIC     ROUND(AVG(avg_speed_heavy_rain_kmh), 2) AS network_avg_heavy_rain_speed_kmh,
# MAGIC     ROUND(AVG(speed_drop_heavy_rain_pct), 2) AS network_avg_rain_speed_drop_pct,
# MAGIC     ROUND(AVG(rainfall_delay_index), 3) AS network_rainfall_delay_index,
# MAGIC     ROUND(AVG(overall_peak_reliability_score), 3) AS network_peak_reliability_score
# MAGIC FROM gold_route_weather_performance;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Layer Complete
# MAGIC
# MAGIC The full Medallion Architecture is operational:
# MAGIC - **Bronze Layer:** Raw data ingestion (`bronze_kl_weather`, `bronze_rapidkl_transit`).
# MAGIC - **Silver Layer:** Cleaned, deduplicated, and validated data (`silver_kl_weather`, `silver_rapidkl_transit`, `silver_quarantine_transit`).
# MAGIC - **Gold Layer:** Business aggregates & KPIs (`gold_route_weather_performance`).
