# Databricks notebook source
# MAGIC %md
# MAGIC # 🚌 Klang Valley Transit & Weather Data Pipeline
# MAGIC ## 🥉 Step 1: Bronze Layer (Raw Ingestion)
# MAGIC
# MAGIC **Objective:** Ingest raw feeds with minimal transformations into Delta Lake:
# MAGIC 1. **Open-Meteo Weather API:** Hourly precipitation, temperature, and rain observations for Kuala Lumpur (`Lat 3.1390`, `Lon 101.6869`).
# MAGIC 2. **data.gov.my RapidKL Transit API:** Official GTFS Static schedules (routes, trips, stops, scheduled times) for RapidKL buses.
# MAGIC
# MAGIC **Output Managed Delta Tables:**
# MAGIC - `bronze_kl_weather`: Raw hourly weather observations.
# MAGIC - `bronze_rapidkl_transit`: Raw transit route and scheduled trip telemetry.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Environment Setup & `src` Package Import
# MAGIC
# MAGIC This notebook is designed to run inside a **Databricks Git Folder** (Repo),
# MAGIC where the repository root is automatically on `sys.path`.
# MAGIC
# MAGIC If running outside a Git Folder (e.g., uploaded manually), the `sys.path` insertion
# MAGIC below ensures `src/` is always discoverable on any Databricks cluster.

# COMMAND ----------

import os
import sys

# ---------------------------------------------------------------------------
# Make the repository root importable in all Databricks environments.
# ---------------------------------------------------------------------------
# - In Databricks Git Folders: the repo root is already on sys.path automatically.
# - In manual uploads or Workflows: the insertion below guarantees src/ is found.
# Update the path below to match where your repo is mounted in the workspace.
_REPO_ROOT = "/Workspace/Repos/<your-username>/Klang-Valley-Transit-Weather-Pipeline"

if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

# ---------------------------------------------------------------------------
# Import pipeline configuration and ingestion logic from src/
# ---------------------------------------------------------------------------
from src.config import WEATHER_CONFIG, TRANSIT_CONFIG, STORAGE_CONFIG
from src.ingestion.weather_ingestion import (
    fetch_kl_weather_json,
    transform_weather_json_to_records,
    ingest_weather_to_spark_df,
    save_weather_to_delta,
)
from src.ingestion.transit_ingestion import (
    download_rapidkl_gtfs_archive,
    save_transit_to_delta,
)
from src.utils.logger import get_logger

# PySpark SQL functions needed for transit relational linking in this notebook
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import DoubleType, IntegerType

import pandas as pd

logger = get_logger("BronzeIngestionNotebook")
logger.info("All src/ modules loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Pipeline Configuration & Widgets

# COMMAND ----------

# ---------------------------------------------------------------------------
# Databricks Interactive Widgets
# ---------------------------------------------------------------------------
# Widgets allow this notebook to be parameterized dynamically.
# Users can change these values in the Databricks UI header or pass parameters
# through automated Databricks Workflow Jobs without editing the code.
dbutils.widgets.text("start_date", "2024-01-01", "Start Date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "2024-01-07", "End Date (YYYY-MM-DD)")
dbutils.widgets.text("transit_category", TRANSIT_CONFIG.DEFAULT_CATEGORY, "Transit Category")

# Retrieve widget input values into Python variables
START_DATE = dbutils.widgets.get("start_date")
END_DATE = dbutils.widgets.get("end_date")
TRANSIT_CATEGORY = dbutils.widgets.get("transit_category")

# ---------------------------------------------------------------------------
# Resolve target table names from centralized StorageConfig
# ---------------------------------------------------------------------------
WEATHER_TABLE = STORAGE_CONFIG.BRONZE_WEATHER_TABLE
TRANSIT_TABLE = STORAGE_CONFIG.BRONZE_TRANSIT_TABLE

print(f"Ingestion Window  : {START_DATE} to {END_DATE}")
print(f"Transit Category  : {TRANSIT_CATEGORY}")
print(f"Weather Table     : {WEATHER_TABLE}")
print(f"Transit Table     : {TRANSIT_TABLE}")
print(f"Weather API       : {WEATHER_CONFIG.ARCHIVE_URL}")
print(f"Transit API       : {TRANSIT_CONFIG.GTFS_STATIC_URL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Ingest Weather Data (Open-Meteo API → `bronze_kl_weather`)
# MAGIC
# MAGIC Fetches hourly historical precipitation, rain, temperature, and weather codes
# MAGIC directly from the Open-Meteo Archive API via `src.ingestion.weather_ingestion`.

# COMMAND ----------

# ---------------------------------------------------------------------------
# Step 3a: Fetch raw JSON payload from Open-Meteo Archive API
# ---------------------------------------------------------------------------
logger.info(f"Fetching weather data from {START_DATE} to {END_DATE}...")

raw_weather_json = fetch_kl_weather_json(start_date=START_DATE, end_date=END_DATE)

hourly = raw_weather_json.get("hourly", {})
times = hourly.get("time", [])
print(f"Fetched {len(times)} hourly records for Kuala Lumpur "
      f"({WEATHER_CONFIG.LATITUDE}, {WEATHER_CONFIG.LONGITUDE}).")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unpack Raw JSON Arrays into Normalized Spark Records

# COMMAND ----------

# ---------------------------------------------------------------------------
# Step 3b: Transform columnar JSON arrays into normalized row records
# ---------------------------------------------------------------------------
# transform_weather_json_to_records() unzips Open-Meteo's parallel arrays into
# flat row dicts and attaches audit metadata (_ingested_at, _source_api).
weather_records = transform_weather_json_to_records(raw_weather_json)
logger.info(f"Transformed {len(weather_records)} hourly weather records.")

# ---------------------------------------------------------------------------
# Step 3c: Load records into a PySpark DataFrame with enforced StructType schema
# ---------------------------------------------------------------------------
# ingest_weather_to_spark_df() applies an explicit schema (no inferSchema),
# casts 'timestamp' string → native TimestampType as 'weather_timestamp',
# and appends '_ingestion_time' from the Spark cluster's current timestamp.
df_weather = ingest_weather_to_spark_df(spark, weather_records)

# Preview first 10 rows in Databricks interactive UI
display(df_weather.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save Weather to Managed Delta Lake: `bronze_kl_weather`

# COMMAND ----------

# ---------------------------------------------------------------------------
# Step 3d: Persist weather DataFrame to a Managed Delta Table
# ---------------------------------------------------------------------------
# save_weather_to_delta() writes with:
# - format("delta")                : ACID-compliant Delta Lake storage.
# - mode("overwrite")              : Replaces data for the ingestion window.
# - option("overwriteSchema","true"): Allows safe schema evolution over time.
# - saveAsTable(table_name)        : Registers as Managed Table in active catalog.
save_weather_to_delta(df_weather, table_name=WEATHER_TABLE)

print(f"Successfully wrote {df_weather.count():,} rows to managed Delta table: {WEATHER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Ingest Transit Data (data.gov.my GTFS → `bronze_rapidkl_transit`)
# MAGIC
# MAGIC Downloads the official Prasarana/RapidKL GTFS Static feed from `api.data.gov.my`
# MAGIC and extracts routes, trips, stops, and schedules via `src.ingestion.transit_ingestion`.

# COMMAND ----------

# ---------------------------------------------------------------------------
# Step 4a: Download and extract core GTFS files to driver-node /tmp directory
# ---------------------------------------------------------------------------
# download_rapidkl_gtfs_archive() streams the ZIP in-memory (no temp zip on disk),
# extracts core GTFS .txt files (routes, trips, stops, stop_times),
# and returns a {filename: local_path} dict.
LOCAL_GTFS_DIR = "/tmp/rapidkl_gtfs"
logger.info(f"Downloading RapidKL GTFS feed (category={TRANSIT_CATEGORY})...")

extracted_files = download_rapidkl_gtfs_archive(
    category=TRANSIT_CATEGORY,
    extract_to_dir=LOCAL_GTFS_DIR,
)

for fname, fpath in extracted_files.items():
    print(f"  Extracted: {fname} ({os.path.getsize(fpath):,} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load and Link GTFS Telemetry into Bronze Transit DataFrame

# COMMAND ----------

# ---------------------------------------------------------------------------
# Step 4b: Load CSVs into Spark DataFrames (Spark Connect compatible)
# ---------------------------------------------------------------------------
# In Databricks runtimes (incl. Spark Connect), driver-local /tmp files cannot
# always be read directly by worker executors via spark.read.csv("file:/tmp/...").
# Reading via pandas on the driver and converting with spark.createDataFrame()
# uses Apache Arrow memory buffers — ensuring reliable execution on all cluster types.
# dtype=str preserves leading zeroes on IDs (e.g. stop_id '010023').
pdf_trips      = pd.read_csv(extracted_files["trips.txt"],      dtype=str)
pdf_routes     = pd.read_csv(extracted_files["routes.txt"],     dtype=str)
pdf_stop_times = pd.read_csv(extracted_files["stop_times.txt"], dtype=str)
pdf_stops      = pd.read_csv(extracted_files["stops.txt"],      dtype=str)

df_trips      = spark.createDataFrame(pdf_trips)
df_routes     = spark.createDataFrame(pdf_routes)
df_stop_times = spark.createDataFrame(pdf_stop_times)
df_stops      = spark.createDataFrame(pdf_stops)

# ---------------------------------------------------------------------------
# Step 4c: Explicit type casting for numeric columns
# ---------------------------------------------------------------------------
# Convert geographic coordinates to DoubleType for distance/spatial operations.
df_stops = (
    df_stops
    .withColumn("stop_lat", col("stop_lat").cast(DoubleType()))
    .withColumn("stop_lon", col("stop_lon").cast(DoubleType()))
)
# Convert stop sequence to IntegerType to preserve scheduled stop ordering.
df_stop_times = (
    df_stop_times
    .withColumn("stop_sequence", col("stop_sequence").cast(IntegerType()))
)

print(f"Trips count      : {df_trips.count():,}")
print(f"Routes count     : {df_routes.count():,}")
print(f"Stop Times count : {df_stop_times.count():,}")
print(f"Stops count      : {df_stops.count():,}")

# ---------------------------------------------------------------------------
# Step 4d: Relational joins to assemble unified Bronze Transit DataFrame
# ---------------------------------------------------------------------------
# 1. Trips + Routes (on route_id)  : Enriches trips with route numbers & descriptions.
# 2. + Stop Times  (on trip_id)    : Expands trips into their scheduled stop sequences.
# 3. + Stops       (on stop_id)    : Attaches stop GPS coordinates and names.
# 4. Audit metadata appended       : _ingested_at, _feed_category, _source.
df_transit = (
    df_trips
    .join(df_routes.select("route_id", "route_short_name", "route_long_name", "route_type"),
          on="route_id", how="left")
    .join(df_stop_times.select("trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"),
          on="trip_id", how="left")
    .join(df_stops.select("stop_id", "stop_name", "stop_lat", "stop_lon"),
          on="stop_id", how="left")
    .withColumn("_ingested_at",   current_timestamp())
    .withColumn("_feed_category", lit(TRANSIT_CATEGORY))
    .withColumn("_source",        lit(TRANSIT_CONFIG.GTFS_STATIC_URL))
)

# Preview sample records from the merged telemetry DataFrame
display(df_transit.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save Transit Telemetry to Managed Delta Lake: `bronze_rapidkl_transit`

# COMMAND ----------

# ---------------------------------------------------------------------------
# Step 4e: Persist transit DataFrame to a Managed Delta Table
# ---------------------------------------------------------------------------
# save_transit_to_delta() mirrors the weather write pattern:
# format("delta"), mode("overwrite"), overwriteSchema=true, saveAsTable.
save_transit_to_delta(df_transit, table_name=TRANSIT_TABLE)

print(f"Successfully wrote {df_transit.count():,} rows to managed Delta table: {TRANSIT_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Verification & Validation Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verification Query 1: Weather Observations & Rain Intensity
# MAGIC -- Confirms that weather records are queryable and checks the heaviest rainfall hours in KL.
# MAGIC SELECT
# MAGIC     weather_timestamp,
# MAGIC     temperature_2m,
# MAGIC     precipitation,
# MAGIC     rain,
# MAGIC     weather_code,
# MAGIC     _source_api
# MAGIC FROM bronze_kl_weather
# MAGIC ORDER BY precipitation DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verification Query 2: Route Coverage & Schedule Distribution
# MAGIC -- Verifies transit telemetry integrity by calculating trips and scheduled stops per route.
# MAGIC SELECT
# MAGIC     route_short_name,
# MAGIC     route_long_name,
# MAGIC     COUNT(DISTINCT trip_id)  AS total_trips,
# MAGIC     COUNT(stop_sequence)     AS scheduled_stops
# MAGIC FROM bronze_rapidkl_transit
# MAGIC GROUP BY route_short_name, route_long_name
# MAGIC ORDER BY scheduled_stops DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🚀 Bronze Layer Complete!
# MAGIC Both raw feeds have been successfully ingested into:
# MAGIC 1. `bronze_kl_weather`
# MAGIC 2. `bronze_rapidkl_transit`
# MAGIC
# MAGIC **Next Phase (Step 2 - Silver Layer):**
# MAGIC - Clean and cast GTFS arrival/departure times.
# MAGIC - Join transit stop times with Open-Meteo hourly precipitation by timestamp (`weather_timestamp`).
# MAGIC - Identify high precipitation / heavy rainfall events (> 5.0 mm/hr).
# MAGIC - Compute trip duration deviations and schedule delay metrics.
