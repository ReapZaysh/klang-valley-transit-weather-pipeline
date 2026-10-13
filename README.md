# Klang Valley Transit & Weather Data Pipeline

[![Databricks](https://img.shields.io/badge/Platform-Databricks%20Community%20Edition-FF3621?logo=databricks&logoColor=white)](#)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake%20(Medallion)-00ADD8?logo=apachespark&logoColor=white)](#)
[![PySpark](https://img.shields.io/badge/Engine-PySpark%203.5-E25A1C?logo=apachespark&logoColor=white)](#)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](#)

A production-grade Databricks PySpark pipeline built on the **Medallion Architecture (Delta Lake)** that ingests, cleans, and joins **RapidKL public bus transit feeds** with **Kuala Lumpur meteorological data** to measure route performance and delay metrics during heavy rainfall events.

---

## Architecture & Medallion Pattern

```mermaid
flowchart LR
    subgraph Raw_Sources["External Feeds"]
        direction TB
        W["Open-Meteo API<br/>(KL Hourly Weather)"]
        T["data.gov.my API<br/>(RapidKL GTFS Static)"]
    end

    subgraph Bronze["Bronze Layer (Raw Delta)"]
        direction TB
        BW[("bronze_kl_weather<br/>(Hourly Weather Logs)")]
        BT[("bronze_rapidkl_transit<br/>(Trips, Routes, Schedules)")]
    end

    subgraph Silver["Silver Layer (Cleaned & Validated)"]
        direction TB
        SW[("silver_kl_weather<br/>(Deduplicated, Validated)")]
        ST[("silver_rapidkl_transit<br/>(Hour-aligned, In-bounds)")]
        SQ[("silver_quarantine_transit<br/>(Dead-Letter Quarantine)")]
    end

    subgraph Gold["Gold Layer (Aggregates & Metrics)"]
        G[("gold_route_rain_impact_metrics<br/>(Delay Index, Rain Severity)")]
    end

    W -->|requests / JSON| BW
    T -->|GTFS Static / CSV| BT
    BW --> SW
    BT --> ST
    BT -.->|Data Quality Faults| SQ
    SW --> G
    ST --> G
```

---

## Step 1: Bronze Layer (Raw Ingestion)

The **Bronze Layer** ingests raw source feeds with minimal transformations into **Delta Lake**, strictly capturing raw fidelity while appending pipeline audit metadata.

### 1. Kuala Lumpur Weather Feed (`bronze_kl_weather`)
* **Source:** Open-Meteo Historical Archive / Forecast API
* **Coordinates:** Lat `3.1390`, Lon `101.6869` (Kuala Lumpur, Malaysia)
* **Timezone:** `Asia/Kuala_Lumpur` (UTC+8)
* **Variables:** `temperature_2m`, `precipitation` (mm), `rain` (mm), `weather_code`, `relative_humidity_2m`, `wind_speed_10m`
* **Format:** Unpacked from parallel hourly JSON arrays into PySpark DataFrame with timestamp casting and audit fields (`_ingested_at`, `_source_api`).

### 2. RapidKL Bus Transit Feed (`bronze_rapidkl_transit`)
* **Source:** `https://api.data.gov.my/gtfs-static/prasarana?category=rapid-bus-kl`
* **Entities Ingested:**
  - `routes.txt`: Bus route IDs, names, and transit types (~138 routes).
  - `trips.txt`: Scheduled trip identifiers, direction, and shape bindings (~2,098 trips).
  - `stops.txt`: Stop names, unique stop IDs, and coordinates (~4,069 stops).
  - `stop_times.txt`: Scheduled arrival/departure sequences (~88,106 scheduled stop events).
* **Format:** Linked and ingested into Delta Lake table with lineage columns (`_ingested_at`, `_feed_category`, `_source`).

---

## Step 2: Silver Layer (Data Cleaning & Validation)

The **Silver Layer** enforces schemas, performs data deduplication, applies geospatial boundary filtering, handles GTFS timestamp anomalies, and implements an automated **quarantine dead-letter pattern**.

### 1. Filtering & Deduplication
* **Weather:** Deduplicated on `weather_timestamp`, keeping the latest ingested observation via windowed ranking (`row_number()`).
* **Transit:** Deduplicated on composite key `(trip_id, stop_id, stop_sequence)`, keeping the latest record.

### 2. Klang Valley Bounding Box Validation
Bus stops located outside Greater Kuala Lumpur / Klang Valley are filtered using the bounding envelope:
* **Latitude:** `2.8°` to `3.5° N`
* **Longitude:** `101.3°` to `101.9° E`

### 3. Timestamp Alignment for Weather Joining
* GTFS `stop_times.txt` formats scheduled arrivals/departures as strings (e.g. `08:30:00`).
* **Midnight Rollover Handling:** GTFS allows timestamps `> 24:00:00` (e.g. `25:15:00` for 1:15 AM next service day). These are normalized with modulo-24.
* **Hour Rounding:** Timestamps are rounded to the nearest integer hour `(0–23)` creating the `scheduled_hour_aligned` column, enabling direct analytical joins with Open-Meteo hourly weather observations.

### 4. Data Quality Quarantine (`silver_quarantine_transit`)
Records that violate data contracts are split from the main pipeline and written to `silver_quarantine_transit` with explicit `_quarantine_reason` tags:
* Missing or empty `route_id` (`MISSING_ROUTE_ID`)
* Negative vehicle speeds or invalid stop sequences (`NEGATIVE_SPEED_OR_SEQUENCE`)
* Coordinates outside the Klang Valley bounding box (`OUT_OF_BOUNDS_COORDINATES`)

---

## Repository Structure

```
Klang Valley Transit & Weather Data Pipeline/
├── notebooks/
│   ├── 01_bronze_ingestion.py         # Bronze Raw Ingestion Notebook
│   └── 02_silver_transformation.py    # Silver Cleaning, Validation & Quarantine Notebook
├── src/
│   ├── config.py                      # Centralized coordinates, bounding box & table names
│   ├── utils/
│   │   ├── __init__.py
│   │   └── logger.py                  # Structured logging utility
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── weather_ingestion.py       # Open-Meteo fetch & PySpark parser
│   │   └── transit_ingestion.py       # data.gov.my GTFS downloader & builder
│   └── transformation/
│       ├── __init__.py
│       ├── silver_weather.py          # Weather deduplication & bounds validation
│       └── silver_transit.py          # Transit deduplication, bounding box & quarantine
├── scripts/
│   └── fetch_raw_samples.py           # Local script to fetch & cache raw data samples
├── tests/
│   ├── __init__.py
│   └── test_raw_ingestion.py          # Unit & schema validation tests
├── data/
│   └── raw/                           # Downloaded sample data (JSON & GTFS CSVs)
│       ├── weather/
│       └── gtfs/
├── requirements.txt                   # Pipeline dependencies
├── .gitignore                         # Delta logs, checkpoints, cache ignores
└── README.md                          # Pipeline documentation
```

---

## How to Run on Databricks

### 1. Link Repo via Databricks Repos / Git Folders
1. In your Databricks workspace sidebar, open **Workspace → Repos (or Git Folders)**.
2. Click **Add Repo** and paste your GitHub repository URL:
   `https://github.com/ReapZaysh/klang-valley-transit-weather-pipeline.git`
3. All notebooks and `src/` modular code will be directly available in your workspace.

### 2. Run Bronze Ingestion Notebook
1. Open `notebooks/01_bronze_ingestion`.
2. Configure parameters via the interactive widgets (e.g. `start_date`, `end_date`, `transit_category`).
3. Click **Run All** to populate `bronze_kl_weather` and `bronze_rapidkl_transit`.

### 3. Run Silver Transformation Notebook
1. Open `notebooks/02_silver_transformation`.
2. Click **Run All** to execute deduplication, bounding box validation, and quarantine routing.
3. The notebook writes clean data to `silver_kl_weather` and `silver_rapidkl_transit`, while routing bad records to `silver_quarantine_transit`.

---

## Data Dictionary

### Bronze Tables
* `bronze_kl_weather`: Raw hourly Open-Meteo weather logs (`observation_time`, `temperature_2m`, `precipitation`, `rain`, etc.).
* `bronze_rapidkl_transit`: Raw linked GTFS feeds (`route_id`, `trip_id`, `stop_id`, `stop_name`, `stop_lat`, `stop_lon`, `arrival_time`, etc.).

### Silver Tables
* `silver_kl_weather`: Cleaned, validated, deduplicated hourly weather readings within Malaysia meteorological thresholds.
* `silver_rapidkl_transit`: Cleaned transit schedules with validated Klang Valley GPS bounds and `scheduled_hour_aligned`.
* `silver_quarantine_transit`: Invalid/corrupted records separated with diagnostic column `_quarantine_reason` and `_quarantined_at`.

---

## Next Step: Gold Layer (Analytical Aggregates & Rain Impact)

In **Step 3 (Gold Layer)**, we will:
1. Join `silver_rapidkl_transit` with `silver_kl_weather` on date and `scheduled_hour_aligned`.
2. Classify rain event intensities (e.g. *Clear*, *Light Rain*, *Heavy Downpour / Thunderstorm*).
3. Compute schedule density, route vulnerability, and rain-impact performance metrics across Klang Valley transit corridors.
