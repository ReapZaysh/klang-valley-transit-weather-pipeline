"""
RapidKL / Prasarana Transit Data Ingestion from data.gov.my.

This module handles:
1. Downloading and decompressing the official GTFS Static ZIP feed from data.gov.my.
2. Parsing and loading GTFS entities (routes, trips, stops, stop_times).
3. Relational linking into a unified transit telemetry DataFrame.
4. Writing the bronze transit telemetry to Delta Lake (Managed Table).
"""

from datetime import datetime, timezone
import io
import os
from typing import Any, Dict, List, Optional
import zipfile
import requests

from src.config import TRANSIT_CONFIG, STORAGE_CONFIG
from src.utils.logger import get_logger

logger = get_logger("TransitIngestion")


def download_rapidkl_gtfs_archive(
    category: str = TRANSIT_CONFIG.DEFAULT_CATEGORY,
    extract_to_dir: Optional[str] = None,
) -> Dict[str, str]:
    """
    Download the official GTFS Static ZIP from data.gov.my and extract core CSV files.
    
    Parameters:
        category: Prasarana feed category (e.g., 'rapid-bus-kl', 'rapid-bus', 'rapid-rail-kl').
        extract_to_dir: Target directory on disk. Defaults to 'data/raw/gtfs'.
        
    Returns:
        Dict mapping filename (e.g. 'routes.txt') to local absolute path.
    """
    if extract_to_dir is None:
        extract_to_dir = os.path.abspath(os.path.join(os.getcwd(), "data", "raw", "gtfs"))
    os.makedirs(extract_to_dir, exist_ok=True)

    # Malaysia Open Data API GTFS Static endpoint
    url = f"{TRANSIT_CONFIG.GTFS_STATIC_URL}?category={category}"
    logger.info(f"Downloading RapidKL GTFS archive from: {url}")
    
    # User-agent header to avoid bot filtering by the government API gateway
    headers = {"User-Agent": "Mozilla/5.0 (DataPipeline/1.0)"}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    # Decompress in-memory without needing a temporary zip file on disk
    extracted_files: Dict[str, str] = {}
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        for file_name in z.namelist():
            # Extract core transit files required for route/schedule analysis
            if file_name in TRANSIT_CONFIG.CORE_GTFS_FILES or file_name.endswith(".txt"):
                target_path = os.path.join(extract_to_dir, file_name)
                with open(target_path, "wb") as f_out:
                    f_out.write(z.read(file_name))
                extracted_files[file_name] = target_path
                logger.info(f"Extracted: {file_name} -> {target_path}")

    logger.info(f"Successfully extracted {len(extracted_files)} GTFS files.")
    return extracted_files


def load_gtfs_to_spark_df(
    spark: Any,
    file_path: str,
    file_name: str,
    category: str = TRANSIT_CONFIG.DEFAULT_CATEGORY,
) -> Any:
    """
    Load an individual GTFS CSV/TXT file into a PySpark DataFrame with audit columns.
    
    Appends:
    - _ingested_at: Timestamp when Spark read the file into DataFrame.
    - _feed_category: Agency category (e.g. 'rapid-bus-kl').
    - _source_file: Original filename (e.g. 'routes.txt') for lineage.
    """
    from pyspark.sql.functions import current_timestamp, lit

    logger.info(f"Loading {file_name} into PySpark DataFrame from {file_path}")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(file_path)
    )

    df = (
        df.withColumn("_ingested_at", current_timestamp())
          .withColumn("_feed_category", lit(category))
          .withColumn("_source_file", lit(file_name))
    )
    return df


def build_bronze_rapidkl_transit_df(
    spark: Any,
    extracted_files: Dict[str, str],
    category: str = TRANSIT_CONFIG.DEFAULT_CATEGORY,
) -> Any:
    """
    Create the unified bronze_rapidkl_transit DataFrame by joining trips, routes,
    and scheduled stop times with audit metadata for downstream delay analysis.
    
    Joins performed:
    1. Trips + Routes (on route_id): Enriches trips with route line numbers and descriptions.
    2. Enriched Trips + Stop Times (on trip_id): Expands trips into their scheduled stop sequences.
    3. Merged Schedule + Stops (on stop_id): Attaches physical stop coordinates and names.
    """
    from pyspark.sql.functions import current_timestamp, lit

    trips_path = extracted_files.get("trips.txt")
    routes_path = extracted_files.get("routes.txt")
    stop_times_path = extracted_files.get("stop_times.txt")
    stops_path = extracted_files.get("stops.txt")

    if not (trips_path and routes_path):
        raise ValueError("Both trips.txt and routes.txt are required to build bronze transit table.")

    df_trips = spark.read.option("header", "true").option("inferSchema", "true").csv(trips_path)
    df_routes = spark.read.option("header", "true").option("inferSchema", "true").csv(routes_path)

    # 1. Join trips with routes to bind route metadata to each trip journey
    transit_df = df_trips.join(
        df_routes.select("route_id", "route_short_name", "route_long_name", "route_type"),
        on="route_id",
        how="left"
    )

    # 2. Join with stop_times to attach scheduled arrival and departure times
    if stop_times_path:
        df_stop_times = spark.read.option("header", "true").option("inferSchema", "true").csv(stop_times_path)
        transit_df = transit_df.join(
            df_stop_times.select("trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"),
            on="trip_id",
            how="left"
        )

    # 3. Join with stops to attach physical GPS coordinates (latitude, longitude)
    if stops_path:
        df_stops = spark.read.option("header", "true").option("inferSchema", "true").csv(stops_path)
        transit_df = transit_df.join(
            df_stops.select("stop_id", "stop_name", "stop_lat", "stop_lon"),
            on="stop_id",
            how="left"
        )

    # 4. Enrich with pipeline lineage and audit metadata
    transit_df = (
        transit_df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_feed_category", lit(category))
        .withColumn("_source", lit("data.gov.my/gtfs-static"))
    )

    return transit_df


def save_transit_to_delta(
    df: Any,
    table_name: str = STORAGE_CONFIG.BRONZE_TRANSIT_TABLE,
    delta_path: Optional[str] = None,
    mode: str = "overwrite",
) -> None:
    """
    Save PySpark Transit DataFrame to Delta Lake format as a Managed Table.
    
    Using saveAsTable without a relative path guarantees compatibility with Databricks
    Unity Catalog and Hive Metastore.
    """
    logger.info(f"Writing transit data to Delta table: {table_name} (mode={mode})")
    writer = (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
    )
    if delta_path and delta_path.startswith(("dbfs:/", "s3://", "abfss://", "gs://")):
        writer = writer.option("path", delta_path)
    
    writer.saveAsTable(table_name)
    logger.info(f"Successfully saved and registered Delta table: {table_name}")
