"""
Weather Data Ingestion for Kuala Lumpur from Open-Meteo API.

This module handles:
1. Fetching hourly meteorological data via Open-Meteo API.
2. Unpacking columnar JSON array payloads into normalized row records.
3. Enforcing an explicit PySpark StructType schema.
4. Writing the bronze weather telemetry to Delta Lake (Managed Table).
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import requests

from src.config import WEATHER_CONFIG, STORAGE_CONFIG
from src.utils.logger import get_logger

logger = get_logger("WeatherIngestion")


def fetch_kl_weather_json(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    past_days: int = 7,
) -> Dict[str, Any]:
    """
    Fetch hourly weather observations for Kuala Lumpur from Open-Meteo.
    
    If start_date and end_date (format 'YYYY-MM-DD') are provided, queries the 
    Historical Archive API. Otherwise, uses the forecast API with past_days to fetch
    the most recent trailing observations.
    
    Parameters:
        start_date: Optional start date string ('YYYY-MM-DD').
        end_date: Optional end date string ('YYYY-MM-DD').
        past_days: Number of historical days if date bounds not supplied.
        
    Returns:
        Raw JSON payload dictionary from Open-Meteo.
    """
    # Build query parameters using centralized configuration
    params = {
        "latitude": WEATHER_CONFIG.LATITUDE,
        "longitude": WEATHER_CONFIG.LONGITUDE,
        "hourly": ",".join(WEATHER_CONFIG.HOURLY_VARIABLES),
        "timezone": WEATHER_CONFIG.TIMEZONE,
    }

    if start_date and end_date:
        url = WEATHER_CONFIG.ARCHIVE_URL
        params["start_date"] = start_date
        params["end_date"] = end_date
        logger.info(f"Fetching historical weather from {start_date} to {end_date} via {url}")
    else:
        url = WEATHER_CONFIG.FORECAST_URL
        params["past_days"] = past_days
        logger.info(f"Fetching recent weather for past {past_days} days via {url}")

    # Send request with a 20-second timeout to avoid indefinite blocking
    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()
    payload = response.json()
    logger.info("Successfully fetched Open-Meteo weather payload.")
    return payload


def transform_weather_json_to_records(raw_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Transform raw Open-Meteo columnar JSON into a list of normalized row dictionaries.
    
    Open-Meteo returns hourly data as parallel arrays (e.g. time: [...], precipitation: [...]).
    This function unzips these arrays by index into individual hourly observation records
    and attaches audit metadata for data governance.
    """
    hourly = raw_payload.get("hourly", {})
    times = hourly.get("time", [])
    now_utc = datetime.now(timezone.utc).isoformat()
    
    records = []
    lat = raw_payload.get("latitude", WEATHER_CONFIG.LATITUDE)
    lon = raw_payload.get("longitude", WEATHER_CONFIG.LONGITUDE)
    elevation = raw_payload.get("elevation")
    tz = raw_payload.get("timezone", WEATHER_CONFIG.TIMEZONE)

    for i, t in enumerate(times):
        record = {
            "timestamp": t,
            "latitude": float(lat),
            "longitude": float(lon),
            "elevation": float(elevation) if elevation is not None else None,
            "timezone": tz,
            # Null-safe extraction of meteorological metrics
            "temperature_2m": float(hourly["temperature_2m"][i]) if hourly.get("temperature_2m") and hourly["temperature_2m"][i] is not None else None,
            "precipitation": float(hourly["precipitation"][i]) if hourly.get("precipitation") and hourly["precipitation"][i] is not None else 0.0,
            "rain": float(hourly["rain"][i]) if hourly.get("rain") and hourly["rain"][i] is not None else 0.0,
            "weather_code": int(hourly["weather_code"][i]) if hourly.get("weather_code") and hourly["weather_code"][i] is not None else None,
            "relative_humidity_2m": float(hourly["relative_humidity_2m"][i]) if hourly.get("relative_humidity_2m") and hourly["relative_humidity_2m"][i] is not None else None,
            "wind_speed_10m": float(hourly["wind_speed_10m"][i]) if hourly.get("wind_speed_10m") and hourly["wind_speed_10m"][i] is not None else None,
            # Lineage audit metadata
            "_ingested_at": now_utc,
            "_source_api": "open-meteo",
        }
        records.append(record)

    logger.info(f"Transformed {len(records)} hourly weather observations.")
    return records


def ingest_weather_to_spark_df(spark: Any, records: List[Dict[str, Any]]) -> Any:
    """
    Load normalized weather records into a PySpark DataFrame with strict schema enforcement.
    
    Explicit StructType schema ensures data contract integrity across pipeline executions
    and converts the timestamp string into a native Spark TimestampType.
    """
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        DoubleType,
        IntegerType,
    )
    from pyspark.sql.functions import col, to_timestamp, current_timestamp

    schema = StructType([
        StructField("timestamp", StringType(), False),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("elevation", DoubleType(), True),
        StructField("timezone", StringType(), True),
        StructField("temperature_2m", DoubleType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("rain", DoubleType(), True),
        StructField("weather_code", IntegerType(), True),
        StructField("relative_humidity_2m", DoubleType(), True),
        StructField("wind_speed_10m", DoubleType(), True),
        StructField("_ingested_at", StringType(), True),
        StructField("_source_api", StringType(), True),
    ])

    df = spark.createDataFrame(records, schema=schema)
    
    # Cast timestamp string to native Spark TimestampType for temporal join acceleration
    df = (
        df
        .withColumn("weather_timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm"))
        .withColumn("_ingestion_time", current_timestamp())
    )

    return df


def save_weather_to_delta(
    df: Any,
    table_name: str = STORAGE_CONFIG.BRONZE_WEATHER_TABLE,
    delta_path: Optional[str] = None,
    mode: str = "overwrite",
) -> None:
    """
    Save PySpark DataFrame into Delta Lake format.
    
    Defaults to Managed Delta Table semantics, avoiding cloud file system scheme errors
    in Databricks Unity Catalog while supporting schema evolution with overwriteSchema=true.
    """
    logger.info(f"Writing weather data to Delta table: {table_name} (mode={mode})")
    writer = (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
    )
    # If a fully qualified cloud URI (dbfs:/, s3://, abfss://) is provided, set path option
    if delta_path and delta_path.startswith(("dbfs:/", "s3://", "abfss://", "gs://")):
        writer = writer.option("path", delta_path)
    
    writer.saveAsTable(table_name)
    logger.info(f"Successfully saved and registered Delta table: {table_name}")
