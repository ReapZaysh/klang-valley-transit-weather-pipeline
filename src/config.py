"""
Pipeline Configuration & Constants for Klang Valley Transit & Weather Data Pipeline.
"""

import os
from dataclasses import dataclass, field
from typing import List


@dataclass(frozen=True)
class WeatherConfig:
    """Configuration for Open-Meteo Weather API."""
    LATITUDE: float = 3.1390
    LONGITUDE: float = 101.6869
    TIMEZONE: str = "Asia/Kuala_Lumpur"
    ARCHIVE_URL: str = "https://archive-api.open-meteo.com/v1/archive"
    FORECAST_URL: str = "https://api.open-meteo.com/v1/forecast"
    HOURLY_VARIABLES: List[str] = field(default_factory=lambda: [
        "temperature_2m",
        "precipitation",
        "rain",
        "weather_code",
        "relative_humidity_2m",
        "wind_speed_10m"
    ])


@dataclass(frozen=True)
class TransitConfig:
    """Configuration for data.gov.my RapidKL Transit API."""
    GTFS_STATIC_URL: str = "https://api.data.gov.my/gtfs-static/prasarana"
    GTFS_REALTIME_URL: str = "https://api.data.gov.my/gtfs-realtime/vehicle-position/prasarana"
    DEFAULT_CATEGORY: str = "rapid-bus-kl"
    CORE_GTFS_FILES: List[str] = field(default_factory=lambda: [
        "routes.txt",
        "trips.txt",
        "stops.txt",
        "stop_times.txt"
    ])


@dataclass(frozen=True)
class ValidationConfig:
    """
    Data quality thresholds and geographic bounds for the Silver Layer.
    Klang Valley bounding box covers Greater KL from Rawang to Sepang.
    """
    # Geographic bounding box for Klang Valley / Greater KL
    LAT_MIN: float = 2.8
    LAT_MAX: float = 3.5
    LON_MIN: float = 101.3
    LON_MAX: float = 101.9

    # Reasonable meteorological bounds for equatorial Malaysia
    TEMP_MIN_C: float = 15.0     # Below this is unrealistic for KL
    TEMP_MAX_C: float = 45.0     # Above this is unrealistic for KL
    PRECIP_MIN_MM: float = 0.0   # Precipitation cannot be negative
    HUMIDITY_MIN: float = 0.0
    HUMIDITY_MAX: float = 100.0


@dataclass(frozen=True)
class StorageConfig:
    """Storage paths for Databricks Delta Lake Medallion Architecture."""
    # Fully qualified cloud scheme for DBFS
    DBFS_BASE_PATH: str = "dbfs:/FileStore/klang_valley_transit"
    
    # Bronze layer Delta paths (with dbfs:/ scheme)
    BRONZE_WEATHER_PATH: str = "dbfs:/FileStore/klang_valley_transit/bronze/kl_weather"
    BRONZE_TRANSIT_PATH: str = "dbfs:/FileStore/klang_valley_transit/bronze/rapidkl_transit"
    
    # Managed Delta table names (Standard for Unity Catalog / Hive Metastore)
    BRONZE_WEATHER_TABLE: str = "bronze_kl_weather"
    BRONZE_TRANSIT_TABLE: str = "bronze_rapidkl_transit"
    
    # Silver layer — cleaned, validated, deduplicated tables
    SILVER_WEATHER_TABLE: str = "silver_kl_weather"
    SILVER_TRANSIT_TABLE: str = "silver_rapidkl_transit"
    SILVER_QUARANTINE_TABLE: str = "silver_quarantine_transit"

    # Gold layer — joined & aggregated analytical table
    SILVER_JOINED_TABLE: str = "silver_rapidkl_weather_delays"
    GOLD_METRICS_TABLE: str = "gold_route_rain_impact_metrics"


WEATHER_CONFIG = WeatherConfig()
TRANSIT_CONFIG = TransitConfig()
VALIDATION_CONFIG = ValidationConfig()
STORAGE_CONFIG = StorageConfig()
