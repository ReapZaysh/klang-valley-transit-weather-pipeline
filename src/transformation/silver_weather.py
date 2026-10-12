"""
Silver Layer: Weather Data Cleaning & Validation.

Reads from `bronze_kl_weather` and applies:
1. Deduplication on weather_timestamp.
2. Meteorological bounds validation (temperature, precipitation, humidity).
3. Schema enforcement and audit column enrichment.
4. Writes clean records to `silver_kl_weather` Delta table.
"""

from typing import Any, Optional

from src.config import STORAGE_CONFIG, VALIDATION_CONFIG
from src.utils.logger import get_logger

logger = get_logger("SilverWeather")


def deduplicate_weather(df: Any) -> Any:
    """
    Remove duplicate hourly observations based on weather_timestamp.

    Open-Meteo ingestions can produce duplicate rows if a window overlaps a
    previous run. We keep the first occurrence ordered by _ingestion_time to
    retain the most recent ingest run's data.

    Parameters:
        df: Bronze weather PySpark DataFrame.

    Returns:
        Deduplicated PySpark DataFrame.
    """
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, col

    # Assign row numbers within each timestamp partition, ordered by latest ingest first
    window = Window.partitionBy("weather_timestamp").orderBy(col("_ingestion_time").desc())
    df_deduped = (
        df.withColumn("_row_num", row_number().over(window))
          .filter(col("_row_num") == 1)
          .drop("_row_num")
    )

    original_count = df.count()
    clean_count = df_deduped.count()
    dropped = original_count - clean_count
    logger.info(f"Weather deduplication: {original_count:,} → {clean_count:,} rows ({dropped:,} duplicates removed).")
    return df_deduped


def validate_weather_bounds(df: Any) -> tuple:
    """
    Apply meteorological bounds validation for equatorial Malaysia.

    Valid ranges sourced from ValidationConfig:
    - temperature_2m : [{TEMP_MIN_C}°C, {TEMP_MAX_C}°C]
    - precipitation  : >= {PRECIP_MIN_MM} mm
    - relative_humidity_2m: [0%, 100%]

    Parameters:
        df: Deduplicated weather PySpark DataFrame.

    Returns:
        Tuple of (valid_df, invalid_df) PySpark DataFrames.
    """
    from pyspark.sql.functions import col

    v = VALIDATION_CONFIG

    # Rows that pass ALL meteorological validity checks
    valid_mask = (
        # Temperature within realistic KL range
        (col("temperature_2m").isNull() | col("temperature_2m").between(v.TEMP_MIN_C, v.TEMP_MAX_C))
        # Precipitation must be non-negative
        & (col("precipitation").isNull() | (col("precipitation") >= v.PRECIP_MIN_MM))
        # Rainfall must be non-negative
        & (col("rain").isNull() | (col("rain") >= v.PRECIP_MIN_MM))
        # Humidity within 0–100%
        & (col("relative_humidity_2m").isNull() | col("relative_humidity_2m").between(v.HUMIDITY_MIN, v.HUMIDITY_MAX))
    )

    valid_df = df.filter(valid_mask)
    invalid_df = df.filter(~valid_mask)

    logger.info(f"Weather validation: {valid_df.count():,} valid | {invalid_df.count():,} invalid records.")
    return valid_df, invalid_df


def enrich_silver_weather(df: Any) -> Any:
    """
    Add silver layer audit and derived columns to the clean weather DataFrame.

    Adds:
    - _silver_processed_at: Timestamp when silver transformation was applied.
    - _layer: Constant 'silver' for easy cross-layer queries.
    - is_rainy_hour: Boolean flag — precipitation > 0.5 mm indicates meaningful rainfall.

    Parameters:
        df: Validated weather PySpark DataFrame.

    Returns:
        Enriched PySpark DataFrame.
    """
    from pyspark.sql.functions import current_timestamp, lit, col, when

    return (
        df
        .withColumn("is_rainy_hour", when(col("precipitation") > 0.5, True).otherwise(False))
        .withColumn("_layer", lit("silver"))
        .withColumn("_silver_processed_at", current_timestamp())
    )


def save_silver_weather(
    df: Any,
    table_name: str = STORAGE_CONFIG.SILVER_WEATHER_TABLE,
    mode: str = "overwrite",
) -> None:
    """
    Persist the clean silver weather DataFrame to a Managed Delta Table.

    Parameters:
        df         : Enriched silver weather PySpark DataFrame.
        table_name : Target Delta table name (default: silver_kl_weather).
        mode       : Spark write mode ('overwrite' or 'append').
    """
    logger.info(f"Writing silver weather data to Delta table: {table_name} (mode={mode})")
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    logger.info(f"Successfully saved silver weather table: {table_name} ({df.count():,} rows)")


def run_silver_weather_pipeline(
    spark: Any,
    bronze_table: str = STORAGE_CONFIG.BRONZE_WEATHER_TABLE,
    silver_table: str = STORAGE_CONFIG.SILVER_WEATHER_TABLE,
) -> Any:
    """
    End-to-end Silver Weather pipeline orchestrator.

    Reads from the bronze table, applies deduplication and validation,
    enriches with silver audit columns, and writes to the silver table.

    Parameters:
        spark        : Active SparkSession.
        bronze_table : Source bronze Delta table name.
        silver_table : Target silver Delta table name.

    Returns:
        Final silver weather PySpark DataFrame.
    """
    logger.info(f"Starting Silver Weather pipeline: {bronze_table} → {silver_table}")

    # 1. Read from Bronze Delta table
    df_bronze = spark.table(bronze_table)
    logger.info(f"Loaded {df_bronze.count():,} rows from {bronze_table}.")

    # 2. Deduplicate on weather_timestamp
    df_deduped = deduplicate_weather(df_bronze)

    # 3. Validate meteorological bounds — all weather records are retained
    #    (no quarantine for weather since it's a trusted API with no GPS coords)
    df_valid, df_invalid = validate_weather_bounds(df_deduped)

    if df_invalid.count() > 0:
        logger.warning(f"{df_invalid.count():,} weather records failed validation — review bronze source.")

    # 4. Enrich with silver audit columns and derived flags
    df_silver = enrich_silver_weather(df_valid)

    # 5. Persist to Delta Lake
    save_silver_weather(df_silver, table_name=silver_table)

    return df_silver
