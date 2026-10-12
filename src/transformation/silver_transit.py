"""
Silver Layer: Transit Data Cleaning, Validation & Quarantine.

Reads from `bronze_rapidkl_transit` and applies:
1. Deduplication on (trip_id, stop_id, stop_sequence).
2. Bounding Box Validation: filters GPS coordinates outside Klang Valley bounds.
3. Data Quality Quarantine: rows with missing route_id or out-of-bounds coordinates
   are routed to `silver_quarantine_transit`.
4. Timestamp Alignment: GTFS arrival/departure times are parsed and rounded to the
   nearest hour, producing a `scheduled_hour_aligned` column for weather joins.
5. Writes clean records to `silver_rapidkl_transit` Delta table.

Notes on GTFS Timestamps
------------------------
GTFS stop_times.txt represents times as "HH:MM:SS" strings that can exceed 24:00:00
(e.g., "25:30:00" = 1:30am the next service day). This module normalises such values
with a modulo-24 operation and extracts the aligned integer hour (0–23) for joining
with Open-Meteo's hourly weather observations.
"""

from typing import Any, Optional, Tuple

from src.config import STORAGE_CONFIG, VALIDATION_CONFIG
from src.utils.logger import get_logger

logger = get_logger("SilverTransit")


def deduplicate_transit(df: Any) -> Any:
    """
    Remove duplicate stop-time records based on the composite key
    (trip_id, stop_id, stop_sequence).

    A duplicate can arise if the GTFS feed is re-downloaded mid-pipeline or
    if the same trip appears in multiple service calendar dates.

    Parameters:
        df: Bronze transit PySpark DataFrame.

    Returns:
        Deduplicated PySpark DataFrame.
    """
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, col

    # Partition by the logical primary key of a scheduled stop event
    window = Window.partitionBy("trip_id", "stop_id", "stop_sequence") \
                   .orderBy(col("_ingested_at").desc())

    df_deduped = (
        df.withColumn("_row_num", row_number().over(window))
          .filter(col("_row_num") == 1)
          .drop("_row_num")
    )

    original_count = df.count()
    clean_count = df_deduped.count()
    dropped = original_count - clean_count
    logger.info(
        f"Transit deduplication: {original_count:,} → {clean_count:,} rows "
        f"({dropped:,} duplicates removed)."
    )
    return df_deduped


def apply_bounding_box_and_quarantine(df: Any) -> Tuple[Any, Any]:
    """
    Split the transit DataFrame into clean and quarantine partitions.

    Quarantine conditions (either condition triggers quarantine):
    1. `route_id` is NULL or empty — the stop cannot be attributed to a route.
    2. `stop_lat` or `stop_lon` are NULL — coordinates are required for weather joins.
    3. `stop_lat` or `stop_lon` fall outside the Klang Valley bounding box:
       Lat [{LAT_MIN}, {LAT_MAX}] × Lon [{LON_MIN}, {LON_MAX}].

    Parameters:
        df: Deduplicated transit PySpark DataFrame.

    Returns:
        Tuple of (clean_df, quarantine_df) PySpark DataFrames.
    """
    from pyspark.sql.functions import col, trim, when, lit, current_timestamp

    v = VALIDATION_CONFIG

    # --- Define individual quality rules ---

    # Rule 1: Missing or blank route_id (cannot be attributed to any route)
    missing_route = col("route_id").isNull() | (trim(col("route_id")) == "")

    # Rule 2: Missing GPS coordinates (cannot join with weather or be mapped)
    missing_coords = col("stop_lat").isNull() | col("stop_lon").isNull()

    # Rule 3: GPS coordinates outside Klang Valley bounding box
    out_of_bounds = (
        (col("stop_lat") < v.LAT_MIN) | (col("stop_lat") > v.LAT_MAX) |
        (col("stop_lon") < v.LON_MIN) | (col("stop_lon") > v.LON_MAX)
    )

    # A row is quarantined if ANY rule fails
    quarantine_mask = missing_route | missing_coords | out_of_bounds

    # Tag quarantine rows with the reason for downstream diagnostics
    quarantine_df = (
        df.filter(quarantine_mask)
          .withColumn(
              "_quarantine_reason",
              when(missing_route, lit("missing_route_id"))
              .when(missing_coords, lit("missing_gps_coordinates"))
              .when(out_of_bounds, lit("gps_outside_klang_valley_bounds"))
              .otherwise(lit("unknown"))
          )
          .withColumn("_quarantined_at", current_timestamp())
    )

    # Clean rows pass all quality rules
    clean_df = df.filter(~quarantine_mask)

    clean_count = clean_df.count()
    quarantine_count = quarantine_df.count()
    logger.info(
        f"Bounding box & quarantine split: {clean_count:,} clean | "
        f"{quarantine_count:,} quarantined."
    )
    return clean_df, quarantine_df


def align_timestamps(df: Any) -> Any:
    """
    Parse GTFS HH:MM:SS time strings and round to the nearest hour.

    GTFS times can be > 24:00:00 for overnight trips. This function:
    1. Extracts the integer hour from `departure_time` (e.g., "08:34:00" → 8).
    2. Applies modulo 24 to normalise overnight times (e.g., "25:30:00" → 1).
    3. Rounds to nearest hour: if minutes >= 30, rounds up (modulo 24).
    4. Produces `scheduled_hour_aligned` (0–23) for joining with weather data.

    Parameters:
        df: Clean transit PySpark DataFrame with `departure_time` string column.

    Returns:
        DataFrame with `scheduled_hour_aligned` integer column added.
    """
    from pyspark.sql.functions import col, split, when, expr

    # Split "HH:MM:SS" into parts and extract hour and minute as integers
    df = (
        df.withColumn("_dep_parts", split(col("departure_time"), ":"))
          .withColumn("_dep_hour_raw", col("_dep_parts")[0].cast("int"))
          .withColumn("_dep_minute", col("_dep_parts")[1].cast("int"))
    )

    # Round to nearest hour (add 1 if minutes >= 30), then normalise with mod 24
    df = df.withColumn(
        "scheduled_hour_aligned",
        when(col("_dep_minute") >= 30,
             ((col("_dep_hour_raw") + 1) % 24).cast("int"))
        .otherwise(
             (col("_dep_hour_raw") % 24).cast("int"))
    ).drop("_dep_parts", "_dep_hour_raw", "_dep_minute")

    logger.info("Timestamp alignment complete: 'scheduled_hour_aligned' column added.")
    return df


def enrich_silver_transit(df: Any) -> Any:
    """
    Append silver layer audit metadata to the clean transit DataFrame.

    Adds:
    - _layer: Constant 'silver' for easy cross-layer queries.
    - _silver_processed_at: Timestamp when silver transformation ran.

    Parameters:
        df: Clean, timestamp-aligned transit PySpark DataFrame.

    Returns:
        Enriched PySpark DataFrame.
    """
    from pyspark.sql.functions import current_timestamp, lit

    return (
        df
        .withColumn("_layer", lit("silver"))
        .withColumn("_silver_processed_at", current_timestamp())
    )


def save_silver_transit(
    df: Any,
    table_name: str = STORAGE_CONFIG.SILVER_TRANSIT_TABLE,
    mode: str = "overwrite",
) -> None:
    """
    Persist clean silver transit records to a Managed Delta Table.

    Parameters:
        df         : Enriched clean transit PySpark DataFrame.
        table_name : Target Delta table (default: silver_rapidkl_transit).
        mode       : Spark write mode ('overwrite' or 'append').
    """
    logger.info(f"Writing silver transit data to Delta table: {table_name} (mode={mode})")
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    logger.info(f"Successfully saved silver transit table: {table_name} ({df.count():,} rows)")


def save_quarantine_transit(
    df: Any,
    table_name: str = STORAGE_CONFIG.SILVER_QUARANTINE_TABLE,
    mode: str = "overwrite",
) -> None:
    """
    Persist quarantined transit records to a separate Managed Delta Table.

    Maintaining a dedicated quarantine table enables:
    - Auditing data quality issues per feed category and ingestion date.
    - Re-processing quarantined rows after upstream GTFS feed corrections.

    Parameters:
        df         : Quarantine PySpark DataFrame (with _quarantine_reason column).
        table_name : Target Delta table (default: silver_quarantine_transit).
        mode       : Spark write mode ('overwrite' or 'append').
    """
    logger.info(f"Writing quarantine data to Delta table: {table_name} (mode={mode})")
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    logger.info(f"Successfully saved quarantine table: {table_name} ({df.count():,} rows)")


def run_silver_transit_pipeline(
    spark: Any,
    bronze_table: str = STORAGE_CONFIG.BRONZE_TRANSIT_TABLE,
    silver_table: str = STORAGE_CONFIG.SILVER_TRANSIT_TABLE,
    quarantine_table: str = STORAGE_CONFIG.SILVER_QUARANTINE_TABLE,
) -> Any:
    """
    End-to-end Silver Transit pipeline orchestrator.

    Reads from bronze, deduplicates, validates bounds, quarantines bad rows,
    aligns timestamps, enriches with audit columns, and writes outputs.

    Parameters:
        spark            : Active SparkSession.
        bronze_table     : Source bronze Delta table name.
        silver_table     : Target clean silver Delta table name.
        quarantine_table : Target quarantine Delta table name.

    Returns:
        Final clean silver transit PySpark DataFrame.
    """
    logger.info(
        f"Starting Silver Transit pipeline: {bronze_table} → "
        f"{silver_table} + {quarantine_table}"
    )

    # 1. Read from Bronze Delta table
    df_bronze = spark.table(bronze_table)
    logger.info(f"Loaded {df_bronze.count():,} rows from {bronze_table}.")

    # 2. Deduplicate on (trip_id, stop_id, stop_sequence)
    df_deduped = deduplicate_transit(df_bronze)

    # 3. Bounding box validation + quarantine split
    df_clean, df_quarantine = apply_bounding_box_and_quarantine(df_deduped)

    # 4. Align GTFS departure times to nearest hour
    df_aligned = align_timestamps(df_clean)

    # 5. Enrich with silver audit metadata
    df_silver = enrich_silver_transit(df_aligned)

    # 6. Persist clean silver table
    save_silver_transit(df_silver, table_name=silver_table)

    # 7. Persist quarantine table (always overwrite to reflect latest run)
    save_quarantine_transit(df_quarantine, table_name=quarantine_table)

    return df_silver
