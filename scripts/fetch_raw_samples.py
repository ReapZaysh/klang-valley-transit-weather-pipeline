"""
Fetch raw sample data from Open-Meteo and data.gov.my APIs.
Saves local samples into data/raw/ for inspection, offline testing, and development.
"""

import json
import os
import sys

# Ensure repository root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.config import WEATHER_CONFIG, TRANSIT_CONFIG
from src.ingestion.weather_ingestion import fetch_kl_weather_json, transform_weather_json_to_records
from src.ingestion.transit_ingestion import download_rapidkl_gtfs_archive
from src.utils.logger import get_logger

logger = get_logger("SampleFetcher")


def main():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw"))
    weather_dir = os.path.join(base_dir, "weather")
    gtfs_dir = os.path.join(base_dir, "gtfs")

    os.makedirs(weather_dir, exist_ok=True)
    os.makedirs(gtfs_dir, exist_ok=True)

    # Reconfigure stdout for utf-8 if supported
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    print("=" * 60)
    print("[INIT] Fetching Raw Sample Data for Klang Valley Pipeline")
    print("=" * 60)

    # 1. Fetch Weather Data (e.g., 7 days of historical weather for KL)
    print("\n[1/2] Fetching Open-Meteo Weather Data for Kuala Lumpur...")
    weather_json = fetch_kl_weather_json(
        start_date="2024-01-01",
        end_date="2024-01-07"
    )
    weather_file = os.path.join(weather_dir, "kl_weather_2024-01-01_to_2024-01-07.json")
    with open(weather_file, "w", encoding="utf-8") as f:
        json.dump(weather_json, f, indent=2)
    print(f" Saved raw weather JSON: {weather_file} ({os.path.getsize(weather_file):,} bytes)")

    # Transform and preview records
    records = transform_weather_json_to_records(weather_json)
    records_file = os.path.join(weather_dir, "kl_weather_records_normalized.json")
    with open(records_file, "w", encoding="utf-8") as f:
        json.dump(records[:10], f, indent=2)
    print(f" Saved normalized records preview (10 items): {records_file}")

    # 2. Fetch RapidKL Transit Data (data.gov.my GTFS)
    print("\n[2/2] Fetching data.gov.my RapidKL GTFS static feed...")
    extracted_files = download_rapidkl_gtfs_archive(
        category=TRANSIT_CONFIG.DEFAULT_CATEGORY,
        extract_to_dir=gtfs_dir
    )
    print(f" Extracted {len(extracted_files)} GTFS files into {gtfs_dir}")

    for file_name, path in extracted_files.items():
        size = os.path.getsize(path)
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            header = f.readline().strip()
            line_count = sum(1 for _ in f) + 1
        print(f"   - {file_name}: {size:,} bytes | ~{line_count:,} lines")
        print(f"     Header: {header}")

    print("\n[DONE] Sample data extraction complete!")


if __name__ == "__main__":
    main()
