"""
Unit and integration tests for Bronze Layer Raw Ingestion.
"""

import json
import os
import unittest
from unittest.mock import patch, MagicMock

from src.config import WEATHER_CONFIG, TRANSIT_CONFIG
from src.ingestion.weather_ingestion import (
    fetch_kl_weather_json,
    transform_weather_json_to_records,
)
from src.ingestion.transit_ingestion import download_rapidkl_gtfs_archive


class TestWeatherIngestion(unittest.TestCase):
    """Test suite for Open-Meteo weather ingestion."""

    def setUp(self):
        self.mock_weather_payload = {
            "latitude": 3.14,
            "longitude": 101.69,
            "elevation": 50.0,
            "timezone": "Asia/Kuala_Lumpur",
            "hourly": {
                "time": ["2024-01-01T00:00", "2024-01-01T01:00"],
                "temperature_2m": [25.5, 24.8],
                "precipitation": [0.0, 12.4],
                "rain": [0.0, 12.4],
                "weather_code": [1, 65],
                "relative_humidity_2m": [85.0, 92.0],
                "wind_speed_10m": [5.2, 8.4],
            },
        }

    def test_transform_weather_json_to_records(self):
        """Verify transformation of Open-Meteo parallel arrays into normalized records."""
        records = transform_weather_json_to_records(self.mock_weather_payload)
        self.assertEqual(len(records), 2)
        
        first = records[0]
        self.assertEqual(first["timestamp"], "2024-01-01T00:00")
        self.assertEqual(first["temperature_2m"], 25.5)
        self.assertEqual(first["precipitation"], 0.0)
        self.assertEqual(first["weather_code"], 1)
        self.assertEqual(first["_source_api"], "open-meteo")
        self.assertIn("_ingested_at", first)

        second = records[1]
        self.assertEqual(second["timestamp"], "2024-01-01T01:00")
        self.assertEqual(second["precipitation"], 12.4)
        self.assertEqual(second["rain"], 12.4)

    @patch("requests.get")
    def test_fetch_kl_weather_json_mock(self, mock_get):
        """Verify fetch_kl_weather_json constructs correct URL parameters."""
        mock_response = MagicMock()
        mock_response.json.return_value = self.mock_weather_payload
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        data = fetch_kl_weather_json("2024-01-01", "2024-01-02")
        self.assertIn("hourly", data)
        mock_get.assert_called_once()
        args, kwargs = mock_get.call_args
        self.assertEqual(kwargs["params"]["latitude"], WEATHER_CONFIG.LATITUDE)
        self.assertEqual(kwargs["params"]["longitude"], WEATHER_CONFIG.LONGITUDE)
        self.assertEqual(kwargs["params"]["start_date"], "2024-01-01")


class TestTransitIngestion(unittest.TestCase):
    """Test suite for data.gov.my RapidKL GTFS static ingestion."""

    def test_extracted_gtfs_files_exist_and_valid(self):
        """Verify local GTFS extracted files have non-empty content and valid headers."""
        sample_gtfs_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "data", "raw", "gtfs")
        )
        if not os.path.exists(sample_gtfs_dir):
            self.skipTest("Sample GTFS directory data/raw/gtfs not found. Run fetch script first.")

        expected_files = ["routes.txt", "trips.txt", "stops.txt", "stop_times.txt"]
        for fname in expected_files:
            file_path = os.path.join(sample_gtfs_dir, fname)
            self.assertTrue(os.path.exists(file_path), f"Expected file {fname} to exist")
            self.assertGreater(os.path.getsize(file_path), 0, f"File {fname} is empty")

            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                header = f.readline().strip().split(",")
                self.assertGreater(len(header), 2, f"Header for {fname} should have multiple columns")


if __name__ == "__main__":
    unittest.main()
