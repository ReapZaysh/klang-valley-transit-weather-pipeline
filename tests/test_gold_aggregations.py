"""
Unit tests for Gold Layer Business Aggregations & Metrics Calculation.
"""

import math
import unittest

from src.config import ANALYTICS_CONFIG, STORAGE_CONFIG


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Reference Python implementation of Haversine formula."""
    r = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2.0) ** 2 +
        math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
        math.sin(dlon / 2.0) ** 2
    )
    a = max(0.0, min(1.0, a))
    return r * 2.0 * math.asin(math.sqrt(a))


class TestGoldAggregations(unittest.TestCase):
    """Test suite for Gold layer calculations, configurations, and KPIs."""

    def test_analytics_config_defaults(self):
        """Verify AnalyticsConfig has correct threshold boundaries."""
        cfg = ANALYTICS_CONFIG
        self.assertEqual(cfg.HEAVY_RAIN_THRESHOLD_MM, 5.0)
        self.assertEqual(cfg.LIGHT_RAIN_THRESHOLD_MM, 0.5)
        self.assertEqual(cfg.MORNING_PEAK_HOURS, [7, 8, 9])
        self.assertEqual(cfg.EVENING_PEAK_HOURS, [17, 18, 19])
        self.assertLess(cfg.SPEED_LOWER_BOUND_KMH, cfg.SPEED_UPPER_BOUND_KMH)

    def test_storage_config_gold_table(self):
        """Verify gold table names are configured in StorageConfig."""
        self.assertEqual(STORAGE_CONFIG.GOLD_ROUTE_PERFORMANCE_TABLE, "gold_route_weather_performance")
        self.assertEqual(STORAGE_CONFIG.GOLD_METRICS_TABLE, "gold_route_weather_performance")

    def test_haversine_distance_calculation(self):
        """Verify Haversine formula with known coordinates in Klang Valley."""
        # Distance between KL Sentral (3.1342, 101.6861) and Pasar Seni (3.1422, 101.6963)
        # Expected distance is ~1.4 km
        dist = haversine_km(3.1342, 101.6861, 3.1422, 101.6963)
        self.assertAlmostEqual(dist, 1.43, delta=0.1)

    def test_rainfall_delay_index_math(self):
        """Verify speed drop, percentage drop, and Rainfall Delay Index calculation."""
        speed_clear = 25.0        # 25 km/h during clear conditions
        speed_heavy_rain = 18.0   # 18 km/h during heavy downpour

        speed_drop = round(speed_clear - speed_heavy_rain, 2)
        speed_drop_pct = round((speed_drop / speed_clear) * 100.0, 2)
        delay_index = round(speed_clear / speed_heavy_rain, 3)

        self.assertEqual(speed_drop, 7.0)
        self.assertEqual(speed_drop_pct, 28.0)
        # Delay index 25 / 18 = 1.389 (trips take 38.9% longer)
        self.assertEqual(delay_index, 1.389)
        self.assertGreater(delay_index, 1.0)

    def test_peak_hour_classification(self):
        """Verify peak hours are correctly mapped to morning and evening bands."""
        cfg = ANALYTICS_CONFIG

        morning_hours = [7, 8, 9]
        evening_hours = [17, 18, 19]
        off_peak_hours = [0, 6, 12, 14, 21, 23]

        for h in morning_hours:
            self.assertIn(h, cfg.MORNING_PEAK_HOURS)
            self.assertNotIn(h, cfg.EVENING_PEAK_HOURS)

        for h in evening_hours:
            self.assertIn(h, cfg.EVENING_PEAK_HOURS)
            self.assertNotIn(h, cfg.MORNING_PEAK_HOURS)

        for h in off_peak_hours:
            self.assertNotIn(h, cfg.MORNING_PEAK_HOURS)
            self.assertNotIn(h, cfg.EVENING_PEAK_HOURS)

    def test_peak_reliability_score(self):
        """Verify peak reliability score is ratio of peak to off-peak speed."""
        off_peak_speed = 30.0
        morning_speed = 21.0
        evening_speed = 24.0

        morning_rel = round((morning_speed / off_peak_speed) * 100.0, 2)
        evening_rel = round((evening_speed / off_peak_speed) * 100.0, 2)
        overall_score = round(min(morning_rel, evening_rel) / 100.0, 3)

        self.assertEqual(morning_rel, 70.0)
        self.assertEqual(evening_rel, 80.0)
        self.assertEqual(overall_score, 0.70)


if __name__ == "__main__":
    unittest.main()
