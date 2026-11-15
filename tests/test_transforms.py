import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from processing.transforms import (
    classify_alert_level,
    classify_weather,
    compute_congestion_score,
    compute_demand_intensity,
    compute_predicted_delay_mins,
    compute_service_deficit,
    haversine_meters,
    next_hour_slot,
    normalize_station_name,
    normalize_stop_id,
    spark_day_of_week,
)


class TransformTests(unittest.TestCase):
    def test_normalize_stop_id_strips_direction(self):
        self.assertEqual(normalize_stop_id("127N"), "127")
        self.assertEqual(normalize_stop_id("127S"), "127")
        self.assertEqual(normalize_stop_id(" L06 "), "L06")

    def test_normalize_station_name(self):
        self.assertEqual(normalize_station_name("Times Sq-42 St"), "times sq 42 st")
        self.assertEqual(normalize_station_name("Jay St-MetroTech"), "jay st metrotech")

    def test_haversine_meters(self):
        self.assertLess(haversine_meters(40.7558, -73.9878, 40.7558, -73.9878), 0.1)
        self.assertGreater(haversine_meters(40.7558, -73.9878, 40.7527, -73.9772), 900)

    def test_weather_bucket(self):
        self.assertEqual(classify_weather(0.0, 0.0), "clear")
        self.assertEqual(classify_weather(0.2, 0.0), "rain")
        self.assertEqual(classify_weather(0.2, 0.2), "snow")

    def test_service_deficit(self):
        self.assertEqual(compute_service_deficit(30), 0.0)
        self.assertAlmostEqual(compute_service_deficit(180), 0.5)
        self.assertEqual(compute_service_deficit(600), 1.0)

    def test_demand_and_score(self):
        self.assertEqual(compute_demand_intensity(50, 0), 0.0)
        self.assertAlmostEqual(compute_demand_intensity(50, 100), 0.5)
        self.assertAlmostEqual(compute_congestion_score(0.6, 0.5), 0.3)

    def test_predicted_delay_and_alert(self):
        self.assertAlmostEqual(compute_predicted_delay_mins(180, 0.5), 4.5)
        self.assertEqual(classify_alert_level(0.1), "NORMAL")
        self.assertEqual(classify_alert_level(0.2), "MODERATE")
        self.assertEqual(classify_alert_level(0.5), "SEVERE")


    def test_spark_day_of_week_matches_spark_numbering(self):
        nyc = ZoneInfo("America/New_York")
        self.assertEqual(spark_day_of_week(datetime(2026, 11, 8, 12, tzinfo=nyc)), 1)   # Sunday
        self.assertEqual(spark_day_of_week(datetime(2026, 11, 9, 12, tzinfo=nyc)), 2)   # Monday
        self.assertEqual(spark_day_of_week(datetime(2026, 11, 11, 12, tzinfo=nyc)), 4)  # Wednesday
        self.assertEqual(spark_day_of_week(datetime(2026, 11, 14, 12, tzinfo=nyc)), 7)  # Saturday

    def test_next_hour_slot_rolls_over_midnight(self):
        nyc = ZoneInfo("America/New_York")
        self.assertEqual(next_hour_slot(datetime(2026, 11, 9, 8, 30, tzinfo=nyc)), (9, 2))
        # Monday 23:30 -> Tuesday 00:00
        self.assertEqual(next_hour_slot(datetime(2026, 11, 9, 23, 30, tzinfo=nyc)), (0, 3))
        # Saturday 23:10 -> Sunday 00:00
        self.assertEqual(next_hour_slot(datetime(2026, 11, 14, 23, 10, tzinfo=nyc)), (0, 1))

    def test_next_hour_slot_converts_utc_to_nyc(self):
        utc = ZoneInfo("UTC")
        # 2026-11-10 03:30 UTC is Monday 22:30 NYC (EST) -> next slot is 23:00 Monday
        self.assertEqual(next_hour_slot(datetime(2026, 11, 10, 3, 30, tzinfo=utc)), (23, 2))


if __name__ == "__main__":
    unittest.main()
