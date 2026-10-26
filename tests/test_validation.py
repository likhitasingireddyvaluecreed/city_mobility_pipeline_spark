import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from city_mobility.validation import validate_dataset


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("ValidationTests")
        .getOrCreate()
    )
    yield session
    session.stop()


def check_trip(spark, duration="30", parsed_duration=30.0,
               start_station="S001", trip_id="T001"):

    trip = {
        "trip_id": trip_id,
        "user_id": "U001",
        "bike_id": "BK001",
        "start_station_id": start_station,
        "end_station_id": "S002",
        "start_time": "2026-08-01 10:00:00",
        "end_time": "2026-08-01 10:30:00",
        "duration_minutes": duration,
        "trip_status": "completed",
        "duration_minutes_parsed": parsed_duration,
        "start_time_parsed": datetime(2026, 8, 1, 10, 0, 0),
        "end_time_parsed": datetime(2026, 8, 1, 10, 30, 0),
    }

    trips = spark.createDataFrame([trip])

    datasets = {
        "trips": trips,
        "users": spark.createDataFrame([("U001",)], ["user_id"]),
        "bikes": spark.createDataFrame([("BK001",)], ["bike_id"]),
        "stations": spark.createDataFrame(
            [("S001",), ("S002",)], ["station_id"]
        ),
    }

    return validate_dataset("trips", trips, datasets)


def test_valid_trip_passes(spark):
    clean, quarantined = check_trip(spark)
    assert clean.count() == 1
    assert quarantined.count() == 0


def test_negative_duration_is_quarantined(spark):
    clean, quarantined = check_trip(
        spark, duration="-5", parsed_duration=-5.0
    )
    assert quarantined.count() == 1


def test_duration_mismatch_is_quarantined(spark):
    clean, quarantined = check_trip(
        spark, duration="10", parsed_duration=10.0
    )
    assert quarantined.count() == 1


def test_unmatched_station_is_quarantined(spark):
    clean, quarantined = check_trip(
        spark, start_station="S999"
    )
    assert quarantined.count() == 1


def test_duplicate_trip_ids_are_quarantined(spark):
    trip = {
        "trip_id": "T001",
        "user_id": "U001",
        "bike_id": "BK001",
        "start_station_id": "S001",
        "end_station_id": "S002",
        "start_time": "2026-08-01 10:00:00",
        "end_time": "2026-08-01 10:30:00",
        "duration_minutes": "30",
        "trip_status": "completed",
        "duration_minutes_parsed": 30.0,
        "start_time_parsed": datetime(2026, 8, 1, 10, 0, 0),
        "end_time_parsed": datetime(2026, 8, 1, 10, 30, 0),
    }

    trips = spark.createDataFrame([trip, trip])

    datasets = {
        "trips": trips,
        "users": spark.createDataFrame([("U001",)], ["user_id"]),
        "bikes": spark.createDataFrame([("BK001",)], ["bike_id"]),
        "stations": spark.createDataFrame(
            [("S001",), ("S002",)], ["station_id"]
        ),
    }

    clean, quarantined = validate_dataset("trips", trips, datasets)
    assert quarantined.count() == 2