"""
tests/conftest.py

Shared pytest fixtures for Silver and Gold layer unit tests.
Uses a local SparkSession (no Databricks required).
"""

import pytest
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, LongType, IntegerType, TimestampType,
)


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession for unit testing (no Databricks or AWS required)."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("skystream-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


# ── Bronze schema (flat JSON after Lambda parsing) ────────────────────────────
BRONZE_SCHEMA = StructType([
    StructField("icao24",          StringType(),  True),
    StructField("callsign",        StringType(),  True),
    StructField("origin_country",  StringType(),  True),
    StructField("time_position",   LongType(),    True),
    StructField("last_contact",    LongType(),    True),
    StructField("longitude",       DoubleType(),  True),
    StructField("latitude",        DoubleType(),  True),
    StructField("baro_altitude",   DoubleType(),  True),
    StructField("on_ground",       BooleanType(), True),
    StructField("velocity",        DoubleType(),  True),
    StructField("true_track",      DoubleType(),  True),
    StructField("vertical_rate",   DoubleType(),  True),
    StructField("geo_altitude",    DoubleType(),  True),
    StructField("squawk",          StringType(),  True),
    StructField("spi",             BooleanType(), True),
    StructField("position_source", IntegerType(), True),
    StructField("snapshot_time",   LongType(),    True),
    StructField("ingestion_time",  StringType(),  True),
])

# ── Silver schema (after transformation) ──────────────────────────────────────
SILVER_SCHEMA = StructType([
    StructField("icao24",           StringType(),    True),
    StructField("callsign",         StringType(),    True),
    StructField("origin_country",   StringType(),    True),
    StructField("airline_icao",     StringType(),    True),
    StructField("carrier_name",     StringType(),    True),
    StructField("is_cargo",         BooleanType(),   True),
    StructField("latitude",         DoubleType(),    True),
    StructField("longitude",        DoubleType(),    True),
    StructField("altitude_m",       DoubleType(),    True),
    StructField("altitude_ft",      DoubleType(),    True),
    StructField("on_ground",        BooleanType(),   True),
    StructField("velocity_ms",      DoubleType(),    True),
    StructField("velocity_kmh",     DoubleType(),    True),
    StructField("true_track",       DoubleType(),    True),
    StructField("vertical_rate",    DoubleType(),    True),
    StructField("flight_phase",     StringType(),    True),
    StructField("lat_bin",          IntegerType(),   True),
    StructField("lon_bin",          IntegerType(),   True),
    StructField("snapshot_time",    LongType(),      True),
    StructField("snapshot_time_ts", TimestampType(), True),
])


@pytest.fixture(scope="session")
def sample_bronze_rows():
    """Raw aircraft state rows mimicking Lambda output."""
    ts = int(datetime(2024, 5, 20, 12, 0, 0, tzinfo=timezone.utc).timestamp())
    return [
        # Normal cruising flight
        ("a1b2c3", "UAL123  ", "United States", ts, ts, -87.6298, 41.8781, 10500.0, False, 245.0, 95.0,  0.1, 10200.0, "1234", False, 0, ts, "2024-05-20T12:00:00Z"),
        # Climbing flight
        ("d4e5f6", "BAW456  ", "United Kingdom", ts, ts,   -0.45, 51.47,   4000.0, False, 210.0, 10.0,  8.5,  3800.0, "2345", False, 0, ts, "2024-05-20T12:00:00Z"),
        # Descending (rapid)
        ("g7h8i9", "SIA789  ", "Singapore",      ts, ts,  103.99,  1.36,   3000.0, False, 180.0, 180.0,-16.0,  2900.0, "3456", False, 0, ts, "2024-05-20T12:00:00Z"),
        # On ground
        ("j1k2l3", "VNA100  ", "Vietnam",        ts, ts,  106.65, 10.82,      0.0, True,    5.0, 90.0,  0.0,     0.0, "4567", False, 0, ts, "2024-05-20T12:00:00Z"),
        # FedEx cargo
        ("m4n5o6", "FDX001  ", "United States",  ts, ts,  -90.00, 35.00,   9000.0, False, 240.0, 270.0, 0.0,  8800.0, "5678", False, 0, ts, "2024-05-20T12:00:00Z"),
        # Invalid: null latitude (should be filtered)
        ("p7q8r9", "BAD001  ", "Unknown",        ts, ts,   0.0,   None,    1000.0, False, 100.0,  0.0,  0.0,  1000.0, "6789", False, 0, ts, "2024-05-20T12:00:00Z"),
        # Callsign with trailing spaces
        ("s1t2u3", "DLH500  ", "Germany",        ts, ts,    8.57, 50.04,  11000.0, False, 255.0, 270.0, 0.2, 10800.0, "7890", False, 0, ts, "2024-05-20T12:00:00Z"),
    ]
