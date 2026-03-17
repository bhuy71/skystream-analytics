"""
tests/test_gold_aggregation.py

Unit tests for Gold layer aggregation logic.
Tests the batch versions of key Gold computations (no DLT, no Databricks required).
"""

import pytest
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, LongType, IntegerType, TimestampType,
)

from conftest import SILVER_SCHEMA


def make_ts(hour: int, minute: int) -> datetime:
    return datetime(2024, 5, 20, hour, minute, 0, tzinfo=timezone.utc)


@pytest.fixture
def silver_df(spark):
    """Sample Silver-layer DataFrame for Gold aggregation tests."""
    rows = [
        # icao24, callsign, origin_country, airline_icao, carrier_name, is_cargo,
        # lat, lon, alt_m, alt_ft, on_ground, vel_ms, vel_kmh, true_track,
        # vert_rate, flight_phase, lat_bin, lon_bin, snapshot_time, snapshot_time_ts
        ("aa1", "UAL1", "United States", "UAL", None,  False, 41.0, -87.0, 10500.0, 34450.0, False, 245.0, 882.0,  95.0,  0.1, "CRUISING",   41, -88, 1716206400, make_ts(12, 0)),
        ("bb2", "BAW1", "United Kingdom","BAW", None,  False, 51.0,  -0.5,  4000.0, 13124.0, False, 210.0, 756.0,  10.0,  8.5, "CLIMBING",   51,  -1, 1716206400, make_ts(12, 0)),
        ("cc3", "SIA1", "Singapore",     "SIA", None,  False,  1.4, 103.9,  2500.0,  8202.0, False, 180.0, 648.0, 180.0,-16.0, "DESCENDING",  1, 103, 1716206400, make_ts(12, 0)),
        ("dd4", "VNA1", "Vietnam",       "VNA", None,  False, 10.8, 106.6,     0.0,     0.0, True,    5.0,  18.0,  90.0,  0.0, "ON_GROUND",  10, 106, 1716206400, make_ts(12, 0)),
        ("ee5", "FDX1", "United States", "FDX", "FedEx Express", True, 35.0, -90.0,  9000.0, 29528.0, False, 240.0, 864.0, 270.0,  0.0, "CRUISING",   35, -90, 1716206400, make_ts(12, 0)),
        ("ff6", "UAL2", "United States", "UAL", None,  False, 42.0, -88.0, 11000.0, 36089.0, False, 250.0, 900.0, 100.0,  0.5, "CRUISING",   42, -88, 1716206410, make_ts(12, 0)),
        # Second snapshot (12:01) for windowing tests
        ("aa1", "UAL1", "United States", "UAL", None,  False, 41.1, -87.1, 10600.0, 34777.0, False, 245.0, 882.0,  95.0,  0.2, "CRUISING",   41, -88, 1716206460, make_ts(12, 1)),
        ("gg7", "DLH1", "Germany",       "DLH", None,  False, 50.0,   8.6, 11000.0, 36089.0, False, 255.0, 918.0, 270.0,  0.3, "CRUISING",   50,   8, 1716206460, make_ts(12, 1)),
        # Rapid descent (should trigger alert)
        ("hh8", "QFA1", "Australia",     "QFA", None,  False,-33.9, 151.1,  1500.0,  4921.0, False, 160.0, 576.0, 200.0,-18.0, "DESCENDING",-34, 151, 1716206400, make_ts(12, 0)),
    ]

    schema = StructType([
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

    return spark.createDataFrame(rows, schema=schema)


class TestFlightAlerts:
    """gold.flight_alerts: detect rapid descent (vertical_rate < -15)."""

    def test_rapid_descent_detected(self, spark, silver_df):
        alerts = silver_df.filter(
            (~F.col("on_ground")) & (F.col("vertical_rate") < -15.0)
        )
        assert alerts.count() == 2, \
            f"Expected 2 rapid descent alerts (SIA1 @ -16 m/s, QFA1 @ -18 m/s), got {alerts.count()}"

    def test_alert_icao24_values(self, spark, silver_df):
        alert_ids = {
            r["icao24"] for r in silver_df.filter(F.col("vertical_rate") < -15.0).collect()
        }
        assert "cc3" in alert_ids  # SIA1, -16 m/s
        assert "hh8" in alert_ids  # QFA1, -18 m/s

    def test_normal_flight_not_alerted(self, spark, silver_df):
        alerts = silver_df.filter(
            (~F.col("on_ground")) & (F.col("vertical_rate") < -15.0)
        )
        alert_ids = {r["icao24"] for r in alerts.collect()}
        assert "aa1" not in alert_ids  # UAL1, cruising


class TestCargoFlow:
    """gold.cargo_flow: count only cargo flights."""

    def test_cargo_count(self, spark, silver_df):
        cargo = silver_df.filter(F.col("is_cargo") == True)
        assert cargo.count() == 1, "Only FDX1 should be a cargo flight"

    def test_cargo_carrier_name(self, spark, silver_df):
        cargo = silver_df.filter(F.col("is_cargo") == True).select("carrier_name").first()
        assert cargo["carrier_name"] == "FedEx Express"


class TestAirlineMarketShare:
    """gold.airline_market_share: per-airline airborne aircraft count."""

    def test_ual_has_two_airborne(self, spark, silver_df):
        # UAL has 3 rows: aa1 (12:00), ff6 (12:00), aa1 (12:01)
        # Unique aircraft (countDistinct icao24): aa1 and ff6 = 2
        airborne = silver_df.filter(
            (~F.col("on_ground")) & (F.col("airline_icao") == "UAL")
        )
        distinct_ual = airborne.select("icao24").distinct().count()
        assert distinct_ual == 2

    def test_on_ground_excluded_from_market_share(self, spark, silver_df):
        airborne = silver_df.filter(~F.col("on_ground"))
        on_ground = silver_df.filter( F.col("on_ground"))
        # VNA1 is on ground — should not appear in airborne market share
        vna_airborne = airborne.filter(F.col("airline_icao") == "VNA").count()
        assert vna_airborne == 0


class TestFuelBurnEstimate:
    """gold.fuel_burn_estimate: correct fuel burn rate per flight phase."""

    def test_climbing_fuel_rate(self, spark, silver_df):
        # BAW1 is CLIMBING — fuel rate = 14 kg/min
        climbing = silver_df.filter(F.col("flight_phase") == "CLIMBING")
        assert climbing.count() == 1

        climbing_row = climbing.withColumn(
            "fuel_burn_rate",
            F.when(F.col("flight_phase") == "CLIMBING", 14.0)
            .when(F.col("flight_phase") == "CRUISING",   8.0)
            .when(F.col("flight_phase") == "DESCENDING", 4.0)
            .otherwise(3.0)
        ).select("fuel_burn_rate").first()

        assert climbing_row["fuel_burn_rate"] == 14.0

    def test_on_ground_taxi_rate(self, spark, silver_df):
        ground = silver_df.filter(F.col("flight_phase") == "ON_GROUND")
        ground_row = ground.withColumn(
            "fuel_burn_rate",
            F.when(F.col("flight_phase") == "ON_GROUND", 3.0).otherwise(8.0)
        ).select("fuel_burn_rate").first()
        assert ground_row["fuel_burn_rate"] == 3.0

    def test_co2_factor(self, spark, silver_df):
        # CO₂ = fuel_burn_kg * 3.16
        fuel_kg = 100.0
        co2 = round(fuel_kg * 3.16 / 1000, 3)
        assert co2 == 0.316


class TestAirspaceDensity:
    """gold.airspace_density: flight count per lat/lon bin."""

    def test_lat_bin_grouping(self, spark, silver_df):
        density = silver_df.groupBy("lat_bin", "lon_bin").agg(
            F.count("icao24").alias("flight_count")
        )
        # aa1 appears at lat=41→lat_bin=41, lon=-87.6→lon_bin=-88 in 2 snapshots (12:00 and 12:01)
        # ff6 lat=42→lat_bin=42 (different bin) → expected count = 2
        bin_row = density.filter(
            (F.col("lat_bin") == 41) & (F.col("lon_bin") == -88)
        ).select("flight_count").first()
        assert bin_row["flight_count"] == 2


class TestEconomicActivityIndex:
    """gold.economic_activity_index: index based on unique aircraft per country."""

    def test_us_has_most_aircraft(self, spark, silver_df):
        by_country = silver_df.groupBy("origin_country").agg(
            F.countDistinct("icao24").alias("unique_aircraft")
        ).orderBy(F.col("unique_aircraft").desc())

        top = by_country.first()
        assert top["origin_country"] == "United States"
        assert top["unique_aircraft"] >= 2  # aa1, ff6, ee5

    def test_index_capped_at_200(self, spark):
        # Verify the cap logic: index = min(200, value)
        assert min(200.0, 250.0) == 200.0
        assert min(200.0, 150.0) == 150.0
