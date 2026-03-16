"""
tests/test_silver_transform.py

Unit tests for Silver layer transformation logic.
Tests are pure Python/PySpark — no Databricks, no AWS required.
"""

import pytest
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, BooleanType, LongType, IntegerType, StringType
from itertools import chain

from conftest import BRONZE_SCHEMA


# ── Helpers ────────────────────────────────────────────────────────────────────

CARGO_AIRLINE_MAP = {
    "FDX": "FedEx Express", "UPS": "UPS Airlines",
    "GTI": "Atlas Air",     "CLX": "Cargolux",
}


def apply_silver_transform(spark, rows):
    """Apply Silver transformation logic to a list of raw rows (mirrors notebook 02)."""
    cargo_map_expr = F.create_map([F.lit(x) for x in chain(*CARGO_AIRLINE_MAP.items())])

    df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)

    return (
        df
        .withColumn("snapshot_time_ts",
            F.to_timestamp(F.from_unixtime(F.col("snapshot_time").cast(LongType())))
        )
        .withColumn("callsign", F.trim(F.col("callsign").cast(StringType())))
        .withColumn("callsign",
            F.when(F.col("callsign") == "", None).otherwise(F.col("callsign"))
        )
        .withColumn("airline_icao",
            F.when(F.col("callsign").isNotNull(),
                F.upper(F.substring("callsign", 1, 3))
            ).otherwise(None)
        )
        .withColumn("carrier_name", cargo_map_expr.getItem(F.col("airline_icao")))
        .withColumn("is_cargo", F.col("carrier_name").isNotNull())
        .withColumn("velocity_kmh", F.round(F.col("velocity").cast(DoubleType()) * 3.6, 1))
        .withColumn("altitude_ft",  F.round(F.col("baro_altitude").cast(DoubleType()) * 3.28084, 0))
        .withColumn("flight_phase",
            F.when(F.col("on_ground").cast(BooleanType()) == True, "ON_GROUND")
            .when(F.col("vertical_rate").cast(DoubleType()) >  2.0, "CLIMBING")
            .when(F.col("vertical_rate").cast(DoubleType()) < -2.0, "DESCENDING")
            .otherwise("CRUISING")
        )
        .withColumn("lat_bin", F.floor(F.col("latitude").cast(DoubleType())).cast(IntegerType()))
        .withColumn("lon_bin", F.floor(F.col("longitude").cast(DoubleType())).cast(IntegerType()))
        .filter(F.col("latitude").isNotNull())
        .filter(F.col("longitude").isNotNull())
        .filter(F.col("latitude").cast(DoubleType()).between(-90.0, 90.0))
        .filter(F.col("longitude").cast(DoubleType()).between(-180.0, 180.0))
    )


# ── Tests ──────────────────────────────────────────────────────────────────────

class TestDataQualityFilters:
    """Rows with null or out-of-range lat/lon must be dropped."""

    def test_null_latitude_dropped(self, spark, sample_bronze_rows):
        df = apply_silver_transform(spark, sample_bronze_rows)
        null_lat_rows = df.filter(F.col("latitude").isNull()).count()
        assert null_lat_rows == 0, "Rows with null latitude should be filtered out"

    def test_null_longitude_dropped(self, spark, sample_bronze_rows):
        df = apply_silver_transform(spark, sample_bronze_rows)
        null_lon_rows = df.filter(F.col("longitude").isNull()).count()
        assert null_lon_rows == 0, "Rows with null longitude should be filtered out"

    def test_row_count_after_filter(self, spark, sample_bronze_rows):
        # 7 input rows, 1 has null latitude → expect 6 output rows
        df = apply_silver_transform(spark, sample_bronze_rows)
        assert df.count() == 6


class TestCallsignCleaning:
    """Callsign trailing spaces must be trimmed."""

    def test_callsign_trimmed(self, spark, sample_bronze_rows):
        df = apply_silver_transform(spark, sample_bronze_rows)
        raw_row = df.filter(F.col("icao24") == "a1b2c3").select("callsign").first()
        assert raw_row["callsign"] == "UAL123", \
            f"Expected 'UAL123', got '{raw_row['callsign']}'"

    def test_airline_icao_extracted(self, spark, sample_bronze_rows):
        df = apply_silver_transform(spark, sample_bronze_rows)
        row = df.filter(F.col("icao24") == "a1b2c3").select("airline_icao").first()
        assert row["airline_icao"] == "UAL"

    def test_german_callsign_prefix(self, spark, sample_bronze_rows):
        df = apply_silver_transform(spark, sample_bronze_rows)
        row = df.filter(F.col("icao24") == "s1t2u3").select("airline_icao").first()
        assert row["airline_icao"] == "DLH"


class TestFlightPhaseClassification:
    """Flight phase must be correctly classified based on on_ground and vertical_rate."""

    def test_on_ground_phase(self, spark, sample_bronze_rows):
        df = apply_silver_transform(spark, sample_bronze_rows)
        row = df.filter(F.col("icao24") == "j1k2l3").select("flight_phase").first()
        assert row["flight_phase"] == "ON_GROUND"

    def test_climbing_phase(self, spark, sample_bronze_rows):
        df = apply_silver_transform(spark, sample_bronze_rows)
        row = df.filter(F.col("icao24") == "d4e5f6").select("flight_phase").first()
        assert row["flight_phase"] == "CLIMBING"

    def test_descending_phase(self, spark, sample_bronze_rows):
        df = apply_silver_transform(spark, sample_bronze_rows)
        row = df.filter(F.col("icao24") == "g7h8i9").select("flight_phase").first()
        assert row["flight_phase"] == "DESCENDING"

    def test_cruising_phase(self, spark, sample_bronze_rows):
        df = apply_silver_transform(spark, sample_bronze_rows)
        row = df.filter(F.col("icao24") == "a1b2c3").select("flight_phase").first()
        assert row["flight_phase"] == "CRUISING"


class TestUnitConversions:
    """velocity_kmh and altitude_ft must be correctly converted."""

    def test_velocity_conversion(self, spark, sample_bronze_rows):
        df = apply_silver_transform(spark, sample_bronze_rows)
        row = df.filter(F.col("icao24") == "a1b2c3").select("velocity_kmh").first()
        expected = round(245.0 * 3.6, 1)  # 882.0 km/h
        assert row["velocity_kmh"] == expected, f"Expected {expected}, got {row['velocity_kmh']}"

    def test_altitude_ft_conversion(self, spark, sample_bronze_rows):
        df = apply_silver_transform(spark, sample_bronze_rows)
        row = df.filter(F.col("icao24") == "a1b2c3").select("altitude_ft").first()
        expected = round(10500.0 * 3.28084, 0)
        assert row["altitude_ft"] == expected


class TestCargoDetection:
    """FedEx flights must be flagged as cargo; regular airline flights must not."""

    def test_fedex_is_cargo(self, spark, sample_bronze_rows):
        df = apply_silver_transform(spark, sample_bronze_rows)
        row = df.filter(F.col("icao24") == "m4n5o6").select("is_cargo", "carrier_name").first()
        assert row["is_cargo"] is True
        assert row["carrier_name"] == "FedEx Express"

    def test_regular_flight_not_cargo(self, spark, sample_bronze_rows):
        df = apply_silver_transform(spark, sample_bronze_rows)
        row = df.filter(F.col("icao24") == "a1b2c3").select("is_cargo", "carrier_name").first()
        assert row["is_cargo"] is False
        assert row["carrier_name"] is None


class TestSpatialBins:
    """lat_bin and lon_bin must be correctly floored to integer grid."""

    def test_lat_bin(self, spark, sample_bronze_rows):
        df = apply_silver_transform(spark, sample_bronze_rows)
        row = df.filter(F.col("icao24") == "a1b2c3").select("lat_bin", "lon_bin").first()
        assert row["lat_bin"] == 41   # floor(41.8781)
        assert row["lon_bin"] == -88  # floor(-87.6298)

    def test_singapore_bins(self, spark, sample_bronze_rows):
        df = apply_silver_transform(spark, sample_bronze_rows)
        row = df.filter(F.col("icao24") == "g7h8i9").select("lat_bin", "lon_bin").first()
        assert row["lat_bin"] == 1    # floor(1.36)
        assert row["lon_bin"] == 103  # floor(103.99)
