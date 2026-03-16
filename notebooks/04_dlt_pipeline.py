# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Delta Live Tables Pipeline (All-in-One)
# MAGIC
# MAGIC **Purpose:** Production-grade DLT pipeline that replaces notebooks 01–03.
# MAGIC Run this as a Delta Live Tables pipeline (Workflows > Delta Live Tables).
# MAGIC
# MAGIC **Configuration:**
# MAGIC - Pipeline mode: CONTINUOUS
# MAGIC - Storage location: s3://skystream-datalake-dev/dlt_storage/
# MAGIC - Cluster: Auto-scaling, Photon enabled
# MAGIC
# MAGIC **Pipeline configuration JSON** (paste in DLT "Configuration" tab):
# MAGIC ```json
# MAGIC {
# MAGIC   "S3_BUCKET": "skystream-datalake-dev",
# MAGIC   "TRIGGER_INTERVAL": "10 seconds"
# MAGIC }
# MAGIC ```

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, BooleanType, LongType, IntegerType, StringType
from itertools import chain

# ── Pipeline configuration (injected via DLT config) ──────────────────────────
S3_BUCKET = spark.conf.get("S3_BUCKET", "skystream-datalake-dev")
S3_BRONZE_PATH  = f"s3://{S3_BUCKET}/bronze/raw_states/"
SCHEMA_LOCATION = f"s3://{S3_BUCKET}/_schemas/dlt_bronze"

WATERMARK_DELAY = "2 minutes"

# ── Cargo airline lookup ───────────────────────────────────────────────────────
CARGO_AIRLINE_MAP = {
    "FDX": "FedEx Express",        "UPS": "UPS Airlines",
    "GTI": "Atlas Air",            "PAC": "Kalitta Air",
    "ABX": "ABX Air",              "ATN": "Air Transport Intl",
    "CLX": "Cargolux",             "MPH": "Martinair",
    "CAL": "China Airlines Cargo", "CCA": "Air China Cargo",
    "CSN": "China Southern Cargo", "SQC": "Singapore Airlines Cargo",
    "TAY": "TNT/DHL Airways",      "DHK": "DHL Air UK",
    "LCO": "LATAM Cargo",          "KZR": "Cargojet",
}
cargo_map_expr = F.create_map([F.lit(x) for x in chain(*CARGO_AIRLINE_MAP.items())])

# ── Tourist regions reference ──────────────────────────────────────────────────
TOURIST_REGIONS = [
    ("Bali, Indonesia",      -9.0, 114.4,  -8.0, 116.0),
    ("Phuket, Thailand",      7.5,  98.0,   8.5,  99.5),
    ("Maldives",             -1.5,  72.5,   1.5,  74.5),
    ("Cancun, Mexico",       20.8, -87.5,  21.5, -86.5),
    ("Paris, France",        48.6,   1.8,  49.1,   3.0),
    ("Dubai, UAE",           24.8,  54.5,  25.5,  56.0),
    ("Hawaii, USA",          18.9,-161.0,  22.3,-154.5),
    ("Barcelona, Spain",     40.8,   1.5,  41.6,   2.5),
    ("Singapore",             1.1, 103.5,   1.5, 104.1),
]

# ── Airport reference ──────────────────────────────────────────────────────────
MAJOR_AIRPORTS = [
    ("KLAX", "Los Angeles",        33.9425, -118.4081, 80),
    ("KJFK", "New York JFK",       40.6413,  -73.7781, 90),
    ("KORD", "Chicago O'Hare",     41.9742,  -87.9073, 110),
    ("EGLL", "London Heathrow",    51.4700,   -0.4543, 100),
    ("LFPG", "Paris CDG",          49.0097,    2.5479, 90),
    ("EDDF", "Frankfurt",          50.0379,    8.5622, 85),
    ("RJTT", "Tokyo Haneda",       35.5494,  139.7798, 90),
    ("VHHH", "Hong Kong",          22.3080,  113.9185, 80),
    ("WSSS", "Singapore Changi",    1.3644,  103.9915, 75),
    ("OMDB", "Dubai Intl",         25.2532,   55.3657, 110),
    ("VTBS", "Bangkok Suvarnabhumi",13.6900,  100.7501, 75),
    ("VVTS", "Ho Chi Minh City",   10.8188,  106.6520, 50),
    ("ZBAA", "Beijing Capital",    40.0799,  116.6031, 100),
    ("ZSPD", "Shanghai Pudong",    31.1443,  121.8083, 95),
    ("RKSI", "Seoul Incheon",      37.4692,  126.4505, 80),
]

# =============================================================================
# BRONZE LAYER
# =============================================================================

@dlt.table(
    name="raw_flight_states",
    comment="Bronze: raw aircraft state vectors from OpenSky Network via Kinesis Firehose → S3",
    table_properties={"quality": "bronze", "pipelines.reset.allowed": "true"},
)
def bronze_raw_flight_states():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.maxFilesPerTrigger", 1000)
        .load(S3_BRONZE_PATH)
        .withColumn("_bronze_ingested_at", F.current_timestamp())
    )

# =============================================================================
# SILVER LAYER
# =============================================================================

@dlt.expect_or_drop("valid_latitude",  "latitude  IS NOT NULL AND latitude  BETWEEN -90  AND 90")
@dlt.expect_or_drop("valid_longitude", "longitude IS NOT NULL AND longitude BETWEEN -180 AND 180")
@dlt.expect("non_negative_altitude",   "baro_altitude IS NULL OR baro_altitude >= 0")
@dlt.table(
    name="flights",
    comment="Silver: cleaned and enriched flight states with flight_phase, unit conversions, cargo flag",
    table_properties={"quality": "silver"},
)
def silver_flights():
    return (
        dlt.read_stream("raw_flight_states")
        .withWatermark(
            F.to_timestamp(F.from_unixtime(F.col("snapshot_time").cast(LongType()))).alias("snapshot_time_ts"),
            WATERMARK_DELAY
        )
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
        .select(
            F.col("icao24").cast(StringType()),
            F.col("callsign").cast(StringType()),
            F.col("origin_country").cast(StringType()),
            F.col("airline_icao").cast(StringType()),
            F.col("carrier_name").cast(StringType()),
            F.col("is_cargo").cast(BooleanType()),
            F.col("latitude").cast(DoubleType()),
            F.col("longitude").cast(DoubleType()),
            F.col("baro_altitude").cast(DoubleType()).alias("altitude_m"),
            F.col("altitude_ft").cast(DoubleType()),
            F.col("on_ground").cast(BooleanType()),
            F.col("velocity").cast(DoubleType()).alias("velocity_ms"),
            F.col("velocity_kmh"),
            F.col("true_track").cast(DoubleType()),
            F.col("vertical_rate").cast(DoubleType()),
            F.col("geo_altitude").cast(DoubleType()),
            F.col("squawk").cast(StringType()),
            F.col("flight_phase"),
            F.col("lat_bin"),
            F.col("lon_bin"),
            F.col("snapshot_time").cast(LongType()),
            F.col("snapshot_time_ts"),
            F.col("ingestion_time").cast(StringType()),
            F.current_timestamp().alias("_silver_processed_at"),
        )
    )

# =============================================================================
# GOLD LAYER — Materialized Views (run on each DLT trigger cycle)
# =============================================================================

@dlt.table(
    name="airspace_density",
    comment="Gold: flight count per 1°×1° grid cell per 1-minute window",
    table_properties={"quality": "gold"},
)
def gold_airspace_density():
    return (
        dlt.read("flights")
        .groupBy(
            F.window("snapshot_time_ts", "1 minute"),
            "lat_bin", "lon_bin", "origin_country",
        )
        .agg(
            F.count("icao24").alias("flight_count"),
            F.countDistinct("airline_icao").alias("distinct_airlines"),
            F.sum(F.when(~F.col("on_ground"), 1).otherwise(0)).alias("airborne_count"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )

# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="flight_phase_stats",
    comment="Gold: fleet phase distribution per 5-minute window",
    table_properties={"quality": "gold"},
)
def gold_flight_phase_stats():
    return (
        dlt.read("flights")
        .groupBy(
            F.window("snapshot_time_ts", "5 minutes"),
            "flight_phase", "origin_country",
        )
        .agg(
            F.count("icao24").alias("aircraft_count"),
            F.avg("altitude_m").alias("avg_altitude_m"),
            F.avg("velocity_kmh").alias("avg_velocity_kmh"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )

# ─────────────────────────────────────────────────────────────────────────────

@dlt.expect("valid_alert_type", "alert_type IN ('RAPID_DESCENT', 'ABNORMAL_LOW_ALTITUDE')")
@dlt.table(
    name="flight_alerts",
    comment="Gold: safety alerts — rapid descent and abnormally low altitude",
    table_properties={"quality": "gold"},
)
def gold_flight_alerts():
    return (
        dlt.read_stream("flights")
        .withColumn("alert_type",
            F.when(
                (~F.col("on_ground")) & (F.col("vertical_rate") < -15.0),
                "RAPID_DESCENT"
            ).when(
                (~F.col("on_ground")) &
                (F.col("altitude_m") < 300.0) & (F.col("altitude_m") > 0.0) &
                (F.col("flight_phase") == "CRUISING"),
                "ABNORMAL_LOW_ALTITUDE"
            ).otherwise(None)
        )
        .filter(F.col("alert_type").isNotNull())
        .select(
            "icao24", "callsign", "airline_icao", "origin_country",
            "latitude", "longitude", "altitude_m", "altitude_ft",
            "velocity_kmh", "vertical_rate", "flight_phase", "alert_type",
            "snapshot_time_ts",
            F.current_timestamp().alias("alert_generated_at"),
        )
    )

# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="hourly_traffic",
    comment="Gold: unique aircraft count per country per hour",
    table_properties={"quality": "gold"},
)
def gold_hourly_traffic():
    return (
        dlt.read("flights")
        .groupBy(F.window("snapshot_time_ts", "1 hour"), "origin_country")
        .agg(
            F.countDistinct("icao24").alias("unique_aircraft"),
            F.sum(F.when(~F.col("on_ground"), 1).otherwise(0)).alias("airborne_snapshots"),
            F.avg("velocity_kmh").alias("avg_velocity_kmh"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )

# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="route_demand_index",
    comment="Gold: flight density per 5°×5° corridor per 15-min window (investment signal)",
    table_properties={"quality": "gold"},
)
def gold_route_demand_index():
    df = (
        dlt.read("flights")
        .filter(~F.col("on_ground"))
        .withColumn("lat_zone", (F.floor(F.col("latitude")  / 5) * 5).cast(IntegerType()))
        .withColumn("lon_zone", (F.floor(F.col("longitude") / 5) * 5).cast(IntegerType()))
        .groupBy(F.window("snapshot_time_ts", "15 minutes"), "lat_zone", "lon_zone")
        .agg(
            F.count("icao24").alias("flight_count"),
            F.countDistinct("origin_country").alias("distinct_countries"),
            F.countDistinct("airline_icao").alias("distinct_airlines"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )
    # World avg for relative comparison (DLT batch context)
    world_avg_row = df.agg(F.avg("flight_count")).collect()
    world_avg = world_avg_row[0][0] if world_avg_row and world_avg_row[0][0] else 1.0
    return (
        df
        .withColumn("demand_vs_world_avg_pct",
            F.round((F.col("flight_count") / world_avg) * 100, 1)
        )
        .withColumn("demand_signal",
            F.when(F.col("demand_vs_world_avg_pct") > 130, "SURGE")
            .when(F.col("demand_vs_world_avg_pct") <  60, "DIP")
            .otherwise("NORMAL")
        )
    )

# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="airline_market_share",
    comment="Gold: per-airline active aircraft count and market share per 5-min window",
    table_properties={"quality": "gold"},
)
def gold_airline_market_share():
    base = dlt.read("flights").filter(~F.col("on_ground") & F.col("airline_icao").isNotNull())
    total = base.groupBy(F.window("snapshot_time_ts", "5 minutes")).agg(
        F.countDistinct("icao24").alias("total_airborne")
    )
    per_airline = base.groupBy(F.window("snapshot_time_ts", "5 minutes"), "airline_icao").agg(
        F.countDistinct("icao24").alias("active_aircraft"),
        F.countDistinct("origin_country").alias("countries_served"),
    )
    return (
        per_airline.join(total, on="window", how="left")
        .withColumn("market_share_pct",
            F.round((F.col("active_aircraft") / F.col("total_airborne")) * 100, 2)
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )

# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="cargo_flow",
    comment="Gold: cargo airline flight volume by region and carrier per 1-min window",
    table_properties={"quality": "gold"},
)
def gold_cargo_flow():
    return (
        dlt.read("flights")
        .filter(F.col("is_cargo") == True)
        .withColumn("lat_zone", (F.floor(F.col("latitude")  / 10) * 10).cast(IntegerType()))
        .withColumn("lon_zone", (F.floor(F.col("longitude") / 10) * 10).cast(IntegerType()))
        .groupBy(
            F.window("snapshot_time_ts", "1 minute"),
            "carrier_name", "airline_icao", "lat_zone", "lon_zone", "flight_phase",
        )
        .agg(
            F.countDistinct("icao24").alias("distinct_aircraft"),
            F.count("icao24").alias("active_cargo_flights"),
            (F.countDistinct("icao24") * 90).alias("estimated_tonnage_mt"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )

# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="airport_congestion",
    comment="Gold: approaching/departing aircraft near 15 major airports per 5-min window",
    table_properties={"quality": "gold"},
)
def gold_airport_congestion():
    airports_df = spark.createDataFrame(MAJOR_AIRPORTS,
        schema=["airport_icao", "airport_name", "airport_lat", "airport_lon", "capacity_baseline"])
    low_alt = dlt.read("flights").filter(
        ~F.col("on_ground") & F.col("altitude_m").between(0, 8000)
    )
    joined = (
        low_alt
        .crossJoin(F.broadcast(airports_df))
        .filter(
            (F.abs(F.col("latitude")  - F.col("airport_lat")) <= 0.8) &
            (F.abs(F.col("longitude") - F.col("airport_lon")) <= 0.8)
        )
    )
    return (
        joined
        .groupBy(
            F.window("snapshot_time_ts", "5 minutes"),
            "airport_icao", "airport_name", "capacity_baseline",
        )
        .agg(
            F.sum(F.when(F.col("flight_phase") == "DESCENDING", 1).otherwise(0)).alias("approaching"),
            F.sum(F.when(F.col("flight_phase") == "CLIMBING",   1).otherwise(0)).alias("departing"),
            F.count("icao24").alias("total_in_vicinity"),
        )
        .withColumn("congestion_score",
            F.round(F.least(F.lit(100.0),
                (F.col("total_in_vicinity") / F.col("capacity_baseline")) * 100
            ), 1)
        )
        .withColumn("congestion_level",
            F.when(F.col("congestion_score") >= 80, "CRITICAL")
            .when(F.col("congestion_score") >= 60, "HIGH")
            .when(F.col("congestion_score") >= 40, "MODERATE")
            .otherwise("LOW")
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )

# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="tourism_demand_signal",
    comment="Gold: inbound aircraft count per tourist destination per 5-min window",
    table_properties={"quality": "gold"},
)
def gold_tourism_demand_signal():
    tourist_df = spark.createDataFrame(TOURIST_REGIONS,
        schema=["region_name", "lat_min", "lon_min", "lat_max", "lon_max"])
    inbound = dlt.read("flights").filter(
        (F.col("flight_phase") == "DESCENDING") &
        (F.col("altitude_m") < 3000) &
        (~F.col("on_ground"))
    )
    return (
        inbound
        .crossJoin(F.broadcast(tourist_df))
        .filter(
            (F.col("latitude")  >= F.col("lat_min")) & (F.col("latitude")  <= F.col("lat_max")) &
            (F.col("longitude") >= F.col("lon_min")) & (F.col("longitude") <= F.col("lon_max"))
        )
        .groupBy(F.window("snapshot_time_ts", "15 minutes"), "region_name")
        .agg(
            F.count("icao24").alias("inbound_aircraft"),
            F.countDistinct("origin_country").alias("origin_countries"),
            F.avg("altitude_m").alias("avg_approach_altitude_m"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )

# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="fuel_burn_estimate",
    comment="Gold: estimated fuel consumption and CO₂ per flight phase per 1-min window",
    table_properties={"quality": "gold"},
)
def gold_fuel_burn_estimate():
    return (
        dlt.read("flights")
        .withColumn("fuel_burn_rate_kg_per_min",
            F.when(F.col("flight_phase") == "CLIMBING",   14.0)
            .when(F.col("flight_phase") == "CRUISING",     8.0)
            .when(F.col("flight_phase") == "DESCENDING",   4.0)
            .otherwise(3.0)
        )
        .groupBy(F.window("snapshot_time_ts", "1 minute"), "flight_phase", "origin_country")
        .agg(
            F.count("icao24").alias("aircraft_count"),
            F.round(F.sum(F.col("fuel_burn_rate_kg_per_min") / 6), 1).alias("fuel_burn_kg"),
        )
        .withColumn("co2_tonnes",     F.round(F.col("fuel_burn_kg") * 3.16  / 1000, 3))
        .withColumn("fuel_cost_usd",  F.round(F.col("fuel_burn_kg") * 0.75, 2))
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )

# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="economic_activity_index",
    comment="Gold: aviation-based economic activity index per country per hour",
    table_properties={"quality": "gold"},
)
def gold_economic_activity_index():
    df = (
        dlt.read("flights")
        .groupBy(F.window("snapshot_time_ts", "1 hour"), "origin_country")
        .agg(
            F.countDistinct("icao24").alias("unique_aircraft"),
            F.sum(F.when(~F.col("on_ground"), 1).otherwise(0)).alias("airborne_snapshots"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )
    world_avg_row = df.agg(F.avg("unique_aircraft")).collect()
    world_avg = world_avg_row[0][0] if world_avg_row and world_avg_row[0][0] else 1.0
    return (
        df
        .withColumn("economic_activity_index",
            F.round(F.least(F.lit(200.0), (F.col("unique_aircraft") / world_avg) * 100), 1)
        )
        .withColumn("activity_signal",
            F.when(F.col("economic_activity_index") >= 110, "EXPANDING")
            .when(F.col("economic_activity_index") <=  90, "CONTRACTING")
            .otherwise("STABLE")
        )
    )
