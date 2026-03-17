# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Layer: Cleaning & Enrichment
# MAGIC
# MAGIC **Purpose:** Read Bronze streaming table → apply data quality rules → enrich →
# MAGIC write to `silver.flights`.
# MAGIC
# MAGIC **Transformations applied:**
# MAGIC - Cast all fields to correct data types
# MAGIC - Convert `snapshot_time` (Unix epoch) to proper Timestamp
# MAGIC - Trim and normalize `callsign`
# MAGIC - Extract `airline_icao` (3-char ICAO airline prefix from callsign)
# MAGIC - Flag cargo flights and set `carrier_name`
# MAGIC - Convert units: velocity m/s → km/h, altitude m → ft
# MAGIC - Classify `flight_phase`: ON_GROUND / CLIMBING / CRUISING / DESCENDING
# MAGIC - Add spatial bins `lat_bin` / `lon_bin` (1°×1° grid, for Gold aggregations)
# MAGIC - Drop records with null lat/lon or out-of-range coordinates

# COMMAND ----------

# ── Configuration ──────────────────────────────────────────────────────────────
S3_BUCKET        = "skystream-datalake-dev"   # Change to your bucket name
BRONZE_TABLE     = "workspace_7474644985505263.bronze.raw_flight_states"
SILVER_TABLE     = "workspace_7474644985505263.silver.flights"
CHECKPOINT_PATH  = f"s3://{S3_BUCKET}/_checkpoints/silver_transform"
TRIGGER_INTERVAL = "30 seconds"
WATERMARK_DELAY  = "2 minutes"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cargo Airline Reference
# MAGIC
# MAGIC Known cargo airline ICAO codes (first 3 characters of callsign = ICAO airline code).
# MAGIC Aircraft matching these codes are flagged as `is_cargo = true`.

# COMMAND ----------

CARGO_AIRLINE_MAP: dict[str, str] = {
    "FDX": "FedEx Express",
    "UPS": "UPS Airlines",
    "GTI": "Atlas Air",
    "PAC": "Kalitta Air",
    "ABX": "ABX Air",
    "ATN": "Air Transport International",
    "CLX": "Cargolux",
    "MPH": "Martinair",
    "CAL": "China Airlines Cargo",
    "CCA": "Air China Cargo",
    "CSN": "China Southern Cargo",
    "SQC": "Singapore Airlines Cargo",
    "TAY": "TNT Airways (DHL)",
    "DHK": "DHL Air UK",
    "LCO": "LATAM Cargo",
    "MAS": "Malaysian Airlines Cargo",
    "NPT": "Night Air Cargo",
    "RCF": "ATSG",
    "CKS": "Evergreen International",
    "KZR": "Cargojet",
}

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, BooleanType, LongType, IntegerType, StringType
from itertools import chain

# Build Spark literal map expression for cargo lookup (broadcast-friendly)
cargo_map_expr = F.create_map([F.lit(x) for x in chain(*CARGO_AIRLINE_MAP.items())])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Transformation Logic

# COMMAND ----------

df_bronze_stream = spark.readStream.table(BRONZE_TABLE)

df_silver = (
    df_bronze_stream

    # ── Timestamps ────────────────────────────────────────────────────────────
    .withColumn(
        "snapshot_time_ts",
        F.to_timestamp(F.from_unixtime(F.col("snapshot_time").cast(LongType())))
    )

    # ── Watermark (for late-arriving data tolerance) ───────────────────────────
    .withWatermark("snapshot_time_ts", WATERMARK_DELAY)

    # ── Callsign cleanup ──────────────────────────────────────────────────────
    .withColumn("callsign", F.trim(F.col("callsign").cast(StringType())))
    .withColumn("callsign",
        F.when(F.col("callsign") == "", None).otherwise(F.col("callsign"))
    )

    # ── Airline ICAO code (3-char prefix) ─────────────────────────────────────
    .withColumn("airline_icao",
        F.when(
            F.col("callsign").isNotNull(),
            F.upper(F.substring("callsign", 1, 3))
        ).otherwise(None)
    )

    # ── Cargo flight detection ─────────────────────────────────────────────────
    .withColumn("carrier_name", cargo_map_expr.getItem(F.col("airline_icao")))
    .withColumn("is_cargo", F.col("carrier_name").isNotNull())

    # ── Unit conversions ──────────────────────────────────────────────────────
    .withColumn("velocity_kmh",
        F.round(F.col("velocity").cast(DoubleType()) * 3.6, 1)
    )
    .withColumn("altitude_ft",
        F.round(F.col("baro_altitude").cast(DoubleType()) * 3.28084, 0)
    )

    # ── Flight phase classification ───────────────────────────────────────────
    # vertical_rate thresholds: >2 m/s = climbing, <-2 m/s = descending
    .withColumn("flight_phase",
        F.when(F.col("on_ground").cast(BooleanType()) == True, "ON_GROUND")
        .when(F.col("vertical_rate").cast(DoubleType()) >  2.0, "CLIMBING")
        .when(F.col("vertical_rate").cast(DoubleType()) < -2.0, "DESCENDING")
        .otherwise("CRUISING")
    )

    # ── Spatial bins (1°×1° grid cells) ──────────────────────────────────────
    .withColumn("lat_bin", F.floor(F.col("latitude").cast(DoubleType())).cast(IntegerType()))
    .withColumn("lon_bin", F.floor(F.col("longitude").cast(DoubleType())).cast(IntegerType()))

    # ── Data quality filters ──────────────────────────────────────────────────
    .filter(F.col("latitude").cast(DoubleType()).isNotNull())
    .filter(F.col("longitude").cast(DoubleType()).isNotNull())
    .filter(F.col("latitude").cast(DoubleType()).between(-90.0, 90.0))
    .filter(F.col("longitude").cast(DoubleType()).between(-180.0, 180.0))
    .filter(
        F.col("baro_altitude").isNull() |
        (F.col("baro_altitude").cast(DoubleType()) >= 0)
    )

    # ── Final column selection ────────────────────────────────────────────────
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
        F.col("spi").cast(BooleanType()),
        F.col("position_source").cast(IntegerType()),
        F.col("flight_phase"),
        F.col("lat_bin"),
        F.col("lon_bin"),
        F.col("snapshot_time").cast(LongType()),
        F.col("snapshot_time_ts"),
        F.col("ingestion_time").cast(StringType()),
        F.col("_bronze_ingested_at"),
        F.current_timestamp().alias("_silver_processed_at"),
    )
)

# COMMAND ----------

silver_query = (
    df_silver.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .toTable(SILVER_TABLE)
)

print(f"✓ Silver streaming query started — ID: {silver_query.id}")
print(f"  Source : {BRONZE_TABLE}")
print(f"  Target : {SILVER_TABLE}")
print(f"  Trigger: every {TRIGGER_INTERVAL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Silver Data

# COMMAND ----------

# display(
#     spark.sql(f"""
#         SELECT
#             icao24, callsign, airline_icao, origin_country,
#             flight_phase, altitude_m, altitude_ft,
#             velocity_ms, velocity_kmh,
#             is_cargo, carrier_name,
#             lat_bin, lon_bin,
#             snapshot_time_ts
#         FROM {SILVER_TABLE}
#         ORDER BY _silver_processed_at DESC
#         LIMIT 30
#     """)
# )

# COMMAND ----------

silver_query.awaitTermination()
