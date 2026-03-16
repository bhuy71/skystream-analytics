# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold Layer: Business Analytics (11 Streaming Queries)
# MAGIC
# MAGIC **Purpose:** Compute 11 real-time analytics tables from `silver.flights`.
# MAGIC Each table serves a specific business/investment use case.
# MAGIC
# MAGIC ## Tables produced:
# MAGIC | # | Table | Window | Business Value |
# MAGIC |---|-------|--------|----------------|
# MAGIC | 1 | `gold.airspace_density`       | 1 min  | Air traffic density by geo grid |
# MAGIC | 2 | `gold.flight_phase_stats`     | 5 min  | Fleet composition by phase |
# MAGIC | 3 | `gold.flight_alerts`          | stream | Safety: rapid descent detection |
# MAGIC | 4 | `gold.hourly_traffic`         | 1 hr   | Traffic volume by country |
# MAGIC | 5 | `gold.route_demand_index`     | 15 min | Investment: route growth signals |
# MAGIC | 6 | `gold.airline_market_share`   | 5 min  | Investment: live airline fleet activity |
# MAGIC | 7 | `gold.cargo_flow`             | 1 min  | Logistics: cargo volume by region |
# MAGIC | 8 | `gold.airport_congestion`     | 5 min  | Ops: real-time airport congestion score |
# MAGIC | 9 | `gold.tourism_demand_signal`  | 15 min | Investment: inbound tourist demand |
# MAGIC |10 | `gold.fuel_burn_estimate`     | 1 min  | Finance: fuel cost & CO₂ estimate |
# MAGIC |11 | `gold.economic_activity_index`| 1 hr   | Macro: aviation-based economic signal |

# COMMAND ----------

# ── Configuration ──────────────────────────────────────────────────────────────
S3_BUCKET        = "skystream-datalake-dev"   # Change to your bucket name
SILVER_TABLE     = "silver.flights"
CKPT_BASE        = f"s3://{S3_BUCKET}/_checkpoints/gold"
TRIGGER_INTERVAL = "30 seconds"
WATERMARK_DELAY  = "2 minutes"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, IntegerType, BooleanType
from pyspark.sql import DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference Data: Major Airports (for Congestion Detection)

# COMMAND ----------

# 23 major world airports used for congestion scoring
# (icao, name, latitude, longitude, hourly_capacity_baseline)
MAJOR_AIRPORTS_DATA = [
    ("KLAX", "Los Angeles",           33.9425, -118.4081, 80),
    ("KJFK", "New York JFK",          40.6413,  -73.7781, 90),
    ("KORD", "Chicago O'Hare",        41.9742,  -87.9073, 110),
    ("KDEN", "Denver",                39.8561, -104.6737, 70),
    ("KATL", "Atlanta Hartsfield",    33.6407,  -84.4277, 120),
    ("EGLL", "London Heathrow",       51.4700,   -0.4543, 100),
    ("LFPG", "Paris CDG",             49.0097,    2.5479, 90),
    ("EDDF", "Frankfurt",             50.0379,    8.5622, 85),
    ("EHAM", "Amsterdam Schiphol",    52.3086,    4.7639, 80),
    ("LEMD", "Madrid Barajas",        40.4936,   -3.5668, 70),
    ("LIRF", "Rome Fiumicino",        41.8003,   12.2389, 60),
    ("RJTT", "Tokyo Haneda",          35.5494,  139.7798, 90),
    ("RJAA", "Tokyo Narita",          35.7720,  140.3929, 70),
    ("VHHH", "Hong Kong",             22.3080,  113.9185, 80),
    ("WSSS", "Singapore Changi",       1.3644,  103.9915, 75),
    ("OMDB", "Dubai Intl",            25.2532,   55.3657, 110),
    ("YSSY", "Sydney",               -33.9461,  151.1772, 65),
    ("VTBS", "Bangkok Suvarnabhumi",  13.6900,  100.7501, 75),
    ("VVTS", "Ho Chi Minh City",      10.8188,  106.6520, 50),
    ("WMKK", "Kuala Lumpur",           2.7456,  101.7099, 65),
    ("ZBAA", "Beijing Capital",       40.0799,  116.6031, 100),
    ("ZSPD", "Shanghai Pudong",       31.1443,  121.8083, 95),
    ("RKSI", "Seoul Incheon",         37.4692,  126.4505, 80),
]

airports_schema = ["airport_icao", "airport_name", "airport_lat", "airport_lon", "capacity_baseline"]
airports_ref = spark.createDataFrame(MAJOR_AIRPORTS_DATA, schema=airports_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference Data: Tourist Destination Regions

# COMMAND ----------

# Each region is defined as (name, min_lat, min_lon, max_lat, max_lon)
TOURIST_REGIONS_DATA = [
    ("Bali, Indonesia",      -9.0, 114.4,  -8.0, 116.0),
    ("Phuket, Thailand",      7.5,  98.0,   8.5,  99.5),
    ("Maldives",             -1.5,  72.5,   1.5,  74.5),
    ("Cancun, Mexico",       20.8, -87.5,  21.5, -86.5),
    ("Paris, France",        48.6,   1.8,  49.1,   3.0),
    ("Dubai, UAE",           24.8,  54.5,  25.5,  56.0),
    ("Hawaii, USA",          18.9,-161.0,  22.3,-154.5),
    ("Barcelona, Spain",     40.8,   1.5,  41.6,   2.5),
    ("Amsterdam, Netherlands",52.1,  4.6,  52.5,   5.2),
    ("Singapore",             1.1, 103.5,   1.5, 104.1),
]

tourist_schema = ["region_name", "lat_min", "lon_min", "lat_max", "lon_max"]
tourist_ref = spark.createDataFrame(TOURIST_REGIONS_DATA, schema=tourist_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference Data: Cargo Airlines

# COMMAND ----------

CARGO_AIRLINE_DATA = [
    ("FDX", "FedEx Express"),       ("UPS", "UPS Airlines"),
    ("GTI", "Atlas Air"),           ("PAC", "Kalitta Air"),
    ("ABX", "ABX Air"),             ("ATN", "Air Transport Intl"),
    ("CLX", "Cargolux"),            ("MPH", "Martinair"),
    ("CAL", "China Airlines Cargo"),("CCA", "Air China Cargo"),
    ("CSN", "China Southern Cargo"),("SQC", "Singapore Airlines Cargo"),
    ("TAY", "TNT/DHL Airways"),     ("DHK", "DHL Air UK"),
    ("LCO", "LATAM Cargo"),         ("KZR", "Cargojet"),
]

cargo_schema = ["airline_icao", "cargo_carrier_name"]
cargo_ref = spark.createDataFrame(CARGO_AIRLINE_DATA, schema=cargo_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Streaming Table with Watermark

# COMMAND ----------

df_silver = (
    spark.readStream
    .table(SILVER_TABLE)
    .withWatermark("snapshot_time_ts", WATERMARK_DELAY)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Gold Table 1: Airspace Density
# MAGIC **1-minute tumbling window — flights per 1°×1° grid cell**
# MAGIC
# MAGIC Used by: Air traffic control, researchers, airlines planning new routes.

# COMMAND ----------

df_airspace_density = (
    df_silver
    .groupBy(
        F.window("snapshot_time_ts", "1 minute"),
        "lat_bin",
        "lon_bin",
        "origin_country",
    )
    .agg(
        F.count("icao24").alias("flight_count"),
        F.countDistinct("airline_icao").alias("distinct_airlines"),
        F.sum(F.when(F.col("on_ground") == False, 1).otherwise(0)).alias("airborne_count"),
        F.sum(F.when(F.col("on_ground") == True, 1).otherwise(0)).alias("on_ground_count"),
    )
    .withColumn("window_start", F.col("window.start"))
    .withColumn("window_end",   F.col("window.end"))
    .drop("window")
)

q1 = (
    df_airspace_density.writeStream
    .format("delta")
    .outputMode("update")
    .option("checkpointLocation", f"{CKPT_BASE}/airspace_density")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .toTable("gold.airspace_density")
)
print(f"✓ [1/11] gold.airspace_density — query ID: {q1.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 2: Flight Phase Stats
# MAGIC **5-minute window — distribution of ON_GROUND/CLIMBING/CRUISING/DESCENDING**
# MAGIC
# MAGIC Used by: Airlines to monitor fleet utilization; airports for gate planning.

# COMMAND ----------

df_phase_stats = (
    df_silver
    .groupBy(
        F.window("snapshot_time_ts", "5 minutes"),
        "flight_phase",
        "origin_country",
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

q2 = (
    df_phase_stats.writeStream
    .format("delta")
    .outputMode("update")
    .option("checkpointLocation", f"{CKPT_BASE}/flight_phase_stats")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .toTable("gold.flight_phase_stats")
)
print(f"✓ [2/11] gold.flight_phase_stats — query ID: {q2.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 3: Flight Alerts
# MAGIC **Append-only — flags rapid descent (>15 m/s) and abnormally low altitude**
# MAGIC
# MAGIC Used by: Aviation safety monitoring, airline operations centers.

# COMMAND ----------

df_flight_alerts = (
    df_silver
    .withColumn("alert_type",
        F.when(
            (~F.col("on_ground")) & (F.col("vertical_rate") < -15.0),
            "RAPID_DESCENT"
        )
        .when(
            (~F.col("on_ground")) &
            (F.col("altitude_m") < 300.0) &
            (F.col("altitude_m") > 0.0) &
            (F.col("flight_phase") == "CRUISING"),
            "ABNORMAL_LOW_ALTITUDE"
        )
        .otherwise(None)
    )
    .filter(F.col("alert_type").isNotNull())
    .select(
        "icao24", "callsign", "airline_icao", "origin_country",
        "latitude", "longitude",
        "altitude_m", "altitude_ft",
        "velocity_kmh", "vertical_rate",
        "flight_phase", "alert_type",
        "snapshot_time_ts",
        F.current_timestamp().alias("alert_generated_at"),
    )
)

q3 = (
    df_flight_alerts.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CKPT_BASE}/flight_alerts")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .toTable("gold.flight_alerts")
)
print(f"✓ [3/11] gold.flight_alerts — query ID: {q3.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 4: Hourly Traffic Summary
# MAGIC **1-hour tumbling window — total flights by country**
# MAGIC
# MAGIC Used by: Aviation authorities, macro analysts, airline revenue planning.

# COMMAND ----------

df_hourly_traffic = (
    df_silver
    .groupBy(
        F.window("snapshot_time_ts", "1 hour"),
        "origin_country",
    )
    .agg(
        F.countDistinct("icao24").alias("unique_aircraft"),
        F.count("icao24").alias("total_snapshots"),
        F.sum(F.when(~F.col("on_ground"), 1).otherwise(0)).alias("airborne_snapshots"),
        F.avg("velocity_kmh").alias("avg_velocity_kmh"),
        F.avg("altitude_m").alias("avg_altitude_m"),
    )
    .withColumn("window_start", F.col("window.start"))
    .withColumn("window_end",   F.col("window.end"))
    .drop("window")
)

q4 = (
    df_hourly_traffic.writeStream
    .format("delta")
    .outputMode("update")
    .option("checkpointLocation", f"{CKPT_BASE}/hourly_traffic")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .toTable("gold.hourly_traffic")
)
print(f"✓ [4/11] gold.hourly_traffic — query ID: {q4.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 5: Route Demand Index
# MAGIC **15-minute tumbling window — flight density by 5°×5° corridor**
# MAGIC
# MAGIC **Investment value:** Identifies growing/shrinking air corridors.
# MAGIC - `SURGE` (>130% of world avg density) → consider investing in airlines on that route
# MAGIC - `DIP` (<60%) → route underperforming, possible airline exit
# MAGIC
# MAGIC Used by: Airline investors, airport development funds, hotel chains.

# COMMAND ----------

df_route_demand = (
    df_silver
    .filter(~F.col("on_ground"))  # only airborne flights
    .withColumn("lat_zone", (F.floor(F.col("latitude")  / 5) * 5).cast(IntegerType()))
    .withColumn("lon_zone", (F.floor(F.col("longitude") / 5) * 5).cast(IntegerType()))
    .groupBy(
        F.window("snapshot_time_ts", "15 minutes"),
        "lat_zone",
        "lon_zone",
    )
    .agg(
        F.count("icao24").alias("flight_count"),
        F.countDistinct("origin_country").alias("distinct_countries"),
        F.countDistinct("airline_icao").alias("distinct_airlines"),
    )
    .withColumn("window_start", F.col("window.start"))
    .withColumn("window_end",   F.col("window.end"))
    .drop("window")
)

# Compute world average density per window to derive relative index
# (uses foreachBatch to allow window-level normalization)
def write_route_demand_with_index(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    world_avg = batch_df.agg(F.avg("flight_count")).collect()[0][0] or 1.0
    batch_df = batch_df.withColumn(
        "demand_vs_world_avg_pct",
        F.round((F.col("flight_count") / world_avg) * 100, 1)
    ).withColumn(
        "demand_signal",
        F.when(F.col("demand_vs_world_avg_pct") > 130, "SURGE")
        .when(F.col("demand_vs_world_avg_pct") <  60, "DIP")
        .otherwise("NORMAL")
    )
    batch_df.write.format("delta").mode("append").saveAsTable("gold.route_demand_index")

q5 = (
    df_route_demand.writeStream
    .foreachBatch(write_route_demand_with_index)
    .option("checkpointLocation", f"{CKPT_BASE}/route_demand_index")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)
print(f"✓ [5/11] gold.route_demand_index — query ID: {q5.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 6: Airline Market Share
# MAGIC **5-minute tumbling window — active aircraft count per airline**
# MAGIC
# MAGIC **Investment value:**
# MAGIC - Tracks real operational capacity of each airline in near real-time
# MAGIC - Sudden drop in a carrier's active aircraft = potential operational disruption
# MAGIC - Rising carrier = market expansion signal
# MAGIC
# MAGIC Used by: Airline stock traders, aviation analysts, competitive intelligence teams.

# COMMAND ----------

# Total airborne count per 5-min window for market share % calculation
df_airline_totals = (
    df_silver
    .filter(~F.col("on_ground"))
    .groupBy(F.window("snapshot_time_ts", "5 minutes"))
    .agg(F.count("icao24").alias("total_airborne"))
)

df_airline_counts = (
    df_silver
    .filter(~F.col("on_ground"))
    .filter(F.col("airline_icao").isNotNull())
    .groupBy(
        F.window("snapshot_time_ts", "5 minutes"),
        "airline_icao",
    )
    .agg(
        F.countDistinct("icao24").alias("active_aircraft"),
        F.countDistinct("origin_country").alias("countries_served"),
        F.avg("altitude_m").alias("avg_altitude_m"),
        F.avg("velocity_kmh").alias("avg_velocity_kmh"),
    )
)

df_airline_market_share = (
    df_airline_counts
    .join(df_airline_totals, on="window", how="left")
    .withColumn(
        "market_share_pct",
        F.round((F.col("active_aircraft") / F.col("total_airborne")) * 100, 2)
    )
    .withColumn("window_start", F.col("window.start"))
    .withColumn("window_end",   F.col("window.end"))
    .drop("window")
)

q6 = (
    df_airline_market_share.writeStream
    .format("delta")
    .outputMode("update")
    .option("checkpointLocation", f"{CKPT_BASE}/airline_market_share")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .toTable("gold.airline_market_share")
)
print(f"✓ [6/11] gold.airline_market_share — query ID: {q6.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 7: Cargo Flow
# MAGIC **1-minute tumbling window — cargo aircraft activity by region**
# MAGIC
# MAGIC **Investment value:**
# MAGIC - FedEx/UPS/DHL flight volume is a real-time proxy for e-commerce demand
# MAGIC - Cargo surge at specific airports = supply chain ramp-up / inventory restocking
# MAGIC - Sudden cargo drop = supply chain disruption before official reporting
# MAGIC
# MAGIC Used by: Logistics investors, supply chain analysts, e-commerce platforms.

# COMMAND ----------

df_cargo_flow = (
    df_silver
    .filter(F.col("is_cargo") == True)
    .withColumn("lat_zone", (F.floor(F.col("latitude")  / 10) * 10).cast(IntegerType()))
    .withColumn("lon_zone", (F.floor(F.col("longitude") / 10) * 10).cast(IntegerType()))
    .groupBy(
        F.window("snapshot_time_ts", "1 minute"),
        "carrier_name",
        "airline_icao",
        "lat_zone",
        "lon_zone",
        "flight_phase",
    )
    .agg(
        F.count("icao24").alias("active_cargo_flights"),
        F.countDistinct("icao24").alias("distinct_aircraft"),
        # Rough tonnage estimate: average cargo plane ~90 metric tons
        (F.countDistinct("icao24") * 90).alias("estimated_tonnage_mt"),
    )
    .withColumn("window_start", F.col("window.start"))
    .withColumn("window_end",   F.col("window.end"))
    .drop("window")
)

q7 = (
    df_cargo_flow.writeStream
    .format("delta")
    .outputMode("update")
    .option("checkpointLocation", f"{CKPT_BASE}/cargo_flow")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .toTable("gold.cargo_flow")
)
print(f"✓ [7/11] gold.cargo_flow — query ID: {q7.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 8: Airport Congestion Score
# MAGIC **5-minute window — aircraft approaching/departing each major airport**
# MAGIC
# MAGIC **Investment value:**
# MAGIC - Ground handling companies, F&B, parking operators predict busy periods
# MAGIC - Airports use score for dynamic slot pricing
# MAGIC - Airlines optimize gate assignment in real-time
# MAGIC
# MAGIC **Method:** Count aircraft within ±0.8° lat/lon of each airport (~90km bounding box)
# MAGIC and separated by phase (DESCENDING=approaching, CLIMBING=departing).

# COMMAND ----------

def compute_airport_congestion(batch_df, batch_id):
    """
    For each micro-batch, cross-join with airports reference and filter
    by bounding box. Compute congestion score per airport.
    """
    if batch_df.count() == 0:
        return

    # Cross join (small airports_ref is broadcast automatically)
    joined = (
        batch_df
        .filter(~F.col("on_ground"))
        .filter(F.col("altitude_m").between(0, 8000))  # only low-altitude traffic
        .crossJoin(F.broadcast(airports_ref))
        .filter(
            (F.abs(F.col("latitude")  - F.col("airport_lat")) <= 0.8) &
            (F.abs(F.col("longitude") - F.col("airport_lon")) <= 0.8)
        )
    )

    congestion = (
        joined
        .groupBy("airport_icao", "airport_name", "airport_lat", "airport_lon", "capacity_baseline")
        .agg(
            F.sum(F.when(F.col("flight_phase") == "DESCENDING", 1).otherwise(0)).alias("approaching"),
            F.sum(F.when(F.col("flight_phase") == "CLIMBING",   1).otherwise(0)).alias("departing"),
            F.count("icao24").alias("total_in_vicinity"),
            F.min("altitude_m").alias("min_altitude_m"),
        )
        .withColumn(
            "congestion_score",
            F.round(
                F.least(
                    F.lit(100.0),
                    (F.col("total_in_vicinity") / F.col("capacity_baseline")) * 100
                ), 1
            )
        )
        .withColumn("congestion_level",
            F.when(F.col("congestion_score") >= 80, "CRITICAL")
            .when(F.col("congestion_score") >= 60, "HIGH")
            .when(F.col("congestion_score") >= 40, "MODERATE")
            .otherwise("LOW")
        )
        .withColumn("window_start", F.current_timestamp())
    )

    congestion.write.format("delta").mode("append").saveAsTable("gold.airport_congestion")

q8 = (
    df_silver.writeStream
    .foreachBatch(compute_airport_congestion)
    .option("checkpointLocation", f"{CKPT_BASE}/airport_congestion")
    .trigger(processingTime="5 minutes")
    .start()
)
print(f"✓ [8/11] gold.airport_congestion — query ID: {q8.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 9: Tourism Demand Signal
# MAGIC **15-minute window — inbound flights to major tourist destinations**
# MAGIC
# MAGIC **Investment value:**
# MAGIC - Hotels can see demand spikes 2-4 hours before passengers land (dynamic pricing)
# MAGIC - Real estate investors spot rising tourist destinations early
# MAGIC - Tour operators adjust capacity and staffing in advance
# MAGIC
# MAGIC **Method:** Count DESCENDING aircraft with altitude <3000m in tourist bounding boxes.

# COMMAND ----------

def compute_tourism_signal(batch_df, batch_id):
    """
    Detect inbound aircraft (descending, <3000m) within tourist region bounding boxes.
    """
    if batch_df.count() == 0:
        return

    inbound = batch_df.filter(
        (F.col("flight_phase") == "DESCENDING") &
        (F.col("altitude_m") < 3000) &
        (~F.col("on_ground"))
    )

    result = (
        inbound
        .crossJoin(F.broadcast(tourist_ref))
        .filter(
            (F.col("latitude")  >= F.col("lat_min")) &
            (F.col("latitude")  <= F.col("lat_max")) &
            (F.col("longitude") >= F.col("lon_min")) &
            (F.col("longitude") <= F.col("lon_max"))
        )
        .groupBy("region_name")
        .agg(
            F.count("icao24").alias("inbound_aircraft"),
            F.countDistinct("origin_country").alias("origin_countries"),
            F.avg("altitude_m").alias("avg_approach_altitude_m"),
            F.current_timestamp().alias("window_start"),
        )
    )

    result.write.format("delta").mode("append").saveAsTable("gold.tourism_demand_signal")

q9 = (
    df_silver.writeStream
    .foreachBatch(compute_tourism_signal)
    .option("checkpointLocation", f"{CKPT_BASE}/tourism_demand_signal")
    .trigger(processingTime="5 minutes")  # evaluate every 5 min, 15-min rolling view in SQL
    .start()
)
print(f"✓ [9/11] gold.tourism_demand_signal — query ID: {q9.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 10: Fuel Burn & CO₂ Estimate
# MAGIC **1-minute tumbling window — estimated fuel consumption and CO₂ by phase/country**
# MAGIC
# MAGIC **Investment value:**
# MAGIC - Airlines track real-time fuel cost (fuel = ~30% of operating costs)
# MAGIC - Oil/Jet-A traders estimate global aviation demand per hour
# MAGIC - ESG funds measure aviation sector carbon footprint continuously
# MAGIC
# MAGIC **Model (simplified, based on average narrow-body aircraft):**
# MAGIC - CRUISING at altitude: 8 kg/min
# MAGIC - CLIMBING:             14 kg/min (higher thrust required)
# MAGIC - DESCENDING:            4 kg/min (low power setting)
# MAGIC - ON_GROUND (taxiing):   3 kg/min (idle/taxi power)
# MAGIC - CO₂ factor: 3.16 kg CO₂ per kg Jet-A burned (ICAO standard)

# COMMAND ----------

df_fuel = (
    df_silver
    .withColumn("fuel_burn_rate_kg_per_min",
        F.when(F.col("flight_phase") == "CLIMBING",    14.0)
        .when(F.col("flight_phase") == "CRUISING",      8.0)
        .when(F.col("flight_phase") == "DESCENDING",    4.0)
        .otherwise(3.0)  # ON_GROUND
    )
    .groupBy(
        F.window("snapshot_time_ts", "1 minute"),
        "flight_phase",
        "origin_country",
    )
    .agg(
        F.count("icao24").alias("aircraft_count"),
        # Each aircraft snapshot represents ~10 seconds → 1/6 minute
        F.round(
            F.sum(F.col("fuel_burn_rate_kg_per_min") / 6), 1
        ).alias("fuel_burn_kg"),
    )
    .withColumn("co2_tonnes",
        F.round(F.col("fuel_burn_kg") * 3.16 / 1000, 3)
    )
    .withColumn("fuel_cost_usd",
        # Jet-A average ~$0.75/kg (indicative, can be parameterized)
        F.round(F.col("fuel_burn_kg") * 0.75, 2)
    )
    .withColumn("window_start", F.col("window.start"))
    .withColumn("window_end",   F.col("window.end"))
    .drop("window")
)

q10 = (
    df_fuel.writeStream
    .format("delta")
    .outputMode("update")
    .option("checkpointLocation", f"{CKPT_BASE}/fuel_burn_estimate")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .toTable("gold.fuel_burn_estimate")
)
print(f"✓ [10/11] gold.fuel_burn_estimate — query ID: {q10.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 11: Economic Activity Index
# MAGIC **1-hour tumbling window — aviation-based leading economic indicator by country**
# MAGIC
# MAGIC **Investment value:**
# MAGIC - Aviation activity leads GDP by 1-2 months (leading indicator)
# MAGIC - Index drop in a country = early warning of economic slowdown
# MAGIC - Index spike after trough = recovery confirmation before official data
# MAGIC
# MAGIC **Index formula:**
# MAGIC - Count unique aircraft per country per hour
# MAGIC - Index = (current_count / avg_world_density * 100), capped at 200

# COMMAND ----------

df_econ = (
    df_silver
    .groupBy(
        F.window("snapshot_time_ts", "1 hour"),
        "origin_country",
    )
    .agg(
        F.countDistinct("icao24").alias("unique_aircraft"),
        F.sum(F.when(~F.col("on_ground"), 1).otherwise(0)).alias("airborne_snapshots"),
        F.sum(F.when( F.col("on_ground"), 1).otherwise(0)).alias("ground_snapshots"),
        F.avg("velocity_kmh").alias("avg_velocity_kmh"),
    )
    .withColumn("window_start", F.col("window.start"))
    .withColumn("window_end",   F.col("window.end"))
    .drop("window")
)

def write_economic_index(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    world_avg = batch_df.agg(F.avg("unique_aircraft")).collect()[0][0] or 1.0
    result = batch_df.withColumn(
        "economic_activity_index",
        F.round(
            F.least(F.lit(200.0), (F.col("unique_aircraft") / world_avg) * 100),
            1
        )
    ).withColumn("activity_signal",
        F.when(F.col("economic_activity_index") >= 110, "EXPANDING")
        .when(F.col("economic_activity_index") <=  90, "CONTRACTING")
        .otherwise("STABLE")
    )
    result.write.format("delta").mode("append").saveAsTable("gold.economic_activity_index")

q11 = (
    df_econ.writeStream
    .foreachBatch(write_economic_index)
    .option("checkpointLocation", f"{CKPT_BASE}/economic_activity_index")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)
print(f"✓ [11/11] gold.economic_activity_index — query ID: {q11.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## All 11 Queries Running
# MAGIC
# MAGIC Monitor active queries:

# COMMAND ----------

active = spark.streams.active
print(f"\n{'='*55}")
print(f"  {len(active)} streaming queries active")
print(f"{'='*55}")
for q in active:
    print(f"  [{q.status['message'][:30]:<30}] id={str(q.id)[:8]}...")

# COMMAND ----------

# Keep all queries alive — blocks until any query terminates (error or manual stop)
spark.streams.awaitAnyTermination()
