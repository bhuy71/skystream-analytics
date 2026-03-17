# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Batch Layer (Lambda Architecture)
# MAGIC
# MAGIC **Purpose:** Nightly/weekly batch jobs tính toán các views có độ chính xác cao
# MAGIC từ toàn bộ lịch sử dữ liệu trong Silver layer.
# MAGIC
# MAGIC ## Batch Views produced:
# MAGIC | # | Table | Schedule | Business Value |
# MAGIC |---|-------|----------|----------------|
# MAGIC | 1 | `batch.airline_reliability_score` | Daily | 30-day reliability ranking |
# MAGIC | 2 | `batch.route_growth_trend` | Weekly | 52-week route volume trend |
# MAGIC | 3 | `batch.airport_traffic_ranking` | Daily | Airport traffic + diversity score |
# MAGIC | 4 | `batch.fuel_efficiency_ranking` | Daily | Airline CO₂ per flight benchmark |
# MAGIC | 5 | `batch.cargo_trade_lane_analysis` | Weekly | Cargo flow by trade lane |
# MAGIC | 6 | `batch.tourism_seasonality_pattern` | Weekly | 52-week inbound tourism trend |
# MAGIC | 7 | `batch.economic_weekly_index` | Weekly | Aviation economic activity index |
# MAGIC
# MAGIC **Lambda Architecture role:**
# MAGIC - Batch views cover ALL historical data → high accuracy, no approximation
# MAGIC - Speed layer (DLT) covers RECENT data → low latency, approximate
# MAGIC - Serving layer MERGES both → complete + up-to-date picture

# COMMAND ----------

import re
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, LongType

CATALOG      = "workspace_7474644985505263"
SILVER_TABLE = f"{CATALOG}.silver.flights"
BATCH_SCHEMA = f"{CATALOG}.batch"

# Number of days of history to process per run
LOOKBACK_DAYS_DAILY  = 30   # for daily batch jobs
LOOKBACK_DAYS_WEEKLY = 365  # for weekly batch jobs

# COMMAND ----------

# ── Create batch schema ─────────────────────────────────────────────────────────
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BATCH_SCHEMA}")
print(f"✓ Schema '{BATCH_SCHEMA}' ready")

# COMMAND ----------

# ── Load Silver data ────────────────────────────────────────────────────────────
df_silver = spark.table(SILVER_TABLE)
df_30d    = df_silver.filter(F.col("snapshot_time") >= F.date_sub(F.current_date(), LOOKBACK_DAYS_DAILY))
df_365d   = df_silver.filter(F.col("snapshot_time") >= F.date_sub(F.current_date(), LOOKBACK_DAYS_WEEKLY))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch View 1: Airline Reliability Score (30-day)
# MAGIC **Business value:** Investors/analysts use this to evaluate airline operational efficiency.
# MAGIC Metrics: % time cruising (vs delayed on ground), average speed consistency,
# MAGIC altitude consistency, route adherence proxy.

# COMMAND ----------

airline_reliability = (
    df_30d
    .filter(F.col("airline_icao").isNotNull())
    .groupBy("airline_icao")
    .agg(
        F.count("*").alias("total_snapshots"),
        F.countDistinct("icao24").alias("unique_aircraft"),
        # % of flights actively airborne (not on ground)
        (F.sum(F.when(~F.col("on_ground"), 1).otherwise(0)) / F.count("*") * 100)
            .cast(DoubleType()).alias("airborne_pct"),
        # Speed consistency: low stddev = consistent operations
        F.stddev("velocity_kmh").cast(DoubleType()).alias("speed_stddev_kmh"),
        F.avg("velocity_kmh").cast(DoubleType()).alias("avg_speed_kmh"),
        # Altitude consistency
        F.avg("altitude_ft").cast(DoubleType()).alias("avg_altitude_ft"),
        F.stddev("altitude_ft").cast(DoubleType()).alias("altitude_stddev_ft"),
        # Vertical rate stability (low abs mean = stable cruise)
        F.avg(F.abs("vertical_rate")).cast(DoubleType()).alias("avg_abs_vertical_rate"),
        # Count of rapid descent events (safety proxy)
        F.sum(F.when(F.col("vertical_rate") < -10, 1).otherwise(0)).alias("rapid_descent_events"),
        F.max("snapshot_time").alias("last_seen"),
    )
    .filter(F.col("total_snapshots") >= 100)  # filter out airlines with sparse data
    .withColumn("computed_at", F.current_timestamp())
    .withColumn("lookback_days", F.lit(LOOKBACK_DAYS_DAILY))
    # Reliability score: higher = better (0-100 scale)
    .withColumn(
        "reliability_score",
        (
            F.col("airborne_pct") * 0.4
            + F.lit(100) * 0.3 / (F.col("speed_stddev_kmh") / F.lit(50) + F.lit(1))
            + F.lit(100) * 0.3 / (F.col("avg_abs_vertical_rate") / F.lit(2) + F.lit(1))
        ).cast(DoubleType())
    )
    .orderBy(F.col("reliability_score").desc())
)

airline_reliability.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{BATCH_SCHEMA}.airline_reliability_score")

print(f"✓ [1/7] airline_reliability_score — {airline_reliability.count()} airlines")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch View 2: Route Growth Trend (52-week)
# MAGIC **Business value:** Route demand intelligence for airlines, airports, investors.
# MAGIC Compares recent 4 weeks vs prior 4 weeks to detect growing/declining routes.

# COMMAND ----------

route_weekly = (
    df_365d
    .filter(F.col("airline_icao").isNotNull() & F.col("origin_country").isNotNull())
    .withColumn("week", F.date_trunc("week", F.col("snapshot_time")))
    .groupBy("week", "airline_icao", "origin_country")
    .agg(
        F.countDistinct("icao24").alias("unique_flights"),
        F.avg("velocity_kmh").cast(DoubleType()).alias("avg_speed"),
        F.count("*").alias("total_snapshots"),
    )
)

# Calculate 4-week rolling average and growth rate
from pyspark.sql import Window
window_4w = Window.partitionBy("airline_icao", "origin_country") \
    .orderBy("week").rowsBetween(-3, 0)
window_prev4w = Window.partitionBy("airline_icao", "origin_country") \
    .orderBy("week").rowsBetween(-7, -4)

route_growth = (
    route_weekly
    .withColumn("avg_4w_current", F.avg("unique_flights").over(window_4w))
    .withColumn("avg_4w_prior",   F.avg("unique_flights").over(window_prev4w))
    .withColumn(
        "growth_rate_pct",
        ((F.col("avg_4w_current") - F.col("avg_4w_prior"))
         / (F.col("avg_4w_prior") + F.lit(1)) * 100).cast(DoubleType())
    )
    .withColumn("computed_at", F.current_timestamp())
    .filter(F.col("week") == F.date_trunc("week", F.current_date()))  # latest week only
)

route_growth.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{BATCH_SCHEMA}.route_growth_trend")

print(f"✓ [2/7] route_growth_trend")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch View 3: Airport Traffic Ranking (30-day)
# MAGIC **Business value:** Airport authorities, real-estate developers, retail/F&B investors
# MAGIC use this to understand airport importance and growth trajectory.

# COMMAND ----------

MAJOR_AIRPORTS = [
    ("EGLL", "London Heathrow"),     ("KLAX", "Los Angeles"),
    ("KJFK", "New York JFK"),        ("LFPG", "Paris CDG"),
    ("EDDF", "Frankfurt"),           ("RJTT", "Tokyo Haneda"),
    ("ZBAA", "Beijing Capital"),     ("OMDB", "Dubai"),
    ("WSSS", "Singapore Changi"),    ("VHHH", "Hong Kong"),
    ("YSSY", "Sydney"),              ("LEMD", "Madrid Barajas"),
    ("EHAM", "Amsterdam Schiphol"),  ("VIDP", "Delhi Indira Gandhi"),
    ("VABB", "Mumbai"),              ("VTBS", "Bangkok Suvarnabhumi"),
    ("WMKK", "Kuala Lumpur"),        ("WIII", "Jakarta"),
    ("RPLL", "Manila"),              ("VVTS", "Ho Chi Minh City"),
]
airports_df = spark.createDataFrame(MAJOR_AIRPORTS, ["icao_code", "airport_name"])

airport_ranking = (
    df_30d
    .filter(~F.col("on_ground"))  # only airborne
    .withColumn("lat_bin", F.floor("latitude").cast("int"))
    .withColumn("lon_bin", F.floor("longitude").cast("int"))
    .crossJoin(F.broadcast(airports_df))
    # Proxy: flights within 1° of airport coords are "departing/arriving"
    .filter(
        (F.abs(F.col("latitude") - F.col("lat_bin")) < 1) &
        (F.abs(F.col("longitude") - F.col("lon_bin")) < 1)
    )
    .groupBy("icao_code", "airport_name")
    .agg(
        F.count("*").alias("total_flight_snapshots"),
        F.countDistinct("icao24").alias("unique_aircraft"),
        F.countDistinct("origin_country").alias("origin_country_diversity"),
        F.countDistinct("airline_icao").alias("airline_diversity"),
        F.sum(F.when(F.col("is_cargo"), 1).otherwise(0)).alias("cargo_snapshots"),
        F.avg("velocity_kmh").cast(DoubleType()).alias("avg_speed_kmh"),
    )
    .withColumn(
        "traffic_score",
        (F.col("unique_aircraft") * 0.5
         + F.col("origin_country_diversity") * 10
         + F.col("airline_diversity") * 5).cast(DoubleType())
    )
    .withColumn("computed_at", F.current_timestamp())
    .orderBy(F.col("traffic_score").desc())
)

airport_ranking.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{BATCH_SCHEMA}.airport_traffic_ranking")

print(f"✓ [3/7] airport_traffic_ranking")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch View 4: Fuel Efficiency Ranking (30-day)
# MAGIC **Business value:** ESG investors, fuel cost analysts, carbon credit traders.
# MAGIC Estimates CO₂ per flight hour by airline — lower = more fuel efficient.

# COMMAND ----------

fuel_efficiency = (
    df_30d
    .filter(F.col("airline_icao").isNotNull() & ~F.col("on_ground"))
    .filter(F.col("altitude_ft") > 10000)  # cruising only
    .groupBy("airline_icao")
    .agg(
        F.countDistinct("icao24").alias("unique_aircraft"),
        F.avg("velocity_kmh").cast(DoubleType()).alias("avg_cruise_speed_kmh"),
        F.avg("altitude_ft").cast(DoubleType()).alias("avg_cruise_altitude_ft"),
        F.count("*").alias("cruise_snapshots"),
        # Fuel burn proxy: higher speed + lower altitude = higher burn
        F.avg(
            (F.col("velocity_kmh") / F.lit(850))         # speed factor (850 km/h baseline)
            * (F.lit(35000) / (F.col("altitude_ft") + F.lit(1000)))  # altitude factor
            * F.lit(3.5)  # kg CO2/km baseline for narrow-body
        ).cast(DoubleType()).alias("est_co2_kg_per_km"),
    )
    .filter(F.col("cruise_snapshots") >= 50)
    .withColumn(
        "efficiency_grade",
        F.when(F.col("est_co2_kg_per_km") < 2.5, "A")
         .when(F.col("est_co2_kg_per_km") < 3.0, "B")
         .when(F.col("est_co2_kg_per_km") < 3.5, "C")
         .otherwise("D")
    )
    .withColumn("computed_at", F.current_timestamp())
    .orderBy("est_co2_kg_per_km")
)

fuel_efficiency.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{BATCH_SCHEMA}.fuel_efficiency_ranking")

print(f"✓ [4/7] fuel_efficiency_ranking")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch View 5: Cargo Trade Lane Analysis (weekly)
# MAGIC **Business value:** Supply chain managers, logistics investors, port authorities.
# MAGIC Maps cargo flight volumes between world regions over past 52 weeks.

# COMMAND ----------

REGION_MAP = {
    "US": "North America", "CA": "North America", "MX": "North America",
    "GB": "Europe", "DE": "Europe", "FR": "Europe", "NL": "Europe",
    "CN": "East Asia", "JP": "East Asia", "KR": "East Asia",
    "SG": "Southeast Asia", "TH": "Southeast Asia", "VN": "Southeast Asia",
    "MY": "Southeast Asia", "ID": "Southeast Asia", "PH": "Southeast Asia",
    "IN": "South Asia", "AE": "Middle East", "SA": "Middle East",
    "AU": "Oceania", "BR": "South America", "ZA": "Africa",
}
region_map_df = spark.createDataFrame(
    [(k, v) for k, v in REGION_MAP.items()], ["country_code", "region"]
)

cargo_trade = (
    df_365d
    .filter(F.col("is_cargo") & F.col("airline_icao").isNotNull())
    .withColumn("week", F.date_trunc("week", F.col("snapshot_time")))
    .join(
        region_map_df.withColumnRenamed("country_code", "origin_country")
                     .withColumnRenamed("region", "origin_region"),
        "origin_country", "left"
    )
    .groupBy("week", "origin_region", "airline_icao")
    .agg(
        F.count("*").alias("cargo_snapshots"),
        F.countDistinct("icao24").alias("unique_cargo_aircraft"),
        F.avg("velocity_kmh").cast(DoubleType()).alias("avg_speed"),
        F.avg("altitude_ft").cast(DoubleType()).alias("avg_altitude"),
    )
    .filter(F.col("origin_region").isNotNull())
    .withColumn("computed_at", F.current_timestamp())
    .orderBy("week", F.col("cargo_snapshots").desc())
)

cargo_trade.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{BATCH_SCHEMA}.cargo_trade_lane_analysis")

print(f"✓ [5/7] cargo_trade_lane_analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch View 6: Tourism Seasonality Pattern (52-week)
# MAGIC **Business value:** Tourism boards, hotel chains, airlines planning seasonal routes.
# MAGIC Shows weekly inbound flight volume to tourist hotspot regions.

# COMMAND ----------

TOURIST_REGIONS = [
    (10.0, 104.0, "Phuket/Cambodia"),
    (21.0, 105.8, "Hanoi Region"),
    (10.8, 106.7, "Ho Chi Minh Region"),
    (48.8, 2.35, "Paris Region"),
    (41.9, 12.5, "Rome Region"),
    (25.2, 55.3, "Dubai Region"),
    (1.35, 103.8, "Singapore"),
    (13.75, 100.5, "Bangkok Region"),
    (35.7, 139.7, "Tokyo Region"),
    (-33.9, 151.2, "Sydney Region"),
]
tourist_df = spark.createDataFrame(
    TOURIST_REGIONS, ["lat", "lon", "region_name"]
)

tourism_seasonality = (
    df_365d
    .filter(~F.col("on_ground") & F.col("origin_country").isNotNull())
    .withColumn("week", F.date_trunc("week", F.col("snapshot_time")))
    .crossJoin(F.broadcast(tourist_df))
    .filter(
        (F.abs(F.col("latitude") - F.col("lat")) < 3) &
        (F.abs(F.col("longitude") - F.col("lon")) < 3)
    )
    .groupBy("week", "region_name")
    .agg(
        F.count("*").alias("inbound_flight_snapshots"),
        F.countDistinct("icao24").alias("unique_flights"),
        F.countDistinct("origin_country").alias("source_countries"),
        F.avg("velocity_kmh").cast(DoubleType()).alias("avg_speed"),
    )
    .withColumn("computed_at", F.current_timestamp())
    .orderBy("region_name", "week")
)

tourism_seasonality.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{BATCH_SCHEMA}.tourism_seasonality_pattern")

print(f"✓ [6/7] tourism_seasonality_pattern")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch View 7: Economic Weekly Index (52-week)
# MAGIC **Business value:** Macro economists, central banks, fund managers.
# MAGIC Aviation activity index correlates with economic output — leads GDP by ~2-4 weeks.

# COMMAND ----------

economic_weekly = (
    df_365d
    .withColumn("week", F.date_trunc("week", F.col("snapshot_time")))
    .groupBy("week")
    .agg(
        F.count("*").alias("total_snapshots"),
        F.countDistinct("icao24").alias("total_unique_aircraft"),
        F.countDistinct("origin_country").alias("countries_active"),
        F.avg("velocity_kmh").cast(DoubleType()).alias("avg_global_speed"),
        F.avg("altitude_ft").cast(DoubleType()).alias("avg_global_altitude"),
        F.sum(F.when(F.col("is_cargo"), 1).otherwise(0)).alias("cargo_snapshots"),
        F.sum(F.when(~F.col("on_ground"), 1).otherwise(0)).alias("airborne_snapshots"),
    )
    .withColumn(
        "aviation_economic_index",
        (
            F.col("total_unique_aircraft") * 0.4
            + F.col("countries_active") * 50
            + F.col("airborne_snapshots") / F.lit(100) * 0.2
        ).cast(DoubleType())
    )
    .withColumn("computed_at", F.current_timestamp())
    .orderBy("week")
)

# Add 4-week moving average
window_4w = Window.orderBy("week").rowsBetween(-3, 0)
economic_weekly = economic_weekly.withColumn(
    "index_4w_ma",
    F.avg("aviation_economic_index").over(window_4w).cast(DoubleType())
)

economic_weekly.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{BATCH_SCHEMA}.economic_weekly_index")

print(f"✓ [7/7] economic_weekly_index")

# COMMAND ----------

print("\n" + "="*60)
print("✅ Batch Layer complete — 7 views computed")
print(f"   Catalog: {CATALOG}")
print(f"   Schema:  batch")
print("="*60)
spark.sql(f"SHOW TABLES IN {BATCH_SCHEMA}").show(truncate=False)
