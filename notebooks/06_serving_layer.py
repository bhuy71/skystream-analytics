# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Serving Layer (Lambda Architecture)
# MAGIC
# MAGIC **Purpose:** Merge Batch Layer views + Speed Layer (streaming) views thành
# MAGIC unified views phục vụ dashboard và BI tools.
# MAGIC
# MAGIC **Lambda Architecture:**
# MAGIC ```
# MAGIC Batch Layer  (batch.*)  ──┐
# MAGIC                            ├──► Serving Layer (serving.*) ──► Dashboard
# MAGIC Speed Layer  (gold.*)   ──┘
# MAGIC ```
# MAGIC
# MAGIC ## Serving Views:
# MAGIC | View | Batch source | Speed source | Use case |
# MAGIC |------|-------------|--------------|----------|
# MAGIC | `serving.airline_360` | reliability_score | live market share | Airline investor dashboard |
# MAGIC | `serving.airport_ops` | traffic_ranking | live congestion | Airport ops center |
# MAGIC | `serving.route_intelligence` | route_growth | live route demand | Route planning |
# MAGIC | `serving.cargo_intelligence` | trade_lane | live cargo flow | Supply chain dashboard |
# MAGIC | `serving.sustainability_report` | fuel_efficiency | live fuel burn | ESG dashboard |
# MAGIC | `serving.economic_dashboard` | economic_weekly | live econ index | Macro dashboard |
# MAGIC | `serving.tourism_dashboard` | seasonality | live tourism signal | Tourism investment |

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG      = "workspace_7474644985505263"
BATCH_SCHEMA = f"{CATALOG}.batch"
GOLD_SCHEMA  = f"{CATALOG}.gold"
SERVING      = f"{CATALOG}.serving"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SERVING}")
print(f"✓ Schema '{SERVING}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Serving View 1: Airline 360° Dashboard
# MAGIC Kết hợp: Batch reliability score (30-day history) + Live market share (last 30 min)

# COMMAND ----------

batch_reliability = spark.table(f"{BATCH_SCHEMA}.airline_reliability_score") \
    .select(
        "airline_icao",
        "reliability_score",
        "airborne_pct",
        "avg_speed_kmh",
        "unique_aircraft",
        "rapid_descent_events",
        F.col("computed_at").alias("batch_computed_at"),
        F.col("lookback_days"),
    )

live_market_share = spark.table(f"{GOLD_SCHEMA}.airline_market_share") \
    .select(
        "airline_icao",
        F.col("active_flights").alias("live_active_flights"),
        F.col("market_share_pct").alias("live_market_share_pct"),
        F.col("window_start").alias("live_window_start"),
    )

airline_360 = (
    batch_reliability
    .join(live_market_share, "airline_icao", "left")
    .withColumn("data_source", F.lit("batch+speed"))
    .withColumn("serving_timestamp", F.current_timestamp())
)

airline_360.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{SERVING}.airline_360")

print(f"✓ [1/7] serving.airline_360 — {airline_360.count()} airlines")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Serving View 2: Airport Ops Center
# MAGIC Kết hợp: Batch traffic ranking (30-day) + Live congestion score (last 5 min)

# COMMAND ----------

batch_airport = spark.table(f"{BATCH_SCHEMA}.airport_traffic_ranking") \
    .select(
        "icao_code",
        "airport_name",
        "traffic_score",
        "unique_aircraft",
        "origin_country_diversity",
        "airline_diversity",
        F.col("computed_at").alias("batch_computed_at"),
    )

live_congestion = spark.table(f"{GOLD_SCHEMA}.airport_congestion") \
    .select(
        "icao_code",
        F.col("congestion_score").alias("live_congestion_score"),
        F.col("aircraft_count").alias("live_aircraft_count"),
        F.col("window_start").alias("live_window_start"),
    )

airport_ops = (
    batch_airport
    .join(live_congestion, "icao_code", "left")
    .withColumn(
        "operational_status",
        F.when(F.col("live_congestion_score") > 0.8, "CRITICAL")
         .when(F.col("live_congestion_score") > 0.6, "HIGH")
         .when(F.col("live_congestion_score") > 0.3, "NORMAL")
         .otherwise("LOW")
    )
    .withColumn("data_source", F.lit("batch+speed"))
    .withColumn("serving_timestamp", F.current_timestamp())
)

airport_ops.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{SERVING}.airport_ops")

print(f"✓ [2/7] serving.airport_ops — {airport_ops.count()} airports")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Serving View 3: Route Intelligence
# MAGIC Kết hợp: Batch route growth trend (52-week) + Live route demand index

# COMMAND ----------

batch_routes = spark.table(f"{BATCH_SCHEMA}.route_growth_trend") \
    .select(
        "airline_icao",
        "origin_country",
        "growth_rate_pct",
        "avg_4w_current",
        F.col("computed_at").alias("batch_computed_at"),
    )

live_route_demand = spark.table(f"{GOLD_SCHEMA}.route_demand_index") \
    .select(
        "airline_icao",
        "origin_country",
        F.col("flight_count").alias("live_flight_count"),
        F.col("demand_score").alias("live_demand_score"),
        F.col("window_start").alias("live_window_start"),
    )

route_intelligence = (
    batch_routes
    .join(live_route_demand, ["airline_icao", "origin_country"], "outer")
    .withColumn(
        "investment_signal",
        F.when(
            (F.col("growth_rate_pct") > 10) & (F.col("live_demand_score") > 50),
            "STRONG_BUY"
        ).when(
            (F.col("growth_rate_pct") > 5) | (F.col("live_demand_score") > 30),
            "BUY"
        ).when(
            F.col("growth_rate_pct") < -10,
            "SELL"
        ).otherwise("HOLD")
    )
    .withColumn("data_source", F.lit("batch+speed"))
    .withColumn("serving_timestamp", F.current_timestamp())
)

route_intelligence.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{SERVING}.route_intelligence")

print(f"✓ [3/7] serving.route_intelligence")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Serving View 4: Cargo Intelligence
# MAGIC Kết hợp: Batch cargo trade lane (52-week) + Live cargo flow (last 1 min)

# COMMAND ----------

batch_cargo = spark.table(f"{BATCH_SCHEMA}.cargo_trade_lane_analysis") \
    .groupBy("origin_region", "airline_icao") \
    .agg(
        F.sum("unique_cargo_aircraft").alias("historical_cargo_aircraft"),
        F.avg("avg_speed").cast("double").alias("historical_avg_speed"),
        F.max("computed_at").alias("batch_computed_at"),
    )

live_cargo = spark.table(f"{GOLD_SCHEMA}.cargo_flow") \
    .select(
        "origin_region",
        F.col("cargo_flights").alias("live_cargo_flights"),
        F.col("cargo_pct").alias("live_cargo_pct"),
        F.col("window_start").alias("live_window_start"),
    )

cargo_intelligence = (
    batch_cargo
    .join(live_cargo, "origin_region", "left")
    .withColumn("data_source", F.lit("batch+speed"))
    .withColumn("serving_timestamp", F.current_timestamp())
)

cargo_intelligence.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{SERVING}.cargo_intelligence")

print(f"✓ [4/7] serving.cargo_intelligence")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Serving View 5: Sustainability / ESG Report
# MAGIC Kết hợp: Batch fuel efficiency (30-day) + Live fuel burn estimate

# COMMAND ----------

batch_fuel = spark.table(f"{BATCH_SCHEMA}.fuel_efficiency_ranking") \
    .select(
        "airline_icao",
        "efficiency_grade",
        "est_co2_kg_per_km",
        "avg_cruise_speed_kmh",
        "unique_aircraft",
        F.col("computed_at").alias("batch_computed_at"),
    )

live_fuel = spark.table(f"{GOLD_SCHEMA}.fuel_burn_estimate") \
    .select(
        "airline_icao",
        F.col("total_fuel_kg_hr").alias("live_fuel_kg_hr"),
        F.col("total_co2_kg_hr").alias("live_co2_kg_hr"),
        F.col("active_flights").alias("live_active_flights"),
        F.col("window_start").alias("live_window_start"),
    )

sustainability_report = (
    batch_fuel
    .join(live_fuel, "airline_icao", "left")
    .withColumn(
        "esg_risk_flag",
        F.when(F.col("efficiency_grade") == "D", "HIGH_RISK")
         .when(F.col("efficiency_grade") == "C", "MEDIUM_RISK")
         .otherwise("LOW_RISK")
    )
    .withColumn("data_source", F.lit("batch+speed"))
    .withColumn("serving_timestamp", F.current_timestamp())
)

sustainability_report.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{SERVING}.sustainability_report")

print(f"✓ [5/7] serving.sustainability_report")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Serving View 6: Economic Intelligence Dashboard
# MAGIC Kết hợp: Batch economic weekly index (52-week) + Live economic activity index

# COMMAND ----------

batch_econ = spark.table(f"{BATCH_SCHEMA}.economic_weekly_index") \
    .orderBy(F.col("week").desc()).limit(52) \
    .select(
        "week",
        "aviation_economic_index",
        "index_4w_ma",
        "total_unique_aircraft",
        "countries_active",
        "cargo_snapshots",
        F.col("computed_at").alias("batch_computed_at"),
    )

live_econ = spark.table(f"{GOLD_SCHEMA}.economic_activity_index") \
    .select(
        F.col("aviation_index").alias("live_aviation_index"),
        F.col("total_countries").alias("live_countries"),
        F.col("total_flights").alias("live_total_flights"),
        F.col("window_start").alias("live_window_start"),
    ).orderBy(F.col("live_window_start").desc()).limit(1)

# Cross join latest live value with all 52 weeks of history
economic_dashboard = batch_econ.crossJoin(F.broadcast(live_econ)) \
    .withColumn("data_source", F.lit("batch+speed")) \
    .withColumn("serving_timestamp", F.current_timestamp())

economic_dashboard.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{SERVING}.economic_dashboard")

print(f"✓ [6/7] serving.economic_dashboard")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Serving View 7: Tourism Investment Dashboard
# MAGIC Kết hợp: Batch tourism seasonality (52-week) + Live tourism demand signal

# COMMAND ----------

batch_tourism = spark.table(f"{BATCH_SCHEMA}.tourism_seasonality_pattern") \
    .groupBy("region_name") \
    .agg(
        F.avg("unique_flights").cast("double").alias("avg_weekly_flights_52w"),
        F.max("unique_flights").alias("peak_weekly_flights"),
        F.min("unique_flights").alias("trough_weekly_flights"),
        F.max("computed_at").alias("batch_computed_at"),
    )

live_tourism = spark.table(f"{GOLD_SCHEMA}.tourism_demand_signal") \
    .select(
        "region_name",
        F.col("inbound_flights").alias("live_inbound_flights"),
        F.col("source_countries").alias("live_source_countries"),
        F.col("window_start").alias("live_window_start"),
    )

tourism_dashboard = (
    batch_tourism
    .join(live_tourism, "region_name", "left")
    .withColumn(
        "vs_seasonal_avg_pct",
        ((F.col("live_inbound_flights") - F.col("avg_weekly_flights_52w"))
         / (F.col("avg_weekly_flights_52w") + F.lit(1)) * 100).cast("double")
    )
    .withColumn(
        "tourism_signal",
        F.when(F.col("vs_seasonal_avg_pct") > 20, "ABOVE_TREND")
         .when(F.col("vs_seasonal_avg_pct") < -20, "BELOW_TREND")
         .otherwise("ON_TREND")
    )
    .withColumn("data_source", F.lit("batch+speed"))
    .withColumn("serving_timestamp", F.current_timestamp())
)

tourism_dashboard.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{SERVING}.tourism_dashboard")

print(f"✓ [7/7] serving.tourism_dashboard")

# COMMAND ----------

print("\n" + "="*60)
print("✅ Serving Layer complete — 7 unified views ready")
print(f"   Catalog : {CATALOG}")
print(f"   Schema  : serving")
print("\nViews available for Databricks SQL Dashboard:")
spark.sql(f"SHOW TABLES IN {SERVING}").show(truncate=False)
print("="*60)
