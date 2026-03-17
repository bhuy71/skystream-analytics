# Databricks notebook source
# MAGIC %md
# MAGIC # Reference: Load Airports Data
# MAGIC One-time batch job: download OurAirports data and load into `reference.airports` Delta table.
# MAGIC Run this notebook once before starting the pipeline.

# COMMAND ----------

import requests
import io
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

CATALOG      = "workspace_7474644985505263"
SCHEMA       = "reference"
REF_TABLE    = f"{CATALOG}.{SCHEMA}.airports"
AIRPORTS_URL = "https://ourairports.com/data/airports.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Download OurAirports CSV

# COMMAND ----------

print(f"Downloading airports data from {AIRPORTS_URL} ...")
response = requests.get(AIRPORTS_URL, timeout=60)
response.raise_for_status()
print(f"Downloaded {len(response.content) / 1024:.0f} KB")

# Load directly into Spark from in-memory string (no DBFS needed)
df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "false") \
    .csv(spark.sparkContext.parallelize(response.text.splitlines()))

print(f"Total airports in raw data: {df_raw.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Select and clean relevant fields

# COMMAND ----------

df_airports = (
    df_raw
    .select(
        F.col("ident").cast(StringType()).alias("icao_code"),
        F.col("type").cast(StringType()),
        F.col("name").cast(StringType()),
        F.col("latitude_deg").cast(DoubleType()).alias("latitude"),
        F.col("longitude_deg").cast(DoubleType()).alias("longitude"),
        F.col("elevation_ft").cast(IntegerType()),
        F.col("continent").cast(StringType()),
        F.col("iso_country").cast(StringType()).alias("country_code"),
        F.col("iso_region").cast(StringType()).alias("region_code"),
        F.col("municipality").cast(StringType()).alias("city"),
        F.col("iata_code").cast(StringType()),
        F.col("scheduled_service").cast(StringType()),
    )
    .filter(F.col("icao_code").isNotNull())
    .filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
    .filter(
        (F.col("scheduled_service") == "yes") |
        (F.col("type").isin("large_airport", "medium_airport"))
    )
)

print(f"Filtered airports: {df_airports.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write to Unity Catalog Delta table

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

df_airports.write.format("delta").mode("overwrite").saveAsTable(REF_TABLE)

print(f"✓ Written to {REF_TABLE}")
display(spark.sql(f"SELECT * FROM {REF_TABLE} WHERE type = 'large_airport' LIMIT 20"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT type, COUNT(*) AS airport_count, COUNT(DISTINCT country_code) AS countries
# MAGIC FROM workspace_7474644985505263.reference.airports
# MAGIC GROUP BY type
# MAGIC ORDER BY airport_count DESC
