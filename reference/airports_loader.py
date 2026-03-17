# Databricks notebook source
# MAGIC %md
# MAGIC # Reference: Load Airports Data
# MAGIC One-time batch job: download OurAirports data and load into `reference.airports` Delta table.
# MAGIC Run this notebook once before starting the pipeline.

# COMMAND ----------
"""
reference/airports_loader.py

One-time batch job: download OurAirports data and load into reference.airports Delta table.
Run this notebook once before starting the pipeline.

Data source: https://ourairports.com/data/airports.csv (public domain, ~80,000 airports)
"""

# Databricks notebook source
# COMMAND ----------

import requests
import io
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

S3_BUCKET    = "skystream-datalake-dev"   # Change to your bucket name
REF_TABLE    = "reference.airports"
AIRPORTS_URL = "https://ourairports.com/data/airports.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Download OurAirports CSV

# COMMAND ----------

print(f"Downloading airports data from {AIRPORTS_URL} ...")
response = requests.get(AIRPORTS_URL, timeout=30)
response.raise_for_status()
print(f"Downloaded {len(response.content) / 1024:.0f} KB")

# Save to S3 via DBFS
dbutils.fs.put(
    f"s3://{S3_BUCKET}/reference/airports_raw.csv",
    response.text,
    overwrite=True
)
print("✓ Saved to S3")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load into Spark DataFrame

# COMMAND ----------

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "false")
    .csv(f"s3://{S3_BUCKET}/reference/airports_raw.csv")
)

print(f"Total airports in raw data: {df_raw.count()}")
df_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Select and clean relevant fields

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
    # Keep only airports with scheduled service or large/medium airports
    .filter(
        (F.col("scheduled_service") == "yes") |
        (F.col("type").isin("large_airport", "medium_airport"))
    )
)

print(f"Filtered airports (scheduled service or large/medium): {df_airports.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write to reference.airports Delta table

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS reference")

df_airports.write.format("delta").mode("overwrite").saveAsTable(REF_TABLE)

print(f"✓ Written {df_airports.count()} airports to {REF_TABLE}")
display(spark.sql(f"SELECT * FROM {REF_TABLE} WHERE type = 'large_airport' LIMIT 20"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create summary stats

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     type,
# MAGIC     COUNT(*) AS airport_count,
# MAGIC     COUNT(DISTINCT country_code) AS countries
# MAGIC FROM reference.airports
# MAGIC GROUP BY type
# MAGIC ORDER BY airport_count DESC
