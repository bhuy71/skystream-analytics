# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Layer: Auto Loader Ingestion
# MAGIC
# MAGIC **Purpose:** Monitor S3 for new raw flight state files delivered by Kinesis Firehose
# MAGIC and incrementally ingest them into the `bronze.raw_flight_states` Delta table.
# MAGIC
# MAGIC **Data flow:** Kinesis Firehose → S3 `/bronze/raw_states/` → Auto Loader → Delta
# MAGIC
# MAGIC **Notes:**
# MAGIC - Append-only, no transformations at this layer
# MAGIC - Auto Loader handles schema inference and evolution automatically
# MAGIC - Each record = one aircraft snapshot at one point in time

# COMMAND ----------

# ── Configuration ──────────────────────────────────────────────────────────────
S3_BUCKET        = "skystream-datalake-dev"           # Change to your bucket name
S3_BRONZE_PATH   = f"s3://{S3_BUCKET}/bronze/raw_states/"
SCHEMA_LOCATION  = f"s3://{S3_BUCKET}/_schemas/bronze"
CHECKPOINT_PATH  = f"s3://{S3_BUCKET}/_checkpoints/bronze_autoloader"
BRONZE_TABLE     = "bronze.raw_flight_states"
TRIGGER_INTERVAL = "30 seconds"

# COMMAND ----------

# ── Create databases ────────────────────────────────────────────────────────────
for db in ["bronze", "silver", "gold", "reference"]:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    print(f"✓ Database '{db}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start Auto Loader Streaming Query
# MAGIC
# MAGIC `cloudFiles` format uses S3 event notifications (or directory listing fallback)
# MAGIC to detect new files. Schema is inferred from the first batch and stored at
# MAGIC `SCHEMA_LOCATION` so it persists across notebook restarts.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name

df_bronze = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.maxFilesPerTrigger", 1000)
    .option("multiLine", "false")                    # one JSON object per line (NDJSON)
    .load(S3_BRONZE_PATH)
    .withColumn("_bronze_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

# COMMAND ----------

bronze_query = (
    df_bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .toTable(BRONZE_TABLE)
)

print(f"✓ Bronze streaming query started — ID: {bronze_query.id}")
print(f"  Source : {S3_BRONZE_PATH}")
print(f"  Target : {BRONZE_TABLE}")
print(f"  Trigger: every {TRIGGER_INTERVAL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Bronze Data
# MAGIC Run the cell below (separately) after data has started flowing to inspect records.

# COMMAND ----------

# display(
#     spark.sql(f"""
#         SELECT
#             icao24,
#             callsign,
#             origin_country,
#             latitude,
#             longitude,
#             baro_altitude,
#             on_ground,
#             velocity,
#             vertical_rate,
#             snapshot_time,
#             ingestion_time,
#             _bronze_ingested_at,
#             _source_file
#         FROM {BRONZE_TABLE}
#         ORDER BY _bronze_ingested_at DESC
#         LIMIT 20
#     """)
# )

# COMMAND ----------

# Keep the streaming query alive until the cluster terminates or query is stopped
bronze_query.awaitTermination()
