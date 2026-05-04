# Databricks notebook source
# MAGIC %md
# MAGIC # Silver NPI Extract ETL
# MAGIC
# MAGIC **Purpose**: Load and cleanse NPI data from Bronze to silver layer with SCD2 history tracking.
# MAGIC
# MAGIC **Cluster**: rcm cluster
# MAGIC
# MAGIC **Dependencies**: 
# MAGIC - Bronze data must exist in ADLS

# COMMAND ----------

# Configuration - ADLS Paths
STORAGE_ACCOUNT = "rcmproject"
BRONZE_CONTAINER = "bronze"

# Bronze source path
BRONZE_NPI_EXTRACT = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/npi_extract"

# COMMAND ----------

# Read Bronze Data
from pyspark.sql import functions as f

# Read NPI extract data
npi_df = spark.read.parquet(BRONZE_NPI_EXTRACT)

# Create temp view for SQL operations
npi_df.createOrReplaceTempView("npi_extract")

print(f"Total NPI records: {npi_df.count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quality Checks: Apply data quality rules and add quarantine flag
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS
# MAGIC SELECT 
# MAGIC     npi_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     position,
# MAGIC     organisation_name,
# MAGIC     last_updated,
# MAGIC     CASE 
# MAGIC         WHEN npi_id IS NULL OR first_name IS NULL OR last_name IS NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC FROM npi_extract

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview quality checks
# MAGIC SELECT * FROM quality_checks LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD2 Step 1: Mark existing records as historical when source data has changed
# MAGIC MERGE INTO rcm_adb.silver.npi_extract AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.npi_id = source.npi_id AND target.is_current = true
# MAGIC WHEN MATCHED AND (
# MAGIC     target.first_name != source.first_name OR
# MAGIC     target.last_name != source.last_name OR
# MAGIC     target.position != source.position OR
# MAGIC     target.organisation_name != source.organisation_name OR
# MAGIC     target.last_updated != source.last_updated OR
# MAGIC     target.is_quarantined != source.is_quarantined
# MAGIC ) THEN
# MAGIC     UPDATE SET
# MAGIC         target.is_current = false,
# MAGIC         target.audit_modifieddate = current_timestamp()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD2 Step 2: Insert new records
# MAGIC MERGE INTO rcm_adb.silver.npi_extract AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.npi_id = source.npi_id AND target.is_current = true
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         npi_id, first_name, last_name, position, organisation_name, last_updated,
# MAGIC         is_quarantined, audit_insertdate, audit_modifieddate, is_current
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         source.npi_id, source.first_name, source.last_name, source.position,
# MAGIC         source.organisation_name, source.last_updated, source.is_quarantined,
# MAGIC         current_timestamp(), current_timestamp(), true
# MAGIC     )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verification: Check silver table
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_records,
# MAGIC     SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) AS current_records,
# MAGIC     SUM(CASE WHEN is_quarantined = true THEN 1 ELSE 0 END) AS quarantined_records
# MAGIC FROM rcm_adb.silver.npi_extract

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample records
# MAGIC SELECT * FROM rcm_adb.silver.npi_extract WHERE is_current = true LIMIT 10