# Databricks notebook source
# MAGIC %md
# MAGIC # Silver ICD Codes ETL
# MAGIC
# MAGIC **Purpose**: Load and cleanse ICD codes data from Bronze to silver layer with SCD2 history tracking.
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
BRONZE_ICD_CODES = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/icd_codes"

# COMMAND ----------

# Read Bronze Data
from pyspark.sql import functions as f

# Read ICD codes data
icd_df = spark.read.parquet(BRONZE_ICD_CODES)

# Create temp view for SQL operations
icd_df.createOrReplaceTempView("staging_icd_codes")

print(f"Total ICD code records: {icd_df.count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quality Checks: Apply data quality rules and add quarantine flag
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS
# MAGIC SELECT 
# MAGIC     icd_code,
# MAGIC     icd_code_type,
# MAGIC     code_description,
# MAGIC     CASE 
# MAGIC         WHEN icd_code IS NULL OR code_description IS NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC FROM staging_icd_codes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview quality checks
# MAGIC SELECT * FROM quality_checks LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD2 Step 1: Mark existing records as historical when source data has changed
# MAGIC MERGE INTO rcm_adb.silver.icd_codes AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.icd_code = source.icd_code AND target.is_current = true
# MAGIC WHEN MATCHED AND (
# MAGIC     target.icd_code_type != source.icd_code_type OR
# MAGIC     target.code_description != source.code_description OR
# MAGIC     target.is_quarantined != source.is_quarantined
# MAGIC ) THEN
# MAGIC     UPDATE SET
# MAGIC         target.is_current = false,
# MAGIC         target.audit_modifieddate = current_timestamp()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD2 Step 2: Insert new records
# MAGIC MERGE INTO rcm_adb.silver.icd_codes AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.icd_code = source.icd_code AND target.is_current = true
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         icd_code, icd_code_type, code_description, is_quarantined,
# MAGIC         audit_insertdate, audit_modifieddate, is_current
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         source.icd_code, source.icd_code_type, source.code_description, source.is_quarantined,
# MAGIC         current_timestamp(), current_timestamp(), true
# MAGIC     )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verification: Check silver table
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_records,
# MAGIC     SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) AS current_records,
# MAGIC     SUM(CASE WHEN is_quarantined = true THEN 1 ELSE 0 END) AS quarantined_records
# MAGIC FROM rcm_adb.silver.icd_codes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample records
# MAGIC SELECT * FROM rcm_adb.silver.icd_codes WHERE is_current = true LIMIT 10