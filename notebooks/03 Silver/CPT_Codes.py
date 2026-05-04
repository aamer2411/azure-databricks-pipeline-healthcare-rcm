# Databricks notebook source
# MAGIC %md
# MAGIC # rcm_adb.silver CPT Codes ETL
# MAGIC
# MAGIC **Purpose**: Load and cleanse CPT codes data from Bronze to rcm_adb.silver layer with SCD2 history tracking.
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
BRONZE_CPTCODES = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/cptcodes"

# COMMAND ----------

# Read Bronze Data
from pyspark.sql import functions as f

# Read CPT codes data
cptcodes_df = spark.read.parquet(BRONZE_CPTCODES)

# Create temp view for SQL operations
cptcodes_df.createOrReplaceTempView("cptcodes")

print(f"Total CPT code records: {cptcodes_df.count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quality Checks: Apply data quality rules and add quarantine flag
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS
# MAGIC SELECT 
# MAGIC     cpt_codes,
# MAGIC     procedure_code_category,
# MAGIC     procedure_code_descriptions,
# MAGIC     code_status,
# MAGIC     CASE 
# MAGIC         WHEN cpt_codes IS NULL OR procedure_code_descriptions IS NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC FROM cptcodes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview quality checks
# MAGIC SELECT * FROM quality_checks LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD2 Step 1: Mark existing records as historical when source data has changed
# MAGIC MERGE INTO rcm_adb.silver.cptcodes AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.cpt_codes = source.cpt_codes AND target.is_current = true
# MAGIC WHEN MATCHED AND (
# MAGIC     target.procedure_code_category != source.procedure_code_category OR
# MAGIC     target.procedure_code_descriptions != source.procedure_code_descriptions OR
# MAGIC     target.code_status != source.code_status OR
# MAGIC     target.is_quarantined != source.is_quarantined
# MAGIC ) THEN
# MAGIC     UPDATE SET
# MAGIC         target.is_current = false,
# MAGIC         target.audit_modifieddate = current_timestamp()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD2 Step 2: Insert new records
# MAGIC MERGE INTO rcm_adb.silver.cptcodes AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.cpt_codes = source.cpt_codes AND target.is_current = true
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         cpt_codes, procedure_code_category, procedure_code_descriptions, code_status,
# MAGIC         is_quarantined, audit_insertdate, audit_modifieddate, is_current
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         source.cpt_codes, source.procedure_code_category, source.procedure_code_descriptions,
# MAGIC         source.code_status, source.is_quarantined, current_timestamp(), current_timestamp(), true
# MAGIC     )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verification: Check silver table
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_records,
# MAGIC     SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) AS current_records,
# MAGIC     SUM(CASE WHEN is_quarantined = true THEN 1 ELSE 0 END) AS quarantined_records
# MAGIC FROM rcm_adb.silver.cptcodes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample records
# MAGIC SELECT * FROM rcm_adb.silver.cptcodes WHERE is_current = true LIMIT 10