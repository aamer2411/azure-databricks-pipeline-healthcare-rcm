# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Encounters ETL
# MAGIC
# MAGIC **Purpose**: Load and cleanse encounters data from Bronze to Silver layer with SCD2 history tracking.
# MAGIC
# MAGIC **Cluster**: rcm cluster
# MAGIC
# MAGIC **Dependencies**: 
# MAGIC - Bronze data must exist in ADLS

# COMMAND ----------

# Configuration - ADLS Paths
STORAGE_ACCOUNT = "rcmproject"
BRONZE_CONTAINER = "bronze"

# Bronze source paths
BRONZE_HOSA_ENCOUNTERS = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/hosa/encounters"
BRONZE_HOSB_ENCOUNTERS = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/hosb/encounters"

# COMMAND ----------

# Read Bronze Data
from pyspark.sql import functions as f

# Read Hospital A encounters data
df_hosa = spark.read.parquet(BRONZE_HOSA_ENCOUNTERS)

# Read Hospital B encounters data
df_hosb = spark.read.parquet(BRONZE_HOSB_ENCOUNTERS)

# Union the two dataframes
encounters_df = df_hosa.unionByName(df_hosb)

# Create temp view for SQL operations
encounters_df.createOrReplaceTempView("encounters")

print(f"Total encounters records: {encounters_df.count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quality Checks: Apply data quality rules and add quarantine flag
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS
# MAGIC SELECT 
# MAGIC     CONCAT(EncounterID, '-', datasource) AS EncounterID,
# MAGIC     EncounterID AS SRC_EncounterID,
# MAGIC     PatientID,
# MAGIC     EncounterDate,
# MAGIC     EncounterType,
# MAGIC     ProviderID,
# MAGIC     DepartmentID,
# MAGIC     ProcedureCode,
# MAGIC     InsertedDate AS SRC_InsertedDate,
# MAGIC     ModifiedDate AS SRC_ModifiedDate,
# MAGIC     datasource,
# MAGIC     CASE 
# MAGIC         WHEN EncounterID IS NULL OR PatientID IS NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC FROM encounters

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview quality checks
# MAGIC SELECT * FROM quality_checks LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD2 Step 1: Mark existing records as historical when source data has changed
# MAGIC MERGE INTO rcm_adb.silver.encounters AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.EncounterID = source.EncounterID AND target.is_current = true
# MAGIC WHEN MATCHED AND (
# MAGIC     target.SRC_EncounterID != source.SRC_EncounterID OR
# MAGIC     target.PatientID != source.PatientID OR
# MAGIC     target.EncounterDate != source.EncounterDate OR
# MAGIC     target.EncounterType != source.EncounterType OR
# MAGIC     target.ProviderID != source.ProviderID OR
# MAGIC     target.DepartmentID != source.DepartmentID OR
# MAGIC     target.ProcedureCode != source.ProcedureCode OR
# MAGIC     target.SRC_InsertedDate != source.SRC_InsertedDate OR
# MAGIC     target.SRC_ModifiedDate != source.SRC_ModifiedDate OR
# MAGIC     target.datasource != source.datasource OR
# MAGIC     target.is_quarantined != source.is_quarantined
# MAGIC ) THEN
# MAGIC     UPDATE SET
# MAGIC         target.is_current = false,
# MAGIC         target.audit_modifieddate = current_timestamp()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD2 Step 2: Insert new records
# MAGIC MERGE INTO rcm_adb.silver.encounters AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.EncounterID = source.EncounterID AND target.is_current = true
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         EncounterID, SRC_EncounterID, PatientID, EncounterDate, EncounterType, ProviderID,
# MAGIC         DepartmentID, ProcedureCode, SRC_InsertedDate, SRC_ModifiedDate, datasource,
# MAGIC         is_quarantined, audit_insertdate, audit_modifieddate, is_current
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         source.EncounterID, source.SRC_EncounterID, source.PatientID, source.EncounterDate,
# MAGIC         source.EncounterType, source.ProviderID, source.DepartmentID, source.ProcedureCode,
# MAGIC         source.SRC_InsertedDate, source.SRC_ModifiedDate, source.datasource,
# MAGIC         source.is_quarantined, current_timestamp(), current_timestamp(), true
# MAGIC     )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verification: Check silver table
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_records,
# MAGIC     SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) AS current_records,
# MAGIC     SUM(CASE WHEN is_quarantined = true THEN 1 ELSE 0 END) AS quarantined_records
# MAGIC FROM rcm_adb.silver.encounters

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample records
# MAGIC SELECT * FROM rcm_adb.silver.encounters WHERE is_current = true LIMIT 10