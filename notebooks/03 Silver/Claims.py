# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Claims ETL
# MAGIC
# MAGIC **Purpose**: Load and cleanse claims data from Bronze to Silver layer with SCD2 history tracking.
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
BRONZE_CLAIMS = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/claims"

# COMMAND ----------

# Read Bronze Data
from pyspark.sql import functions as f

# Read claims data
claims_df = spark.read.parquet(BRONZE_CLAIMS)

# Create temp view for SQL operations
claims_df.createOrReplaceTempView("claims")

print(f"Total claims records: {claims_df.count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quality Checks: Apply data quality rules and add quarantine flag
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS
# MAGIC SELECT 
# MAGIC     CONCAT(ClaimID, '-', datasource) AS ClaimID,
# MAGIC     ClaimID AS SRC_ClaimID,
# MAGIC     TransactionID,
# MAGIC     PatientID,
# MAGIC     EncounterID,
# MAGIC     ProviderID,
# MAGIC     DeptID,
# MAGIC     CAST(ServiceDate AS DATE) AS ServiceDate,
# MAGIC     CAST(ClaimDate AS DATE) AS ClaimDate,
# MAGIC     PayorID,
# MAGIC     ClaimAmount,
# MAGIC     PaidAmount,
# MAGIC     ClaimStatus,
# MAGIC     PayorType,
# MAGIC     Deductible,
# MAGIC     Coinsurance,
# MAGIC     Copay,
# MAGIC     CAST(InsertDate AS DATE) AS SRC_InsertDate,
# MAGIC     CAST(ModifiedDate AS DATE) AS SRC_ModifiedDate,
# MAGIC     datasource,
# MAGIC     CASE 
# MAGIC         WHEN ClaimID IS NULL OR TransactionID IS NULL OR PatientID IS NULL OR ServiceDate IS NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC FROM claims

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview quality checks
# MAGIC SELECT * FROM quality_checks LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD2 Step 1: Mark existing records as historical when source data has changed
# MAGIC MERGE INTO rcm_adb.silver.claims AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.ClaimID = source.ClaimID AND target.is_current = true
# MAGIC WHEN MATCHED AND (
# MAGIC     target.SRC_ClaimID != source.SRC_ClaimID OR
# MAGIC     target.TransactionID != source.TransactionID OR
# MAGIC     target.PatientID != source.PatientID OR
# MAGIC     target.EncounterID != source.EncounterID OR
# MAGIC     target.ProviderID != source.ProviderID OR
# MAGIC     target.DeptID != source.DeptID OR
# MAGIC     target.ServiceDate != source.ServiceDate OR
# MAGIC     target.ClaimDate != source.ClaimDate OR
# MAGIC     target.PayorID != source.PayorID OR
# MAGIC     target.ClaimAmount != source.ClaimAmount OR
# MAGIC     target.PaidAmount != source.PaidAmount OR
# MAGIC     target.ClaimStatus != source.ClaimStatus OR
# MAGIC     target.PayorType != source.PayorType OR
# MAGIC     target.Deductible != source.Deductible OR
# MAGIC     target.Coinsurance != source.Coinsurance OR
# MAGIC     target.Copay != source.Copay OR
# MAGIC     target.SRC_InsertDate != source.SRC_InsertDate OR
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
# MAGIC MERGE INTO rcm_adb.silver.claims AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.ClaimID = source.ClaimID AND target.is_current = true
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         ClaimID, SRC_ClaimID, TransactionID, PatientID, EncounterID, ProviderID, DeptID,
# MAGIC         ServiceDate, ClaimDate, PayorID, ClaimAmount, PaidAmount, ClaimStatus, PayorType,
# MAGIC         Deductible, Coinsurance, Copay, SRC_InsertDate, SRC_ModifiedDate, datasource,
# MAGIC         is_quarantined, audit_insertdate, audit_modifieddate, is_current
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         source.ClaimID, source.SRC_ClaimID, source.TransactionID, source.PatientID,
# MAGIC         source.EncounterID, source.ProviderID, source.DeptID, source.ServiceDate,
# MAGIC         source.ClaimDate, source.PayorID, source.ClaimAmount, source.PaidAmount,
# MAGIC         source.ClaimStatus, source.PayorType, source.Deductible, source.Coinsurance,
# MAGIC         source.Copay, source.SRC_InsertDate, source.SRC_ModifiedDate, source.datasource,
# MAGIC         source.is_quarantined, current_timestamp(), current_timestamp(), true
# MAGIC     )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verification: Check Silver table
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_records,
# MAGIC     SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) AS current_records,
# MAGIC     SUM(CASE WHEN is_quarantined = true THEN 1 ELSE 0 END) AS quarantined_records
# MAGIC FROM rcm_adb.silver.claims

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample records
# MAGIC SELECT * FROM rcm_adb.silver.claims WHERE is_current = true LIMIT 10