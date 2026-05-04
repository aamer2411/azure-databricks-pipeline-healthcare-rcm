# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Transactions ETL
# MAGIC
# MAGIC **Purpose**: Load and cleanse transactions data from Bronze to silver layer with SCD2 history tracking.
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
BRONZE_HOSA_TRANSACTIONS = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/hosa/transactions"
BRONZE_HOSB_TRANSACTIONS = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/hosb/transactions"

# COMMAND ----------

# Read Bronze Data
from pyspark.sql import functions as f

# Read Hospital A transactions data
df_hosa = spark.read.parquet(BRONZE_HOSA_TRANSACTIONS)

# Read Hospital B transactions data
df_hosb = spark.read.parquet(BRONZE_HOSB_TRANSACTIONS)

# Union the two dataframes
transactions_df = df_hosa.unionByName(df_hosb)

# Create temp view for SQL operations
transactions_df.createOrReplaceTempView("transactions")

print(f"Total transactions records: {transactions_df.count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quality Checks: Apply data quality rules and add quarantine flag
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS
# MAGIC SELECT 
# MAGIC     CONCAT(TransactionID, '-', datasource) AS TransactionID,
# MAGIC     TransactionID AS SRC_TransactionID,
# MAGIC     EncounterID,
# MAGIC     PatientID,
# MAGIC     ProviderID,
# MAGIC     DeptID,
# MAGIC     VisitDate,
# MAGIC     ServiceDate,
# MAGIC     PaidDate,
# MAGIC     VisitType,
# MAGIC     Amount,
# MAGIC     AmountType,
# MAGIC     PaidAmount,
# MAGIC     ClaimID,
# MAGIC     PayorID,
# MAGIC     ProcedureCode,
# MAGIC     ICDCode,
# MAGIC     LineOfBusiness,
# MAGIC     MedicaidID,
# MAGIC     MedicareID,
# MAGIC     InsertDate AS SRC_InsertDate,
# MAGIC     ModifiedDate AS SRC_ModifiedDate,
# MAGIC     datasource,
# MAGIC     CASE 
# MAGIC         WHEN EncounterID IS NULL OR PatientID IS NULL OR TransactionID IS NULL OR VisitDate IS NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC FROM transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview quality checks
# MAGIC SELECT * FROM quality_checks LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD2 Step 1: Mark existing records as historical when source data has changed
# MAGIC MERGE INTO rcm_adb.silver.transactions AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.TransactionID = source.TransactionID AND target.is_current = true
# MAGIC WHEN MATCHED AND (
# MAGIC     target.SRC_TransactionID != source.SRC_TransactionID OR
# MAGIC     target.EncounterID != source.EncounterID OR
# MAGIC     target.PatientID != source.PatientID OR
# MAGIC     target.ProviderID != source.ProviderID OR
# MAGIC     target.DeptID != source.DeptID OR
# MAGIC     target.VisitDate != source.VisitDate OR
# MAGIC     target.ServiceDate != source.ServiceDate OR
# MAGIC     target.PaidDate != source.PaidDate OR
# MAGIC     target.VisitType != source.VisitType OR
# MAGIC     target.Amount != source.Amount OR
# MAGIC     target.AmountType != source.AmountType OR
# MAGIC     target.PaidAmount != source.PaidAmount OR
# MAGIC     target.ClaimID != source.ClaimID OR
# MAGIC     target.PayorID != source.PayorID OR
# MAGIC     target.ProcedureCode != source.ProcedureCode OR
# MAGIC     target.ICDCode != source.ICDCode OR
# MAGIC     target.LineOfBusiness != source.LineOfBusiness OR
# MAGIC     target.MedicaidID != source.MedicaidID OR
# MAGIC     target.MedicareID != source.MedicareID OR
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
# MAGIC MERGE INTO rcm_adb.silver.transactions AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.TransactionID = source.TransactionID AND target.is_current = true
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         TransactionID, SRC_TransactionID, EncounterID, PatientID, ProviderID, DeptID,
# MAGIC         VisitDate, ServiceDate, PaidDate, VisitType, Amount, AmountType, PaidAmount,
# MAGIC         ClaimID, PayorID, ProcedureCode, ICDCode, LineOfBusiness, MedicaidID, MedicareID,
# MAGIC         SRC_InsertDate, SRC_ModifiedDate, datasource, is_quarantined,
# MAGIC         audit_insertdate, audit_modifieddate, is_current
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         source.TransactionID, source.SRC_TransactionID, source.EncounterID, source.PatientID,
# MAGIC         source.ProviderID, source.DeptID, source.VisitDate, source.ServiceDate, source.PaidDate,
# MAGIC         source.VisitType, source.Amount, source.AmountType, source.PaidAmount, source.ClaimID,
# MAGIC         source.PayorID, source.ProcedureCode, source.ICDCode, source.LineOfBusiness,
# MAGIC         source.MedicaidID, source.MedicareID, source.SRC_InsertDate, source.SRC_ModifiedDate,
# MAGIC         source.datasource, source.is_quarantined, current_timestamp(), current_timestamp(), true
# MAGIC     )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verification: Check silver table
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_records,
# MAGIC     SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) AS current_records,
# MAGIC     SUM(CASE WHEN is_quarantined = true THEN 1 ELSE 0 END) AS quarantined_records
# MAGIC FROM rcm_adb.silver.transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample records
# MAGIC SELECT * FROM rcm_adb.silver.transactions WHERE is_current = true LIMIT 10