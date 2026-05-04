# Databricks notebook source
# MAGIC %md
# MAGIC # Gold fact_transactions ETL
# MAGIC
# MAGIC **Purpose**: Load transactions fact table from Silver to Gold layer.
# MAGIC
# MAGIC **Cluster**: rcm cluster
# MAGIC
# MAGIC **Dependencies**: 
# MAGIC - DDL notebook must be run first: `ddls/gold_fact_dim_tables_ddl`
# MAGIC - Silver data must exist: `silver.transactions`

# COMMAND ----------

# Configuration
CATALOG = "rcm_adb"

# Set catalog context
spark.sql(f"USE CATALOG {CATALOG}")
print(f"Using catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE gold.fact_transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.fact_transactions
# MAGIC SELECT 
# MAGIC     t.TransactionID, 
# MAGIC     t.SRC_TransactionID,
# MAGIC     t.EncounterID,
# MAGIC     concat(t.PatientID, '-', t.datasource) AS FK_PatientID,
# MAGIC     CASE 
# MAGIC         WHEN t.datasource = 'hos-a' THEN concat('H1-', t.providerID) 
# MAGIC         ELSE concat('H2-', t.providerID) 
# MAGIC     END AS FK_ProviderID, 
# MAGIC     concat(t.DeptID, '-', t.datasource) AS FK_DeptID, 
# MAGIC     t.ICDCode,
# MAGIC     t.ProcedureCode AS ProcedureCode,
# MAGIC     t.VisitType,
# MAGIC     t.ServiceDate, 
# MAGIC     t.PaidDate,
# MAGIC     t.Amount AS Amount, 
# MAGIC     t.PaidAmount AS PaidAmount, 
# MAGIC     t.AmountType,
# MAGIC     t.ClaimID,
# MAGIC     t.datasource,
# MAGIC     current_timestamp() AS refreshed_at
# MAGIC FROM silver.transactions t 
# MAGIC WHERE t.is_current = true AND t.is_quarantined = false

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.fact_transactions