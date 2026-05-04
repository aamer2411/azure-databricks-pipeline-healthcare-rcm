# Databricks notebook source
# MAGIC %md
# MAGIC # Gold dim_icd ETL
# MAGIC
# MAGIC **Purpose**: Load ICD code dimension from Silver to Gold layer.
# MAGIC
# MAGIC **Cluster**: rcm cluster
# MAGIC
# MAGIC **Dependencies**: 
# MAGIC - DDL notebook must be run first: `ddls/gold_fact_dim_tables_ddl`
# MAGIC - Silver data must exist: `silver.icd_codes`

# COMMAND ----------

# Configuration
CATALOG = "rcm_adb"

# Set catalog context
spark.sql(f"USE CATALOG {CATALOG}")
print(f"Using catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE gold.dim_icd

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.dim_icd
# MAGIC SELECT DISTINCT
# MAGIC     icd_code,
# MAGIC     icd_code_type,
# MAGIC     code_description,
# MAGIC     current_timestamp() AS refreshed_at
# MAGIC FROM silver.icd_codes
# MAGIC WHERE is_current = true

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.dim_icd