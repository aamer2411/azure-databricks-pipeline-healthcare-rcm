# Databricks notebook source
# MAGIC %md
# MAGIC # Gold dim_npi ETL
# MAGIC
# MAGIC **Purpose**: Load NPI dimension from Silver to Gold layer.
# MAGIC
# MAGIC **Cluster**: rcm cluster
# MAGIC
# MAGIC **Dependencies**: 
# MAGIC - DDL notebook must be run first: `ddls/gold_fact_dim_tables_ddl`
# MAGIC - Silver data must exist: `silver.npi_extract`

# COMMAND ----------

# Configuration
CATALOG = "rcm_adb"

# Set catalog context
spark.sql(f"USE CATALOG {CATALOG}")
print(f"Using catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE gold.dim_npi

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.dim_npi
# MAGIC SELECT
# MAGIC     npi_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     position,
# MAGIC     organisation_name,
# MAGIC     last_updated,
# MAGIC     current_timestamp() AS refreshed_at
# MAGIC FROM silver.npi_extract
# MAGIC WHERE is_current = true

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.dim_npi