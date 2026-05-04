# Databricks notebook source
# MAGIC %md
# MAGIC # Gold dim_cpt_code ETL
# MAGIC
# MAGIC **Purpose**: Load CPT code dimension from Silver to Gold layer.
# MAGIC
# MAGIC **Cluster**: rcm cluster
# MAGIC
# MAGIC **Dependencies**: 
# MAGIC - DDL notebook must be run first: `ddls/gold_fact_dim_tables_ddl`
# MAGIC - Silver data must exist: `silver.cptcodes`

# COMMAND ----------

# Configuration
CATALOG = "rcm_adb"

# Set catalog context
spark.sql(f"USE CATALOG {CATALOG}")
print(f"Using catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE gold.dim_cpt_code

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.dim_cpt_code
# MAGIC SELECT 
# MAGIC     cpt_codes,
# MAGIC     procedure_code_category,
# MAGIC     procedure_code_descriptions,
# MAGIC     code_status,
# MAGIC     current_timestamp() AS refreshed_at
# MAGIC FROM silver.cptcodes
# MAGIC WHERE is_quarantined = false AND is_current = true

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.dim_cpt_code