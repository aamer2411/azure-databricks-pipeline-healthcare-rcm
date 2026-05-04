# Databricks notebook source
# MAGIC %md
# MAGIC # Gold dim_provider ETL
# MAGIC
# MAGIC **Purpose**: Load provider dimension from Silver to Gold layer.
# MAGIC
# MAGIC **Cluster**: rcm cluster
# MAGIC
# MAGIC **Dependencies**: 
# MAGIC - DDL notebook must be run first: `ddls/gold_fact_dim_tables_ddl`
# MAGIC - Silver data must exist: `silver.providers`

# COMMAND ----------

# Configuration
CATALOG = "rcm_adb"

# Set catalog context
spark.sql(f"USE CATALOG {CATALOG}")
print(f"Using catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE gold.dim_provider

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.dim_provider
# MAGIC SELECT 
# MAGIC     ProviderID,
# MAGIC     FirstName,
# MAGIC     LastName,
# MAGIC     concat(DeptID, '-', datasource) AS DeptID,
# MAGIC     NPI,
# MAGIC     datasource
# MAGIC FROM silver.providers
# MAGIC WHERE is_quarantined = false

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.dim_provider