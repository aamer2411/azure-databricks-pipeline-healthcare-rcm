# Databricks notebook source
# MAGIC %md
# MAGIC # Gold dim_department ETL
# MAGIC
# MAGIC **Purpose**: Load department dimension from Silver to Gold layer.
# MAGIC
# MAGIC **Cluster**: rcm cluster
# MAGIC
# MAGIC **Dependencies**: 
# MAGIC - DDL notebook must be run first: `ddls/gold_fact_dim_tables_ddl`
# MAGIC - Silver data must exist: `silver.departments`

# COMMAND ----------

# Configuration
CATALOG = "rcm_adb"

# Set catalog context
spark.sql(f"USE CATALOG {CATALOG}")
print(f"Using catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE gold.dim_department

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.dim_department
# MAGIC SELECT DISTINCT
# MAGIC     Dept_Id,
# MAGIC     SRC_Dept_Id,
# MAGIC     Name,
# MAGIC     datasource
# MAGIC FROM silver.departments
# MAGIC WHERE is_quarantined = false

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.dim_department