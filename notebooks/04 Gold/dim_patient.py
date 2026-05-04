# Databricks notebook source
# MAGIC %md
# MAGIC # Gold dim_patient ETL
# MAGIC
# MAGIC **Purpose**: Load patient dimension from Silver to Gold layer.
# MAGIC
# MAGIC **Cluster**: rcm cluster
# MAGIC
# MAGIC **Dependencies**: 
# MAGIC - DDL notebook must be run first: `ddls/gold_fact_dim_tables_ddl`
# MAGIC - Silver data must exist: `silver.patients`

# COMMAND ----------

# Configuration
CATALOG = "rcm_adb"

# Set catalog context
spark.sql(f"USE CATALOG {CATALOG}")
print(f"Using catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE gold.dim_patient

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.dim_patient
# MAGIC SELECT 
# MAGIC     patient_key,
# MAGIC     src_patientid,
# MAGIC     firstname,
# MAGIC     lastname,
# MAGIC     middlename,
# MAGIC     ssn,
# MAGIC     phonenumber,
# MAGIC     gender,
# MAGIC     dob,
# MAGIC     address,
# MAGIC     datasource
# MAGIC FROM silver.patients
# MAGIC WHERE is_current = true AND is_quarantined = false

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.dim_patient