# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Providers ETL
# MAGIC
# MAGIC **Purpose**: Load and cleanse providers reference data from Bronze to silver layer.
# MAGIC
# MAGIC **Cluster**: rcm cluster
# MAGIC
# MAGIC **Dependencies**: 
# MAGIC - Bronze data must exist in ADLS
# MAGIC
# MAGIC **Note**: This is a reference table using truncate/insert pattern (not SCD2).

# COMMAND ----------

# Configuration - ADLS Paths
STORAGE_ACCOUNT = "rcmproject"
BRONZE_CONTAINER = "bronze"

# Bronze source paths
BRONZE_HOSA_PROVIDERS = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/hosa/providers"
BRONZE_HOSB_PROVIDERS = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/hosb/providers"

# COMMAND ----------

# Read Bronze Data
from pyspark.sql import functions as f

# Read Hospital A providers data
df_hosa = spark.read.parquet(BRONZE_HOSA_PROVIDERS)

# Read Hospital B providers data
df_hosb = spark.read.parquet(BRONZE_HOSB_PROVIDERS)

# Union the two dataframes
df_merged = df_hosa.unionByName(df_hosb)

# Create temp view for SQL operations
df_merged.createOrReplaceTempView("providers")

print(f"Total providers records: {df_merged.count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Truncate existing data (reference table pattern)
# MAGIC TRUNCATE TABLE rcm_adb.silver.providers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert with quality checks
# MAGIC INSERT INTO rcm_adb.silver.providers
# MAGIC SELECT DISTINCT
# MAGIC     ProviderID,
# MAGIC     FirstName,
# MAGIC     LastName,
# MAGIC     Specialization,
# MAGIC     DeptID,
# MAGIC     CAST(NPI AS LONG) AS NPI,
# MAGIC     datasource,
# MAGIC     CASE 
# MAGIC         WHEN ProviderID IS NULL OR DeptID IS NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC FROM providers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verification: Check silver table
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_records,
# MAGIC     SUM(CASE WHEN is_quarantined = true THEN 1 ELSE 0 END) AS quarantined_records
# MAGIC FROM rcm_adb.silver.providers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample records
# MAGIC SELECT * FROM rcm_adb.silver.providers LIMIT 10