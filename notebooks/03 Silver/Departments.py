# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Departments ETL
# MAGIC
# MAGIC **Purpose**: Load and cleanse departments reference data from Bronze to Silver layer.
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
BRONZE_HOSA_DEPARTMENTS = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/hosa/departments"
BRONZE_HOSB_DEPARTMENTS = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/hosb/departments"

# COMMAND ----------

# Read Bronze Data
from pyspark.sql import functions as f

# Read Hospital A departments data
df_hosa = spark.read.parquet(BRONZE_HOSA_DEPARTMENTS)

# Read Hospital B departments data
df_hosb = spark.read.parquet(BRONZE_HOSB_DEPARTMENTS)

# Union the two dataframes
df_merged = df_hosa.unionByName(df_hosb)

# Create the dept_id column and rename deptid to src_dept_id
df_merged = df_merged.withColumn("SRC_Dept_id", f.col("deptid")) \
                     .withColumn("Dept_id", f.concat(f.col("deptid"), f.lit('-'), f.col("datasource"))) \
                     .drop("deptid")

# Create temp view for SQL operations
df_merged.createOrReplaceTempView("departments")

print(f"Total departments records: {df_merged.count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Truncate existing data
# MAGIC TRUNCATE TABLE rcm_adb.silver.departments

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert with quality checks
# MAGIC INSERT INTO rcm_adb.silver.departments
# MAGIC SELECT 
# MAGIC     Dept_Id,
# MAGIC     SRC_Dept_Id,
# MAGIC     Name,
# MAGIC     datasource,
# MAGIC     CASE 
# MAGIC         WHEN SRC_Dept_Id IS NULL OR Name IS NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC FROM departments

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verification: Check silver table
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_records,
# MAGIC     SUM(CASE WHEN is_quarantined = true THEN 1 ELSE 0 END) AS quarantined_records
# MAGIC FROM rcm_adb.silver.departments

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample records
# MAGIC SELECT * FROM rcm_adb.silver.departments LIMIT 10