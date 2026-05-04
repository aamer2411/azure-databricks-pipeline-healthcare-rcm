# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Patients ETL
# MAGIC
# MAGIC **Purpose**: Load and cleanse patient data from Bronze to silver layer with SCD2 history tracking.
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
BRONZE_HOSA_PATIENTS = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/hosa/patients"
BRONZE_HOSB_PATIENTS = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/hosb/patients"

# COMMAND ----------

# Read Bronze Data
from pyspark.sql import functions as f

# Read Hospital A patients data
df_hosa = spark.read.parquet(BRONZE_HOSA_PATIENTS)
df_hosa.createOrReplaceTempView("patients_hosa")

# Read Hospital B patients data
df_hosb = spark.read.parquet(BRONZE_HOSB_PATIENTS)
df_hosb.createOrReplaceTempView("patients_hosb")

print(f"Hospital A patients: {df_hosa.count()}")
print(f"Hospital B patients: {df_hosb.count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create CDM (Common Data Model) view to standardize column names from both hospitals
# MAGIC CREATE OR REPLACE TEMP VIEW cdm_patients AS
# MAGIC SELECT CONCAT(SRC_PatientID, '-', datasource) AS Patient_Key, *
# MAGIC FROM (
# MAGIC     SELECT 
# MAGIC         PatientID AS SRC_PatientID,
# MAGIC         FirstName,
# MAGIC         LastName,
# MAGIC         MiddleName,
# MAGIC         SSN,
# MAGIC         PhoneNumber,
# MAGIC         Gender,
# MAGIC         DOB,
# MAGIC         Address,
# MAGIC         ModifiedDate,
# MAGIC         datasource
# MAGIC     FROM patients_hosa
# MAGIC     UNION ALL
# MAGIC     SELECT 
# MAGIC         ID AS SRC_PatientID,
# MAGIC         F_Name AS FirstName,
# MAGIC         L_Name AS LastName,
# MAGIC         M_Name AS MiddleName,
# MAGIC         SSN,
# MAGIC         PhoneNumber,
# MAGIC         Gender,
# MAGIC         DOB,
# MAGIC         Address,
# MAGIC         Updated_Date AS ModifiedDate,
# MAGIC         datasource
# MAGIC     FROM patients_hosb
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quality Checks: Apply data quality rules and add quarantine flag
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS
# MAGIC SELECT 
# MAGIC     Patient_Key,
# MAGIC     SRC_PatientID,
# MAGIC     FirstName,
# MAGIC     LastName,
# MAGIC     MiddleName,
# MAGIC     SSN,
# MAGIC     PhoneNumber,
# MAGIC     Gender,
# MAGIC     DOB,
# MAGIC     Address,
# MAGIC     ModifiedDate AS SRC_ModifiedDate,
# MAGIC     datasource,
# MAGIC     CASE 
# MAGIC         WHEN SRC_PatientID IS NULL OR DOB IS NULL OR FirstName IS NULL OR LOWER(FirstName) = 'null' THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC FROM cdm_patients

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview quality checks
# MAGIC SELECT * FROM quality_checks ORDER BY is_quarantined DESC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD2 Step 1: Mark existing records as historical when source data has changed
# MAGIC MERGE INTO rcm_adb.silver.patients AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.Patient_Key = source.Patient_Key AND target.is_current = true
# MAGIC WHEN MATCHED AND (
# MAGIC     target.SRC_PatientID <> source.SRC_PatientID OR
# MAGIC     target.FirstName <> source.FirstName OR
# MAGIC     target.LastName <> source.LastName OR
# MAGIC     target.MiddleName <> source.MiddleName OR
# MAGIC     target.SSN <> source.SSN OR
# MAGIC     target.PhoneNumber <> source.PhoneNumber OR
# MAGIC     target.Gender <> source.Gender OR
# MAGIC     target.DOB <> source.DOB OR
# MAGIC     target.Address <> source.Address OR
# MAGIC     target.SRC_ModifiedDate <> source.SRC_ModifiedDate OR
# MAGIC     target.datasource <> source.datasource OR
# MAGIC     target.is_quarantined <> source.is_quarantined
# MAGIC ) THEN
# MAGIC     UPDATE SET
# MAGIC         target.is_current = false,
# MAGIC         target.audit_modifieddate = current_timestamp()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD2 Step 2: Insert new records
# MAGIC MERGE INTO rcm_adb.silver.patients AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.Patient_Key = source.Patient_Key AND target.is_current = true
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         Patient_Key, SRC_PatientID, FirstName, LastName, MiddleName, SSN, PhoneNumber,
# MAGIC         Gender, DOB, Address, SRC_ModifiedDate, datasource, is_quarantined,
# MAGIC         audit_insertdate, audit_modifieddate, is_current
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         source.Patient_Key, source.SRC_PatientID, source.FirstName, source.LastName,
# MAGIC         source.MiddleName, source.SSN, source.PhoneNumber, source.Gender, source.DOB,
# MAGIC         source.Address, source.SRC_ModifiedDate, source.datasource, source.is_quarantined,
# MAGIC         current_timestamp(), current_timestamp(), true
# MAGIC     )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verification: Check silver table
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_records,
# MAGIC     SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) AS current_records,
# MAGIC     SUM(CASE WHEN is_quarantined = true THEN 1 ELSE 0 END) AS quarantined_records
# MAGIC FROM rcm_adb.silver.patients

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample records
# MAGIC SELECT * FROM rcm_adb.silver.patients WHERE is_current = true LIMIT 10