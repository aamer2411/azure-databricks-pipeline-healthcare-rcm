# Databricks notebook source
# MAGIC %md
# MAGIC # rcm_adb.silver Layer - All DDLs
# MAGIC
# MAGIC **Purpose**: Create all rcm_adb.silver layer tables for the RCM data pipeline.
# MAGIC
# MAGIC **Cluster**: rcm cluster
# MAGIC
# MAGIC **Run Frequency**: One-time setup (run before ETL notebooks)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create rcm_adb.silver schema if not exists
# MAGIC CREATE SCHEMA IF NOT EXISTS rcm_adb.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Claims Table (SCD2)
# MAGIC CREATE TABLE IF NOT EXISTS rcm_adb.silver.claims (
# MAGIC     ClaimID STRING,
# MAGIC     SRC_ClaimID STRING,
# MAGIC     TransactionID STRING,
# MAGIC     PatientID STRING,
# MAGIC     EncounterID STRING,
# MAGIC     ProviderID STRING,
# MAGIC     DeptID STRING,
# MAGIC     ServiceDate DATE,
# MAGIC     ClaimDate DATE,
# MAGIC     PayorID STRING,
# MAGIC     ClaimAmount STRING,
# MAGIC     PaidAmount STRING,
# MAGIC     ClaimStatus STRING,
# MAGIC     PayorType STRING,
# MAGIC     Deductible STRING,
# MAGIC     Coinsurance STRING,
# MAGIC     Copay STRING,
# MAGIC     SRC_InsertDate DATE,
# MAGIC     SRC_ModifiedDate DATE,
# MAGIC     datasource STRING,
# MAGIC     is_quarantined BOOLEAN,
# MAGIC     audit_insertdate TIMESTAMP,
# MAGIC     audit_modifieddate TIMESTAMP,
# MAGIC     is_current BOOLEAN
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CPT Codes Table (SCD2)
# MAGIC CREATE TABLE IF NOT EXISTS rcm_adb.silver.cptcodes (
# MAGIC     cpt_codes STRING,
# MAGIC     procedure_code_category STRING,
# MAGIC     procedure_code_descriptions STRING,
# MAGIC     code_status STRING,
# MAGIC     is_quarantined BOOLEAN,
# MAGIC     audit_insertdate TIMESTAMP,
# MAGIC     audit_modifieddate TIMESTAMP,
# MAGIC     is_current BOOLEAN
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Encounters Table (SCD2)
# MAGIC CREATE TABLE IF NOT EXISTS rcm_adb.silver.encounters (
# MAGIC     EncounterID STRING,
# MAGIC     SRC_EncounterID STRING,
# MAGIC     PatientID STRING,
# MAGIC     EncounterDate DATE,
# MAGIC     EncounterType STRING,
# MAGIC     ProviderID STRING,
# MAGIC     DepartmentID STRING,
# MAGIC     ProcedureCode INTEGER,
# MAGIC     SRC_InsertedDate DATE,
# MAGIC     SRC_ModifiedDate DATE,
# MAGIC     datasource STRING,
# MAGIC     is_quarantined BOOLEAN,
# MAGIC     audit_insertdate TIMESTAMP,
# MAGIC     audit_modifieddate TIMESTAMP,
# MAGIC     is_current BOOLEAN
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Transactions Table (SCD2)
# MAGIC CREATE TABLE IF NOT EXISTS rcm_adb.silver.transactions (
# MAGIC     TransactionID STRING,
# MAGIC     SRC_TransactionID STRING,
# MAGIC     EncounterID STRING,
# MAGIC     PatientID STRING,
# MAGIC     ProviderID STRING,
# MAGIC     DeptID STRING,
# MAGIC     VisitDate DATE,
# MAGIC     ServiceDate DATE,
# MAGIC     PaidDate DATE,
# MAGIC     VisitType STRING,
# MAGIC     Amount DOUBLE,
# MAGIC     AmountType STRING,
# MAGIC     PaidAmount DOUBLE,
# MAGIC     ClaimID STRING,
# MAGIC     PayorID STRING,
# MAGIC     ProcedureCode INTEGER,
# MAGIC     ICDCode STRING,
# MAGIC     LineOfBusiness STRING,
# MAGIC     MedicaidID STRING,
# MAGIC     MedicareID STRING,
# MAGIC     SRC_InsertDate DATE,
# MAGIC     SRC_ModifiedDate DATE,
# MAGIC     datasource STRING,
# MAGIC     is_quarantined BOOLEAN,
# MAGIC     audit_insertdate TIMESTAMP,
# MAGIC     audit_modifieddate TIMESTAMP,
# MAGIC     is_current BOOLEAN
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Departments Table (Reference - Truncate/Insert)
# MAGIC CREATE TABLE IF NOT EXISTS rcm_adb.silver.departments (
# MAGIC     Dept_Id STRING,
# MAGIC     SRC_Dept_Id STRING,
# MAGIC     Name STRING,
# MAGIC     datasource STRING,
# MAGIC     is_quarantined BOOLEAN
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ICD Codes Table (SCD2)
# MAGIC CREATE TABLE IF NOT EXISTS rcm_adb.silver.icd_codes (
# MAGIC     icd_code STRING,
# MAGIC     icd_code_type STRING,
# MAGIC     code_description STRING,
# MAGIC     is_quarantined BOOLEAN,
# MAGIC     audit_insertdate TIMESTAMP,
# MAGIC     audit_modifieddate TIMESTAMP,
# MAGIC     is_current BOOLEAN
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- NPI Extract Table (SCD2)
# MAGIC CREATE TABLE IF NOT EXISTS rcm_adb.silver.npi_extract (
# MAGIC     npi_id STRING,
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     position STRING,
# MAGIC     organisation_name STRING,
# MAGIC     last_updated STRING,
# MAGIC     is_quarantined BOOLEAN,
# MAGIC     audit_insertdate TIMESTAMP,
# MAGIC     audit_modifieddate TIMESTAMP,
# MAGIC     is_current BOOLEAN
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Providers Table (Reference - Truncate/Insert)
# MAGIC CREATE TABLE IF NOT EXISTS rcm_adb.silver.providers (
# MAGIC     ProviderID STRING,
# MAGIC     FirstName STRING,
# MAGIC     LastName STRING,
# MAGIC     Specialization STRING,
# MAGIC     DeptID STRING,
# MAGIC     NPI LONG,
# MAGIC     datasource STRING,
# MAGIC     is_quarantined BOOLEAN
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Patients Table (SCD2)
# MAGIC CREATE TABLE IF NOT EXISTS rcm_adb.silver.patients (
# MAGIC     Patient_Key STRING,
# MAGIC     SRC_PatientID STRING,
# MAGIC     FirstName STRING,
# MAGIC     LastName STRING,
# MAGIC     MiddleName STRING,
# MAGIC     SSN STRING,
# MAGIC     PhoneNumber STRING,
# MAGIC     Gender STRING,
# MAGIC     DOB DATE,
# MAGIC     Address STRING,
# MAGIC     SRC_ModifiedDate TIMESTAMP,
# MAGIC     datasource STRING,
# MAGIC     is_quarantined BOOLEAN,
# MAGIC     audit_insertdate TIMESTAMP,
# MAGIC     audit_modifieddate TIMESTAMP,
# MAGIC     is_current BOOLEAN
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify all tables created
# MAGIC SHOW TABLES IN rcm_adb.silver;