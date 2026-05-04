# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer DDLs - Fact and Dimension Tables
# MAGIC
# MAGIC **Purpose**: Create all Gold layer fact and dimension tables in Unity Catalog.
# MAGIC
# MAGIC **Cluster**: rcm cluster
# MAGIC
# MAGIC **Tables Created**:
# MAGIC - dim_cpt_code
# MAGIC - dim_department
# MAGIC - dim_icd
# MAGIC - dim_npi
# MAGIC - dim_patient
# MAGIC - dim_provider
# MAGIC - fact_transactions

# COMMAND ----------

# Configuration
CATALOG = "rcm_adb"

# Set catalog context
spark.sql(f"USE CATALOG {CATALOG}")
print(f"Using catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create gold schema if not exists
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- dim_cpt_code
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_cpt_code
# MAGIC (
# MAGIC     cpt_codes STRING,
# MAGIC     procedure_code_category STRING,
# MAGIC     procedure_code_descriptions STRING,
# MAGIC     code_status STRING,
# MAGIC     refreshed_at TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- dim_department
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_department
# MAGIC (
# MAGIC     Dept_Id STRING,
# MAGIC     SRC_Dept_Id STRING,
# MAGIC     Name STRING,
# MAGIC     datasource STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- dim_icd
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_icd
# MAGIC (
# MAGIC     icd_code STRING,
# MAGIC     icd_code_type STRING,
# MAGIC     code_description STRING,
# MAGIC     refreshed_at TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- dim_npi
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_npi
# MAGIC (
# MAGIC     npi_id STRING,
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     position STRING,
# MAGIC     organisation_name STRING,
# MAGIC     last_updated STRING,
# MAGIC     refreshed_at TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- dim_patient
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_patient
# MAGIC (
# MAGIC     patient_key STRING,
# MAGIC     src_patientid STRING,
# MAGIC     firstname STRING,
# MAGIC     lastname STRING,
# MAGIC     middlename STRING,
# MAGIC     ssn STRING,
# MAGIC     phonenumber STRING,
# MAGIC     gender STRING,
# MAGIC     dob DATE,
# MAGIC     address STRING,
# MAGIC     datasource STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- dim_provider
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_provider
# MAGIC (
# MAGIC     ProviderID STRING,
# MAGIC     FirstName STRING,
# MAGIC     LastName STRING,
# MAGIC     DeptID STRING,
# MAGIC     NPI LONG,
# MAGIC     datasource STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- fact_transactions
# MAGIC CREATE TABLE IF NOT EXISTS gold.fact_transactions
# MAGIC (
# MAGIC     TransactionID STRING,
# MAGIC     SRC_TransactionID STRING,
# MAGIC     EncounterID STRING,
# MAGIC     FK_PatientID STRING,
# MAGIC     FK_ProviderID STRING,
# MAGIC     FK_DeptID STRING,
# MAGIC     ICDCode STRING,
# MAGIC     ProcedureCode STRING,
# MAGIC     VisitType STRING,
# MAGIC     ServiceDate DATE,
# MAGIC     PaidDate DATE,
# MAGIC     Amount DOUBLE,
# MAGIC     PaidAmount DOUBLE,
# MAGIC     AmountType STRING,
# MAGIC     ClaimID STRING,
# MAGIC     datasource STRING,
# MAGIC     refreshed_at TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify all tables created
# MAGIC SHOW TABLES IN gold