# Notebooks

Databricks notebooks exported in `.py` source format (Databricks notebook source). Each file can be imported directly into a Databricks workspace via the UI (`Import > File`) or the Databricks CLI, and then run against the `rcm` cluster.

All Bronze and Silver/Gold notebooks are triggered by ADF pipelines at runtime via the `ls_adb_notebooks` linked service. They can also be run manually by executing cells in order.

---

## Notebook Inventory

### `01 DDLs/` — Schema Setup (run once before any ETL)

These notebooks create the Unity Catalog schemas and Delta tables that the ETL notebooks write into. Run them in order on first-time setup.

| File | Purpose |
|---|---|
| `load_logs_ddl.py` | Creates `rcm_adb.audit.load_logs` — the pipeline audit and watermark table used by incremental EMR loads |
| `silver_tables_ddl.py` | Creates all 9 Silver Delta tables under `rcm_adb.silver` with SCD2 audit columns (`is_current`, `audit_insertdate`, `audit_modifieddate`) |
| `gold_fact_dim_tables_ddl.py` | Creates all 7 Gold Delta tables under `rcm_adb.gold` — 6 dimension tables and 1 fact table |

---

### `02 Bronze/` — API and Landing to Bronze

These notebooks are triggered in parallel by `pl_others_scr_to_bronze` (ADF `DatabricksNotebook` activities via `ls_adb_notebooks`). Each writes Parquet to ADLS Bronze.

| File | Purpose |
|---|---|
| `Landing to Bronze - Claims Data & CPT Codes.py` | Reads all CSV files from `landing/claims/` and `landing/cptcodes/` in ADLS. For Claims, derives a `datasource` tag from the filename (`hospital1` → `hosa`, `hospital2` → `hosb`). For CPT Codes, normalises column names (lowercase, underscores). Adds `inserted_date` and `updated_date` audit columns. Writes Parquet to `bronze/claims/` and `bronze/cptcodes/` (overwrite). |
| `API extract NPI codes.py` | Calls the CMS NPI Registry public API for Los Angeles, CA (up to 20 results). For each NPI number, makes a second call to retrieve full provider details. Handles both individual providers (NPI-1) and organisations (NPI-2) by reading from the appropriate API response fields. Writes Parquet to `bronze/npi_extract/` (overwrite). |
| `API extract ICD codes.py` | Authenticates with the WHO ICD-10 API using OAuth2 (client credentials flow). Recursively fetches all codes under category `A00-A09` (intestinal infectious diseases). Writes Parquet to `bronze/icd_codes/` (append mode — new codes are added on each run rather than replacing existing ones). |

---

### `03 Silver/` — Bronze to Silver (CDM + SCD Type 2)

These notebooks are triggered by `pl_slv_to_gold` (all 9 run in parallel). Each follows the same common pattern:

1. Read the current Bronze Parquet from ADLS (both `hosa` and `hosb` for EMR tables)
2. Apply the Common Data Model (standardise column names, generate surrogate keys as `sourceID + '-' + datasource`)
3. Union both hospitals using `unionByName()` (EMR tables only)
4. Create a temp view from the CDM data
5. Apply quality checks in a second temp view (`is_quarantined` flag for null/invalid key columns)
6. Write to Silver using Full Load (truncate/insert) or SCD Type 2 MERGE

| File | Load Type | Notes |
|---|---|---|
| `Patients.py` | SCD Type 2 | Surrogate key: `PatientID + datasource`. Hospital B column renames: `ID→PatientID`, `F_Name→FirstName`, `L_Name→LastName`, `M_Name→MiddleName`, `Updated_Date→ModifiedDate` |
| `Transactions.py` | SCD Type 2 | CDM applied across both hospitals |
| `Encounters.py` | SCD Type 2 | CDM applied across both hospitals |
| `Claims.py` | SCD Type 2 | Source-tagged from Landing; no CDM merge required |
| `CPT_Codes.py` | SCD Type 2 | Universal procedure codes; no hospital merge |
| `ICD_Codes.py` | SCD Type 2 | Disease classification codes from WHO API |
| `NPI_Codes.py` | SCD Type 2 | Provider registry codes from CMS API |
| `Providers.py` | Full Load | Truncate Silver table and reload all rows on every run |
| `Departments.py` | Full Load | Truncate Silver table and reload all rows on every run |

**SCD Type 2 audit columns (on all SCD2 tables):**

| Column | Meaning |
|---|---|
| `is_current` | `true` = latest active version. `false` = historical version kept for audit. |
| `audit_insertdate` | Timestamp when this version was first written to Silver |
| `audit_modifieddate` | Timestamp when this version was last expired (set to `is_current = false`) |

---

### `04 Gold/` — Silver to Gold (Star Schema)

These notebooks are triggered by `pl_slv_to_gold` after their corresponding Silver notebooks succeed. Each truncates the Gold table and reloads from the latest Silver data on every run. Gold tables always reflect the current clean snapshot.

**Dimension notebooks** read only current, non-quarantined records from their Silver table and write directly to the corresponding Gold dimension table. They have no dependencies on each other.

**Fact notebook** (`fact_transaction.py`) reads from `silver.transactions` and constructs FK columns whose values match the PKs already in the dimension tables, enabling BI tools to join at query time without additional lookups.

| File | Target Table | Notes |
|---|---|---|
| `dim_patient.py` | `gold.dim_patient` | Reads `silver.patients` where `is_current = true` |
| `dim_provider.py` | `gold.dim_provider` | Reads `silver.providers` |
| `dim_department.py` | `gold.dim_department` | Reads `silver.departments` |
| `dim_cpt_code.py` | `gold.dim_cpt_code` | Reads `silver.cptcodes` where `is_current = true` |
| `dim_icd_code.py` | `gold.dim_icd_code` | Reads `silver.icd_codes` where `is_current = true` |
| `dim_npi.py` | `gold.dim_npi` | Reads `silver.npi_extract` where `is_current = true` |
| `fact_transaction.py` | `gold.fact_transactions` | FK columns: `FK_PatientID = PatientID + datasource`, `FK_DeptID = DeptID + datasource`, `FK_ProviderID = H1- or H2- prefix + ProviderID`. Depends on `slv_Transactions` succeeding. |

---

## Storage Account Configuration

All notebooks that access ADLS reference the storage account name as a variable:

```python
STORAGE_ACCOUNT = "rcmproject"
BRONZE_CONTAINER = "bronze"
```

Update `STORAGE_ACCOUNT` to match your ADLS Gen2 account name before running.

---

## Unity Catalog Configuration

All Silver and Gold notebooks write to the `rcm_adb` Unity Catalog:

```python
# Silver example
spark.sql("MERGE INTO rcm_adb.silver.patients AS target ...")

# Gold example
spark.sql("INSERT OVERWRITE rcm_adb.gold.dim_patient ...")
```

Ensure the `rcm_adb` catalog and `silver`, `gold`, `audit` schemas exist before running ETL notebooks (created by the DDL notebooks in `01 DDLs/`).
