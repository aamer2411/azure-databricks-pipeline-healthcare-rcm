# Azure Healthcare Revenue Cycle Management (RCM) Data Pipeline

An end-to-end Healthcare Revenue Cycle Management (RCM) data engineering pipeline built on Azure Databricks, Azure Data Factory, and Azure Data Lake Storage Gen2.

This project ingests patient and clinical data from multiple hospital source systems, structured flat files, and external healthcare APIs into a Medallion Architecture (Landing, Bronze, Silver, Gold). It resolves multi-hospital schema conflicts using a Common Data Model, preserves full change history via SCD Type 2 Delta Lake tables, and delivers a Star Schema analytics layer for financial and operational reporting.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Technology Stack](#technology-stack)
- [Pipeline Stages](#pipeline-stages)
- [Key Patterns and Techniques](#key-patterns-and-techniques)
- [Repository Structure](#repository-structure)
- [Data Model](#data-model)
- [Setup and Configuration](#setup-and-configuration)
- [Running the Pipeline](#running-the-pipeline)
- [What This Demonstrates](#what-this-demonstrates)

---

## Architecture Overview

```
Azure SQL DB (hosa / hosb)   ─────────────────┐
                                              │
CMS NPI API / WHO ICD-10 API ─────────────────├──► Bronze ──► Silver ──► Gold ──► BI / Analytics
                                              │
Claims & CPT Data (CSV)      ──► Landing ─────┘
```

The pipeline follows Medallion Architecture across four layers:

| Layer | Storage | Format | Purpose |
|---|---|---|---|
| **Landing** | ADLS Gen2 (`landing` container) | CSV | Client-dropped flat files (Claims, CPT codes) |
| **Bronze** | ADLS Gen2 (`bronze` container) | Parquet | Raw structured data, source of truth, archived on each run |
| **Silver** | Unity Catalog (`rcm_adb.silver`) | Delta | CDM-standardised, surrogate keys, SCD Type 2 history |
| **Gold** | Unity Catalog (`rcm_adb.gold`) | Delta | Star Schema: fact and dimension tables for analytics |

### Medallion Architecture

![Medallion Architecture](screenshots/medallion_architecture.png)

### Master ADF Pipeline

![ADF Master Pipeline](screenshots/pl_main_e2e.png)

See [screenshots/README.md](screenshots/README.md) for all pipeline canvases, linked service and dataset screenshots, and workspace views.

---

## Technology Stack

| Component | Service |
|---|---|
| Compute | Azure Databricks (Spark, Delta Lake) |
| Storage | Azure Data Lake Storage Gen2 |
| Orchestration | Azure Data Factory |
| Table governance | Databricks Unity Catalog |
| Source systems | Azure SQL Database (Hospital A EMR, Hospital B EMR) |
| External APIs | CMS NPI Registry API, WHO ICD-10 API (OAuth2) |
| Language | PySpark, Spark SQL, Python |

---

## Pipeline Stages

### Bronze: Ingestion

Two child pipelines run in parallel under `pl_main_e2e`:

**EMR to Bronze (`pl_emr_src_to_bronze`):** A metadata-driven pipeline that reads `load_config.csv` from ADLS, iterates through each configured hospital EMR table (batchCount: 5), archives any existing Parquet file, checks the `is_active` flag, then delegates to the grandchild pipeline `pl_copy_from_emr` for the actual copy. `pl_copy_from_emr` branches on load type:
- **Full Load:** copies the entire table from Azure SQL DB to Bronze as Parquet; logs row count and timestamp to the audit table.
- **Incremental Load:** reads the last load timestamp from `rcm_adb.audit.load_logs`, queries only rows where the watermark column is greater than or equal to that date, copies them to Bronze, then updates the audit log.

**Others to Bronze (`pl_others_scr_to_bronze`):** Triggers three Databricks notebooks in parallel via ADF `DatabricksNotebook` activities:
- Claims and CPT codes from ADLS Landing to Bronze (hospital-tagged by filename)
- NPI provider codes from the CMS public API to Bronze
- ICD-10 disease codes from the WHO API to Bronze (OAuth2, append mode)

### Silver: Transformation

Nine Databricks notebooks (triggered by `pl_slv_to_gold`) read Bronze Parquet and write to Unity Catalog Silver Delta tables:

- **Common Data Model (CDM):** Both hospital dataframes are column-renamed to a standard schema, then combined with `unionByName()`. Surrogate keys are generated as `sourceID + '-' + datasource` (e.g., `123-hosa`, `123-hosb`) to prevent cross-hospital ID conflicts.
- **SCD Type 2 (7 tables):** A two-step Delta MERGE preserves full change history. Step 1 expires changed records (`is_current = false`). Step 2 inserts new active versions (`is_current = true`).
- **Full Load (2 tables):** Departments and Providers are truncated and reloaded on every run.
- **Data Quality:** Every row receives an `is_quarantined` flag. Rows with null or invalid key columns are flagged but retained for investigation.

### Gold: Analytics

Seven Databricks notebooks (also in `pl_slv_to_gold`) build a Star Schema from Silver Delta tables. Each run truncates and reloads Gold tables from the latest Silver data. The fact table (`fact_transactions`) constructs FK values to match existing dimension PKs, enabling BI join-time resolution without a separate surrogate key lookup.

---

## Key Patterns and Techniques

**Metadata-driven ETL:** `load_config.csv` controls table selection, load type (Full or Incremental), watermark column, and active/inactive status per table. Adding a new table or pausing one requires only a config file update.

**Audit watermark table:** `rcm_adb.audit.load_logs` records row counts and timestamps after every run. The next incremental run reads this table to determine the exact cutoff date, enabling precise incremental loads without pipeline code changes.

**Common Data Model with surrogate keys:** Conflicting schemas from two hospital EMR systems are unified in PySpark using column aliases and composite surrogate key generation, enabling single-table cross-facility analytics.

**SCD Type 2 on Delta Lake:** Two-step MERGE on Unity Catalog Delta tables preserves full change history for clinical and financial entities. Required for audit and compliance in healthcare.

**Parallel Bronze ingestion:** EMR and non-EMR Bronze pipelines run concurrently. Silver and Gold only execute after both Bronze pipelines succeed, enforced via ADF dependency conditions.

**Automated archival:** Before each EMR Bronze write, the existing Parquet file is copied to a dated archive path (`bronze/<entity>/archive/yyyy/MM/dd/`), preserving a point-in-time backup of the previous load.

---

## Repository Structure

```
.
├── README.md
├── PROJECT_DETAILS.md                         Full technical write-up
│
├── notebooks/                                 Databricks notebooks (.py source export)
│   ├── README.md                              Notebook inventory and setup guide
│   ├── 01 DDLs/                               Schema setup notebooks (run once before ETL)
│   │   ├── load_logs_ddl.py
│   │   ├── silver_tables_ddl.py
│   │   └── gold_fact_dim_tables_ddl.py
│   ├── 02 Bronze/                             API + Landing to Bronze
│   │   ├── Landing to Bronze - Claims Data & CPT Codes.py
│   │   ├── API extract NPI codes.py
│   │   └── API extract ICD codes.py
│   ├── 03 Silver/                             Bronze to Silver (CDM + SCD Type 2)
│   │   ├── Patients.py
│   │   ├── Transactions.py
│   │   ├── Encounters.py
│   │   ├── Claims.py
│   │   ├── CPT_Codes.py
│   │   ├── ICD_Codes.py
│   │   ├── NPI_Codes.py
│   │   ├── Providers.py
│   │   └── Departments.py
│   └── 04 Gold/                               Silver to Gold (Star Schema)
│       ├── dim_patient.py
│       ├── dim_provider.py
│       ├── dim_department.py
│       ├── dim_cpt_code.py
│       ├── dim_icd_code.py
│       ├── dim_npi.py
│       └── fact_transaction.py
│
├── adf_pipelines/                             ADF pipeline definitions (JSON export)
│   ├── README.md                              Pipeline inventory and activity sequence guide
│   ├── pl_main_e2e.json                       Master pipeline: end-to-end orchestration
│   ├── pl_emr_src_to_bronze.json              Child pipeline: metadata-driven EMR ingestion
│   ├── pl_copy_from_emr.json                  Grandchild pipeline: Full / Incremental copy logic
│   ├── pl_others_scr_to_bronze.json           Child pipeline: Databricks notebook triggers
│   └── pl_slv_to_gold.json                    Child pipeline: Silver and Gold notebook triggers
│
└── screenshots/                               ADF pipeline canvases, Unity Catalog, workspace views
    └── README.md                              Annotated index of all screenshots
```

---

## Data Model

### Source Tables

| Source | Tables | Type |
|---|---|---|
| Azure SQL DB `hosa` | patients, providers, departments, transactions, encounters | Hospital A EMR |
| Azure SQL DB `hosb` | patients, providers, departments, transactions, encounters | Hospital B EMR |
| ADLS Landing | claims, cptcodes | CSV flat files |
| CMS NPI API | npi_extract | Provider registry |
| WHO ICD-10 API | icd_codes | Disease classification |

### Silver Tables (Unity Catalog: `rcm_adb.silver`)

| Table | Load Type | Notes |
|---|---|---|
| `patients` | SCD Type 2 | CDM applied, surrogate key `PatientID + datasource` |
| `transactions` | SCD Type 2 | CDM applied |
| `encounters` | SCD Type 2 | CDM applied |
| `claims` | SCD Type 2 | Source-tagged from landing |
| `cptcodes` | SCD Type 2 | Procedure code reference |
| `icd_codes` | SCD Type 2 | Disease classification, append mode in Bronze |
| `npi_extract` | SCD Type 2 | Provider registry |
| `providers` | Full Load | Truncate and reload |
| `departments` | Full Load | Truncate and reload |

### Gold Tables (Unity Catalog: `rcm_adb.gold`)

| Table | Type | Description |
|---|---|---|
| `dim_patient` | Dimension | Patient demographics |
| `dim_provider` | Dimension | Healthcare provider details |
| `dim_department` | Dimension | Hospital department reference |
| `dim_cpt_code` | Dimension | Procedure code reference |
| `dim_icd_code` | Dimension | Diagnosis code reference |
| `dim_npi` | Dimension | National Provider Identifier reference |
| `fact_transactions` | Fact | Financial transaction records with FK references to all dimensions |

---

## Setup and Configuration

### Prerequisites

- Azure Databricks workspace with Unity Catalog enabled
- Azure Data Lake Storage Gen2 account (`rcmproject`)
- Azure SQL Database (`rcmsqlserver`) with `hosa` and `hosb` databases
- Azure Data Factory instance

### Step 1: Run DDL Notebooks

Run the three notebooks in `notebooks/01 DDLs/` in order against your Databricks cluster:

```
1. load_logs_ddl.py              Creates rcm_adb.audit.load_logs
2. silver_tables_ddl.py          Creates all 9 Silver Delta tables
3. gold_fact_dim_tables_ddl.py   Creates all 7 Gold Delta tables
```

### Step 2: Upload the Config File

Upload `load_config.csv` to ADLS at `configs/emr/load_config.csv`. This controls which EMR tables to load, the load type (Full or Incremental), the watermark column, and the active flag per table.

### Step 3: Update Storage Account References

Update the storage account name in all notebooks that reference it:

```python
STORAGE_ACCOUNT = "rcmproject"
```

### Step 4: Import ADF Pipelines

Import the JSON files from `adf_pipelines/` into your Azure Data Factory instance via `Author > Pipelines > ... > Import from JSON`. Update linked service references (`ls_adls`, `ls_sql_db`, `ls_adb_audit`, `ls_adb_notebooks`) to match your environment. See `adf_pipelines/README.md` for full details.

---

## Running the Pipeline

### Full Pipeline via ADF

Trigger `pl_main_e2e` from ADF. This runs the complete sequence:

1. `pl_emr_src_to_bronze` and `pl_others_scr_to_bronze` run in parallel
2. On success of both, `pl_slv_to_gold` executes all Silver then Gold notebooks

### Run Individual Child Pipelines

Each child pipeline (`pl_emr_src_to_bronze`, `pl_others_scr_to_bronze`, `pl_slv_to_gold`) can be triggered independently for testing or debugging a specific stage.

### Run Notebooks Directly

Each Databricks notebook can be executed manually by importing it into a workspace and running all cells in order.

---

## What This Demonstrates

- End-to-end data engineering on Azure Databricks with Delta Lake and Unity Catalog
- Metadata-driven ADF pipeline design using config files and an audit watermark table, with no code changes required to add tables or switch load types
- Common Data Model implementation in PySpark to unify conflicting schemas across multiple hospital EMR systems
- Surrogate key generation to resolve entity ID conflicts across source systems for unified cross-facility analytics
- SCD Type 2 on Delta Lake using a two-step MERGE pattern with `is_current`, `audit_insertdate`, and `audit_modifieddate` columns for full audit history
- Star Schema design in the Gold layer with FK construction for BI-ready financial and clinical analytics
- API integration including OAuth2 authentication (WHO ICD-10 API) and paginated REST calls (CMS NPI Registry API)
- Healthcare domain knowledge: Revenue Cycle Management (RCM), claims processing, ICD-10 diagnosis codes, CPT procedure codes, NPI provider registry
