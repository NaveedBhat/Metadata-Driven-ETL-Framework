<div align="center">

<img src="https://img.icons8.com/color/120/000000/snowflake.png" width="80" alt="Snowflake"/>
&nbsp;&nbsp;
<img src="https://img.icons8.com/color/120/000000/workflow.png" width="80" alt="Airflow"/>

# Metadata-Driven Airflow & Snowflake ETL Framework

### Production-Grade, Multi-Source Batch Data Pipeline

*A fully automated, metadata-driven ETL framework that dynamically ingests 10 vendor data sources (CSV, TXT, XLSX) from Google Drive, applies configurable data quality gates, and loads clean data into a Snowflake data warehouse — orchestrated by Apache Airflow on Docker. Zero code changes required to add new pipelines.*

---

![Status](https://img.shields.io/badge/Status-Active-brightgreen?style=flat-square)
![Platform](https://img.shields.io/badge/Platform-Apache%20Airflow-017CEE?style=flat-square&logo=apache-airflow&logoColor=white)
![Warehouse](https://img.shields.io/badge/Warehouse-Snowflake-29B5E8?style=flat-square&logo=snowflake&logoColor=white)
![Language](https://img.shields.io/badge/Language-Python%203.11-3776AB?style=flat-square&logo=python&logoColor=white)
![Runtime](https://img.shields.io/badge/Runtime-Docker-2496ED?style=flat-square&logo=docker&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-yellow?style=flat-square)
![Author](https://img.shields.io/badge/Author-Naveed%20Bhat-blueviolet?style=flat-square)
![Updated](https://img.shields.io/badge/Last%20Updated-August%202026-orange?style=flat-square)

---

![topic](https://img.shields.io/badge/topic-Data%20Engineering-blue?style=flat-square)
![topic](https://img.shields.io/badge/topic-Apache%20Airflow-017CEE?style=flat-square)
![topic](https://img.shields.io/badge/topic-Snowflake-29B5E8?style=flat-square)
![topic](https://img.shields.io/badge/topic-ETL%20Pipeline-green?style=flat-square)
![topic](https://img.shields.io/badge/topic-Metadata--Driven-purple?style=flat-square)
![topic](https://img.shields.io/badge/topic-Data%20Quality-red?style=flat-square)
![topic](https://img.shields.io/badge/topic-Docker-2496ED?style=flat-square)
![topic](https://img.shields.io/badge/topic-Google%20Drive%20API-4285F4?style=flat-square)

</div>

---

## Overview

This project is a **production-grade, metadata-driven batch ETL framework** built with **Apache Airflow** and **Snowflake**. It demonstrates real-world Data Engineering practices including:

- **Dynamic pipeline orchestration** using Airflow's Dynamic Task Mapping
- **Configuration-driven architecture** — the entire pipeline is controlled by a Snowflake config table, not Python code
- **Multi-format ingestion** — CSV, TXT (pipe-delimited), and XLSX from Google Drive
- **Layered data quality gates** — schema validation, deduplication, mandatory null checks, and type-safe loading
- **Centralized observability** — every run logged to a Snowflake audit table with row counts, timestamps, and error details
- **Automated alerting** — HTML email notifications with full tracebacks on failure, summary on success

> The entire system scales to any number of tables by inserting a single row into a Snowflake configuration table. **No Python changes required.**

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         APACHE AIRFLOW  (Docker)                              │
│                                                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐  │
│  │                        master_trigger_dag                                │  │
│  │                                                                           │  │
│  │   [1] fetch_active_configs  ──►  SELECT CONFIG_ID FROM SOURCE_CONFIG     │  │
│  │                                   WHERE IS_ACTIVE = TRUE                 │  │
│  │                                   ──► Returns 10 active pipeline IDs     │  │
│  │                                                                           │  │
│  │   [2] trigger_universal_etl_dag   (Dynamic Task Mapping × 10)            │  │
│  │       ├── config_id: 1  ──► spawn Worker for CUSTOMER                   │  │
│  │       ├── config_id: 2  ──► spawn Worker for ORDERS                     │  │
│  │       ├── config_id: 3  ──► spawn Worker for ORDER_ITEMS                │  │
│  │       └── ...           ──► (10 Workers run concurrently)               │  │
│  └─────────────────────────────────────────────────────────────────────────┘  │
│                     │                                                          │
│                     ▼ (10 parallel DAG runs)                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐  │
│  │                     universal_etl_dag  (×10 concurrent)                  │  │
│  │                                                                           │  │
│  │   fetch_metadata ──► download_file ──► validate_file ──► transform_file ──► load_to_snowflake  │
│  │                                                                           │  │
│  │   [1] fetch_metadata   : Query SOURCE_CONFIG for this config_id         │  │
│  │   [2] download_file    : Download CSV / TXT / XLSX from Google Drive    │  │
│  │   [3] validate_file    : Verify schema against COLUMN_LIST              │  │
│  │   [4] transform_file   : Strip extra cols → dedup → null-check         │  │
│  │   [5] load_to_snowflake: write_pandas → UPDATE LOADED_AT → ETL_LOG     │  │
│  └─────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
           │                                        │
           ▼                                        ▼
  ┌─────────────────┐                   ┌────────────────────────┐
  │   Google Drive   │                   │       SNOWFLAKE         │
  │                  │                   │  CUSTOMER_PIPELINE_DB   │
  │  10 Source Files │                   │  ┌─── RAW Schema ──┐   │
  │  ┌─ customers   ─┤                   │  │  10 Data Tables  │   │
  │  ├─ orders      ─┤                   │  └─────────────────┘   │
  │  ├─ shipments   ─┤  (CSV/TXT/XLSX)   │  ┌─── ETL Schema ──┐   │
  │  └─ ...  (10)  ─┘                   │  │  SOURCE_CONFIG   │   │
  └─────────────────┘                   │  │  ETL_LOG         │   │
                                         │  └─────────────────┘   │
                                         └────────────────────────┘
```

---

## Key Features

<table>
  <tr>
    <td><b>🧠 Metadata-Driven</b></td>
    <td>The entire pipeline is controlled by a <code>SOURCE_CONFIG</code> table in Snowflake. Add a new pipeline with a single SQL INSERT.</td>
  </tr>
  <tr>
    <td><b>⚡ Concurrent Execution</b></td>
    <td>All 10 pipelines run simultaneously using Airflow Dynamic Task Mapping (<code>TriggerDagRunOperator.expand_kwargs</code>).</td>
  </tr>
  <tr>
    <td><b>📁 Multi-Format Support</b></td>
    <td>Ingests CSV, TXT (any delimiter), and XLSX (both native Google Sheets and binary uploads) through a single unified Worker DAG.</td>
  </tr>
  <tr>
    <td><b>🛡️ 6-Layer Data Quality</b></td>
    <td>Schema validation → extra column stripping → deduplication → mandatory null check → final column guard → type-safe VARCHAR IDs.</td>
  </tr>
  <tr>
    <td><b>📊 Full Observability</b></td>
    <td>Every pipeline run — success or failure — is logged to <code>ETL.ETL_LOG</code> in Snowflake with row counts, timestamps, and error messages.</td>
  </tr>
  <tr>
    <td><b>📧 Automated Alerting</b></td>
    <td>HTML email alerts with full Python tracebacks on failure, and summary emails with row counts on success.</td>
  </tr>
  <tr>
    <td><b>🔒 Least-Privilege Security</b></td>
    <td>Dedicated <code>AIRFLOW_LOADER_ROLE</code> in Snowflake — SELECT and INSERT only. Service account JSON for Google Drive. Secrets in <code>.env</code>, never in code.</td>
  </tr>
  <tr>
    <td><b>🐳 Fully Dockerized</b></td>
    <td>One <code>docker compose up -d</code> command brings up the complete Airflow stack (webserver, scheduler, Postgres). No local Python install needed.</td>
  </tr>
</table>

---

## Technology Stack

| Category | Technology | Version |
|---|---|---|
| **Orchestration** | Apache Airflow | 2.9.3 |
| **Data Warehouse** | Snowflake | Cloud |
| **Runtime** | Docker + Docker Compose | LocalExecutor + Postgres |
| **Language** | Python | 3.11 |
| **Data Processing** | Pandas + PyArrow | ≥ 2.0.0 |
| **Snowflake Connector** | snowflake-connector-python[pandas] | ≥ 3.0.0 |
| **Source Storage** | Google Drive | API v3 |
| **Auth** | Google Service Account | JSON key |
| **XLSX Parsing** | openpyxl | ≥ 3.1.0 |
| **Alerting** | SMTP (Gmail) | Airflow email utils |

---

## The 10 Data Pipelines

| # | Table | Source Format | Source | Nullable Columns |
|---|---|---|---|---|
| 1 | `CUSTOMER` | CSV | Google Drive | — |
| 2 | `ORDERS` | CSV | Google Drive | — |
| 3 | `ORDER_ITEMS` | CSV | Google Drive | — |
| 4 | `PAYMENTS` | CSV | Google Drive | — |
| 5 | `PRODUCTS` | CSV | Google Drive | — |
| 6 | `EMPLOYEES` | **XLSX** | Google Drive | `manager_id`, `salary` |
| 7 | `RETURNS` | **TXT** (pipe `\|`) | Google Drive | — |
| 8 | `REVIEWS` | CSV | Google Drive | `review_text` |
| 9 | `SHIPMENTS` | **XLSX** | Google Drive | `carrier`, `delivery_date`, `shipping_cost` |
| 10 | `SUPPLIERS` | CSV | Google Drive | `contact_email`, `contact_phone` |

---

## Data Quality Gates

The pipeline has **6 independent layers** of protection against bad vendor data. They execute in a deliberate sequence:

```
Vendor File (CSV / TXT / XLSX)
        │
        ▼
[Gate 1] Schema Validation      ── Missing declared columns? FAIL FAST.
        │
        ▼
[Gate 2] Extra Column Stripping ── Remove ghost/unnamed XLSX columns.
        │                           (Prevents SQL syntax errors in Snowflake)
        ▼
[Gate 3] Deduplication          ── Exact duplicate rows → saved to rejected/
        │
        ▼
[Gate 4] Mandatory Null Check   ── Nulls in non-nullable columns → saved to rejected/
        │                           (Nullable columns defined per-table in SOURCE_CONFIG)
        ▼
[Gate 5] Final Column Guard     ── Secondary strip before write_pandas (safety net)
        │
        ▼
[Gate 6] Type-Safe IDs          ── All ID columns are VARCHAR(50), not NUMBER.
        │                           Handles corrupted/alphanumeric vendor IDs.
        ▼
  Clean Data → Snowflake RAW Table
```

All rejected rows are saved to `data/rejected/` with the table name, rejection reason, and config ID — nothing is silently discarded.

---

## Project Structure

```
airflow_customer_pipeline/
│
├── dags/
│   ├── config.py                    ← Central config: paths, Snowflake, email
│   ├── alerts.py                    ← Shared HTML email callbacks (success + failure)
│   ├── dag_0_master_trigger.py      ← Master DAG: reads SOURCE_CONFIG, fans out workers
│   └── dag_universal_etl.py         ← Worker DAG: full 5-step ETL for one table
│
├── data/
│   ├── extract/                     ← Raw downloaded files  (customer_raw_1.csv, ...)
│   ├── processed/                   ← Clean validated files (customer_clean_1.csv, ...)
│   └── rejected/                    ← Audit trail of dropped rows
│       ├── *_dropped_nulls_*.csv
│       └── *_dropped_duplicates_*.csv
│
├── config/
│   └── gdrive_service_account.json  ← Google Drive API key (NOT committed to git)
│
├── snowflake_ddls/                  ← Run once, in order, in Snowflake
│   ├── 01_setup_database.sql        ← DB, schemas, ETL_LOG, SOURCE_CONFIG, grants
│   ├── 02_setup_raw_tables.sql      ← All 10 RAW tables + grants
│   └── 03_setup_pipeline_configs.sql← All 10 SOURCE_CONFIG rows
│
├── docker-compose.yaml              ← Airflow stack (Postgres + init + webserver + scheduler)
├── requirements.txt                 ← Python package pinning reference
├── .env.example                     ← SMTP config template (copy to .env)
└── full_detail.md                   ← Complete technical reference
```

---

## Quick Start

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- A [Snowflake](https://snowflake.com) account (free trial works)
- A Google Cloud project with [Google Drive API](https://console.cloud.google.com) enabled

### 1. Clone and Configure

```bash
git clone https://github.com/NaveedBhat/airflow-snowflake-etl.git
cd airflow-snowflake-etl

# Set up your SMTP credentials for email alerting
cp .env.example .env
# Edit .env — fill in SMTP_USER, SMTP_PASSWORD (Gmail App Password), SMTP_MAIL_FROM
```

### 2. Add Google Drive Service Account

1. Create a Service Account in Google Cloud Console
2. Download the JSON key → place at `config/gdrive_service_account.json`
3. Share each source file with the service account email as **Viewer**

> See [`GOOGLE_DRIVE_SETUP.md`](GOOGLE_DRIVE_SETUP.md) for the complete walkthrough.

### 3. Set Up Snowflake

Run these three SQL files **in order** in a Snowflake Worksheet:

```sql
-- Run 01_setup_database.sql first
-- Run 02_setup_raw_tables.sql second
-- Run 03_setup_pipeline_configs.sql third
```

Then create the Airflow service role:

```sql
CREATE ROLE IF NOT EXISTS AIRFLOW_LOADER_ROLE;
CREATE USER IF NOT EXISTS AIRFLOW_LOADER
    PASSWORD = '<strong_password>'
    DEFAULT_ROLE = AIRFLOW_LOADER_ROLE
    DEFAULT_WAREHOUSE = COMPUTE_WH;
GRANT ROLE AIRFLOW_LOADER_ROLE TO USER AIRFLOW_LOADER;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE AIRFLOW_LOADER_ROLE;
```

### 4. Start Airflow

```bash
docker compose up -d
```

Open [http://localhost:8081](http://localhost:8081) — login: `airflow` / `airflow`

### 5. Configure Airflow

**Add Snowflake Connection** (Admin → Connections → Add):

| Field | Value |
|---|---|
| Conn ID | `snowflake_customer_pipeline` |
| Conn Type | `Snowflake` |
| Account | `<your-account>.snowflakecomputing.com` |
| Login | `AIRFLOW_LOADER` |
| Password | `<your-password>` |
| Database | `CUSTOMER_PIPELINE_DB` |
| Schema | `RAW` |
| Warehouse | `COMPUTE_WH` |
| Role | `AIRFLOW_LOADER_ROLE` |

**Add Variables** (Admin → Variables → Add):

| Key | Value |
|---|---|
| `customer_pipeline_alert_email` | `your@email.com` |
| `gdrive_service_account_file` | `/opt/airflow/config/gdrive_service_account.json` |

### 6. Run the Pipeline

Click **▶** next to `master_trigger_dag` → **Trigger DAG**

All 10 pipelines start concurrently. You will receive email alerts on completion.

---

## Snowflake Data Model

```
CUSTOMER_PIPELINE_DB
├── RAW                          ← 10 tables, one per data source
│   ├── CUSTOMER                 ← customer_id,name,city,country,...
│   ├── ORDERS                   ← order_id,customer_id,order_date,...
│   ├── ORDER_ITEMS              ← order_item_id,order_id,product_id,...
│   ├── PAYMENTS                 ← payment_id,order_id,payment_method,...
│   ├── PRODUCTS                 ← product_id,product_name,category,...
│   ├── EMPLOYEES                ← employee_id,name,department,...
│   ├── RETURNS                  ← return_id,order_id,customer_id,...
│   ├── REVIEWS                  ← review_id,order_id,customer_id,...
│   ├── SHIPMENTS                ← shipment_id,order_id,carrier,...
│   └── SUPPLIERS                ← supplier_id,supplier_name,city,...
│        │
│        └── (Every table has: LOADED_AT TIMESTAMP_NTZ, SOURCE_RUN_ID VARCHAR)
│
└── ETL
    ├── SOURCE_CONFIG            ← Pipeline registry (10 rows = 10 pipelines)
    └── ETL_LOG                  ← Immutable audit log, one row per run
```

---

## Monitoring

### In the Airflow UI
- `master_trigger_dag` → 1 run, 11 tasks (1 fetch + 10 triggers)
- `universal_etl_dag` → 10 concurrent runs, 5 tasks each
- Click any task → **Logs** tab for full execution details

### In Snowflake

```sql
-- View all pipeline runs (latest first)
SELECT CONFIG_ID, TABLE_NAME, TABLE_LOAD_STATUS,
       INSERTION_ROWCOUNT, TABLE_LOAD_MESSAGE,
       IMPORT_STARTTS, IMPORT_COMPLETETS
FROM CUSTOMER_PIPELINE_DB.ETL.ETL_LOG
ORDER BY CREATED_AT DESC;

-- View only failed runs
SELECT TABLE_NAME, TABLE_LOAD_STATUS, TABLE_LOAD_MESSAGE, CREATED_AT
FROM CUSTOMER_PIPELINE_DB.ETL.ETL_LOG
WHERE TABLE_LOAD_STATUS = 'FAILED'
ORDER BY CREATED_AT DESC;
```

---

## Adding a New Pipeline

No Python code required. Just two SQL statements and a re-trigger:

```sql
-- 1. Register the new source
INSERT INTO CUSTOMER_PIPELINE_DB.ETL.SOURCE_CONFIG (
    CONFIG_ID, VENDOR, EXTRACT_LOCATION, GD_LOCATION, SCHEMA_NAME,
    TABLE_NAME, FILE_NAME_PATTERN, FILE_FORMAT, DATA_DELIMITER,
    COLUMN_LIST, NULLABLE_COLUMNS, IS_ACTIVE
) VALUES (
    11, 'GOOGLE_DRIVE', '<drive_file_id>', '<full_url>',
    'RAW', 'INVENTORY', 'inventory_raw.csv', 'CSV', ',',
    'item_id,item_name,quantity,warehouse_id', '', TRUE
);

-- 2. Create the destination table
CREATE TABLE CUSTOMER_PIPELINE_DB.RAW.INVENTORY (
    ITEM_ID       VARCHAR(50),
    ITEM_NAME     VARCHAR(255),
    QUANTITY      NUMBER,
    WAREHOUSE_ID  VARCHAR(50),
    LOADED_AT     TIMESTAMP_NTZ(9),
    SOURCE_RUN_ID VARCHAR(255)
);
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE
    CUSTOMER_PIPELINE_DB.RAW.INVENTORY TO ROLE AIRFLOW_LOADER_ROLE;
```

Trigger `master_trigger_dag` — the new pipeline runs automatically.

---

## Documentation

| Document | Description |
|---|---|
| [`full_detail.md`](full_detail.md) | Complete technical reference — every column, every task, every design decision |
| [`AIRFLOW_PIPELINE_GUIDE.md`](AIRFLOW_PIPELINE_GUIDE.md) | File-by-file breakdown and setup walkthrough |
| [`architecture_overview.md`](architecture_overview.md) | Step-by-step data flow explanation |
| [`FULL_GUIDE.md`](FULL_GUIDE.md) | The core concept and scaling guide |
| [`SNOWFLAKE_PROJECT_DOCUMENTATION.md`](SNOWFLAKE_PROJECT_DOCUMENTATION.md) | Snowflake schema, table definitions, security model |
| [`GOOGLE_DRIVE_SETUP.md`](GOOGLE_DRIVE_SETUP.md) | Google Cloud service account setup guide |

---

## Author

**Naveed Mohammad Bhat**
Data Engineer | ETL & Data Warehousing | SQL, Python, Apache Airflow, Snowflake

[![LinkedIn](https://img.shields.io/badge/LinkedIn-naveedbhat085-0077B5?style=flat-square&logo=linkedin)](https://linkedin.com/in/naveedbhat085)
[![GitHub](https://img.shields.io/badge/GitHub-NaveedBhat-181717?style=flat-square&logo=github)](https://github.com/NaveedBhat)
[![Portfolio](https://img.shields.io/badge/Portfolio-naveedbhat.in-FF7139?style=flat-square&logo=firefox)](https://naveedbhat.in)

---

<div align="center">

*If you found this project useful, please consider giving it a ⭐ on GitHub!*

</div>
