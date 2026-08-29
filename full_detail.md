# Metadata-Driven Airflow & Snowflake ETL Framework
### Full Project Documentation

---

## Table of Contents
1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Folder Structure](#3-folder-structure)
4. [Snowflake Data Model](#4-snowflake-data-model)
5. [Source Configurations (10 Pipelines)](#5-source-configurations-10-pipelines)
6. [DAG Reference](#6-dag-reference)
7. [Data Quality Gates](#7-data-quality-gates)
8. [Email Alerting](#8-email-alerting)
9. [Environment Setup (Step-by-Step)](#9-environment-setup-step-by-step)
10. [Airflow Variables & Connections](#10-airflow-variables--connections)
11. [Running the Pipeline](#11-running-the-pipeline)
12. [Technology Stack](#12-technology-stack)

---

## 1. Project Overview

This project is a **production-grade, metadata-driven batch ETL framework** that automatically extracts source files from Google Drive, applies configurable data quality transformations, and loads clean data into a Snowflake data warehouse.

**The core design principle:** instead of writing a separate pipeline for every table, a single `SOURCE_CONFIG` table in Snowflake controls what gets ingested, from where, and in what format. Adding a new data source requires only inserting a new row into this config table — no code changes.

### What It Does
- Reads **10 active source configurations** from Snowflake
- Dynamically orchestrates **10 concurrent ETL pipelines** using Airflow Dynamic Task Mapping
- Supports **CSV, TXT (pipe-delimited), and XLSX** source file formats
- Applies strict **data quality gates** (column filtering, deduplication, mandatory-field null checks)
- Loads clean, validated data into **Snowflake** using PyArrow-backed `write_pandas`
- Logs every pipeline execution to a central **ETL_LOG** table in Snowflake
- Sends **HTML email alerts** on both task failure and DAG success

---

## 2. Architecture

```
+---------------------------------------------------------------------------+
|                         APACHE AIRFLOW (Docker)                           |
|                                                                            |
|   +--------------------------------------------------------------------+  |
|   |                    master_trigger_dag                               |  |
|   |                                                                      |  |
|   |  Task 1: fetch_active_configs()                                     |  |
|   |     SELECT CONFIG_ID FROM SOURCE_CONFIG WHERE IS_ACTIVE=TRUE        |  |
|   |     Returns: [{conf:{config_id:1}}, ..., {conf:{config_id:10}}]    |  |
|   |                                                                      |  |
|   |  Task 2: trigger_universal_etl_dag  (Dynamic Task Mapping)         |  |
|   |     Spawns 10 concurrent TriggerDagRunOperator instances            |  |
|   +--------------------------------------------------------------------+  |
|                          | (triggers 10 parallel runs)                     |
|                          v                                                 |
|   +--------------------------------------------------------------------+  |
|   |              universal_etl_dag  (one instance per table)            |  |
|   |                                                                      |  |
|   |  [1] fetch_metadata  -> reads SOURCE_CONFIG row from Snowflake     |  |
|   |  [2] download_file   -> downloads file from Google Drive           |  |
|   |                         (CSV/TXT as-is, XLSX converted to CSV)     |  |
|   |  [3] validate_file   -> checks all declared columns exist          |  |
|   |  [4] transform_file  -> strips extra cols -> dedup -> null check   |  |
|   |                         saves rejected rows to /data/rejected/      |  |
|   |  [5] load_to_snowflake -> write_pandas -> UPDATE LOADED_AT         |  |
|   |                           -> INSERT into ETL_LOG                    |  |
|   +--------------------------------------------------------------------+  |
+---------------------------------------------------------------------------+
                 |                              |
                 v                              v
      +------------------+          +------------------------+
      |   Google Drive    |          |        SNOWFLAKE        |
      |  10 source files  |          |  CUSTOMER_PIPELINE_DB  |
      |  (CSV/TXT/XLSX)   |          |  +-- RAW schema        |
      +------------------+          |  |   +-- 10 tables      |
                                     |  +-- ETL schema        |
                                     |      +-- SOURCE_CONFIG  |
                                     |      +-- ETL_LOG        |
                                     +------------------------+
```

### Key Design Decisions

| Decision | Rationale |
|---|---|
| **Master + Worker DAG pattern** | Master orchestrates; Worker is self-contained and reusable for any table |
| **Metadata-driven (SOURCE_CONFIG)** | Adding a new pipeline requires only a new DB row, not code changes |
| **Dynamic Task Mapping** | Master fan-out to N workers with a single `expand_kwargs()` call |
| **XLSX to CSV conversion at download** | Keeps the entire downstream pipeline format-agnostic |
| **Column filter before dedup** | Ensures deduplication operates on business columns only, not on unnamed/extra XLSX columns |
| **PyArrow `use_logical_type=True`** | Prevents DATE/TIME serialization errors when loading via `write_pandas` |
| **All IDs as `VARCHAR(50)`** | Handles alphanumeric and corrupted vendor IDs without crashing the load |
| **Server-side `LOADED_AT`** | Avoids PyArrow datetime precision issues; Snowflake sets the timestamp |
| **`schedule=None`** | Prevents Airflow from auto-running on Docker restart; manual trigger only |

---

## 3. Folder Structure

```
airflow_customer_pipeline/
|
+-- dags/                              # All Airflow DAG and helper files
|   +-- config.py                      # Central config: paths, Snowflake settings, email
|   +-- alerts.py                      # Shared success/failure email callbacks
|   +-- dag_0_master_trigger.py        # Master DAG: reads SOURCE_CONFIG, fans out workers
|   +-- dag_universal_etl.py           # Worker DAG: full ETL for one table per run
|
+-- data/                              # Local data staging (mounted into Docker container)
|   +-- extract/                       # Raw downloaded files (e.g. customer_raw_1.csv)
|   +-- processed/                     # Clean files after transformation
|   +-- rejected/                      # Rows dropped due to nulls or duplicates
|       +-- *_dropped_nulls_*.csv
|       +-- *_dropped_duplicates_*.csv
|
+-- config/                            # Secrets (NOT committed to Git)
|   +-- gdrive_service_account.json    # Google Drive API service account key
|
+-- snowflake_ddls/                    # Snowflake setup scripts (run once, in order)
|   +-- 01_setup_database.sql          # Creates DB, schemas, ETL_LOG, SOURCE_CONFIG + grants
|   +-- 02_setup_raw_tables.sql        # Creates all 10 RAW destination tables + grants
|   +-- 03_setup_pipeline_configs.sql  # Inserts all 10 SOURCE_CONFIG rows
|
+-- logs/                              # Airflow task logs (auto-generated)
+-- plugins/                           # Airflow plugins (currently empty)
+-- docker-compose.yaml                # Airflow stack definition
+-- .env                               # Real secrets (SMTP, etc) -- NOT committed
+-- .env.example                       # Template showing required variables
+-- requirements.txt                   # Python packages for Airflow containers
+-- full_detail.md                     # This document
```

---

## 4. Snowflake Data Model

### Database & Schema Layout

```
CUSTOMER_PIPELINE_DB
+-- RAW                       (raw ingestion destination -- 10 tables)
|   +-- CUSTOMER
|   +-- ORDERS
|   +-- ORDER_ITEMS
|   +-- PAYMENTS
|   +-- PRODUCTS
|   +-- EMPLOYEES
|   +-- RETURNS
|   +-- REVIEWS
|   +-- SHIPMENTS
|   +-- SUPPLIERS
|
+-- ETL                       (metadata and observability)
    +-- SOURCE_CONFIG         (pipeline configuration registry)
    +-- ETL_LOG               (per-run execution audit log)
```

### SOURCE_CONFIG Table (ETL Schema)

| Column | Type | Description |
|---|---|---|
| `CONFIG_ID` | NUMBER (autoincrement) | Unique pipeline identifier |
| `VENDOR` | VARCHAR(100) | Source system (e.g. `GOOGLE_DRIVE`) |
| `EXTRACT_LOCATION` | VARCHAR(1000) | Google Drive File ID for download |
| `GD_LOCATION` | VARCHAR(1000) | Full Google Drive URL (for logging/audit) |
| `SCHEMA_NAME` | VARCHAR(100) | Target Snowflake schema (e.g. `RAW`) |
| `TABLE_NAME` | VARCHAR(100) | Target Snowflake table name |
| `FILE_NAME_PATTERN` | VARCHAR(255) | Source filename (used in ETL_LOG) |
| `FILE_FORMAT` | VARCHAR(50) | `CSV`, `TXT`, or `XLSX` |
| `DATA_DELIMITER` | VARCHAR(10) | `,` for CSV/XLSX, `\|` for TXT |
| `COLUMN_LIST` | VARCHAR(2000) | Comma-separated list of expected columns (schema contract) |
| `NULLABLE_COLUMNS` | VARCHAR(2000) | Columns allowed to be NULL (blank = all mandatory) |
| `IS_ACTIVE` | BOOLEAN | `TRUE` = pipeline runs; `FALSE` = skipped |
| `CREATED_AT` | TIMESTAMP | Auto-set at row creation |
| `CREATED_BY` | VARCHAR(100) | Auto-set to current Snowflake user |

### ETL_LOG Table (ETL Schema)

| Column | Description |
|---|---|
| `LOG_ID` | Auto-incrementing primary key |
| `CONFIG_ID` | Links back to SOURCE_CONFIG |
| `SOURCE_PATH` | Google Drive File ID that was processed |
| `GD_PATH` | Full Google Drive URL |
| `SOURCE_FILE_NAME` | Pattern from SOURCE_CONFIG |
| `FILE_STATUS` | `SUCCESS` or `FAILED` |
| `FILE_FORMAT` | `CSV`, `TXT`, or `XLSX` |
| `IMPORT_STARTTS` | When `load_to_snowflake` began |
| `IMPORT_COMPLETETS` | When the load completed (server-side) |
| `SOURCE_ROWCOUNT` | Row count from `transform_file` XCom |
| `TABLE_NAME` | Destination table |
| `TABLE_LOAD_STATUS` | `SUCCESS` or `FAILED` |
| `INSERTION_ROWCOUNT` | Rows actually inserted by `write_pandas` |
| `TABLE_LOAD_MESSAGE` | Error message if failed (first 1000 chars) |
| `TABLE_LOADTS` | Server-side timestamp of load completion |
| `CREATED_AT` | Server-side timestamp of log record creation |
| `RUNID` | Airflow `run_id` -- unique per DAG run |
| `DATADATE` | Airflow logical date (`ds`) |

### RAW Table System Columns
Every RAW table has two system columns appended by the pipeline:

| Column | Type | Description |
|---|---|---|
| `LOADED_AT` | `TIMESTAMP_NTZ(9)` | Set server-side via `UPDATE` after `write_pandas` |
| `SOURCE_RUN_ID` | `VARCHAR(255)` | Airflow `run_id` -- links every row back to its ETL_LOG entry |

---

## 5. Source Configurations (10 Pipelines)

| Config ID | Table | Format | Delimiter | Nullable Columns |
|---|---|---|---|---|
| 1 | `CUSTOMER` | CSV | `,` | none |
| 2 | `ORDERS` | CSV | `,` | none |
| 3 | `ORDER_ITEMS` | CSV | `,` | none |
| 4 | `PAYMENTS` | CSV | `,` | none |
| 5 | `PRODUCTS` | CSV | `,` | none |
| 6 | `EMPLOYEES` | XLSX | `,` | `manager_id`, `salary` |
| 7 | `RETURNS` | TXT | `\|` | none |
| 8 | `REVIEWS` | CSV | `,` | `review_text` |
| 9 | `SHIPMENTS` | XLSX | `,` | `carrier`, `delivery_date`, `shipping_cost` |
| 10 | `SUPPLIERS` | CSV | `,` | `contact_email`, `contact_phone` |

### Column Lists per Table

| Table | Columns |
|---|---|
| CUSTOMER | `customer_id, name, city, country, signup_date, email, phone, age` |
| ORDERS | `order_id, customer_id, order_date, status, total_amount, product_name, category, quantity, discount` |
| ORDER_ITEMS | `order_item_id, order_id, product_id, quantity, unit_price` |
| PAYMENTS | `payment_id, order_id, payment_method, payment_date, amount, payment_status` |
| PRODUCTS | `product_id, product_name, category, brand, price, stock_quantity, supplier` |
| EMPLOYEES | `employee_id, name, department, hire_date, email, manager_id, salary` |
| RETURNS | `return_id, order_id, customer_id, return_date, reason, refund_amount, status` |
| REVIEWS | `review_id, order_id, customer_id, rating, review_text, review_date` |
| SHIPMENTS | `shipment_id, order_id, carrier, ship_date, delivery_date, tracking_number, shipping_cost` |
| SUPPLIERS | `supplier_id, supplier_name, city, country, contact_email, contact_phone` |

---

## 6. DAG Reference

### `master_trigger_dag`

| Property | Value |
|---|---|
| DAG ID | `master_trigger_dag` |
| Schedule | `None` (manual trigger only) |
| Max Active Runs | `1` |
| Retries | `1` with 5-minute delay |

**Task flow:**
```
fetch_active_configs()
       |
       +---> trigger_universal_etl_dag (expanded x N active configs)
```

**`fetch_active_configs`** -- `@task` decorated Python function. Connects to Snowflake via `SnowflakeHook`, queries `SOURCE_CONFIG WHERE IS_ACTIVE = TRUE`, and returns a list of `{"conf": {"config_id": N}}` dicts. Each dict becomes one `TriggerDagRunOperator` instance via `.expand_kwargs()`.

**`trigger_universal_etl_dag`** -- `TriggerDagRunOperator.partial(...).expand_kwargs(...)`. Fires `universal_etl_dag` once for each config, passing `config_id` as `dag_run.conf`. Does NOT wait for worker completion (`wait_for_completion=False`).

---

### `universal_etl_dag`

| Property | Value |
|---|---|
| DAG ID | `universal_etl_dag` |
| Schedule | `None` (triggered by master only) |
| Max Active Runs | `10` (allows all 10 to run in parallel) |
| Retries | `2` with 5-minute delay |

**Task flow:**
```
fetch_metadata -> download_file -> validate_file -> transform_file -> load_to_snowflake
```

#### Task: `fetch_metadata`
- Reads `config_id` from `dag_run.conf`
- Executes `SELECT ... FROM SOURCE_CONFIG WHERE CONFIG_ID = %s AND IS_ACTIVE = TRUE`
- Pushes full metadata dict to XCom key `source_metadata`
- Raises `ValueError` if no active config found

#### Task: `download_file`
- Authenticates with Google Drive using the service account JSON key
- **CSV / TXT:** downloads raw bytes directly via `get_media()`, writes to `data/extract/`
- **XLSX (native Google Sheet):** detects `vnd.google-apps.spreadsheet` MIME type, exports via `export_media(mimeType="text/csv")`
- **XLSX (uploaded binary):** downloads via `get_media()`, reads with `pd.read_excel(engine="openpyxl")`, writes to CSV
- All output files are written as `.csv` -- downstream tasks are format-agnostic
- Raises `FileNotFoundError` if Google Drive file is not accessible
- Raises `ValueError` if downloaded file is 0 bytes

#### Task: `validate_file`
- Reads raw CSV with declared delimiter
- Verifies row count > 0
- Compares `set(df.columns)` against `COLUMN_LIST` -- raises `ValueError` listing any missing columns
- Extra (undeclared) columns do NOT fail validation -- they are stripped in `transform_file`

#### Task: `transform_file`
Execution order is deliberate and critical:

```
1. pd.read_csv(raw_file, sep=delimiter)
2. Parse COLUMN_LIST -> all_columns, nullable_cols, mandatory_cols
3. df = df[cols in COLUMN_LIST only]         <- strip extra/unnamed columns FIRST
4. Detect duplicate rows -> save to rejected/
5. df_clean = df.drop_duplicates()
6. null_mask on mandatory_cols only          <- null check on business cols only
7. Null rows -> save to rejected/
8. df_clean.dropna(subset=mandatory_cols)
9. df_clean.to_csv(clean_file)              <- only COLUMN_LIST cols written
10. XCom push row_count
```

Rejected files saved as:
- `data/rejected/<table>_dropped_nulls_<config_id>.csv`
- `data/rejected/<table>_dropped_duplicates_<config_id>.csv`

#### Task: `load_to_snowflake`
```
1. pd.read_csv(clean_file)
2. Uppercase all column names
3. Filter to COLUMN_LIST cols (secondary safety guard)
4. Add SOURCE_RUN_ID = airflow run_id
5. write_pandas(conn, df, table, schema, database,
                quote_identifiers=False,
                use_logical_type=True)
6. If success -> UPDATE SET LOADED_AT = CURRENT_TIMESTAMP() WHERE SOURCE_RUN_ID = %s
7. Finally -> INSERT into ETL_LOG (always runs, success or failure)
```

`on_success_callback` fires `on_etl_success` which sends a success email with table name, row count, and config ID.

---

## 7. Data Quality Gates

### Layer 1: Schema Validation (`validate_file`)
Ensures every column declared in `COLUMN_LIST` exists in the source file. Fails fast with a descriptive error before any transformation is attempted.

### Layer 2: Extra Column Stripping (`transform_file`)
Strips any columns present in the source file that are NOT in `COLUMN_LIST`. This is critical for XLSX files, which can have trailing empty columns that Pandas auto-names `Unnamed: 7`, `Unnamed: 8`, etc.

> **Why this matters:** The colon character (`:`) in `Unnamed: 7` causes a SQL syntax error in the Snowflake `COPY INTO` statement generated internally by `write_pandas`. This stripping step prevents that crash entirely.

### Layer 3: Deduplication (`transform_file`)
Runs AFTER column stripping, so dedup operates only on declared business columns. Duplicate rows are saved to a rejected file for audit purposes before being dropped.

### Layer 4: Mandatory-Field Null Check (`transform_file`)
Uses `NULLABLE_COLUMNS` from `SOURCE_CONFIG` to distinguish optional fields (like `review_text`, `manager_id`) from mandatory fields. Rows with null values in mandatory columns are rejected and saved separately.

### Layer 5: Final Column Guard (`load_to_snowflake`)
A secondary filter before `write_pandas` that ensures no unexpected column can reach Snowflake, even if the clean file somehow had extra columns.

### Layer 6: Type-Safe IDs (Snowflake DDL)
All ID columns across all tables are `VARCHAR(50)` -- not `NUMBER`. This allows the pipeline to safely ingest alphanumeric, prefixed, or even corrupted ID values from vendor data without a casting error crashing the load. Data type enforcement can be applied downstream in transformation layers.

---

## 8. Email Alerting

Defined in `dags/alerts.py`. Two callbacks:

### `task_failure_alert` (on_failure_callback)
Attached at the DAG level via `default_args` -- fires automatically for any task that fails. The email contains:
- DAG ID, Task ID, Logical Date, Attempt number
- **Full Python traceback** (not just the error message)
- Direct link to the Airflow task log URL
- All values HTML-escaped to prevent broken email rendering

### `dag_success_alert` (on_success_callback)
Attached explicitly to `load_to_snowflake`. Contains:
- Table name, rows inserted, config ID
- Run logical date

Both callbacks use `airflow.utils.email.send_email` which reads SMTP configuration from `.env`.

---

## 9. Environment Setup (Step-by-Step)

### Prerequisites
- Docker Desktop installed and running
- A Snowflake account with ACCOUNTADMIN access
- A Google Cloud project with Google Drive API enabled

### Step 1: Clone and Configure
```bash
git clone <your-repo-url>
cd airflow_customer_pipeline
cp .env.example .env
# Edit .env and fill in your SMTP credentials
```

### Step 2: Add Google Drive Service Account
1. Go to Google Cloud Console and create a Service Account
2. Download the JSON key
3. Enable the **Google Drive API**
4. Place the JSON key at `config/gdrive_service_account.json`
5. Share each source Google Drive file/sheet with the service account email address

### Step 3: Set Up Snowflake
Run these three SQL scripts **in order** in a Snowflake worksheet:

```
-- STEP 1: Creates DB, RAW/ETL schemas, ETL_LOG, SOURCE_CONFIG tables, and grants
snowflake_ddls/01_setup_database.sql

-- STEP 2: Creates all 10 RAW destination tables with VARCHAR(50) ID columns
snowflake_ddls/02_setup_raw_tables.sql

-- STEP 3: Inserts all 10 pipeline configs into SOURCE_CONFIG
snowflake_ddls/03_setup_pipeline_configs.sql
```

### Step 4: Start Airflow
```bash
docker compose up -d
```

Wait ~30 seconds, then open http://localhost:8081
- Username: `airflow`
- Password: `airflow`

### Step 5: Configure Airflow Connection (Snowflake)
Admin -> Connections -> Add:

| Field | Value |
|---|---|
| Conn ID | `snowflake_customer_pipeline` |
| Conn Type | `Snowflake` |
| Account | `<your_account>` (e.g. `abc123.ap-southeast-1`) |
| Login | `AIRFLOW_LOADER` |
| Password | `<your_password>` |
| Database | `CUSTOMER_PIPELINE_DB` |
| Schema | `RAW` |
| Warehouse | `COMPUTE_WH` |
| Role | `AIRFLOW_LOADER_ROLE` |

### Step 6: Configure Airflow Variables
Admin -> Variables -> Add:

| Key | Value |
|---|---|
| `customer_pipeline_alert_email` | `your_email@gmail.com` |
| `gdrive_service_account_file` | `/opt/airflow/config/gdrive_service_account.json` |

---

## 10. Airflow Variables & Connections

### Variables Used by `config.py`

| Variable Key | Default Value | Description |
|---|---|---|
| `gdrive_service_account_file` | `/opt/airflow/config/gdrive_service_account.json` | Path to Google Drive service account key inside container |
| `customer_pipeline_data_root` | `/opt/airflow/data` | Root directory for all local data staging |
| `snowflake_conn_id` | `snowflake_customer_pipeline` | Airflow Connection ID for Snowflake |
| `customer_pipeline_alert_email` | `you@example.com` | Comma-separated email address(es) for alerts |

### File Path Conventions

| Function | Path Pattern |
|---|---|
| `get_raw_file_path(table, id)` | `/opt/airflow/data/extract/<table>_raw_<id>.csv` |
| `get_clean_file_path(table, id)` | `/opt/airflow/data/processed/<table>_clean_<id>.csv` |
| `get_rejected_nulls_path(table, id)` | `/opt/airflow/data/rejected/<table>_dropped_nulls_<id>.csv` |
| `get_rejected_dupes_path(table, id)` | `/opt/airflow/data/rejected/<table>_dropped_duplicates_<id>.csv` |

---

## 11. Running the Pipeline

### Trigger a Full Run (All 10 Tables)
1. Open http://localhost:8081
2. Navigate to **DAGs**
3. Click the Play button next to `master_trigger_dag`
4. Click **Trigger DAG**
5. Watch the Master DAG expand into 10 Worker DAG runs

### What You Will See
- `master_trigger_dag` -- 1 run, 11 recent tasks (1 fetch + 10 triggers)
- `universal_etl_dag` -- 10 concurrent runs, each with 5 tasks
- Email alerts arrive for each successful table load
- Failed tables receive a failure email with full traceback

### Monitor Pipeline Health in Snowflake

```sql
-- All runs, latest first
SELECT CONFIG_ID, TABLE_NAME, TABLE_LOAD_STATUS, INSERTION_ROWCOUNT,
       TABLE_LOAD_MESSAGE, IMPORT_STARTTS, IMPORT_COMPLETETS
FROM CUSTOMER_PIPELINE_DB.ETL.ETL_LOG
ORDER BY CREATED_AT DESC;

-- Only failures
SELECT CONFIG_ID, TABLE_NAME, TABLE_LOAD_STATUS, TABLE_LOAD_MESSAGE, CREATED_AT
FROM CUSTOMER_PIPELINE_DB.ETL.ETL_LOG
WHERE TABLE_LOAD_STATUS = 'FAILED'
ORDER BY CREATED_AT DESC;
```

### Add a New Pipeline (No Code Required)
Simply insert a row into `SOURCE_CONFIG` and create the corresponding RAW table:

```sql
INSERT INTO CUSTOMER_PIPELINE_DB.ETL.SOURCE_CONFIG (
    CONFIG_ID, VENDOR, EXTRACT_LOCATION, GD_LOCATION, SCHEMA_NAME,
    TABLE_NAME, FILE_NAME_PATTERN, FILE_FORMAT, DATA_DELIMITER,
    COLUMN_LIST, NULLABLE_COLUMNS, IS_ACTIVE
) VALUES (
    11, 'GOOGLE_DRIVE', '<file_id>', '<full_url>',
    'RAW', 'NEW_TABLE', 'new_table.csv', 'CSV', ',',
    'col1,col2,col3', '', TRUE
);
```

Then trigger `master_trigger_dag`. The new pipeline runs automatically with no code changes.

---

## 12. Technology Stack

| Category | Technology | Version / Notes |
|---|---|---|
| **Orchestration** | Apache Airflow | 2.9.3 |
| **Runtime** | Docker + Docker Compose | LocalExecutor + Postgres backend |
| **Language** | Python | 3.11 |
| **Data Warehouse** | Snowflake | Cloud (ap-southeast-1) |
| **Data Processing** | Pandas | >= 2.0.0 |
| **Snowflake Connector** | snowflake-connector-python[pandas] | >= 3.0.0 (includes PyArrow) |
| **Airflow Snowflake Provider** | apache-airflow-providers-snowflake | >= 5.1.0 |
| **Source Storage** | Google Drive | Via Drive API v3 |
| **Google Auth** | google-api-python-client + google-auth | Service Account JSON key |
| **XLSX Support** | openpyxl | >= 3.1.0 |
| **Metadata Store** | Airflow Postgres | v15 |
| **Alerting** | Airflow SMTP Email | Gmail App Password |

---

*Last updated: August 2026*
