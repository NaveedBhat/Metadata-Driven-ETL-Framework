# Metadata-Driven Airflow Pipeline — Architecture Overview

This document explains the end-to-end data flow of the scalable,
metadata-driven Airflow & Snowflake ETL framework.

---

## The Core Concept

Instead of writing a separate Airflow DAG for every file we want to ingest
(e.g., a `customers_dag.py`, an `orders_dag.py`, a `shipments_dag.py`...),
this project has a **single, universal Worker DAG** that can process any table.

It knows what to process by reading a configuration row from Snowflake:
`CUSTOMER_PIPELINE_DB.ETL.SOURCE_CONFIG`.

This architecture uses the **Master-Worker Pattern**:
1. **Master DAG** (`dag_0_master_trigger.py`): Discovers what work needs to be done.
2. **Worker DAG** (`dag_universal_etl.py`): Does the actual work for one table.

---

## Step-by-Step Flow

### Step 1: The Master DAG Is Triggered
- **File:** `dag_0_master_trigger.py`
- **When:** Manual trigger (schedule is set to `None` to prevent automatic
  re-runs on Docker restart).
- **Action:** Connects to Snowflake and queries for all active pipeline configs:
  ```sql
  SELECT CONFIG_ID, TABLE_NAME
  FROM CUSTOMER_PIPELINE_DB.ETL.SOURCE_CONFIG
  WHERE IS_ACTIVE = TRUE
  ```
- **Result:** Discovers 10 active config IDs (one per table).

### Step 2: The Master Spawns 10 Workers in Parallel
- **Mechanism:** Airflow Dynamic Task Mapping — `TriggerDagRunOperator.partial(...).expand_kwargs(...)`.
- The Master fan-out spawns **10 simultaneous, independent runs** of
  `universal_etl_dag`, each receiving its own `config_id` via `dag_run.conf`.
- The Master does not wait for the workers to finish (`wait_for_completion=False`).

### Step 3: Each Worker Fetches Its Instructions
- **Task:** `fetch_metadata`
- The worker reads its assigned `config_id` from `dag_run.conf` and queries
  `SOURCE_CONFIG` to retrieve its full configuration:
  - **Where is the file?** (`EXTRACT_LOCATION` = Google Drive File ID)
  - **What format?** (`FILE_FORMAT` = CSV, TXT, or XLSX; `DATA_DELIMITER`)
  - **What columns?** (`COLUMN_LIST` — the schema contract)
  - **Which columns may be null?** (`NULLABLE_COLUMNS`)
  - **Where does it go?** (`SCHEMA_NAME`, `TABLE_NAME`)
- Pushes full metadata dict to XCom for downstream tasks.

### Step 4: Download the Source File
- **Task:** `download_file`
- Authenticates with Google Drive using a Service Account JSON key.
- **CSV / TXT files:** downloaded as raw bytes.
- **XLSX files (native Google Sheet):** exported via the Drive API as CSV.
- **XLSX files (binary upload):** downloaded, parsed with `openpyxl`, written to CSV.
- All files are saved as `.csv` to `data/extract/` — downstream tasks are
  format-agnostic regardless of the original source format.

### Step 5: Validate the File's Schema
- **Task:** `validate_file`
- Opens the CSV and compares its column headers against `COLUMN_LIST`.
- **Fails immediately** (triggering a failure email) if any declared column
  is missing from the file — prevents silent bad data from reaching Snowflake.
- Extra undeclared columns are allowed at this stage; they are stripped in
  the next step.

### Step 6: Transform and Apply Data Quality Gates
- **Task:** `transform_file`
- Execution order is deliberate:
  1. **Strip extra columns first** — removes any columns not in `COLUMN_LIST`
     (e.g. empty `Unnamed: 7` columns from XLSX files).
  2. **Deduplicate** — exact duplicate rows are dropped and saved to
     `data/rejected/<table>_dropped_duplicates_<id>.csv`.
  3. **Null check** — rows with nulls in mandatory columns (those NOT in
     `NULLABLE_COLUMNS`) are dropped and saved to
     `data/rejected/<table>_dropped_nulls_<id>.csv`.
  4. Clean data is saved to `data/processed/<table>_clean_<id>.csv`.
- Row count is pushed to XCom for the ETL log.

### Step 7: Load Into Snowflake
- **Task:** `load_to_snowflake`
- Reads the clean CSV, uppercases column names, adds `SOURCE_RUN_ID`.
- Uses `write_pandas` with `use_logical_type=True` for type-safe DATE/TIME loading.
- After a successful insert, runs:
  ```sql
  UPDATE <table> SET LOADED_AT = CURRENT_TIMESTAMP()
  WHERE SOURCE_RUN_ID = '<run_id>'
  ```
  (Server-side timestamp avoids PyArrow serialization issues.)
- On success: fires `dag_success_alert` email with table name and row count.

### Step 8: Write the Audit Log
- **Task:** `load_to_snowflake` (in its `finally` block)
- **Always runs**, whether the load succeeded or failed.
- Inserts one row into `CUSTOMER_PIPELINE_DB.ETL.ETL_LOG` recording:
  - Start time, completion time, row count, load status, error message (if any),
    Airflow run ID, and logical date.

---

## Why This Is Production-Ready

| Feature | Implementation |
|---|---|
| **No code changes to scale** | Adding a new table = one SQL INSERT into SOURCE_CONFIG |
| **Concurrent execution** | All 10 pipelines run simultaneously via Dynamic Task Mapping |
| **Multi-format support** | CSV, TXT (pipe-delimited), XLSX all handled in one DAG |
| **Schema safety** | Extra vendor columns stripped before they can cause SQL errors |
| **Data quality audit** | Every rejected row is saved to a dated file, never silently lost |
| **Type-safe IDs** | All ID columns are VARCHAR(50) — handles corrupted vendor IDs |
| **Failure isolation** | One table failing does not affect the other 9 pipelines |
| **Full observability** | Every run logged in ETL_LOG + HTML email with traceback |

---

## Adding a New Source (Zero Python Code Required)

1. Upload the source file to Google Drive and share it with the service account.
2. Run a SQL INSERT into `SOURCE_CONFIG` with the file ID, table name, and column list.
3. Create the target RAW table in Snowflake.
4. Trigger `master_trigger_dag`.

The new pipeline runs automatically alongside all existing ones.

For the complete technical reference, see **`full_detail.md`**.
