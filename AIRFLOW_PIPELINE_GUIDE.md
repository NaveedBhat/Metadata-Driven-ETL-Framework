# Airflow Pipeline Guide — Metadata-Driven ETL Framework

This document explains every file in this project, what it does, and
walks through the entire setup process from scratch.

---

## 1. What This Pipeline Does

10 vendor data files (CSV, TXT, XLSX) are stored on Google Drive.
Once triggered, Airflow downloads each file, validates its schema,
applies data quality transformations, and loads the clean data into a
Snowflake data warehouse — all 10 tables running concurrently, in a
single trigger.

The entire system is controlled by a configuration table in Snowflake
(`SOURCE_CONFIG`). Adding a new data source requires only a SQL INSERT
— no Python changes.

---

## 2. The Two-DAG Design

```
master_trigger_dag                    universal_etl_dag
+-------------------------------+     +--------------------------------+
| 1. fetch_active_configs       |     | 1. fetch_metadata              |
|    (reads SOURCE_CONFIG)      | --> | 2. download_file               |
| 2. trigger_universal_etl_dag  |     | 3. validate_file               |
|    (10x, Dynamic Task Mapping)|     | 4. transform_file              |
+-------------------------------+     | 5. load_to_snowflake           |
                                      +--------------------------------+
```

**Why two DAGs instead of one?**
- **Separation of concerns:** The Master only discovers and delegates
  work. The Worker only does work. Neither knows about the other's internals.
- **Scalability:** One Worker DAG handles any table. Adding a new source
  does not change either DAG file.
- **Failure isolation:** If SHIPMENTS fails, the other 9 pipelines are
  completely unaffected. Each Worker is an independent DAG run.

**The hand-off mechanism:** `TriggerDagRunOperator` with Dynamic Task
Mapping (`expand_kwargs`). The Master reads the list of active config IDs
from Snowflake and spawns exactly that many Worker instances in parallel,
passing `config_id` via `dag_run.conf`.

---

## 3. Every File Explained

### `dags/config.py`
Central configuration — the only place global settings are defined.
Nothing else in the project hard-codes a path, credential, or table name.

Key settings:
- `SNOWFLAKE_CONN_ID` — Airflow Connection ID for Snowflake
- `SNOWFLAKE_DATABASE`, `SNOWFLAKE_ETL_SCHEMA`, `SNOWFLAKE_LOG_TABLE`
- `SOURCE_CONFIG_DATABASE`, `SOURCE_CONFIG_SCHEMA`, `SOURCE_CONFIG_TABLE`
- `DATA_ROOT`, `RAW_DIR`, `PROCESSED_DIR`, `REJECTED_DIR`
- Helper functions `get_raw_file_path()`, `get_clean_file_path()`,
  `get_rejected_nulls_path()`, `get_rejected_dupes_path()`
- `ALERT_EMAIL_TO` — read from Airflow Variable at runtime

All real values are read from **Airflow Variables** at runtime, with
safe fallback defaults so the file never crashes if a variable is unset.

### `dags/alerts.py`
Two shared email callbacks used by both DAGs, so email logic is
maintained in exactly one place:

- **`task_failure_alert(context)`** — attached via `default_args`
  `on_failure_callback`. Fires automatically for any task failure in
  either DAG. Contains the full Python traceback (via
  `traceback.format_exception`, not just the last error line), the
  Airflow task log URL, and all values HTML-escaped for safe rendering.

- **`dag_success_alert(context, summary_lines)`** — called explicitly
  from `load_to_snowflake`'s `on_success_callback`. Includes table
  name, inserted row count, config ID, and logical date.

### `dags/dag_0_master_trigger.py`
The Master DAG. Two tasks:

1. **`fetch_active_configs()`** — `@task` decorated. Connects to
   Snowflake via `SnowflakeHook`, queries `SOURCE_CONFIG WHERE IS_ACTIVE = TRUE`,
   returns a list of `{"conf": {"config_id": N}}` dicts.

2. **`trigger_universal_etl_dag`** — `TriggerDagRunOperator.partial(...).expand_kwargs(...)`.
   Spawns one Worker run per config dict returned by step 1. Does not wait
   for workers to finish (`wait_for_completion=False`).

Key settings: `schedule=None` (manual-only, prevents auto-runs on Docker
restart), `max_active_runs=1` (prevents overlapping master runs).

### `dags/dag_universal_etl.py`
The Worker DAG. Five tasks in sequence:

**`fetch_metadata`**
- Reads `config_id` from `dag_run.conf`
- Queries full `SOURCE_CONFIG` row for that ID
- Raises `ValueError` if config not found or not active
- Pushes metadata dict to XCom

**`download_file`**
- Authenticates with Google Drive via Service Account JSON
- **CSV/TXT:** download as raw bytes, write to `data/extract/`
- **Native Google Sheet (XLSX):** export as CSV via Drive API
- **Uploaded XLSX binary:** download bytes, parse with `openpyxl`,
  write to CSV
- Raises `ValueError` if file is 0 bytes
- All output is a `.csv` file — downstream tasks are format-agnostic

**`validate_file`**
- Reads CSV with declared delimiter
- Checks `set(COLUMN_LIST) ⊆ set(df.columns)`
- Fails with descriptive `ValueError` listing missing columns if not satisfied
- Extra (undeclared) columns do NOT fail validation — stripped next step

**`transform_file`**
In this exact order:
1. Strip all columns not in `COLUMN_LIST` (removes XLSX ghost columns)
2. Detect and save duplicate rows to `rejected/`
3. `drop_duplicates()`
4. Detect rows with nulls in mandatory columns and save to `rejected/`
5. `dropna(subset=mandatory_cols)`
6. Write clean file to `data/processed/`
7. Push `row_count` to XCom

**`load_to_snowflake`**
1. Read clean CSV
2. Uppercase all column names
3. Second filter — keep only `COLUMN_LIST` columns (safety guard)
4. Add `SOURCE_RUN_ID` column
5. `write_pandas(conn, df, table, schema, database, quote_identifiers=False, use_logical_type=True)`
6. On success: `UPDATE ... SET LOADED_AT = CURRENT_TIMESTAMP() WHERE SOURCE_RUN_ID = %s`
7. In `finally`: INSERT one row into `ETL_LOG` (always runs)

### `snowflake_ddls/01_setup_database.sql`
Creates `CUSTOMER_PIPELINE_DB`, `RAW` and `ETL` schemas, the `ETL_LOG`
audit table, and the `SOURCE_CONFIG` configuration table. Applies
security grants to `AIRFLOW_LOADER_ROLE`. Run this first.

### `snowflake_ddls/02_setup_raw_tables.sql`
Creates all 10 RAW destination tables. Every ID column is `VARCHAR(50)`
(not `NUMBER`) to handle alphanumeric or corrupted vendor IDs. Every
table includes `LOADED_AT` and `SOURCE_RUN_ID` system columns.
Applies grants. Run this second.

### `snowflake_ddls/03_setup_pipeline_configs.sql`
Truncates and re-inserts all 10 `SOURCE_CONFIG` rows. Safe to re-run.
Contains the actual Google Drive File IDs, format specs, column lists,
and nullable column declarations for all 10 data sources. Run this third.

### `docker-compose.yaml`
Defines four containers:
- **`postgres`** — Airflow's metadata database (DAG runs, task states, variables)
- **`airflow-init`** — one-time setup: DB migration + admin user creation
- **`airflow-webserver`** — UI at `http://localhost:8081`
- **`airflow-scheduler`** — background process that executes DAGs

Important settings:
- Port `8081:8080` — Airflow's container-internal port is always `8080`
- `_PIP_ADDITIONAL_REQUIREMENTS` — installs all Python dependencies
  (`pandas`, `openpyxl`, `snowflake-connector-python[pandas]`,
  `google-api-python-client`, etc.) at container startup
- `schedule=None` on both DAGs prevents ghost runs on restart
- `AIRFLOW__CORE__DEFAULT_TIMEZONE: "Asia/Kolkata"` — local timezone

### `requirements.txt`
Documents the same Python packages installed via
`_PIP_ADDITIONAL_REQUIREMENTS` in `docker-compose.yaml`. Kept in sync
for reference and for any future direct `pip install` use.

### `.env` / `.env.example`
SMTP credentials for email alerting live in `.env` (never committed).
`.env.example` shows the required keys with placeholder values.
Gmail setup requires a 16-character App Password, not the normal password.

---

## 4. Full Setup Process

### Step 1 — Google Drive & Service Account
See **`GOOGLE_DRIVE_SETUP.md`** for the full walkthrough.
Short version:
1. Create a Google Cloud Service Account, download its JSON key
2. Enable the Google Drive API
3. Place the key at `config/gdrive_service_account.json`
4. Share all 10 source files/sheets with the service account email as Viewer

### Step 2 — Fill In `.env`
```bash
cp .env.example .env
# Edit .env: fill in SMTP_USER, SMTP_PASSWORD (16-char App Password),
# SMTP_MAIL_FROM. All three must refer to the same Gmail account.
```

### Step 3 — Set Up Snowflake
Run these in a Snowflake Worksheet, in order:
```
snowflake_ddls/01_setup_database.sql
snowflake_ddls/02_setup_raw_tables.sql
snowflake_ddls/03_setup_pipeline_configs.sql
```

Create the Airflow service login (as ACCOUNTADMIN):
```sql
CREATE ROLE IF NOT EXISTS AIRFLOW_LOADER_ROLE;
CREATE USER IF NOT EXISTS AIRFLOW_LOADER
    PASSWORD = '<strong_password>'
    DEFAULT_ROLE = AIRFLOW_LOADER_ROLE
    DEFAULT_WAREHOUSE = COMPUTE_WH;
GRANT ROLE AIRFLOW_LOADER_ROLE TO USER AIRFLOW_LOADER;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE AIRFLOW_LOADER_ROLE;
```

### Step 4 — Start Airflow
```bash
docker compose up -d
```
Wait ~30 seconds, then open http://localhost:8081 (`airflow` / `airflow`).

### Step 5 — Add Snowflake Connection
Admin -> Connections -> Add (`snowflake_customer_pipeline`):

| Field | Value |
|---|---|
| Conn Type | Snowflake |
| Account | your_account (e.g. abc123.ap-southeast-1) |
| Login | AIRFLOW_LOADER |
| Password | your_password |
| Database | CUSTOMER_PIPELINE_DB |
| Schema | RAW |
| Warehouse | COMPUTE_WH |
| Role | AIRFLOW_LOADER_ROLE |

### Step 6 — Add Airflow Variables
Admin -> Variables -> Add:

| Key | Value |
|---|---|
| `customer_pipeline_alert_email` | `your_email@gmail.com` |
| `gdrive_service_account_file` | `/opt/airflow/config/gdrive_service_account.json` |

### Step 7 — Trigger the Pipeline
Click ▶ next to `master_trigger_dag` -> **Trigger DAG**.

Watch the Master expand into 10 parallel Worker runs. All 10 tables
load simultaneously. Each sends a success email on completion.

---

## 5. Day-to-Day Operations

### Reading the Logs
Click any DAG -> click a run -> click a task box -> **Logs** tab.
Every task logs its exact actions including file paths, row counts,
and what was dropped.

### Monitoring in Snowflake
```sql
-- Latest pipeline runs
SELECT TABLE_NAME, TABLE_LOAD_STATUS, INSERTION_ROWCOUNT,
       TABLE_LOAD_MESSAGE, IMPORT_STARTTS, IMPORT_COMPLETETS
FROM CUSTOMER_PIPELINE_DB.ETL.ETL_LOG
ORDER BY CREATED_AT DESC;

-- Failed runs only
SELECT TABLE_NAME, TABLE_LOAD_STATUS, TABLE_LOAD_MESSAGE, CREATED_AT
FROM CUSTOMER_PIPELINE_DB.ETL.ETL_LOG
WHERE TABLE_LOAD_STATUS = 'FAILED'
ORDER BY CREATED_AT DESC;
```

### When a Task Fails
1. A `[FAILED]` email arrives immediately with the full traceback
2. The task turns red in the Airflow UI
3. It retries automatically (2 retries, 5 minutes apart)
4. If still failing: fix the root cause, then click **Clear** on the
   failed task in the Airflow UI to re-run it

---

## 6. Common Issues & Fixes

| Symptom | Cause | Fix |
|---|---|---|
| `ERR_CONNECTION_RESET` on localhost:8081 | Port mapped incorrectly | Container-side port must be `8080`, format is `8081:8080` |
| Webserver keeps restarting | Postgres not ready before webserver | `depends_on: condition: service_healthy` in compose |
| `download_file` fails — file not found | File not shared with service account | Re-share as Viewer with service account `client_email` |
| `Failed to cast variant value ... to FIXED` | Vendor sent non-numeric value for an ID column | ID columns are now `VARCHAR(50)` — re-run `02_setup_raw_tables.sql` in Snowflake |
| `unexpected ':'` syntax error in Snowflake | XLSX had unnamed trailing columns (`Unnamed: 7`) | Extra column stripping is now done in `transform_file` automatically |
| Email fails to send | `SMTP_USER` and `SMTP_MAIL_FROM` are different accounts | Both must match the Gmail account the App Password was generated for |
| `validate_file` fails — missing columns | Column name mismatch between file and SOURCE_CONFIG | Check actual file headers vs COLUMN_LIST in SOURCE_CONFIG |

---

For the complete column-by-column technical reference, see **`full_detail.md`**.
