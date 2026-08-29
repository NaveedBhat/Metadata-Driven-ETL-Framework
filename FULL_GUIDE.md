# Full Guide: Metadata-Driven Airflow & Snowflake ETL Framework

This guide explains from scratch exactly how this pipeline works, why it was
designed this way, and how to scale it to handle any number of data sources.

---

## 1. The Core Problem (Why Was This Built?)

In traditional data engineering, loading 10 different source files means
writing 10 completely separate Airflow DAGs. If you need to change how
logging works, you update 10 files. If you add 50 more sources, you write
50 more scripts. This is an unscalable maintenance problem.

**The Solution: A Metadata-Driven Architecture.**

Instead of hardcoding instructions in Python, this project builds a
**single, universal Worker DAG** that reads its instructions from a
database table (`SOURCE_CONFIG` in Snowflake) and configures itself
dynamically at runtime.

Result: 10 pipelines run from 2 Python files. Adding a new source
requires only a SQL INSERT — no Python changes whatsoever.

---

## 2. The Snowflake Infrastructure (The Brain)

The Snowflake database (`CUSTOMER_PIPELINE_DB`) holds the rules.
The setup is defined in three SQL files in `snowflake_ddls/`:

### `01_setup_database.sql`
Builds the foundation. Creates two schemas and two control tables:

- **`ETL.SOURCE_CONFIG`** — the configuration registry. Every row
  represents a pipeline. It stores the Google Drive file ID, the target
  table, the expected columns, which columns can be null, and whether
  the pipeline is active.

- **`ETL.ETL_LOG`** — the audit trail. Every time any pipeline runs
  (success or failure), Airflow appends one row recording the exact
  row counts, timestamps, load status, and error message.

### `02_setup_raw_tables.sql`
Creates all 10 RAW destination tables in Snowflake. Key design choice:
every ID column (`CUSTOMER_ID`, `ORDER_ID`, `SHIPMENT_ID`, etc.) is
defined as `VARCHAR(50)` — not `NUMBER`. This ensures corrupted,
alphanumeric, or prefixed vendor IDs never cause a casting error.

Every RAW table has two system columns added by the pipeline:
- `LOADED_AT TIMESTAMP_NTZ(9)` — set server-side by Snowflake after load
- `SOURCE_RUN_ID VARCHAR(255)` — the Airflow run ID linking every row
  back to its `ETL_LOG` entry

### `03_setup_pipeline_configs.sql`
Inserts all 10 `SOURCE_CONFIG` rows — one per table. This is what tells
the pipeline where each source file lives, what format it is, and what
columns to expect. The pipeline reads this at runtime; it is the only
configuration that needs to change if a source file moves or changes.

---

## 3. The Local File System (The Staging Ground)

Airflow uses `data/` as a local staging area while processing files.
`config.py` dynamically names all files using the table name and
`CONFIG_ID` so concurrent pipelines never collide:

| Directory | File pattern | Purpose |
|---|---|---|
| `data/extract/` | `<table>_raw_<id>.csv` | Raw file downloaded from Google Drive |
| `data/processed/` | `<table>_clean_<id>.csv` | Clean data, ready for Snowflake |
| `data/rejected/` | `<table>_dropped_nulls_<id>.csv` | Rows rejected for missing mandatory fields |
| `data/rejected/` | `<table>_dropped_duplicates_<id>.csv` | Rows rejected as exact duplicates |

---

## 4. The Two DAGs (The Muscle)

### `dag_0_master_trigger.py` — The Master

Triggered manually (schedule is `None`). Runs two tasks:

1. **`fetch_active_configs`**: Connects to Snowflake and queries
   `SELECT CONFIG_ID FROM SOURCE_CONFIG WHERE IS_ACTIVE = TRUE`.
   Currently returns 10 active configs.

2. **`trigger_universal_etl_dag`**: Uses Airflow Dynamic Task Mapping
   (`TriggerDagRunOperator.partial(...).expand_kwargs(...)`) to
   instantaneously spawn 10 concurrent runs of the Worker DAG, each
   receiving its own `config_id`. The Master does not wait for workers
   to finish.

### `dag_universal_etl.py` — The Worker

Receives one `config_id` and processes that table end-to-end in 5 tasks:

1. **`fetch_metadata`**: Reads the full `SOURCE_CONFIG` row for this
   `config_id` from Snowflake. Pushes it to XCom.

2. **`download_file`**: Downloads the source file from Google Drive.
   Supports CSV, TXT (any delimiter), and XLSX (both native Google
   Sheets and uploaded binary files). All output is written as `.csv`
   to `data/extract/` — downstream tasks are format-agnostic.

3. **`validate_file`**: Verifies the file's column headers against
   `COLUMN_LIST`. Fails immediately if any declared column is missing,
   preventing bad data from reaching Snowflake.

4. **`transform_file`**: Applies data quality gates in this exact order:
   - Strip extra/unnamed columns (critical for XLSX files)
   - Deduplicate rows; save rejected rows
   - Drop rows with nulls in mandatory columns; save rejected rows

5. **`load_to_snowflake`**: Loads the clean CSV into Snowflake using
   `write_pandas` (PyArrow-backed for type safety). Updates `LOADED_AT`
   server-side. Appends a record to `ETL_LOG` regardless of success or failure.

---

## 5. The Data Quality Gates (In Detail)

The pipeline has 6 layers of protection against bad vendor data:

| Layer | Location | What It Catches |
|---|---|---|
| 1. Schema validation | `validate_file` | Missing expected columns |
| 2. Extra column stripping | `transform_file` | Unnamed ghost columns from XLSX files |
| 3. Deduplication | `transform_file` | Exact duplicate rows |
| 4. Mandatory null check | `transform_file` | Nulls in non-nullable fields only |
| 5. Final column guard | `load_to_snowflake` | Secondary strip before write_pandas |
| 6. VARCHAR(50) IDs | Snowflake DDL | Corrupted/alphanumeric vendor IDs |

**Why column stripping must happen before deduplication:**
XLSX files from vendors can contain empty trailing columns auto-named
`Unnamed: 7` by Pandas. The colon (`:`) in that name causes a SQL
syntax error inside `write_pandas`. More importantly, if deduplication
ran first, it would consider `Unnamed: 7` a business column and
potentially miss real duplicates. Stripping first ensures all downstream
logic operates only on declared business columns.

---

## 6. Email Alerting

Defined in `dags/alerts.py`. Shared by both DAGs:

- **`task_failure_alert`**: Attached via `default_args["on_failure_callback"]`
  — fires for ANY task failure in either DAG automatically. Contains
  the full Python traceback (not just the last line) and a direct link
  to the Airflow task log.

- **`dag_success_alert`**: Attached to `load_to_snowflake`'s
  `on_success_callback`. Contains table name, row count, and config ID.

Both use Airflow's `send_email` and read SMTP credentials from `.env`.
All dynamic values are HTML-escaped before embedding in the email body.

---

## 7. How to Scale: Adding a New Source

Because of this architecture, adding a new data source takes under 2
minutes and requires **zero Python coding**:

```sql
-- Step 1: Register the new source in the Brain
INSERT INTO CUSTOMER_PIPELINE_DB.ETL.SOURCE_CONFIG (
    CONFIG_ID, VENDOR, EXTRACT_LOCATION, GD_LOCATION, SCHEMA_NAME,
    TABLE_NAME, FILE_NAME_PATTERN, FILE_FORMAT, DATA_DELIMITER,
    COLUMN_LIST, NULLABLE_COLUMNS, IS_ACTIVE
) VALUES (
    11, 'GOOGLE_DRIVE', '<drive_file_id>', '<full_url>',
    'RAW', 'INVENTORY', 'inventory_raw.csv', 'CSV', ',',
    'item_id,item_name,quantity,warehouse_id', '', TRUE
);

-- Step 2: Create the destination table
CREATE TABLE CUSTOMER_PIPELINE_DB.RAW.INVENTORY (
    ITEM_ID        VARCHAR(50),
    ITEM_NAME      VARCHAR(255),
    QUANTITY       NUMBER,
    WAREHOUSE_ID   VARCHAR(50),
    LOADED_AT      TIMESTAMP_NTZ(9),
    SOURCE_RUN_ID  VARCHAR(255)
);

-- Step 3: Grant access to the Airflow role
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE
    CUSTOMER_PIPELINE_DB.RAW.INVENTORY TO ROLE AIRFLOW_LOADER_ROLE;
```

Trigger `master_trigger_dag` — the new pipeline runs automatically
alongside all existing ones. No Airflow restart required.

---

## 8. The 10 Active Pipelines

| Config ID | Table | Source Format | Nullable Columns |
|---|---|---|---|
| 1 | CUSTOMER | CSV | none |
| 2 | ORDERS | CSV | none |
| 3 | ORDER_ITEMS | CSV | none |
| 4 | PAYMENTS | CSV | none |
| 5 | PRODUCTS | CSV | none |
| 6 | EMPLOYEES | XLSX | manager_id, salary |
| 7 | RETURNS | TXT (pipe `\|`) | none |
| 8 | REVIEWS | CSV | review_text |
| 9 | SHIPMENTS | XLSX | carrier, delivery_date, shipping_cost |
| 10 | SUPPLIERS | CSV | contact_email, contact_phone |

---

For the complete file-by-file and column-by-column reference, see
**`full_detail.md`**.
