# CUSTOMER_PIPELINE_DB — Snowflake Documentation

## Overview

This document describes the Snowflake data warehouse that powers the
Metadata-Driven Airflow ETL Framework. It covers the database layout,
all table schemas, the security model, and the SQL setup scripts.

**Account:** co68958 (ap-southeast-7.aws)
**Owner:** NAVEEDBHAT
**Role for setup:** ACCOUNTADMIN
**Warehouse:** COMPUTE_WH

---

## Database & Schema Layout

```
CUSTOMER_PIPELINE_DB
+-- RAW                     <- Landing zone for all ingested data (10 tables)
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
+-- ETL                     <- Pipeline control and observability
    +-- SOURCE_CONFIG       <- Configuration registry (one row = one pipeline)
    +-- ETL_LOG             <- Audit log (one row per pipeline run)
```

---

## ETL Schema Tables

### ETL.SOURCE_CONFIG

The "brain" of the pipeline. Every row registers a data source and tells
the Worker DAG exactly how to process it. The Master DAG reads this table
at runtime to determine what pipelines are active.

| Column | Type | Description |
|---|---|---|
| `CONFIG_ID` | NUMBER (autoincrement) | Unique pipeline identifier |
| `VENDOR` | VARCHAR(100) | Source system — always `GOOGLE_DRIVE` in this project |
| `EXTRACT_LOCATION` | VARCHAR(1000) | Google Drive File ID used to download |
| `GD_LOCATION` | VARCHAR(1000) | Full Google Drive URL (for audit) |
| `SCHEMA_NAME` | VARCHAR(100) | Target Snowflake schema (always `RAW`) |
| `TABLE_NAME` | VARCHAR(100) | Target Snowflake table name |
| `FILE_NAME_PATTERN` | VARCHAR(255) | Source filename (recorded in ETL_LOG) |
| `FILE_FORMAT` | VARCHAR(50) | `CSV`, `TXT`, or `XLSX` |
| `DATA_DELIMITER` | VARCHAR(10) | `,` for CSV/XLSX; `\|` for TXT |
| `COLUMN_LIST` | VARCHAR(2000) | Comma-separated list of expected columns (schema contract) |
| `NULLABLE_COLUMNS` | VARCHAR(2000) | Columns allowed to be NULL (blank = all mandatory) |
| `IS_ACTIVE` | BOOLEAN | `TRUE` = pipeline runs; `FALSE` = skipped by Master |
| `CREATED_AT` | TIMESTAMP | Auto-set at row creation |
| `CREATED_BY` | VARCHAR(100) | Auto-set to current Snowflake user |

**Active Configs (10 rows):**

| Config ID | Table | Format | Nullable |
|---|---|---|---|
| 1 | CUSTOMER | CSV | none |
| 2 | ORDERS | CSV | none |
| 3 | ORDER_ITEMS | CSV | none |
| 4 | PAYMENTS | CSV | none |
| 5 | PRODUCTS | CSV | none |
| 6 | EMPLOYEES | XLSX | manager_id, salary |
| 7 | RETURNS | TXT (pipe) | none |
| 8 | REVIEWS | CSV | review_text |
| 9 | SHIPMENTS | XLSX | carrier, delivery_date, shipping_cost |
| 10 | SUPPLIERS | CSV | contact_email, contact_phone |

---

### ETL.ETL_LOG

Audit trail. One row is appended per pipeline run, always — success or failure.
Never overwritten or deleted.

| Column | Description |
|---|---|
| `LOG_ID` | Auto-incrementing primary key |
| `CONFIG_ID` | Links back to SOURCE_CONFIG |
| `SOURCE_PATH` | Google Drive File ID that was processed |
| `GD_PATH` | Full Google Drive URL |
| `SOURCE_FILE_NAME` | File name pattern from SOURCE_CONFIG |
| `FILE_STATUS` | Overall status: `SUCCESS` or `FAILED` |
| `FILE_FORMAT` | `CSV`, `TXT`, or `XLSX` |
| `IMPORT_STARTTS` | Timestamp when load_to_snowflake task began |
| `IMPORT_COMPLETETS` | Timestamp when load completed (server-side) |
| `SOURCE_ROWCOUNT` | Row count from transform_file (via XCom) |
| `TABLE_NAME` | Destination RAW table |
| `TABLE_LOAD_STATUS` | `SUCCESS` or `FAILED` |
| `INSERTION_ROWCOUNT` | Rows actually written by write_pandas |
| `TABLE_LOAD_MESSAGE` | Error message if load failed (truncated to 1000 chars) |
| `TABLE_LOADTS` | Server-side timestamp of load completion |
| `CREATED_AT` | Server-side timestamp of log record creation |
| `RUNID` | Airflow `run_id` — unique per DAG run |
| `DATADATE` | Airflow logical date (`ds`) |

**Useful queries:**
```sql
-- Most recent runs
SELECT CONFIG_ID, TABLE_NAME, TABLE_LOAD_STATUS, INSERTION_ROWCOUNT,
       TABLE_LOAD_MESSAGE, IMPORT_STARTTS
FROM CUSTOMER_PIPELINE_DB.ETL.ETL_LOG
ORDER BY CREATED_AT DESC
LIMIT 20;

-- Failed runs only
SELECT TABLE_NAME, TABLE_LOAD_STATUS, TABLE_LOAD_MESSAGE, CREATED_AT
FROM CUSTOMER_PIPELINE_DB.ETL.ETL_LOG
WHERE TABLE_LOAD_STATUS = 'FAILED'
ORDER BY CREATED_AT DESC;
```

---

## RAW Schema Tables

All 10 RAW tables share the same structural conventions:

**ID columns are `VARCHAR(50)`, not `NUMBER`.** This is intentional.
Vendor data often contains alphanumeric IDs (e.g. `C1`, `ORD-123`), and
sometimes corrupted values. Using `VARCHAR(50)` ensures the pipeline
never crashes during load due to a casting error. Type enforcement can
be applied downstream in transformation layers.

**Every table has two system columns added by the pipeline:**
- `LOADED_AT TIMESTAMP_NTZ(9)` — set server-side via `UPDATE` after `write_pandas`
- `SOURCE_RUN_ID VARCHAR(255)` — Airflow `run_id`, links every row back to its `ETL_LOG` record

### RAW.CUSTOMER
| Column | Type |
|---|---|
| CUSTOMER_ID | VARCHAR(50) |
| NAME | VARCHAR(255) |
| CITY | VARCHAR(100) |
| COUNTRY | VARCHAR(100) |
| SIGNUP_DATE | DATE |
| EMAIL | VARCHAR(255) |
| PHONE | VARCHAR(50) |
| AGE | NUMBER |
| LOADED_AT | TIMESTAMP_NTZ(9) |
| SOURCE_RUN_ID | VARCHAR(255) |

### RAW.ORDERS
| Column | Type |
|---|---|
| ORDER_ID | VARCHAR(50) |
| CUSTOMER_ID | VARCHAR(50) |
| ORDER_DATE | DATE |
| STATUS | VARCHAR(50) |
| TOTAL_AMOUNT | NUMBER(12,2) |
| PRODUCT_NAME | VARCHAR(255) |
| CATEGORY | VARCHAR(100) |
| QUANTITY | NUMBER |
| DISCOUNT | NUMBER(12,2) |
| LOADED_AT | TIMESTAMP_NTZ(9) |
| SOURCE_RUN_ID | VARCHAR(255) |

### RAW.ORDER_ITEMS
| Column | Type |
|---|---|
| ORDER_ITEM_ID | VARCHAR(50) |
| ORDER_ID | VARCHAR(50) |
| PRODUCT_ID | VARCHAR(50) |
| QUANTITY | NUMBER |
| UNIT_PRICE | NUMBER(12,2) |
| LOADED_AT | TIMESTAMP_NTZ(9) |
| SOURCE_RUN_ID | VARCHAR(255) |

### RAW.PAYMENTS
| Column | Type |
|---|---|
| PAYMENT_ID | VARCHAR(50) |
| ORDER_ID | VARCHAR(50) |
| PAYMENT_METHOD | VARCHAR(50) |
| PAYMENT_DATE | DATE |
| AMOUNT | NUMBER(12,2) |
| PAYMENT_STATUS | VARCHAR(50) |
| LOADED_AT | TIMESTAMP_NTZ(9) |
| SOURCE_RUN_ID | VARCHAR(255) |

### RAW.PRODUCTS
| Column | Type |
|---|---|
| PRODUCT_ID | VARCHAR(50) |
| PRODUCT_NAME | VARCHAR(255) |
| CATEGORY | VARCHAR(100) |
| BRAND | VARCHAR(100) |
| PRICE | NUMBER(12,2) |
| STOCK_QUANTITY | NUMBER |
| SUPPLIER | VARCHAR(255) |
| LOADED_AT | TIMESTAMP_NTZ(9) |
| SOURCE_RUN_ID | VARCHAR(255) |

### RAW.EMPLOYEES
| Column | Type |
|---|---|
| EMPLOYEE_ID | VARCHAR(50) |
| NAME | VARCHAR(255) |
| DEPARTMENT | VARCHAR(100) |
| HIRE_DATE | DATE |
| EMAIL | VARCHAR(255) |
| MANAGER_ID | VARCHAR(50) |
| SALARY | NUMBER(12,2) |
| LOADED_AT | TIMESTAMP_NTZ(9) |
| SOURCE_RUN_ID | VARCHAR(255) |

### RAW.RETURNS
| Column | Type |
|---|---|
| RETURN_ID | VARCHAR(50) |
| ORDER_ID | VARCHAR(50) |
| CUSTOMER_ID | VARCHAR(50) |
| RETURN_DATE | DATE |
| REASON | VARCHAR(500) |
| REFUND_AMOUNT | NUMBER(12,2) |
| STATUS | VARCHAR(50) |
| LOADED_AT | TIMESTAMP_NTZ(9) |
| SOURCE_RUN_ID | VARCHAR(255) |

### RAW.REVIEWS
| Column | Type |
|---|---|
| REVIEW_ID | VARCHAR(50) |
| ORDER_ID | VARCHAR(50) |
| CUSTOMER_ID | VARCHAR(50) |
| RATING | NUMBER(3,1) |
| REVIEW_TEXT | VARCHAR(2000) |
| REVIEW_DATE | DATE |
| LOADED_AT | TIMESTAMP_NTZ(9) |
| SOURCE_RUN_ID | VARCHAR(255) |

### RAW.SHIPMENTS
| Column | Type |
|---|---|
| SHIPMENT_ID | VARCHAR(50) |
| ORDER_ID | VARCHAR(50) |
| CARRIER | VARCHAR(100) |
| SHIP_DATE | DATE |
| DELIVERY_DATE | DATE |
| TRACKING_NUMBER | VARCHAR(100) |
| SHIPPING_COST | NUMBER(10,2) |
| LOADED_AT | TIMESTAMP_NTZ(9) |
| SOURCE_RUN_ID | VARCHAR(255) |

### RAW.SUPPLIERS
| Column | Type |
|---|---|
| SUPPLIER_ID | VARCHAR(50) |
| SUPPLIER_NAME | VARCHAR(255) |
| CITY | VARCHAR(100) |
| COUNTRY | VARCHAR(100) |
| CONTACT_EMAIL | VARCHAR(255) |
| CONTACT_PHONE | VARCHAR(50) |
| LOADED_AT | TIMESTAMP_NTZ(9) |
| SOURCE_RUN_ID | VARCHAR(255) |

---

## Security & Access Control

### Role: AIRFLOW_LOADER_ROLE

The least-privilege role used by the Airflow service user. It can only
SELECT and INSERT — it cannot DELETE, UPDATE arbitrary rows, or DROP
any object.

| Permission | Scope |
|---|---|
| USAGE | CUSTOMER_PIPELINE_DB (database) |
| USAGE | CUSTOMER_PIPELINE_DB.RAW (schema) |
| USAGE | CUSTOMER_PIPELINE_DB.ETL (schema) |
| SELECT, INSERT, UPDATE, DELETE | All 10 RAW tables |
| SELECT, INSERT | ETL.ETL_LOG |
| SELECT | ETL.SOURCE_CONFIG |
| USAGE | COMPUTE_WH |

> Note: UPDATE and DELETE are granted on RAW tables to allow the
> `UPDATE ... SET LOADED_AT = CURRENT_TIMESTAMP()` call that runs
> after each `write_pandas`.

### Service User: AIRFLOW_LOADER

```sql
CREATE USER IF NOT EXISTS AIRFLOW_LOADER
    PASSWORD = '<strong_password>'
    DEFAULT_ROLE = AIRFLOW_LOADER_ROLE
    DEFAULT_WAREHOUSE = COMPUTE_WH
    DEFAULT_NAMESPACE = CUSTOMER_PIPELINE_DB.RAW
    MUST_CHANGE_PASSWORD = FALSE;
GRANT ROLE AIRFLOW_LOADER_ROLE TO USER AIRFLOW_LOADER;
```

This user's credentials are stored in the Airflow Connection
(`snowflake_customer_pipeline`) — never in any Python file.

---

## SQL Setup Scripts Reference

### `01_setup_database.sql` — Run First
Creates the database, schemas (`RAW`, `ETL`), the `ETL_LOG` table,
the `SOURCE_CONFIG` table, and the base security grants.

### `02_setup_raw_tables.sql` — Run Second
Creates all 10 RAW tables with `VARCHAR(50)` ID columns and grants
`SELECT, INSERT, UPDATE, DELETE` on each to `AIRFLOW_LOADER_ROLE`.
Safe to re-run — uses `CREATE OR REPLACE TABLE` (which clears data).

### `03_setup_pipeline_configs.sql` — Run Third
`TRUNCATE`s `SOURCE_CONFIG` and re-inserts all 10 config rows cleanly.
Safe to re-run — always produces a clean, known state.

---

## Current State

| Object | Location | Rows | Status |
|---|---|---|---|
| CUSTOMER | RAW | active | Loaded by pipeline |
| ORDERS | RAW | active | Loaded by pipeline |
| ORDER_ITEMS | RAW | active | Loaded by pipeline |
| PAYMENTS | RAW | active | Loaded by pipeline |
| PRODUCTS | RAW | active | Loaded by pipeline |
| EMPLOYEES | RAW | active | Loaded by pipeline |
| RETURNS | RAW | active | Loaded by pipeline |
| REVIEWS | RAW | active | Loaded by pipeline |
| SHIPMENTS | RAW | active | Loaded by pipeline |
| SUPPLIERS | RAW | active | Loaded by pipeline |
| SOURCE_CONFIG | ETL | 10 | Active (10 pipelines configured) |
| ETL_LOG | ETL | growing | Written after every pipeline run |
| AIRFLOW_LOADER | Account | — | Active service user |
| AIRFLOW_LOADER_ROLE | Account | — | Fully granted |

*Last updated: August 2026*
