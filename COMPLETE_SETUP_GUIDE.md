# Metadata-Driven ETL Pipeline — Complete Setup Guide

This document is the **single source of truth** for setting up this project
from zero to a fully running pipeline. Follow every section in order, top
to bottom. Nothing is skipped.

---

## What This Project Does

This is a **metadata-driven ETL (Extract → Transform → Load) pipeline** built
on Apache Airflow, Google Drive, and Snowflake.

Every night (or on demand), Airflow automatically:
1. **Reads** configuration from a Snowflake table (`SOURCE_CONFIG`) to know which files to process.
2. **Downloads** raw CSV / XLSX / TXT files from Google Drive.
3. **Validates** that the files have the correct columns and are not empty.
4. **Transforms** the data — drops duplicates and null rows, saves rejected records separately for auditing.
5. **Loads** the clean data into Snowflake raw tables.
6. **Logs** every run into `ETL_LOG` with row counts, timestamps, and status.
7. **Emails** you a success or failure alert automatically.

---

## Architecture Overview

```
Google Drive (10 CSV/XLSX/TXT files)
        │
        │  (Service Account credentials)
        ▼
  Apache Airflow (Docker on your Mac)
        │  dag_google_drive_dynamic.py
        │  ├── fetch_metadata  (reads config from Snowflake)
        │  ├── download_file   (pulls file from Drive)
        │  ├── validate_file   (checks columns, row count)
        │  ├── transform_file  (drops dupes + nulls)
        │  └── load_to_snowflake (bulk inserts clean data)
        │
        ▼
  Snowflake
        ├── CUSTOMER_PIPELINE_DB.RAW.*  (10 destination tables)
        └── CUSTOMER_PIPELINE_DB.ETL.ETL_LOG  (audit trail)
```

---

## PART 1 — Google Drive Setup

### Step 1.1 — Create a Google Cloud Project

1. Go to **https://console.cloud.google.com/**.
2. Click the project dropdown at the top → **New Project**.
3. Name it `airflow-pipeline` → Click **Create**.
4. Make sure this new project is selected before proceeding.

### Step 1.2 — Enable the Google Drive API

1. In the top search bar, type **"Google Drive API"**.
2. Click the result → Click the blue **Enable** button.
3. Wait a few seconds.

### Step 1.3 — Create the Service Account (the "Robot")

1. Left sidebar: **IAM & Admin → Service Accounts**.
2. Click **+ Create Service Account**.
3. **Name:** `airflow-drive-reader` → Click **Create and Continue**.
4. **Step 2 (Permissions):** Leave completely empty → Click **Continue**.
5. **Step 3 (Principals):** Leave completely empty → Click **Done**.

> Do NOT add any roles in Step 2. Drive access is granted separately in Step 1.5.

### Step 1.4 — Download the JSON Key File

1. Click on the service account you just created.
2. Go to the **Keys** tab.
3. Click **Add Key → Create new key → JSON → Create**.
4. A `.json` file downloads to your Mac automatically.

**NEVER commit this file to git. NEVER share it.**

**Rename and place the file:**
1. Rename it to exactly: `gdrive_service_account.json`
   - Watch for double extensions (`.json.json`) — this happened to us. Check carefully.
2. Move it to: `airflow_customer_pipeline/config/gdrive_service_account.json`

**Find the robot email address (needed for Step 1.5):**
1. Open the JSON file in VS Code.
2. Find `"client_email"`. It looks like:
   `airflow-drive-reader@airflow-pipeline-505504.iam.gserviceaccount.com`
3. Copy this value.

### Step 1.5 — Share Each Google Drive File with the Robot

For EACH of your 10 source files on Google Drive:
1. Right-click the file → **Share**.
2. Paste the `client_email` address from Step 1.4.
3. Set permission to **Viewer** (not Editor).
4. Uncheck "Notify people".
5. Click **Share**.

> Even if your file is "Anyone with the link," service accounts are NOT included
> in that. The robot must be explicitly listed as a collaborator.

### Step 1.6 — Get Each File's Drive ID

1. Right-click the file → **Get link**.
2. The link looks like:
   `https://drive.google.com/file/d/1kALm-0ALQGv59jYSBksRw-LD_TOdrofh/view?usp=sharing`
3. The **File ID** is between `/d/` and `/view`:
   `1kALm-0ALQGv59jYSBksRw-LD_TOdrofh`
4. Note down the File ID for each of your 10 files. These go into `SOURCE_CONFIG` in Part 2.

---

## PART 2 — Snowflake Setup

### Step 2.1 — Create the Airflow Service User and Role

Run this in Snowflake **as ACCOUNTADMIN**:

```sql
-- Create a dedicated role for Airflow
CREATE ROLE IF NOT EXISTS AIRFLOW_LOADER_ROLE;

-- Create the service user Airflow logs in as
CREATE USER IF NOT EXISTS AIRFLOW_SERVICE_USER
    PASSWORD = 'YourStrongPasswordHere'
    DEFAULT_ROLE = AIRFLOW_LOADER_ROLE
    MUST_CHANGE_PASSWORD = FALSE;

-- Grant role to user and give warehouse access
GRANT ROLE AIRFLOW_LOADER_ROLE TO USER AIRFLOW_SERVICE_USER;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE AIRFLOW_LOADER_ROLE;
```

> Save the password — you will need it when creating the Airflow connection in Part 4.

### Step 2.2 — Run the Database Setup Script

Run `snowflake_ddls/01_setup_database.sql` in Snowflake.

This creates:
- `CUSTOMER_PIPELINE_DB` database with `RAW` and `ETL` schemas
- `ETL_LOG` table (one row per pipeline run — the audit trail)
- `SOURCE_CONFIG` table (the metadata table that drives every DAG)
- Grants to `AIRFLOW_LOADER_ROLE`

### Step 2.3 — Run the Raw Tables Script

Run `snowflake_ddls/02_setup_raw_tables.sql`.

This creates 10 destination tables in `RAW` schema:

| Table | Source File | Optional Columns |
|---|---|---|
| `RAW.CUSTOMER` | CSV | none |
| `RAW.ORDERS` | CSV | none |
| `RAW.ORDER_ITEMS` | CSV | none |
| `RAW.PAYMENTS` | CSV | none |
| `RAW.PRODUCTS` | CSV | none |
| `RAW.EMPLOYEES` | XLSX | `manager_id`, `salary` |
| `RAW.RETURNS` | TXT (pipe `\|`) | none |
| `RAW.REVIEWS` | CSV | `review_text` |
| `RAW.SHIPMENTS` | XLSX | `carrier`, `delivery_date`, `shipping_cost` |
| `RAW.SUPPLIERS` | CSV | `contact_email`, `contact_phone` |

Every table also has `LOADED_AT` (when inserted) and `SOURCE_RUN_ID` (Airflow run ID) added automatically.

### Step 2.4 — Populate SOURCE_CONFIG

Run `snowflake_ddls/03_setup_pipeline_configs.sql`.

This inserts 10 configuration rows. Each row tells Airflow everything about one source file:
- The Google Drive File ID (`EXTRACT_LOCATION`)
- The destination table name (`TABLE_NAME`)
- Expected column list (`COLUMN_LIST`)
- Which columns may be null (`NULLABLE_COLUMNS`)
- The file format (`CSV`, `XLSX`, or `TXT`)
- The delimiter (`','` or `'|'`)

> **To add a new table in the future:** INSERT a new row into `SOURCE_CONFIG` with
> `IS_ACTIVE = TRUE`. Within 5 minutes a new DAG will automatically appear in
> the Airflow UI. No Python changes needed.

---

## PART 3 — Docker and Airflow Setup

### Step 3.1 — Prerequisites

Install on your Mac:
- **Docker Desktop**: https://www.docker.com/products/docker-desktop/

### Step 3.2 — Clone the Repository

```bash
git clone https://github.com/NaveedBhat/Metadata-Driven-ETL-Framework.git
cd airflow_customer_pipeline
```

### Step 3.3 — Configure the .env File

The `.env` file is already in the project. Open it and fill in your SMTP settings:

```
AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
AIRFLOW__SMTP__SMTP_STARTTLS=True
AIRFLOW__SMTP__SMTP_SSL=False
AIRFLOW__SMTP__SMTP_USER=your@gmail.com
AIRFLOW__SMTP__SMTP_PASSWORD=xxxx xxxx xxxx xxxx   # 16-char Gmail App Password
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_MAIL_FROM=your@gmail.com
```

> The `SMTP_PASSWORD` must be a Gmail App Password, NOT your normal password.
> Create one at: https://myaccount.google.com/apppasswords

### Step 3.4 — Place the Google Drive Key File

Confirm the service account key is here:
```
airflow_customer_pipeline/config/gdrive_service_account.json
```
The `docker-compose.yaml` mounts `config/` into the container at `/opt/airflow/config/`.

### Step 3.5 — Start Airflow

Run in order in your terminal:

```bash
# Step 1: Initialize the Airflow metadata database (first time only)
docker compose up airflow-init
# Wait for "User airflow created with role Admin", then Ctrl+C

# Step 2: Start all services in the background
docker compose up -d

# Step 3: Confirm everything is running
docker compose ps
```

### Step 3.6 — Open the Airflow UI

Open: **http://localhost:8081**
- Username: `airflow`
- Password: `airflow`

The DAG list will be empty until you add the Snowflake connection in Part 4.

---

## PART 4 — Connect Airflow to Snowflake

### Step 4.1 — Add the Connection in the Airflow UI

1. Go to **Admin → Connections → +** (add new).
2. Fill in the fields exactly as below:

| Field | Value |
|---|---|
| **Connection Id** | `snowflake_customer_pipeline` |
| **Connection Type** | `Snowflake` |
| **Schema** | `ETL` |
| **Login** | `AIRFLOW_SERVICE_USER` |
| **Password** | *(your password from Step 2.1)* |
| **Account** | `YTJMFGP-VF07092` |
| **Warehouse** | `COMPUTE_WH` |
| **Database** | `CUSTOMER_PIPELINE_DB` |
| **Role** | `AIRFLOW_LOADER_ROLE` |
| **Region** | *(leave blank)* |
| **Extra** | *(leave blank)* |

3. Click **Save**.

> **How to find your Account identifier:**
> Log into Snowflake → click your name in the bottom-left → "Account" →
> "Copy Account Identifier". It looks like `YTJMFGP-VF07092`.

### Step 4.2 — Confirm DAGs Appear

Within 30 seconds of saving the connection, the Airflow Scheduler will:
1. Re-read `dag_google_drive_dynamic.py`
2. Connect to Snowflake
3. Query `SOURCE_CONFIG` — find 10 active rows
4. Auto-generate 10 independent DAGs

You should see:
```
google_drive_customer_dag
google_drive_employees_dag
google_drive_order_items_dag
google_drive_orders_dag
google_drive_payments_dag
google_drive_products_dag
google_drive_returns_dag
google_drive_reviews_dag
google_drive_shipments_dag
google_drive_suppliers_dag
```

---

## PART 5 — Set Airflow Variables

Go to **Admin → Variables → +** to add:

| Key | Value | Required? |
|---|---|---|
| `customer_pipeline_alert_email` | `your@gmail.com` | **YES** |
| `gdrive_service_account_file` | `/opt/airflow/config/gdrive_service_account.json` | Optional (already default) |

> Only `customer_pipeline_alert_email` must be set manually. All other variables
> have correct defaults built into `config.py` for this Docker environment.

---

## PART 6 — Running the Pipeline

### Trigger a DAG Manually

1. Click the **toggle switch** on the left of a DAG to unpause it (turns blue).
2. Click the **▶** button on the right to trigger it now.
3. Click the DAG name → **Grid** view to watch tasks run in real time.

### What Each Task Does

| Task | What happens |
|---|---|
| `fetch_metadata` | Reads the full config row from `SOURCE_CONFIG` and stores it for downstream tasks via XCom |
| `download_file` | Downloads the file from Google Drive. Handles CSV, TXT, and XLSX (XLSX is converted to CSV) |
| `validate_file` | Checks all expected columns exist and the file has at least 1 row |
| `transform_file` | Drops duplicate rows and rows with nulls in mandatory columns. Saves rejected rows to `data/rejected/` |
| `load_to_snowflake` | Bulk-inserts clean rows into the RAW table. Writes run result to `ETL_LOG` |

### On Success
- Data is in `CUSTOMER_PIPELINE_DB.RAW.<TABLE_NAME>`
- A success email arrives with the row count
- A `SUCCESS` row is inserted in `ETL_LOG`

### On Failure
- A failure email arrives with the full Python traceback
- A `FAILED` row is inserted in `ETL_LOG` with the error message
- Airflow retries automatically up to 2 times (5-minute wait between retries)

---

## PART 7 — Full "Start From Scratch" Checklist

**Snowflake (one time):**
- [ ] `AIRFLOW_LOADER_ROLE` created
- [ ] `AIRFLOW_SERVICE_USER` created with password
- [ ] `01_setup_database.sql` executed
- [ ] `02_setup_raw_tables.sql` executed
- [ ] `03_setup_pipeline_configs.sql` executed

**Google Drive (one time):**
- [ ] Google Cloud project created
- [ ] Google Drive API enabled
- [ ] Service account `airflow-drive-reader` created (no roles)
- [ ] JSON key downloaded → renamed to `gdrive_service_account.json` → placed in `config/`
- [ ] All 10 Drive files shared with the service account `client_email` as Viewer

**Airflow / Docker (every time you start fresh):**
- [ ] `docker compose up airflow-init` (first time only)
- [ ] `docker compose up -d`
- [ ] http://localhost:8081 opens (user: `airflow`, pass: `airflow`)
- [ ] **Admin → Connections** → `snowflake_customer_pipeline` added (see Part 4)
- [ ] **Admin → Variables** → `customer_pipeline_alert_email` added
- [ ] 10 DAGs appear in the DAG list

---

## PART 8 — Troubleshooting

| Problem | Cause | Fix |
|---|---|---|
| DAG list empty after setup | Snowflake connection missing or wrong | Edit `snowflake_customer_pipeline` in Admin → Connections |
| `Role ACCOUNTADMIN is not granted to this user` | Wrong Role in connection | Change Role field to `AIRFLOW_LOADER_ROLE` |
| `download_file` fails with "File not found" | File not shared with service account | Re-share the file on Google Drive with the service account `client_email` as Viewer |
| `validate_file` fails with "Missing expected columns" | Column names in file don't match `COLUMN_LIST` | Check actual CSV headers vs `COLUMN_LIST` in `SOURCE_CONFIG` |
| Bounce email from `you@example.com` | `customer_pipeline_alert_email` Variable missing | Admin → Variables → add `customer_pipeline_alert_email` |
| `gdrive_service_account.json` not found | File not in `config/` or has double extension | Check `config/` folder; filename must be exactly `gdrive_service_account.json` |
| DAGs were working, now 0 DAGs | Snowflake connection lost or password changed | Edit the connection and update the password |

---

## PART 9 — Key Files Reference

| File | Purpose |
|---|---|
| `docker-compose.yaml` | Defines the 4 Docker containers: postgres, init, webserver, scheduler |
| `.env` | SMTP email secrets (never committed to git) |
| `config/gdrive_service_account.json` | Google Drive robot key (never committed to git) |
| `dags/dag_google_drive_dynamic.py` | Dynamic DAG factory — generates one DAG per SOURCE_CONFIG row |
| `dags/config.py` | Central config — connection IDs, paths, email list |
| `dags/alerts.py` | Email success and failure callbacks |
| `scripts/etl_tasks.py` | All ETL logic — fetch, download, validate, transform, load |
| `snowflake_ddls/01_setup_database.sql` | Creates database, schemas, ETL_LOG, SOURCE_CONFIG |
| `snowflake_ddls/02_setup_raw_tables.sql` | Creates the 10 destination RAW tables |
| `snowflake_ddls/03_setup_pipeline_configs.sql` | Populates SOURCE_CONFIG with the 10 active pipeline rows |
