# Metadata-Driven ETL Pipeline — Complete Setup Guide

> **Who is this document for?**
> This is written for a colleague who has never seen this project before.
> Follow every section in order, top to bottom. Nothing is skipped.
> By the end, you will have 10 automated DAGs running and loading data
> from Google Drive into Snowflake.

---

## Table of Contents

1. [What You Need Before You Start](#part-0--what-you-need-before-you-start)
2. [Understand the Project Structure](#part-1--understand-the-project-structure)
3. [Get the Code](#part-2--get-the-code)
4. [Google Drive Setup](#part-3--google-drive-setup)
5. [Snowflake Setup](#part-4--snowflake-setup)
6. [Update SOURCE_CONFIG with Your File IDs](#part-5--update-source_config-with-your-file-ids)
7. [Configure the Project Files](#part-6--configure-the-project-files)
8. [Start Apache Airflow](#part-7--start-apache-airflow)
9. [Connect Airflow to Snowflake](#part-8--connect-airflow-to-snowflake)
10. [Set Airflow Variables](#part-9--set-airflow-variables)
11. [Run the Pipeline](#part-10--run-the-pipeline)
12. [Verify the Results in Snowflake](#part-11--verify-the-results-in-snowflake)
13. [Schedule DAGs to Run Automatically](#part-12--schedule-dags-to-run-automatically)
14. [Add a New Data Source in the Future](#part-13--add-a-new-data-source-in-the-future)
15. [Daily Operations — Start and Stop](#part-14--daily-operations--start-and-stop)
16. [Full Checklist](#part-15--full-checklist)
17. [Troubleshooting](#part-16--troubleshooting)

---

## What This Project Does

This is a **metadata-driven ETL pipeline**. On a schedule (or on demand):

1. Airflow reads a configuration table in Snowflake (`SOURCE_CONFIG`) to
   know what files exist and where they live on Google Drive.
2. It downloads each file (CSV, XLSX, or TXT) from Google Drive.
3. It validates the file has the right columns and is not empty.
4. It cleans the data — drops duplicates and rows with missing required values.
5. It loads the clean data into Snowflake.
6. It sends you an email — success with row counts, or failure with the full error.

```
Google Drive (your source files)
        │
        │ — service account key (robot login)
        ▼
Apache Airflow (runs in Docker on your laptop)
        │
        │ — 10 auto-generated DAGs (one per table)
        ▼
Snowflake
        ├── CUSTOMER_PIPELINE_DB.RAW.CUSTOMER
        ├── CUSTOMER_PIPELINE_DB.RAW.ORDERS
        ├── ... (8 more tables)
        └── CUSTOMER_PIPELINE_DB.ETL.ETL_LOG  ← audit log of every run
```

---

## PART 0 — What You Need Before You Start

### Accounts You Must Have

| What | Why | Link |
|---|---|---|
| **Google account** | To create the Cloud project and access Drive | https://accounts.google.com |
| **Google Cloud account** | Free tier is enough — only used for the API | https://console.cloud.google.com |
| **Snowflake account** | The destination data warehouse | https://app.snowflake.com |
| **Gmail account** | For pipeline alert emails (can be same as Google account) | — |

### Software You Must Install

**1. Docker Desktop** — This is what runs Airflow on your Mac.
- Download: https://www.docker.com/products/docker-desktop/
- Install it, open it, and make sure the Docker icon appears in your Mac menu bar.
- After install, open Terminal and run:
  ```bash
  docker --version
  ```
  You should see something like `Docker version 24.x.x`. If you get
  "command not found", Docker is not installed correctly.

**2. Git** — Already installed on Mac.
- Confirm by running:
  ```bash
  git --version
  ```

### Information You Must Collect

Before starting, collect these pieces of information and keep them handy:

| Item | Where to find it | You will need it in... |
|---|---|---|
| Snowflake account identifier | Snowflake UI → bottom-left name → Account → Copy Account Identifier | Part 8 |
| Snowflake username | Your Snowflake login email or username | Part 4 |
| Gmail App Password (16 chars) | https://myaccount.google.com/apppasswords | Part 6 |

---

## PART 1 — Understand the Project Structure

After cloning (Part 2), your project folder will look like this:

```
airflow_customer_pipeline/
│
├── dags/                          ← Airflow reads this folder
│   ├── dag_google_drive_dynamic.py   ← THE main file — generates 10 DAGs automatically
│   ├── config.py                     ← Central settings (connection IDs, paths)
│   ├── alerts.py                     ← Email success/failure helpers
│   └── .airflowignore                ← Tells Airflow to ignore config.py and alerts.py
│
├── scripts/
│   └── etl_tasks.py               ← All the ETL logic (download, validate, transform, load)
│
├── snowflake_ddls/                ← Run these in Snowflake one time
│   ├── 01_setup_database.sql      ← Creates database, schemas, ETL_LOG, SOURCE_CONFIG
│   ├── 02_setup_raw_tables.sql    ← Creates the 10 destination RAW tables
│   └── 03_setup_pipeline_configs.sql  ← Populates SOURCE_CONFIG (YOU must update File IDs here)
│
├── config/
│   └── gdrive_service_account.json   ← YOU place this here (secret — never in git)
│
├── data/                          ← Airflow writes files here during each run
│   ├── extract/                   ← Raw files downloaded from Drive
│   ├── processed/                 ← Clean files ready for Snowflake
│   └── rejected/                  ← Rows dropped (duplicates, nulls) — for auditing
│
├── docker-compose.yaml            ← Defines all Docker containers
├── .env                           ← Your email secrets (never in git)
└── COMPLETE_SETUP_GUIDE.md        ← This file
```

**The most important concept:** The file `snowflake_ddls/03_setup_pipeline_configs.sql`
contains Google Drive File IDs that were specific to the original developer's
Google Drive. **You must replace them with your own File IDs.** This is covered
in Part 5.

---

## PART 2 — Get the Code

Open Terminal and run:

```bash
# Clone the repository
git clone https://github.com/NaveedBhat/Metadata-Driven-ETL-Framework.git

# Enter the project folder
cd airflow_customer_pipeline

# Switch to the production branch
git checkout feature/vendor-specific-master-dags

# Confirm you are on the right branch
git branch
# You should see:  * feature/vendor-specific-master-dags
```

---

## PART 3 — Google Drive Setup

> Airflow runs automatically with no human logged in. It needs a "robot"
> Google account (called a Service Account) that has its own credentials,
> so it can authenticate to Google Drive without prompting for a password.

### Step 3.1 — Create a Google Cloud Project

1. Go to **https://console.cloud.google.com/**.
2. Sign in with your Google account.
3. At the top of the page, click the project dropdown → **New Project**.
4. **Project name:** `airflow-pipeline` (or any name you like).
5. Click **Create**.
6. Wait a few seconds, then make sure the new project is selected in the
   top dropdown. If it is not, click the dropdown and select it.

---

### Step 3.2 — Enable the Google Drive API

Your project cannot talk to Google Drive until you explicitly enable the API.

1. In the top search bar, type **`Google Drive API`**.
2. Click the first result (it says "Google Drive API" with the Drive icon).
3. Click the blue **Enable** button.
4. Wait 5–10 seconds for it to activate. The page will refresh and show
   "API enabled".

---

### Step 3.3 — Create the Service Account

A service account is a robot identity — it has its own email address and
credentials that your code uses to authenticate automatically.

1. In the left sidebar, click **IAM & Admin → Service Accounts**.
2. Click **+ Create Service Account** at the top.
3. Fill in:
   - **Service account name:** `airflow-drive-reader`
   - **Service account ID:** This fills in automatically.
   - **Description:** `Robot account for Airflow to read Google Drive files`
4. Click **Create and Continue**.
5. On **Step 2 (Grant this service account access to project):**
   - Leave this completely empty. Do not select any role.
   - Click **Continue**.
6. On **Step 3 (Grant users access to this service account):**
   - Leave this completely empty.
   - Click **Done**.

You should see the service account appear in the list with a green success toast.

> **Why no roles in Step 2?** This service account only needs to read files
> from Google Drive. That permission is granted directly inside Google Drive
> (Step 3.5), not here. Adding Cloud roles here is unnecessary and not recommended.

---

### Step 3.4 — Download the JSON Key File

This key file is the service account's "password". Your code uses it to
prove to Google that it is allowed to log in as the robot.

1. Click on the service account name you just created in the list.
2. Click the **Keys** tab at the top.
3. Click **Add Key → Create new key**.
4. Select **JSON** (it should be selected by default).
5. Click **Create**.
6. A `.json` file automatically downloads to your Mac (usually to `~/Downloads`).

**⚠️ CRITICAL — This file is a secret:**
- NEVER commit it to Git.
- NEVER email it or share it publicly.
- NEVER put it anywhere except the project's `config/` folder.

**Rename and place the file:**

1. Find the downloaded file in your Downloads folder. It has a long name like
   `airflow-pipeline-505504-22e498a0fb90.json`.
2. Rename it to exactly: **`gdrive_service_account.json`**
   - ⚠️ Watch for double extensions! If your Mac hides extensions, you might
     accidentally end up with `gdrive_service_account.json.json`. To check:
     right-click the file → Get Info → look at the full name at the top.
     It must end with exactly ONE `.json`.
3. Move the renamed file into your project:
   ```
   airflow_customer_pipeline/config/gdrive_service_account.json
   ```

**Find the robot's email address (you need it for the next step):**

1. Open `gdrive_service_account.json` in VS Code or any text editor.
2. Find the line that says `"client_email"`. It looks like:
   ```
   "client_email": "airflow-drive-reader@airflow-pipeline-505504.iam.gserviceaccount.com"
   ```
3. Copy the full email address value. Keep it ready for Step 3.5.

---

### Step 3.5 — Upload Your Source Files to Google Drive

The pipeline reads 10 source files. Upload all 10 to Google Drive.

**Each file must have these exact column headers (case-sensitive):**

**1. customers_raw.csv** (CSV, comma-delimited)
```
customer_id,name,city,country,signup_date,email,phone,age
```

**2. orders_raw.csv** (CSV, comma-delimited)
```
order_id,customer_id,order_date,status,total_amount,product_name,category,quantity,discount
```

**3. order_items_raw.csv** (CSV, comma-delimited)
```
order_item_id,order_id,product_id,quantity,unit_price
```

**4. payments_raw.csv** (CSV, comma-delimited)
```
payment_id,order_id,payment_method,payment_date,amount,payment_status
```

**5. products_raw.csv** (CSV, comma-delimited)
```
product_id,product_name,category,brand,price,stock_quantity,supplier
```

**6. employees_raw.xlsx** (Excel file — `manager_id` and `salary` may be empty)
```
employee_id,name,department,hire_date,email,manager_id,salary
```

**7. returns_raw.txt** (TXT, pipe `|` delimited — NOT comma)
```
return_id|order_id|customer_id|return_date|reason|refund_amount|status
```

**8. reviews_raw.csv** (CSV, comma-delimited — `review_text` may be empty)
```
review_id,order_id,customer_id,rating,review_text,review_date
```

**9. shipments_raw.xlsx** (Excel — `carrier`, `delivery_date`, `shipping_cost` may be empty)
```
shipment_id,order_id,carrier,ship_date,delivery_date,tracking_number,shipping_cost
```

**10. suppliers_raw.csv** (CSV — `contact_email`, `contact_phone` may be empty)
```
supplier_id,supplier_name,city,country,contact_email,contact_phone
```

> ⚠️ Column names must match exactly. If your CSV has `Customer_ID` instead of
> `customer_id`, the pipeline will fail at the validation step.

---

### Step 3.6 — Share Each File with the Robot

You must explicitly share each of the 10 files with the service account.

For **EACH** of the 10 files on Google Drive:
1. Right-click the file → **Share** (or open it and click the Share button).
2. In the "Add people and groups" box, paste the `client_email` address you
   copied in Step 3.4.
3. Click the permission dropdown next to the email and set it to **Viewer**.
   (Not Editor — the pipeline only ever reads files, never writes to Drive.)
4. Uncheck the **"Notify people"** checkbox. The robot cannot receive email.
5. Click **Share**.

> ⚠️ Even if your file is set to "Anyone with the link can view," this does NOT
> include service accounts. You must add it explicitly, like a human collaborator.

Repeat this for all 10 files.

---

### Step 3.7 — Get the File ID for Each File

Airflow identifies files by their unique Drive ID, not by filename.

For **each** of your 10 files:
1. Right-click the file → **Get link** (or **Share → Copy link**).
2. The URL looks like:
   ```
   https://drive.google.com/file/d/1kALm-0ALQGv59jYSBksRw-LD_TOdrofh/view?usp=sharing
   ```
3. The **File ID** is the long string between `/d/` and `/view`:
   ```
   1kALm-0ALQGv59jYSBksRw-LD_TOdrofh
   ```
4. Record all 10 File IDs in a table like this:

| File | Your File ID |
|---|---|
| customers_raw.csv | _________________________ |
| orders_raw.csv | _________________________ |
| order_items_raw.csv | _________________________ |
| payments_raw.csv | _________________________ |
| products_raw.csv | _________________________ |
| employees_raw.xlsx | _________________________ |
| returns_raw.txt | _________________________ |
| reviews_raw.csv | _________________________ |
| shipments_raw.xlsx | _________________________ |
| suppliers_raw.csv | _________________________ |

You will need all 10 File IDs in Part 5.

---

## PART 4 — Snowflake Setup

### Step 4.1 — Find Your Snowflake Account Identifier

You need this later when setting up the Airflow connection.

1. Log into Snowflake: https://app.snowflake.com
2. In the **bottom-left corner**, click on your account name/organization.
3. Hover over **Account**.
4. Click **Copy Account Identifier**.
5. It will look like: `YTJMFGP-VF07092` (OrgName-AccountName with a hyphen).
6. Write it down — you will need it in Part 8.

Alternatively, you can also find it in:
**Settings → Account → General → Account Identifier** row.

---

### Step 4.2 — Create the Airflow Service User and Role

Open a **Snowflake Worksheet** and run the following SQL.
Make sure you are using the **ACCOUNTADMIN** role (top-right role dropdown).

```sql
-- ================================================================
-- Run this entire block as ACCOUNTADMIN
-- ================================================================

-- 1. Create a dedicated role for Airflow (least privilege principle)
CREATE ROLE IF NOT EXISTS AIRFLOW_LOADER_ROLE;

-- 2. Create the Airflow service user
--    IMPORTANT: Change 'YourStrongPasswordHere' to a real password.
--    Write it down — you will need it when setting up the connection in Part 8.
CREATE USER IF NOT EXISTS AIRFLOW_SERVICE_USER
    PASSWORD = 'YourStrongPasswordHere'
    DEFAULT_ROLE = AIRFLOW_LOADER_ROLE
    DEFAULT_WAREHOUSE = COMPUTE_WH
    MUST_CHANGE_PASSWORD = FALSE;

-- 3. Grant the role to the user
GRANT ROLE AIRFLOW_LOADER_ROLE TO USER AIRFLOW_SERVICE_USER;

-- 4. Give the role access to the warehouse (needed to run queries)
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE AIRFLOW_LOADER_ROLE;
```

> **Password rules:** Use a password at least 12 characters long with upper,
> lower, numbers, and a special character. Save it somewhere safe.

---

### Step 4.3 — Run the Database Setup Script

In your Snowflake worksheet, copy and paste the **entire contents** of
`snowflake_ddls/01_setup_database.sql` and run it.

This creates:
- `CUSTOMER_PIPELINE_DB` — the main database
- `CUSTOMER_PIPELINE_DB.RAW` — schema for the 10 destination tables
- `CUSTOMER_PIPELINE_DB.ETL` — schema for configuration and audit logging
- `ETL_LOG` table — one row written per pipeline run (the audit trail)
- `SOURCE_CONFIG` table — the master configuration table (drives all DAGs)
- All required GRANT statements for `AIRFLOW_LOADER_ROLE`

After running, verify it worked:
```sql
SHOW DATABASES LIKE 'CUSTOMER_PIPELINE_DB';
SHOW SCHEMAS IN DATABASE CUSTOMER_PIPELINE_DB;
```
You should see both `RAW` and `ETL` schemas.

---

### Step 4.4 — Run the Raw Tables Script

Copy and paste `snowflake_ddls/02_setup_raw_tables.sql` and run it.

This creates all 10 destination tables in `CUSTOMER_PIPELINE_DB.RAW`:

| Table | Columns |
|---|---|
| `CUSTOMER` | customer_id, name, city, country, signup_date, email, phone, age, LOADED_AT, SOURCE_RUN_ID |
| `ORDERS` | order_id, customer_id, order_date, status, total_amount, product_name, category, quantity, discount, LOADED_AT, SOURCE_RUN_ID |
| `ORDER_ITEMS` | order_item_id, order_id, product_id, quantity, unit_price, LOADED_AT, SOURCE_RUN_ID |
| `PAYMENTS` | payment_id, order_id, payment_method, payment_date, amount, payment_status, LOADED_AT, SOURCE_RUN_ID |
| `PRODUCTS` | product_id, product_name, category, brand, price, stock_quantity, supplier, LOADED_AT, SOURCE_RUN_ID |
| `EMPLOYEES` | employee_id, name, department, hire_date, email, manager_id, salary, LOADED_AT, SOURCE_RUN_ID |
| `RETURNS` | return_id, order_id, customer_id, return_date, reason, refund_amount, status, LOADED_AT, SOURCE_RUN_ID |
| `REVIEWS` | review_id, order_id, customer_id, rating, review_text, review_date, LOADED_AT, SOURCE_RUN_ID |
| `SHIPMENTS` | shipment_id, order_id, carrier, ship_date, delivery_date, tracking_number, shipping_cost, LOADED_AT, SOURCE_RUN_ID |
| `SUPPLIERS` | supplier_id, supplier_name, city, country, contact_email, contact_phone, LOADED_AT, SOURCE_RUN_ID |

> `LOADED_AT` and `SOURCE_RUN_ID` are added automatically by the pipeline to
> every row. You never need to provide them in your source files.

Verify:
```sql
SHOW TABLES IN SCHEMA CUSTOMER_PIPELINE_DB.RAW;
```
You should see all 10 tables.

---

## PART 5 — Update SOURCE_CONFIG with Your File IDs

> **This is the most important customization step.** The file
> `snowflake_ddls/03_setup_pipeline_configs.sql` has Google Drive File IDs
> from the original developer's Drive. You MUST replace them with your own.

### Step 5.1 — Edit the SQL File

Open `snowflake_ddls/03_setup_pipeline_configs.sql` in VS Code.

You will see 10 INSERT rows. For each row, find the `EXTRACT_LOCATION` value
(the 3rd column in the VALUES list). Replace each one with your File ID from
the table you filled in during Step 3.7.

Example — change this:
```sql
(1, 'GOOGLE_DRIVE',
 '1kALm-0ALQGv59jYSBksRw-LD_TOdrofh',       ← REPLACE THIS with your File ID
 'https://drive.google.com/file/d/...',       ← REPLACE THIS with your full URL
 'RAW', 'CUSTOMER', ...
```

To this:
```sql
(1, 'GOOGLE_DRIVE',
 'YOUR_CUSTOMERS_FILE_ID_HERE',
 'https://drive.google.com/file/d/YOUR_CUSTOMERS_FILE_ID_HERE/view',
 'RAW', 'CUSTOMER', ...
```

Do this for all 10 rows.

### Step 5.2 — Run the Updated Script

Once all 10 File IDs are updated, copy the full contents of the file and run
it in your Snowflake worksheet.

Verify it worked:
```sql
SELECT CONFIG_ID, TABLE_NAME, EXTRACT_LOCATION, IS_ACTIVE
FROM CUSTOMER_PIPELINE_DB.ETL.SOURCE_CONFIG
ORDER BY CONFIG_ID;
```
You should see 10 rows, all with `IS_ACTIVE = TRUE`, and your File IDs in
`EXTRACT_LOCATION`.

---

## PART 6 — Configure the Project Files

### Step 6.1 — Set Up the .env File

The `.env` file holds your email settings. It is already in the project.
Open it in VS Code:

```
airflow_customer_pipeline/.env
```

Fill it in with your own values:

```bash
# Email settings — Airflow will send alerts from this Gmail account
AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
AIRFLOW__SMTP__SMTP_STARTTLS=True
AIRFLOW__SMTP__SMTP_SSL=False
AIRFLOW__SMTP__SMTP_USER=your.email@gmail.com
AIRFLOW__SMTP__SMTP_PASSWORD=abcd efgh ijkl mnop    # 16-char Gmail App Password
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_MAIL_FROM=your.email@gmail.com

# Airflow core settings — leave these as-is
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=False
```

**How to create a Gmail App Password:**
1. Go to https://myaccount.google.com/apppasswords
2. Sign in with the Gmail account you want to send alerts from.
3. Under "App name", type `Airflow Pipeline`.
4. Click **Create**.
5. Google shows you a 16-character password like `abcd efgh ijkl mnop`.
6. Copy it into the `.env` file as `AIRFLOW__SMTP__SMTP_PASSWORD`.

> **Why App Password?** Google blocks "less secure app access" by default.
> An App Password is a special one-time password that lets Airflow send email
> without your normal password or 2FA.

---

### Step 6.2 — Confirm the Service Account Key Is in Place

Make sure the file you downloaded in Step 3.4 is at this exact path:

```
airflow_customer_pipeline/config/gdrive_service_account.json
```

Run this in your terminal to confirm:
```bash
ls -la config/
```
You should see `gdrive_service_account.json` listed.

---

## PART 7 — Start Apache Airflow

All Airflow commands must be run from inside the project folder:

```bash
cd airflow_customer_pipeline
```

### Step 7.1 — Initialize Airflow (First Time Only)

This creates the Airflow metadata database and the default admin user.
Only run this once — the first time you set up the project.

```bash
docker compose up airflow-init
```

Watch the output. Wait until you see the line:
```
User "airflow" created with role "Admin"
airflow-init-1 exited with code 0
```

Then press **Ctrl+C** to stop watching the output.

---

### Step 7.2 — Start All Services

```bash
docker compose up -d
```

This starts 3 services in the background:
- `postgres` — Airflow's internal metadata database
- `airflow-webserver` — The web UI at http://localhost:8081
- `airflow-scheduler` — The background process that runs your DAGs

---

### Step 7.3 — Verify Everything Is Running

```bash
docker compose ps
```

You should see output like:
```
NAME                                           STATUS
airflow_customer_pipeline-postgres-1           running (healthy)
airflow_customer_pipeline-airflow-webserver-1  running
airflow_customer_pipeline-airflow-scheduler-1  running
```

If any service shows `Exit` or `Restarting`, check the logs:
```bash
docker compose logs airflow-scheduler
docker compose logs airflow-webserver
```

---

### Step 7.4 — Open the Airflow UI

Open your browser and go to: **http://localhost:8081**

| Field | Value |
|---|---|
| Username | `airflow` |
| Password | `airflow` |

You will see the Airflow DAGs list. It will say **"No results"** — this is
normal. The DAGs cannot be generated until Airflow is connected to Snowflake
(next step).

---

## PART 8 — Connect Airflow to Snowflake

This is the most critical setup step. Without this connection, Airflow cannot
read the `SOURCE_CONFIG` table, and no DAGs will appear.

### Step 8.1 — Add the Snowflake Connection

1. In the Airflow UI, click **Admin** in the top menu.
2. Click **Connections**.
3. Click the **+** (blue plus button) to add a new connection.
4. Fill in the form with these exact values:

| Field | What to enter |
|---|---|
| **Connection Id** | `snowflake_customer_pipeline` — must be exactly this |
| **Connection Type** | Select `Snowflake` from the dropdown |
| **Description** | *(optional — anything descriptive)* |
| **Schema** | `ETL` |
| **Login** | `AIRFLOW_SERVICE_USER` |
| **Password** | The password you set in Step 4.2 |
| **Account** | Your account identifier from Step 4.1 (e.g. `YTJMFGP-VF07092`) |
| **Warehouse** | `COMPUTE_WH` |
| **Database** | `CUSTOMER_PIPELINE_DB` |
| **Region** | *(leave completely blank)* |
| **Role** | `AIRFLOW_LOADER_ROLE` |
| **Extra** | *(leave completely blank — delete anything already there)* |

5. Click **Save**.

> ⚠️ The `Extra` field is important. If it has any JSON content in it, delete
> it completely. Having values in both the form fields AND the Extra JSON causes
> conflicts and authentication failures.

---

### Step 8.2 — Test the Connection (Optional but Recommended)

Airflow 2.9 has a "Test" button on connection edit pages. If you see it:
1. Click the pencil (edit) icon next to the connection you just saved.
2. Click the **Test** button at the bottom.
3. A green "Connection successfully tested" message should appear.

If the test fails, re-check the Account identifier, password, and Role field.

---

### Step 8.3 — Watch the DAGs Appear

After saving the connection:
1. Click **DAGs** in the top navigation bar.
2. Wait 20–30 seconds.
3. The Airflow Scheduler will automatically re-read the DAG file, connect to
   Snowflake, query `SOURCE_CONFIG`, and generate 10 DAGs.

You should see all 10 DAGs appear:
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

If they do not appear after 60 seconds, force a scheduler restart:
```bash
docker compose restart airflow-scheduler
```

---

## PART 9 — Set Airflow Variables

Variables are key-value settings stored in Airflow's database. They let you
change runtime settings without editing Python code.

1. In the Airflow UI, click **Admin → Variables**.
2. Click the **+** button.
3. Add this variable:

| Key | Value |
|---|---|
| `customer_pipeline_alert_email` | `your.email@gmail.com` |

4. Click **Save**.

> This is the ONLY variable you must set. Everything else already has correct
> default values in the code for this Docker environment.

---

## PART 10 — Run the Pipeline

### Step 10.1 — Unpause a DAG

By default, all DAGs are **paused** (grey toggle on the left side). You must
unpause them before they can run.

1. Find `google_drive_customer_dag` in the list.
2. Click the **grey toggle switch** on the far left. It will turn blue.
3. A confirmation popup may appear — click **OK**.

### Step 10.2 — Trigger a Manual Run

1. On the same row as `google_drive_customer_dag`, click the **▶ (play)
   button** on the right side under "Actions".
2. Click **Trigger DAG** in the popup.

### Step 10.3 — Watch It Run

1. Click on the DAG name `google_drive_customer_dag` to open its detail view.
2. Click **Grid** in the top tab bar.
3. You will see colored squares appear in real time:
   - 🟡 **Yellow** — Running
   - 🟢 **Green** — Success
   - 🔴 **Red** — Failed

The task sequence is always:
```
fetch_metadata → download_file → validate_file → transform_file → load_to_snowflake
```

Each task runs sequentially — the next one only starts when the previous one
succeeds.

### Step 10.4 — View Task Logs

If any task fails (red), click on the red square, then click **Logs**.
The full Python output, including the exact error message, is shown here.

### Step 10.5 — Check Your Email

After `load_to_snowflake` completes:
- **Success:** You receive an email like:
  ```
  Subject: [SUCCESS] Airflow DAG 'google_drive_customer_dag' completed
  Body:
    Table: CUSTOMER
    Rows inserted: 52
    Config ID: 1
    ETL pipeline completed successfully!
  ```
- **Failure:** You receive an email with the full error traceback.

---

## PART 11 — Verify the Results in Snowflake

After a successful run, verify the data landed correctly.

Open a Snowflake worksheet and run:

```sql
-- Check the CUSTOMER table was loaded
SELECT COUNT(*) AS row_count FROM CUSTOMER_PIPELINE_DB.RAW.CUSTOMER;

-- Preview the first 10 rows
SELECT * FROM CUSTOMER_PIPELINE_DB.RAW.CUSTOMER LIMIT 10;

-- Check the audit log — one row per pipeline run
SELECT
    CONFIG_ID,
    TABLE_NAME,
    TABLE_LOAD_STATUS,
    INSERTION_ROWCOUNT,
    IMPORT_STARTTS,
    IMPORT_COMPLETETS,
    RUNID
FROM CUSTOMER_PIPELINE_DB.ETL.ETL_LOG
ORDER BY IMPORT_STARTTS DESC
LIMIT 20;
```

A successful run will have `TABLE_LOAD_STATUS = 'SUCCESS'` in `ETL_LOG`.

---

## PART 12 — Schedule DAGs to Run Automatically

By default, all DAGs have `schedule=None` (manual trigger only). To make a
DAG run on a schedule, edit the `SOURCE_CONFIG` row or modify the DAG file.

**Option A: Set a cron schedule in the Python code**

Open `dags/dag_google_drive_dynamic.py` and find the line:
```python
schedule=None,    # Manual trigger
```
Change it to a cron expression:
```python
schedule="0 6 * * *",    # Every day at 6:00 AM UTC
```

Common cron expressions:
| Schedule | Cron |
|---|---|
| Every day at 6 AM UTC | `"0 6 * * *"` |
| Every hour | `"0 * * * *"` |
| Every Monday at 8 AM | `"0 8 * * 1"` |
| Every 30 minutes | `"*/30 * * * *"` |

After saving the file, the Airflow Scheduler will pick up the new schedule
within 5 minutes.

---

## PART 13 — Add a New Data Source in the Future

This is where the metadata-driven design pays off. **You do not need to write
any Python code to add a new table.**

1. Upload the new file to Google Drive and share it with the service account.
2. Get the new file's Drive ID.
3. Run this SQL in Snowflake (adjust the values for your new table):

```sql
INSERT INTO CUSTOMER_PIPELINE_DB.ETL.SOURCE_CONFIG (
    VENDOR, EXTRACT_LOCATION, GD_LOCATION, SCHEMA_NAME,
    TABLE_NAME, FILE_NAME_PATTERN, FILE_FORMAT, DATA_DELIMITER,
    COLUMN_LIST, NULLABLE_COLUMNS, IS_ACTIVE
) VALUES (
    'GOOGLE_DRIVE',
    'YOUR_NEW_FILE_ID_HERE',
    'https://drive.google.com/file/d/YOUR_NEW_FILE_ID_HERE/view',
    'RAW',
    'NEW_TABLE_NAME',         -- e.g. 'CONTRACTS'
    'new_file_raw.csv',
    'CSV',
    ',',
    'col1,col2,col3',         -- exact column names from your file
    '',                       -- nullable columns, or '' if all mandatory
    TRUE
);
```

4. Also create the destination table in Snowflake:
```sql
CREATE OR REPLACE TABLE CUSTOMER_PIPELINE_DB.RAW.NEW_TABLE_NAME (
    COL1 VARCHAR(100),
    COL2 VARCHAR(100),
    COL3 VARCHAR(100),
    LOADED_AT     TIMESTAMP_NTZ(9),
    SOURCE_RUN_ID VARCHAR(255)
);
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE CUSTOMER_PIPELINE_DB.RAW.NEW_TABLE_NAME
    TO ROLE AIRFLOW_LOADER_ROLE;
```

Within 5 minutes, a new DAG named `google_drive_new_table_name_dag` will
appear in the Airflow UI automatically. No Python file changes needed.

---

## PART 14 — Daily Operations — Start and Stop

### To stop Airflow (preserve all data and history):
```bash
docker compose down
```
Your DAG history, connections, and variables are stored in the Postgres volume
and will be there when you restart.

### To start Airflow again after stopping:
```bash
docker compose up -d
```
Open http://localhost:8081 — your connections and DAGs are still there.

### To stop and wipe EVERYTHING (complete reset):
```bash
docker compose down -v     # The -v flag deletes the Postgres volume too
docker compose up airflow-init   # Re-initialize from scratch
docker compose up -d
```
After a full reset, you must re-add the Snowflake connection (Part 8) and
the Variables (Part 9) because they are stored in the now-deleted database.

### To check logs of a running service:
```bash
# Scheduler logs (most useful for debugging DAG issues)
docker compose logs -f airflow-scheduler

# Webserver logs
docker compose logs -f airflow-webserver
```

---

## PART 15 — Full Checklist

Print this out and tick each box as you go.

**Google Cloud / Drive (one time):**
- [ ] Google Cloud project created (`airflow-pipeline`)
- [ ] Google Drive API enabled
- [ ] Service account `airflow-drive-reader` created (no roles added)
- [ ] JSON key downloaded and renamed to `gdrive_service_account.json`
- [ ] JSON key placed at `config/gdrive_service_account.json`
- [ ] `client_email` copied from the JSON file
- [ ] All 10 source files uploaded to Google Drive with correct column headers
- [ ] All 10 files shared with the service account `client_email` as Viewer
- [ ] All 10 File IDs recorded

**Snowflake (one time):**
- [ ] Snowflake Account Identifier noted down
- [ ] `AIRFLOW_LOADER_ROLE` created
- [ ] `AIRFLOW_SERVICE_USER` created with a password
- [ ] Role granted to user; warehouse access granted
- [ ] `01_setup_database.sql` executed — `CUSTOMER_PIPELINE_DB`, `RAW`, `ETL` schemas created
- [ ] `02_setup_raw_tables.sql` executed — all 10 RAW tables created
- [ ] `03_setup_pipeline_configs.sql` updated with YOUR 10 File IDs
- [ ] `03_setup_pipeline_configs.sql` executed — 10 rows in `SOURCE_CONFIG`
- [ ] Verified: `SELECT * FROM CUSTOMER_PIPELINE_DB.ETL.SOURCE_CONFIG` shows 10 rows

**Project Setup (one time):**
- [ ] Repository cloned
- [ ] Switched to branch `feature/vendor-specific-master-dags`
- [ ] `.env` file filled in with Gmail address and App Password
- [ ] `config/gdrive_service_account.json` is in place

**Airflow (every fresh start):**
- [ ] `docker compose up airflow-init` completed — saw "User airflow created"
- [ ] `docker compose up -d` — all 3 services running
- [ ] http://localhost:8081 opens
- [ ] **Admin → Connections** — `snowflake_customer_pipeline` added with all fields
- [ ] **Admin → Variables** — `customer_pipeline_alert_email` added
- [ ] 10 DAGs appear in the DAG list
- [ ] One DAG triggered manually — all 5 tasks turned green
- [ ] Success email received in Gmail
- [ ] Snowflake data verified: `SELECT COUNT(*) FROM CUSTOMER_PIPELINE_DB.RAW.CUSTOMER`

---

## PART 16 — Troubleshooting

### DAG list shows "No results" after setup

**Cause:** The Snowflake connection is missing, wrong, or failing silently.

**Fix:**
1. Go to Admin → Connections → edit `snowflake_customer_pipeline`.
2. Check: Connection Id is exactly `snowflake_customer_pipeline`.
3. Check: Role is `AIRFLOW_LOADER_ROLE`, NOT `ACCOUNTADMIN`.
4. Check: Extra field is completely empty.
5. Save, then run: `docker compose restart airflow-scheduler`

---

### Error: `Role 'ACCOUNTADMIN' is not granted to this user`

**Cause:** The Role field in the Airflow connection is set to `ACCOUNTADMIN`,
but `AIRFLOW_SERVICE_USER` was only granted `AIRFLOW_LOADER_ROLE`.

**Fix:** Edit the connection → change Role to `AIRFLOW_LOADER_ROLE`.

---

### `download_file` task fails with "File not found" or "403"

**Cause:** The Google Drive file was not shared with the service account, OR
the File ID in `SOURCE_CONFIG` is wrong.

**Fix:**
1. Go to Google Drive, right-click the file → Share.
2. Confirm the service account `client_email` is listed as a Viewer.
3. Check `SOURCE_CONFIG`: `SELECT EXTRACT_LOCATION FROM CUSTOMER_PIPELINE_DB.ETL.SOURCE_CONFIG WHERE CONFIG_ID = <n>` and verify the File ID is correct.

---

### `validate_file` fails with "Missing expected columns"

**Cause:** The actual column names in your CSV file don't match the
`COLUMN_LIST` in `SOURCE_CONFIG`.

**Fix:**
1. Open the CSV in Excel or VS Code and check the exact header row.
2. Run: `SELECT COLUMN_LIST FROM CUSTOMER_PIPELINE_DB.ETL.SOURCE_CONFIG WHERE TABLE_NAME = 'CUSTOMER'`
3. Make sure they match exactly (case-sensitive).

---

### I get a bounce email from `you@example.com`

**Cause:** The `customer_pipeline_alert_email` Airflow Variable is not set.
The code falls back to `you@example.com` as a default placeholder.

**Fix:** Admin → Variables → add key `customer_pipeline_alert_email` with your real email.

---

### `gdrive_service_account.json` file not found error

**Cause:** The key file is not at `config/gdrive_service_account.json`, or
it has a double extension like `gdrive_service_account.json.json`.

**Fix:**
```bash
ls -la config/
```
You should see exactly `gdrive_service_account.json`. If it says `.json.json`,
rename it. If `config/` is empty, you forgot to place the file there.

---

### DAGs were working, now none appear (0 DAGs)

**Cause A:** Docker was restarted with `docker compose down -v` which wiped
the Postgres volume including all connections and variables.

**Fix A:** Re-add the connection (Part 8) and variable (Part 9).

**Cause B:** Snowflake password was changed.

**Fix B:** Edit the Airflow connection and update the password field.

---

### A task shows "yellow" and never moves to green or red

**Cause:** The task is queued but the scheduler is not picking it up. This
sometimes happens after a docker restart.

**Fix:**
```bash
docker compose restart airflow-scheduler
```

---

## Quick Reference

| What | Value |
|---|---|
| Airflow UI | http://localhost:8081 |
| Airflow Username | `airflow` |
| Airflow Password | `airflow` |
| Snowflake Connection ID | `snowflake_customer_pipeline` |
| Snowflake User | `AIRFLOW_SERVICE_USER` |
| Snowflake Role | `AIRFLOW_LOADER_ROLE` |
| Snowflake Warehouse | `COMPUTE_WH` |
| Snowflake Database | `CUSTOMER_PIPELINE_DB` |
| Source Config Table | `CUSTOMER_PIPELINE_DB.ETL.SOURCE_CONFIG` |
| Audit Log Table | `CUSTOMER_PIPELINE_DB.ETL.ETL_LOG` |
| Start Airflow | `docker compose up -d` |
| Stop Airflow | `docker compose down` |
| Full Reset | `docker compose down -v` |
| View Logs | `docker compose logs -f airflow-scheduler` |
