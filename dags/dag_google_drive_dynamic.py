"""
dag_google_drive_dynamic.py
----------------------------
Dynamically generates one independent Airflow DAG per active Google Drive
source configuration found in SOURCE_CONFIG.

Pattern: At parse time, this file queries Snowflake for all active
GOOGLE_DRIVE configs, calls build_dag() for each one, and registers the
resulting DAG objects into globals() so Airflow's scheduler discovers them.

Result in Airflow UI:
  google_drive_customer_dag        -> config_id=1  (CUSTOMER table)
  google_drive_orders_dag          -> config_id=2  (ORDERS table)
  google_drive_order_items_dag     -> config_id=3  (ORDER_ITEMS table)
  ... (one DAG per active config row, auto-generated)

Adding a new source: INSERT a row into SOURCE_CONFIG with VENDOR='GOOGLE_DRIVE'
and IS_ACTIVE=TRUE. On the next DAG parse cycle Airflow picks it up automatically.
No Python changes required.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from config import (
    SNOWFLAKE_CONN_ID,
    SOURCE_CONFIG_DATABASE,
    SOURCE_CONFIG_SCHEMA,
    SOURCE_CONFIG_TABLE,
)
from alerts import task_failure_alert
from scripts.etl_tasks import (
    fetch_metadata,
    download_file,
    validate_file,
    transform_file,
    load_to_snowflake,
    on_etl_success,
)

# =============================================================================
# SERVICE PROVIDER — controls which SOURCE_CONFIG rows this file owns
# =============================================================================
SERVICE_PROVIDER = "GOOGLE_DRIVE"

# =============================================================================
# DEFAULT ARGS — applied to every task in every generated DAG
# =============================================================================
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": task_failure_alert,
}


# =============================================================================
# CONFIG LOADER — runs at parse time to discover active pipelines
# =============================================================================

def fetch_google_drive_configs() -> list[dict]:
    """
    Queries Snowflake for all active SOURCE_CONFIG rows where VENDOR matches
    SERVICE_PROVIDER. Called once at DAG file parse time.
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    rows = hook.get_records(
        f"""
        SELECT CONFIG_ID, VENDOR, TABLE_NAME
        FROM {SOURCE_CONFIG_DATABASE}.{SOURCE_CONFIG_SCHEMA}.{SOURCE_CONFIG_TABLE}
        WHERE IS_ACTIVE  = TRUE
          AND VENDOR     = %(provider)s
        ORDER BY TABLE_NAME
        """,
        parameters={"provider": SERVICE_PROVIDER},
    )
    return [
        {"config_id": r[0], "vendor": r[1], "table_name": r[2]}
        for r in rows
    ]


# =============================================================================
# DAG FACTORY — builds one complete DAG per config row
# =============================================================================

def build_dag(cfg: dict):
    """
    Generates a fully independent Airflow DAG for a single SOURCE_CONFIG row.

    The config_id from cfg is embedded directly into the fetch_metadata task
    via op_kwargs — no master DAG or dag_run.conf needed.
    """
    # Airflow dag_id must be lowercase alphanumeric + underscores
    table_slug = re.sub(r"[^a-z0-9]+", "_", cfg["table_name"].lower()).strip("_")
    dag_id = f"google_drive_{table_slug}_dag"

    @dag(
        dag_id=dag_id,
        default_args=default_args,
        schedule=None,              # Manual trigger; change to "@daily" etc. per table if needed
        
        # Option A: Every day at 6:00 AM IST (1:30 AM UTC)
        # schedule="30 1 * * *",

        # Option B: Every day at midnight UTC
        # schedule="0 0 * * *",

        # Option C: Every hour
        # schedule="0 * * * *",

        # Option D: Every Monday at 8 AM UTC (weekly)
        # schedule="0 8 * * 1",
        
        start_date=datetime(2025, 1, 1),
        catchup=False,
        max_active_runs=1,          # One run at a time per table
        dagrun_timeout=timedelta(hours=2),  # Kill stuck runs after 2 hours — prevents zombie runs
        tags=["google-drive", "etl", table_slug],
        doc_md=(
            f"**{SERVICE_PROVIDER} / {cfg['table_name']}**\n\n"
            f"fetch_metadata → download_file → validate_file → transform_file → load_to_snowflake\n\n"
            f"`config_id={cfg['config_id']}`"
        ),
    )
    def _generated_dag():
        # config_id is passed directly via op_kwargs — no dag_run.conf needed.
        # This is possible because cfg is captured in the closure from build_dag().
        t1 = PythonOperator(
            task_id="fetch_metadata",
            python_callable=fetch_metadata,
            op_kwargs={"config_id": cfg["config_id"]},
        )

        t2 = PythonOperator(
            task_id="download_file",
            python_callable=download_file,
        )

        t3 = PythonOperator(
            task_id="validate_file",
            python_callable=validate_file,
        )

        t4 = PythonOperator(
            task_id="transform_file",
            python_callable=transform_file,
        )

        t5 = PythonOperator(
            task_id="load_to_snowflake",
            python_callable=load_to_snowflake,
            on_success_callback=on_etl_success,
        )

        t1 >> t2 >> t3 >> t4 >> t5

    return _generated_dag()


# =============================================================================
# REGISTRATION — Airflow discovers DAGs via globals()
# =============================================================================

try:
    google_drive_configs = fetch_google_drive_configs()
    print(f"[{SERVICE_PROVIDER}] Found {len(google_drive_configs)} active config(s)")
except Exception as e:
    # If Snowflake is unreachable at parse time, log and continue with empty list.
    # The scheduler will retry on the next parse cycle.
    print(f"[{SERVICE_PROVIDER}] Failed to load configs: {e}")
    google_drive_configs = []

for cfg in google_drive_configs:
    generated = build_dag(cfg)
    if generated is not None:
        globals()[generated.dag_id] = generated
