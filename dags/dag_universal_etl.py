"""
dag_universal_etl.py
---------------------
Worker DAG — Airflow wiring only.

All ETL business logic lives in scripts/etl_tasks.py.
This file only defines the DAG structure: tasks, dependencies, and callbacks.

Triggered by master_trigger_dag with a `config_id` in dag_run.conf.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from alerts import task_failure_alert
from scripts.etl_tasks import (
    fetch_metadata,
    download_file,
    validate_file,
    transform_file,
    load_to_snowflake,
    on_etl_success,
)

# ---------------------------------------------------------------------------
# Default arguments — applied to every task in this DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": task_failure_alert,
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="universal_etl_dag",
    description="Worker DAG: Extracts, transforms, and loads a single table based on SOURCE_CONFIG metadata",
    default_args=default_args,
    schedule=None,          # Triggered by master_trigger_dag — never runs on its own
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=10,     # Allow all 10 pipelines to run concurrently
    tags=["metadata-driven", "worker", "etl"],
) as dag:

    t1 = PythonOperator(task_id="fetch_metadata",    python_callable=fetch_metadata)
    t2 = PythonOperator(task_id="download_file",     python_callable=download_file)
    t3 = PythonOperator(task_id="validate_file",     python_callable=validate_file)
    t4 = PythonOperator(task_id="transform_file",    python_callable=transform_file)
    t5 = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
        on_success_callback=on_etl_success,
    )

    t1 >> t2 >> t3 >> t4 >> t5
