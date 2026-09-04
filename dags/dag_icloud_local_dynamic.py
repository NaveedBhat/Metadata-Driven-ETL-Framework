"""
dags/dag_icloud_local_dynamic.py
---------------------------------
Dynamic DAG factory for iCloud Local files.

Queries SOURCE_CONFIG for VENDOR = 'ICLOUD_LOCAL' and generates an
independent Airflow DAG for each row.

This completely isolates iCloud data loads from Google Drive data loads.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from config import (
    SNOWFLAKE_CONN_ID,
    SOURCE_CONFIG_DATABASE,
    SOURCE_CONFIG_SCHEMA,
    SOURCE_CONFIG_TABLE,
)
from scripts.etl_tasks import (
    fetch_metadata,
    download_file,
    validate_file,
    transform_file,
    load_to_snowflake,
)
from alerts import dag_success_alert, task_failure_alert

logger = logging.getLogger("airflow.task")

SERVICE_PROVIDER = "ICLOUD_LOCAL"

DEFAULT_ARGS = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_alert,
}

def fetch_icloud_configs():
    """
    Connects to Snowflake and returns a list of dictionaries, one for each
    active SOURCE_CONFIG row where VENDOR = 'ICLOUD_LOCAL'.
    """
    logger.info("Fetching dynamic DAG configs for %s from Snowflake...", SERVICE_PROVIDER)
    try:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        query = f"""
            SELECT CONFIG_ID, TABLE_NAME
            FROM {SOURCE_CONFIG_DATABASE}.{SOURCE_CONFIG_SCHEMA}.{SOURCE_CONFIG_TABLE}
            WHERE VENDOR = '{SERVICE_PROVIDER}' AND IS_ACTIVE = TRUE
        """
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        
        configs = []
        for row in rows:
            configs.append({
                "config_id": row[0],
                "table_name": row[1]
            })
            
        cursor.close()
        conn.close()
        logger.info("Successfully fetched %d active %s configs.", len(configs), SERVICE_PROVIDER)
        return configs
        
    except Exception as e:
        logger.error(f"Failed to fetch {SERVICE_PROVIDER} configs from Snowflake: {str(e)}")
        # Return an empty list so the DAG parser doesn't crash the whole scheduler.
        # It just means 0 DAGs will be generated until the connection is fixed.
        return []

def build_dag(config):
    """
    Constructs and returns an Airflow DAG object for a single source config.
    """
    config_id = config["config_id"]
    table_name = config["table_name"].lower()
    
    dag_id = f"icloud_local_{table_name}_dag"
    
    dag = DAG(
        dag_id=dag_id,
        default_args=DEFAULT_ARGS,
        description=f"ETL pipeline for {table_name} from {SERVICE_PROVIDER} (Config ID {config_id})",
        schedule=None,  # Manual trigger by default
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=[SERVICE_PROVIDER.lower(), table_name, "dynamic"],
        on_success_callback=dag_success_alert,
        # Avoid running multiple loads for the same table at the exact same time
        max_active_runs=1, 
    )

    with dag:
        task_fetch_metadata = PythonOperator(
            task_id="fetch_metadata",
            python_callable=fetch_metadata,
            op_kwargs={"config_id": config_id},
        )

        task_download = PythonOperator(
            task_id="download_file",
            python_callable=download_file,
        )

        task_validate = PythonOperator(
            task_id="validate_file",
            python_callable=validate_file,
        )

        task_transform = PythonOperator(
            task_id="transform_file",
            python_callable=transform_file,
        )

        task_load = PythonOperator(
            task_id="load_to_snowflake",
            python_callable=load_to_snowflake,
        )

        # Build the sequence
        (
            task_fetch_metadata
            >> task_download
            >> task_validate
            >> task_transform
            >> task_load
        )

    return dag

# =============================================================================
# DAG FACTORY EXECUTION (Runs every time Airflow parses this file)
# =============================================================================

for cfg in fetch_icloud_configs():
    generated_dag = build_dag(cfg)
    # To make Airflow see the DAG, it must be added to the module's global variables
    globals()[generated_dag.dag_id] = generated_dag
