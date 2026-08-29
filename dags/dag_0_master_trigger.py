"""
dag_0_master_trigger.py
------------------------
Master DAG that runs on a schedule, queries SOURCE_CONFIG for all active files,
and uses Dynamic Task Mapping to spawn instances of universal_etl_dag for each.
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from config import (
    SNOWFLAKE_CONN_ID,
    SOURCE_CONFIG_DATABASE,
    SOURCE_CONFIG_SCHEMA,
    SOURCE_CONFIG_TABLE,
)
from alerts import task_failure_alert

logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": task_failure_alert,
}

with DAG(
    dag_id="master_trigger_dag",
    description="Queries Snowflake for active sources and triggers worker DAGs",
    default_args=default_args,
    schedule=None,  # Manual trigger only — prevents duplicate runs on Docker restart
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["metadata-driven", "master"],
) as dag:

    @task
    def fetch_active_configs():
        """Queries Snowflake for all active CONFIG_ID values."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        query = f"""
            SELECT CONFIG_ID, TABLE_NAME 
            FROM {SOURCE_CONFIG_DATABASE}.{SOURCE_CONFIG_SCHEMA}.{SOURCE_CONFIG_TABLE}
            WHERE IS_ACTIVE = TRUE
        """
        
        configs = []
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            for row in rows:
                config_id = row[0]
                table_name = row[1]
                logger.info("Found active config: ID=%s, Table=%s", config_id, table_name)
                # For TriggerDagRunOperator's expand(), we return a list of dictionaries
                # where each dictionary contains the arguments for one trigger.
                configs.append({
                    "conf": {"config_id": config_id}
                })
        finally:
            cursor.close()
            conn.close()
            
        if not configs:
            logger.warning("No active configs found in SOURCE_CONFIG.")
            
        return configs

    # Use Dynamic Task Mapping to trigger the worker DAG for every config returned
    # by fetch_active_configs
    trigger_workers = TriggerDagRunOperator.partial(
        task_id="trigger_universal_etl_dag",
        trigger_dag_id="universal_etl_dag",
        wait_for_completion=False, # We don't block the master DAG
    ).expand_kwargs(fetch_active_configs())
