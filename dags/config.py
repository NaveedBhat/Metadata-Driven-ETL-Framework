"""
config.py
---------
Central configuration for the metadata-driven data pipeline.

This file only contains GLOBAL infrastructure settings.
Specific file paths, table names, and sources are looked up dynamically
from the SOURCE_CONFIG table in Snowflake.
"""

from airflow.models import Variable  # type: ignore[import]
import os

# -----------------------------------------------------------------------
# Google Drive settings
# -----------------------------------------------------------------------
GDRIVE_SERVICE_ACCOUNT_FILE = Variable.get(
    "gdrive_service_account_file",
    default_var="/opt/airflow/config/gdrive_service_account.json",
)

# -----------------------------------------------------------------------
# Local filesystem layout (inside the Airflow worker/container)
# -----------------------------------------------------------------------
DATA_ROOT = Variable.get("customer_pipeline_data_root", default_var="/opt/airflow/data")

RAW_DIR = f"{DATA_ROOT}/extract"
PROCESSED_DIR = f"{DATA_ROOT}/processed"
REJECTED_DIR = f"{DATA_ROOT}/rejected"

# Helper functions to dynamically generate file paths based on config ID or table name
def get_raw_file_path(table_name: str, config_id: int) -> str:
    return f"{RAW_DIR}/{table_name.lower()}_raw_{config_id}.csv"

def get_clean_file_path(table_name: str, config_id: int) -> str:
    return f"{PROCESSED_DIR}/{table_name.lower()}_clean_{config_id}.csv"

def get_rejected_nulls_path(table_name: str, config_id: int) -> str:
    return f"{REJECTED_DIR}/{table_name.lower()}_dropped_nulls_{config_id}.csv"

def get_rejected_dupes_path(table_name: str, config_id: int) -> str:
    return f"{REJECTED_DIR}/{table_name.lower()}_dropped_duplicates_{config_id}.csv"

# -----------------------------------------------------------------------
# Snowflake settings
# -----------------------------------------------------------------------
SNOWFLAKE_CONN_ID = Variable.get("snowflake_conn_id", default_var="snowflake_customer_pipeline")
SNOWFLAKE_DATABASE = "CUSTOMER_PIPELINE_DB"

# Where ETL run logs and pipeline configuration live.
SNOWFLAKE_ETL_SCHEMA = "ETL"
SNOWFLAKE_LOG_TABLE = "ETL_LOG"

# -----------------------------------------------------------------------
# SOURCE_CONFIG lookup
# Table: CUSTOMER_PIPELINE_DB.ETL.SOURCE_CONFIG
# -----------------------------------------------------------------------
SOURCE_CONFIG_DATABASE = "CUSTOMER_PIPELINE_DB"
SOURCE_CONFIG_SCHEMA = "ETL"
SOURCE_CONFIG_TABLE = "SOURCE_CONFIG"

# -----------------------------------------------------------------------
# Email alerting
# -----------------------------------------------------------------------
ALERT_EMAIL_TO = [
    e.strip() for e in Variable.get("customer_pipeline_alert_email", default_var="you@example.com").split(",")
]
