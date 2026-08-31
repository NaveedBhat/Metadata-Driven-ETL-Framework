"""
scripts/etl_tasks.py
---------------------
ETL business logic for the Google Drive dynamic DAG pipeline.

All functions here are pure Python — no Airflow DAG code.
They are called by PythonOperator in dags/dag_google_drive_dynamic.py via the
`python_callable` argument, receiving the Airflow task context as **kwargs.

config_id resolution:
- Dynamic DAG pattern  : passed directly via op_kwargs from build_dag() closure
- Master-Worker pattern: read from dag_run.conf (backward compatible)

Separation rationale:
- DAG files (dags/)     : Airflow wiring — schedules, task dependencies, callbacks.
- Scripts (scripts/)    : Business logic — data processing, API calls, DB writes.

This makes business logic independently testable without importing Airflow.
"""

import io
import os
import logging
from datetime import datetime, timezone

import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from config import (
    GDRIVE_SERVICE_ACCOUNT_FILE,
    SNOWFLAKE_CONN_ID,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_ETL_SCHEMA,
    SNOWFLAKE_LOG_TABLE,
    SOURCE_CONFIG_DATABASE,
    SOURCE_CONFIG_SCHEMA,
    SOURCE_CONFIG_TABLE,
    get_raw_file_path,
    get_clean_file_path,
    get_rejected_nulls_path,
    get_rejected_dupes_path,
)
from alerts import dag_success_alert

logger = logging.getLogger("airflow.task")


# =============================================================================
# TASK 1 — FETCH METADATA
# =============================================================================

def fetch_metadata(config_id=None, **context):
    """
    Fetches the SOURCE_CONFIG metadata row for the given config_id and pushes
    it to XCom for downstream tasks.

    config_id resolution (two supported patterns):
    1. Dynamic DAG pattern  : passed directly via PythonOperator op_kwargs
                              e.g. op_kwargs={"config_id": cfg["config_id"]}
    2. Master-Worker pattern: read from dag_run.conf
                              e.g. conf={"config_id": 3}
    """
    # Pattern 1: config_id came in directly via op_kwargs (dynamic dag factory)
    # Pattern 2: config_id is in dag_run.conf (master-worker trigger)
    if config_id is None:
        dag_run_conf = context["dag_run"].conf or {}
        config_id = dag_run_conf.get("config_id")

    if not config_id:
        raise ValueError(
            "Missing config_id. Provide it via op_kwargs (dynamic DAG) "
            "or dag_run.conf (master-worker trigger)."
        )

    logger.info("Fetching metadata for config_id: %s", config_id)

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    query = f"""
        SELECT
            VENDOR, EXTRACT_LOCATION, GD_LOCATION, SCHEMA_NAME,
            TABLE_NAME, FILE_NAME_PATTERN, FILE_FORMAT, DATA_DELIMITER,
            COLUMN_LIST, NULLABLE_COLUMNS
        FROM {SOURCE_CONFIG_DATABASE}.{SOURCE_CONFIG_SCHEMA}.{SOURCE_CONFIG_TABLE}
        WHERE CONFIG_ID = %s AND IS_ACTIVE = TRUE
    """

    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(query, (config_id,))
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"No active SOURCE_CONFIG row found for config_id={config_id}.")

        metadata = {
            "config_id": config_id,
            "vendor": row[0],
            "extract_location": row[1],
            "gd_location": row[2],
            "schema_name": row[3],
            "table_name": row[4],
            "file_name_pattern": row[5],
            "file_format": row[6].upper(),  # Normalise to uppercase for all format checks
            "data_delimiter": row[7],
            "column_list": row[8],
            # Columns allowed to be NULL. Empty string = all columns are mandatory.
            "nullable_columns": row[9] or "",
        }

        context["ti"].xcom_push(key="source_metadata", value=metadata)
        logger.info(
            "Metadata loaded — table: %s | nullable cols: [%s]",
            metadata["table_name"], metadata["nullable_columns"] or "none",
        )
    finally:
        cursor.close()
        conn.close()


# =============================================================================
# TASK 2 — DOWNLOAD FILE
# =============================================================================

def download_file(**context):
    """
    Downloads the file from the vendor using metadata from XCom.

    Supported formats:
    - CSV / TXT : downloaded as-is using Drive get_media.
    - XLSX      : Google Sheets files are exported as CSV via the Drive export API;
                  uploaded XLSX binaries are read with pd.read_excel() and written
                  out as CSV. Either way the raw_file_path on disk is always a CSV
                  so the rest of the pipeline remains format-agnostic.
    """
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    from google.oauth2 import service_account

    metadata = context["ti"].xcom_pull(task_ids="fetch_metadata", key="source_metadata")
    config_id = metadata["config_id"]
    table_name = metadata["table_name"]
    vendor = metadata["vendor"]
    file_format = metadata["file_format"]  # Already uppercased in fetch_metadata

    if vendor != "GOOGLE_DRIVE":
        raise NotImplementedError(f"Vendor '{vendor}' not supported yet in download_file task.")

    file_id = metadata["extract_location"]
    raw_file_path = get_raw_file_path(table_name, config_id)  # Always .csv on disk

    os.makedirs(os.path.dirname(raw_file_path), exist_ok=True)

    credentials = service_account.Credentials.from_service_account_file(
        GDRIVE_SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
    drive_service = build("drive", "v3", credentials=credentials)

    if file_format == "XLSX":
        # ----------------------------------------------------------------
        # XLSX handling: detect whether the Drive file is a native Google
        # Sheet (mimeType = vnd.google-apps.spreadsheet) or an uploaded
        # binary XLSX. Both paths produce a CSV on disk so downstream tasks
        # don't need to know the original format.
        # ----------------------------------------------------------------
        try:
            file_meta = drive_service.files().get(
                fileId=file_id, fields="id,mimeType"
            ).execute()
        except Exception as e:
            raise FileNotFoundError(
                f"Drive file '{file_id}' not found or no access."
            ) from e

        google_sheets_mime = "application/vnd.google-apps.spreadsheet"

        if file_meta.get("mimeType") == google_sheets_mime:
            # Native Google Sheet → export directly as CSV
            logger.info("Detected native Google Sheet — exporting as CSV.")
            csv_bytes = (
                drive_service.files()
                .export_media(fileId=file_id, mimeType="text/csv")
                .execute()
            )
            with open(raw_file_path, "wb") as f:
                f.write(csv_bytes)
        else:
            # Uploaded XLSX binary → download then convert with pandas
            logger.info("Detected uploaded XLSX — downloading and converting to CSV.")
            request = drive_service.files().get_media(fileId=file_id)
            buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(buffer, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            buffer.seek(0)
            df_xlsx = pd.read_excel(buffer, engine="openpyxl")
            df_xlsx.to_csv(raw_file_path, index=False)

    else:
        # ----------------------------------------------------------------
        # CSV / TXT (or any other plain-text format) — download as-is.
        # ----------------------------------------------------------------
        try:
            drive_service.files().get(fileId=file_id, fields="id").execute()
        except Exception as e:
            raise FileNotFoundError(
                f"Drive file '{file_id}' not found or no access."
            ) from e

        request = drive_service.files().get_media(fileId=file_id)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        with open(raw_file_path, "wb") as f:
            f.write(buffer.getvalue())

    downloaded_size = os.path.getsize(raw_file_path)
    if downloaded_size == 0:
        raise ValueError("Downloaded file is 0 bytes.")

    logger.info(
        "[%s] Ready at %s (%d bytes)", file_format, raw_file_path, downloaded_size
    )


# =============================================================================
# TASK 3 — VALIDATE FILE
# =============================================================================

def validate_file(**context):
    """
    Sanity-checks the downloaded file.
    Uses DATA_DELIMITER from SOURCE_CONFIG so TXT (pipe) and other
    delimited formats are read correctly.
    """
    metadata = context["ti"].xcom_pull(task_ids="fetch_metadata", key="source_metadata")
    config_id = metadata["config_id"]
    table_name = metadata["table_name"]
    delimiter = metadata.get("data_delimiter", ",")  # e.g. ',' for CSV, '|' for TXT
    raw_file_path = get_raw_file_path(table_name, config_id)

    if not os.path.exists(raw_file_path):
        raise FileNotFoundError(f"File not found: {raw_file_path}")

    df = pd.read_csv(raw_file_path, dtype=str, sep=delimiter, engine="python")
    row_count = df.shape[0]

    if row_count == 0:
        raise ValueError("Downloaded file has 0 data rows.")

    expected_columns = set(c.strip() for c in metadata["column_list"].split(","))
    missing_columns = expected_columns - set(df.columns)

    if missing_columns:
        raise ValueError(f"Missing expected columns: {missing_columns}")

    logger.info("Validation OK — %d rows, delimiter=%r", row_count, delimiter)


# =============================================================================
# TASK 4 — TRANSFORM FILE
# =============================================================================

def transform_file(**context):
    """
    Drops exact duplicates and null-containing rows.

    Schema validation vs null-quality separation (production pattern):
    - COLUMN_LIST     : ALL expected columns. validate_file checks these all EXIST
                        in the source file (schema check).
    - NULLABLE_COLUMNS: Subset of COLUMN_LIST allowed to be NULL. Only the remaining
                        mandatory columns trigger row rejection when null values are found.

    This means a record is only rejected when a genuinely required column is missing —
    not because an optional field like manager_id or review_text happens to be blank.

    The output clean file is always written as standard CSV regardless of the
    original source format.
    """
    metadata = context["ti"].xcom_pull(task_ids="fetch_metadata", key="source_metadata")
    config_id = metadata["config_id"]
    table_name = metadata["table_name"]
    delimiter = metadata.get("data_delimiter", ",")  # e.g. ',' for CSV, '|' for TXT

    raw_file_path = get_raw_file_path(table_name, config_id)
    clean_file_path = get_clean_file_path(table_name, config_id)

    os.makedirs(os.path.dirname(clean_file_path), exist_ok=True)

    # Always read with the correct delimiter; output is always comma-CSV
    df = pd.read_csv(raw_file_path, sep=delimiter, engine="python")

    # ----------------------------------------------------------------
    # Null-quality check — production pattern:
    #   all_columns    : every column declared in COLUMN_LIST (schema-level)
    #   nullable_cols  : columns explicitly allowed to be NULL
    #   mandatory_cols : all_columns minus nullable_cols → null here = bad row
    # ----------------------------------------------------------------
    all_columns = [c.strip() for c in metadata["column_list"].split(",")]
    nullable_raw = metadata.get("nullable_columns", "") or ""
    nullable_cols_set = {c.strip() for c in nullable_raw.split(",") if c.strip()}
    mandatory_columns = [c for c in all_columns if c not in nullable_cols_set]

    # Strip extra columns FIRST (e.g. 'Unnamed: 7' from XLSX files with trailing
    # empty columns). This must happen before dedup so that dedup operates only
    # on declared business columns — not on spurious unnamed columns.
    df = df[[c for c in all_columns if c in df.columns]]

    if nullable_cols_set:
        logger.info(
            "Null check — mandatory: %s | nullable (allowed): %s",
            mandatory_columns, sorted(nullable_cols_set),
        )

    dupes = df[df.duplicated(keep="first")]
    if not dupes.empty:
        dupes_path = get_rejected_dupes_path(table_name, config_id)
        os.makedirs(os.path.dirname(dupes_path), exist_ok=True)
        dupes.to_csv(dupes_path, index=False)
        logger.info("Dropped %d duplicates → %s", len(dupes), dupes_path)

    df_clean = df.drop_duplicates(keep="first")

    null_mask = df_clean[mandatory_columns].isnull().any(axis=1)
    null_rows = df_clean[null_mask]

    if not null_rows.empty:
        nulls_path = get_rejected_nulls_path(table_name, config_id)
        os.makedirs(os.path.dirname(nulls_path), exist_ok=True)
        null_rows.to_csv(nulls_path, index=False)
        logger.info("Dropped %d null rows → %s", len(null_rows), nulls_path)

    df_clean = df_clean.dropna(subset=mandatory_columns)
    # Output is always comma-CSV so write_pandas + Snowflake can read it uniformly
    df_clean.to_csv(clean_file_path, index=False)

    context["ti"].xcom_push(key="row_count", value=len(df_clean))
    logger.info(
        "Transformation OK — clean rows: %d, mandatory null-checked: %s",
        len(df_clean), mandatory_columns,
    )


# =============================================================================
# TASK 5 — LOAD TO SNOWFLAKE
# =============================================================================

def load_to_snowflake(**context):
    """
    Loads the clean CSV to Snowflake via write_pandas, updates LOADED_AT
    server-side, and always appends a row to ETL_LOG (success or failure).
    """
    from snowflake.connector.pandas_tools import write_pandas

    task_start_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    metadata = context["ti"].xcom_pull(task_ids="fetch_metadata", key="source_metadata")
    row_count = context["ti"].xcom_pull(task_ids="transform_file", key="row_count")

    config_id = metadata["config_id"]
    schema_name = metadata["schema_name"]
    table_name = metadata["table_name"]
    source_path = metadata["extract_location"]
    gd_path = metadata["gd_location"]

    clean_file_path = get_clean_file_path(table_name, config_id)

    if not os.path.exists(clean_file_path):
        raise FileNotFoundError(f"Clean file not found: {clean_file_path}")

    upload_df = pd.read_csv(clean_file_path)
    upload_df.columns = [c.upper() for c in upload_df.columns]

    # Final guard: keep only the columns declared in COLUMN_LIST.
    # This prevents any residual extra columns (e.g. 'Unnamed: X' from XLSX files)
    # from reaching write_pandas and causing SQL compilation errors in Snowflake.
    expected_cols = [c.strip().upper() for c in metadata["column_list"].split(",")]
    upload_df = upload_df[[col for col in expected_cols if col in upload_df.columns]]

    upload_df["SOURCE_RUN_ID"] = context["run_id"]
    # NOTE: LOADED_AT is intentionally excluded from write_pandas.
    # PyArrow serializes datetime64[ns] as nanosecond integers which Snowflake
    # cannot parse back to TIMESTAMP_NTZ. Instead we UPDATE after insert using
    # Snowflake's own CURRENT_TIMESTAMP() which is guaranteed to be valid.

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()

    table_load_status = "FAILED"
    table_load_message = None
    insertion_rowcount = 0

    try:
        success, _, n_rows, _ = write_pandas(
            conn=conn,
            df=upload_df,
            table_name=table_name.upper(),
            schema=schema_name.upper(),
            database=SNOWFLAKE_DATABASE,
            quote_identifiers=False,    # Columns are already uppercased; case-insensitive match
            use_logical_type=True,      # Ensures PyArrow uses proper DATE/TIME types
        )
        if success:
            table_load_status = "SUCCESS"
            insertion_rowcount = n_rows
            # Set LOADED_AT server-side to avoid PyArrow datetime precision issues.
            update_cursor = conn.cursor()
            try:
                update_cursor.execute(
                    f"UPDATE {SNOWFLAKE_DATABASE}.{schema_name.upper()}.{table_name.upper()} "
                    f"SET LOADED_AT = CURRENT_TIMESTAMP() WHERE SOURCE_RUN_ID = %s",
                    (context["run_id"],),
                )
                conn.commit()
                logger.info(
                    "Set LOADED_AT for %d rows in %s", insertion_rowcount, table_name.upper()
                )
            finally:
                update_cursor.close()
        else:
            table_load_message = "write_pandas returned success=False"
    except Exception as e:
        table_load_message = str(e)[:1000]
        raise
    finally:
        insert_sql = f"""
            INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_ETL_SCHEMA}.{SNOWFLAKE_LOG_TABLE} (
                CONFIG_ID, SOURCE_PATH, GD_PATH,
                SOURCE_FILE_NAME, FILE_STATUS, FILE_FORMAT,
                IMPORT_STARTTS, IMPORT_COMPLETETS, SOURCE_ROWCOUNT,
                TABLE_NAME, TABLE_LOAD_STATUS, INSERTION_ROWCOUNT, TABLE_LOAD_MESSAGE, TABLE_LOADTS,
                CREATED_AT, RUNID, DATADATE
            )
            VALUES (
                %s, %s, %s,
                %s, %s, %s,
                %s, CURRENT_TIMESTAMP(), %s,
                %s, %s, %s, %s, CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP(), %s, %s
            )
        """
        cursor = conn.cursor()
        try:
            cursor.execute(insert_sql, (
                config_id, source_path, gd_path,
                metadata["file_name_pattern"], table_load_status, metadata["file_format"],
                task_start_ts, row_count,
                table_name.upper(), table_load_status, insertion_rowcount, table_load_message,
                context["run_id"], context["ds"],
            ))
            conn.commit()
            logger.info(
                "Logged run to %s (status=%s, config_id=%s)",
                SNOWFLAKE_LOG_TABLE, table_load_status, config_id,
            )
        except Exception as log_err:
            # Log the error but do NOT re-raise — this prevents the log failure
            # from masking the original write_pandas exception if one was raised.
            logger.error("Failed to write ETL log entry: %s", log_err)
        finally:
            cursor.close()
            conn.close()


# =============================================================================
# SUCCESS CALLBACK
# =============================================================================

def on_etl_success(context):
    """Called by load_to_snowflake's on_success_callback."""
    metadata = context["ti"].xcom_pull(task_ids="fetch_metadata", key="source_metadata")
    row_count = context["ti"].xcom_pull(task_ids="transform_file", key="row_count")
    dag_success_alert(
        context,
        summary_lines=[
            f"Table: {metadata['table_name']}",
            f"Rows inserted: {row_count}",
            f"Config ID: {metadata['config_id']}",
            "ETL pipeline completed successfully!",
        ],
    )
