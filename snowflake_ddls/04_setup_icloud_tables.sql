-- =================================================================
-- 04_setup_icloud_tables.sql
-- Creates the 3 new RAW tables for iCloud Drive source files and
-- inserts their configuration rows into SOURCE_CONFIG.
--
-- Run this entire script in Snowflake as ACCOUNTADMIN (or as
-- AIRFLOW_SERVICE_USER if you have CREATE TABLE privilege).
-- =================================================================

USE DATABASE CUSTOMER_PIPELINE_DB;
USE SCHEMA RAW;

-- -----------------------------------------------------------------
-- TABLE 1: ADDRESSES (from addresses_raw.csv)
-- -----------------------------------------------------------------
CREATE OR REPLACE TABLE CUSTOMER_PIPELINE_DB.RAW.ADDRESSES (
    ADDRESS_ID      VARCHAR(100),
    CUSTOMER_ID     VARCHAR(100),
    ADDRESS_LINE    VARCHAR(500),
    CITY            VARCHAR(100),
    STATE           VARCHAR(100),
    PINCODE         VARCHAR(20),
    ADDRESS_TYPE    VARCHAR(50),
    -- Pipeline audit columns (added automatically, never in source file)
    LOADED_AT       TIMESTAMP_NTZ(9),
    SOURCE_RUN_ID   VARCHAR(255)
);

-- -----------------------------------------------------------------
-- TABLE 2: DISCOUNTS (from discounts_raw.txt — TAB delimited)
-- -----------------------------------------------------------------
CREATE OR REPLACE TABLE CUSTOMER_PIPELINE_DB.RAW.DISCOUNTS (
    DISCOUNT_ID       VARCHAR(100),
    ORDER_ID          VARCHAR(100),
    COUPON_CODE       VARCHAR(100),
    DISCOUNT_PERCENT  NUMBER(5,2),
    DISCOUNT_AMOUNT   NUMBER(12,2),
    APPLIED_DATE      DATE,
    -- Pipeline audit columns
    LOADED_AT         TIMESTAMP_NTZ(9),
    SOURCE_RUN_ID     VARCHAR(255)
);

-- -----------------------------------------------------------------
-- TABLE 3: INVENTORY_LOG (from inventory_log_raw.xlsx)
-- -----------------------------------------------------------------
CREATE OR REPLACE TABLE CUSTOMER_PIPELINE_DB.RAW.INVENTORY_LOG (
    LOG_ID            VARCHAR(100),
    PRODUCT_ID        VARCHAR(100),
    WAREHOUSE         VARCHAR(100),
    TRANSACTION_TYPE  VARCHAR(50),
    QUANTITY          NUMBER(12,0),
    TRANSACTION_DATE  DATE,
    REMARKS           VARCHAR(1000),
    -- Pipeline audit columns
    LOADED_AT         TIMESTAMP_NTZ(9),
    SOURCE_RUN_ID     VARCHAR(255)
);

-- Grant Airflow service user access to all 3 new tables
GRANT SELECT, INSERT, UPDATE, DELETE
    ON TABLE CUSTOMER_PIPELINE_DB.RAW.ADDRESSES
    TO ROLE AIRFLOW_LOADER_ROLE;

GRANT SELECT, INSERT, UPDATE, DELETE
    ON TABLE CUSTOMER_PIPELINE_DB.RAW.DISCOUNTS
    TO ROLE AIRFLOW_LOADER_ROLE;

GRANT SELECT, INSERT, UPDATE, DELETE
    ON TABLE CUSTOMER_PIPELINE_DB.RAW.INVENTORY_LOG
    TO ROLE AIRFLOW_LOADER_ROLE;

-- Verify tables were created
SHOW TABLES IN SCHEMA CUSTOMER_PIPELINE_DB.RAW;

-- =================================================================
-- INSERT SOURCE_CONFIG ROWS
-- These 3 rows tell Airflow:
--   1. WHERE to find the file (relative path inside iCloud mount)
--   2. WHAT the file format is
--   3. WHAT columns to expect
--   4. WHICH columns are allowed to be empty
--
-- IMPORTANT: The EXTRACT_LOCATION is the RELATIVE PATH inside
-- iCloud Drive as mounted in Docker at /opt/airflow/icloud/
-- So "Downloads/Airflow/addresses_raw.csv" means the file is at
-- /opt/airflow/icloud/Downloads/Airflow/addresses_raw.csv
-- =================================================================

USE SCHEMA ETL;

-- =================================================================
-- STEP 1: Remove the incorrectly interleaved iCloud rows
-- (They got CONFIG_IDs 2, 4, 6 instead of 11, 12, 13)
-- =================================================================
DELETE FROM CUSTOMER_PIPELINE_DB.ETL.SOURCE_CONFIG
WHERE VENDOR = 'ICLOUD_LOCAL';

-- =================================================================
-- STEP 2: Re-insert iCloud rows with explicit CONFIG_IDs 11, 12, 13
-- Using explicit IDs keeps numbering clean regardless of sequence state:
--   1–10  → Google Drive (set by 03_setup_pipeline_configs.sql)
--   11–13 → iCloud Local
--   14+   → any future source
-- =================================================================
INSERT INTO CUSTOMER_PIPELINE_DB.ETL.SOURCE_CONFIG (
    CONFIG_ID,
    VENDOR,
    EXTRACT_LOCATION,
    GD_LOCATION,
    SCHEMA_NAME,
    TABLE_NAME,
    FILE_NAME_PATTERN,
    FILE_FORMAT,
    DATA_DELIMITER,
    COLUMN_LIST,
    NULLABLE_COLUMNS,
    IS_ACTIVE
)
VALUES
-- 11: ADDRESSES (CSV, comma delimited)
(11, 'ICLOUD_LOCAL',
 'Downloads/Airflow/addresses_raw.csv',
 'iCloud Drive/Downloads/Airflow/addresses_raw.csv',
 'RAW', 'ADDRESSES', 'addresses_raw.csv', 'CSV', ',',
 'address_id,customer_id,address_line,city,state,pincode,address_type',
 '', TRUE),

-- 12: DISCOUNTS (TXT, TAB delimited)
(12, 'ICLOUD_LOCAL',
 'Downloads/Airflow/discounts_raw.txt',
 'iCloud Drive/Downloads/Airflow/discounts_raw.txt',
 'RAW', 'DISCOUNTS', 'discounts_raw.txt', 'TXT', '\t',
 'discount_id,order_id,coupon_code,discount_percent,discount_amount,applied_date',
 '', TRUE),

-- 13: INVENTORY_LOG (XLSX — remarks may be empty)
(13, 'ICLOUD_LOCAL',
 'Downloads/Airflow/inventory_log_raw.xlsx',
 'iCloud Drive/Downloads/Airflow/inventory_log_raw.xlsx',
 'RAW', 'INVENTORY_LOG', 'inventory_log_raw.xlsx', 'XLSX', ',',
 'log_id,product_id,warehouse,transaction_type,quantity,transaction_date,remarks',
 'remarks', TRUE);

-- =================================================================
-- Verify: You should now see exactly 13 rows in order
--   1–10  GOOGLE_DRIVE
--   11–13 ICLOUD_LOCAL
-- =================================================================
SELECT
    CONFIG_ID,
    VENDOR,
    TABLE_NAME,
    EXTRACT_LOCATION,
    FILE_FORMAT,
    IS_ACTIVE
FROM CUSTOMER_PIPELINE_DB.ETL.SOURCE_CONFIG
ORDER BY CONFIG_ID;
