-- ====================================================================
-- STEP 3: SETUP PIPELINE CONFIGURATIONS
-- Run this AFTER 02_setup_raw_tables.sql
-- ====================================================================
-- Clears any existing configurations and inserts exactly 10 configs 
-- cleanly into the SOURCE_CONFIG table.
-- ====================================================================

TRUNCATE TABLE CUSTOMER_PIPELINE_DB.ETL.SOURCE_CONFIG;

INSERT INTO CUSTOMER_PIPELINE_DB.ETL.SOURCE_CONFIG (
    CONFIG_ID, VENDOR, EXTRACT_LOCATION, GD_LOCATION, SCHEMA_NAME,
    TABLE_NAME, FILE_NAME_PATTERN, FILE_FORMAT, DATA_DELIMITER,
    COLUMN_LIST, NULLABLE_COLUMNS, IS_ACTIVE
)
VALUES
    -- 1: CUSTOMER
    (1, 'GOOGLE_DRIVE',
     '1kALm-0ALQGv59jYSBksRw-LD_TOdrofh',
     'https://drive.google.com/file/d/1kALm-0ALQGv59jYSBksRw-LD_TOdrofh/view?usp=sharing',
     'RAW', 'CUSTOMER', 'customers_raw.csv', 'CSV', ',',
     'customer_id,name,city,country,signup_date,email,phone,age',
     '', TRUE),

    -- 2: ORDERS
    (2, 'GOOGLE_DRIVE',
     '1XFFWVsW7vM-M8YwPSFJkWimFd7dG7OeE',
     'https://drive.google.com/file/d/1XFFWVsW7vM-M8YwPSFJkWimFd7dG7OeE/view?usp=drive_link',
     'RAW', 'ORDERS', 'orders_raw.csv', 'CSV', ',',
     'order_id,customer_id,order_date,status,total_amount,product_name,category,quantity,discount',
     '', TRUE),

    -- 3: ORDER_ITEMS
    (3, 'GOOGLE_DRIVE',
     '1bnNFmEc4tp6GFImAcqjBJDKVodkZCtyT',
     'https://drive.google.com/file/d/1bnNFmEc4tp6GFImAcqjBJDKVodkZCtyT/view?usp=drive_link',
     'RAW', 'ORDER_ITEMS', 'order_items_raw.csv', 'CSV', ',',
     'order_item_id,order_id,product_id,quantity,unit_price',
     '', TRUE),

    -- 4: PAYMENTS
    (4, 'GOOGLE_DRIVE',
     '1vqYrSBAns4-Y2s_6AH_lUoslLe58FnkJ',
     'https://drive.google.com/file/d/1vqYrSBAns4-Y2s_6AH_lUoslLe58FnkJ/view?usp=drive_link',
     'RAW', 'PAYMENTS', 'payments_raw.csv', 'CSV', ',',
     'payment_id,order_id,payment_method,payment_date,amount,payment_status',
     '', TRUE),

    -- 5: PRODUCTS
    (5, 'GOOGLE_DRIVE',
     '18s4CT4d6hgMqZv5jCrMTSbX4I9jajb2i',
     'https://drive.google.com/file/d/18s4CT4d6hgMqZv5jCrMTSbX4I9jajb2i/view?usp=drive_link',
     'RAW', 'PRODUCTS', 'products_raw.csv', 'CSV', ',',
     'product_id,product_name,category,brand,price,stock_quantity,supplier',
     '', TRUE),

    -- 6: EMPLOYEES (XLSX — manager_id and salary are optional)
    (6, 'GOOGLE_DRIVE',
     '1LxtlTWtU0Zx15A08tWyCWwU4PzJXxCkG',
     'https://docs.google.com/spreadsheets/d/1LxtlTWtU0Zx15A08tWyCWwU4PzJXxCkG/edit?usp=drive_link',
     'RAW', 'EMPLOYEES', 'employees_raw.xlsx', 'XLSX', ',',
     'employee_id,name,department,hire_date,email,manager_id,salary',
     'manager_id,salary', TRUE),

    -- 7: RETURNS (TXT pipe-delimited — all columns mandatory)
    (7, 'GOOGLE_DRIVE',
     '1gUxGYDUMXt5Ed_SqoGsOjVS4mx5XbjBN',
     'https://drive.google.com/file/d/1gUxGYDUMXt5Ed_SqoGsOjVS4mx5XbjBN/view?usp=drive_link',
     'RAW', 'RETURNS', 'returns_raw.txt', 'TXT', '|',
     'return_id,order_id,customer_id,return_date,reason,refund_amount,status',
     '', TRUE),

    -- 8: REVIEWS (review_text is optional)
    (8, 'GOOGLE_DRIVE',
     '1hvwjpK1qKHE6lr7siSqux1eqwB3-R8DL',
     'https://drive.google.com/file/d/1hvwjpK1qKHE6lr7siSqux1eqwB3-R8DL/view?usp=drive_link',
     'RAW', 'REVIEWS', 'reviews_raw.csv', 'CSV', ',',
     'review_id,order_id,customer_id,rating,review_text,review_date',
     'review_text', TRUE),

    -- 9: SHIPMENTS (XLSX — carrier, delivery_date, shipping_cost are optional)
    (9, 'GOOGLE_DRIVE',
     '14i7fj_2y2_wCa-AFt90pcSczzJobc5kE',
     'https://docs.google.com/spreadsheets/d/14i7fj_2y2_wCa-AFt90pcSczzJobc5kE/edit?usp=drive_link',
     'RAW', 'SHIPMENTS', 'shipments_raw.xlsx', 'XLSX', ',',
     'shipment_id,order_id,carrier,ship_date,delivery_date,tracking_number,shipping_cost',
     'carrier,delivery_date,shipping_cost', TRUE),

    -- 10: SUPPLIERS (contact_email and contact_phone are optional)
    (10, 'GOOGLE_DRIVE',
     '1FtlFrMF6bBus7lbmFTiyzoLkRecpsTOH',
     'https://drive.google.com/file/d/1FtlFrMF6bBus7lbmFTiyzoLkRecpsTOH/view?usp=drive_link',
     'RAW', 'SUPPLIERS', 'suppliers_raw.csv', 'CSV', ',',
     'supplier_id,supplier_name,city,country,contact_email,contact_phone',
     'contact_email,contact_phone', TRUE);
