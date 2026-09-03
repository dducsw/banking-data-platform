-- ====================================================================
-- BRONZE LAYER ICEBERG TABLES (lakehouse.bronze)
-- ====================================================================

CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze;

CREATE TABLE IF NOT EXISTS lakehouse.bronze.customers (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone_number STRING,
    date_of_birth STRING,
    gender STRING,
    address STRING,
    city STRING,
    state STRING,
    postal_code STRING,
    country STRING,
    identification_number STRING,
    occupation STRING,
    annual_income DOUBLE,
    customer_since STRING,
    is_active BOOLEAN,
    _ingested_at TIMESTAMP,
    _source_system STRING
)
USING iceberg;

CREATE TABLE IF NOT EXISTS lakehouse.bronze.accounts (
    account_id STRING,
    customer_id STRING,
    account_number STRING,
    account_type STRING,
    account_status STRING,
    currency STRING,
    current_balance DOUBLE,
    available_balance DOUBLE,
    interest_rate DOUBLE,
    open_date STRING,
    close_date STRING,
    branch_code STRING,
    overdraft_limit DOUBLE,
    _ingested_at TIMESTAMP,
    _source_system STRING
)
USING iceberg;

CREATE TABLE IF NOT EXISTS lakehouse.bronze.transactions (
    transaction_id BIGINT,
    account_id BIGINT,
    customer_id BIGINT,
    card_id BIGINT,
    merchant_id BIGINT,
    branch_code STRING,
    txn_type STRING,
    direction STRING,
    amount DOUBLE,
    currency STRING,
    status STRING,
    channel STRING,
    location_city STRING,
    device_type STRING,
    ip_address STRING,
    is_fraud BOOLEAN,
    is_disputed BOOLEAN,
    timestamp TIMESTAMP,
    _ingested_at TIMESTAMP,
    _source_system STRING
)
USING iceberg;
