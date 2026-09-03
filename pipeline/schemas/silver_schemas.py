"""
Apache Iceberg DDL schemas for Silver Stage Layer.
"""

from pipeline.config.pipeline_config import CATALOG_NAME, NAMESPACE_SILVER

SILVER_DB_NAME = f"{CATALOG_NAME}.{NAMESPACE_SILVER}"

STAGE_DIM_CUSTOMERS_DDL = f"""
CREATE TABLE IF NOT EXISTS {SILVER_DB_NAME}.dim_customers (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    full_name STRING,
    email STRING,
    phone_number STRING,
    gender STRING,
    address STRING,
    city STRING,
    state STRING,
    postal_code STRING,
    country STRING,
    identification_number STRING,
    occupation STRING,
    date_of_birth DATE,
    customer_since DATE,
    annual_income DECIMAL(18,2),
    is_active BOOLEAN,
    _silver_processed_at TIMESTAMP,
    source_system STRING
)
USING iceberg
"""

STAGE_DIM_ACCOUNTS_DDL = f"""
CREATE TABLE IF NOT EXISTS {SILVER_DB_NAME}.dim_accounts (
    account_id STRING,
    customer_id STRING,
    account_number STRING,
    account_type STRING,
    account_status STRING,
    currency STRING,
    current_balance DOUBLE,
    available_balance DOUBLE,
    interest_rate DOUBLE,
    open_date DATE,
    close_date STRING,
    branch_code STRING,
    overdraft_limit DECIMAL(18,2),
    _silver_processed_at TIMESTAMP,
    source_system STRING
)
USING iceberg
"""

STAGE_FACT_TRANSACTIONS_DDL = f"""
CREATE TABLE IF NOT EXISTS {SILVER_DB_NAME}.fact_transactions (
    transaction_id BIGINT,
    account_id BIGINT,
    customer_id BIGINT,
    card_id BIGINT,
    merchant_id BIGINT,
    branch_code STRING,
    txn_type STRING,
    direction STRING,
    amount DECIMAL(18,2),
    currency STRING,
    status STRING,
    channel STRING,
    location_city STRING,
    device_type STRING,
    ip_address STRING,
    is_fraud BOOLEAN,
    is_disputed BOOLEAN,
    timestamp TIMESTAMP,
    txn_date DATE,
    _silver_processed_at TIMESTAMP,
    source_system STRING
)
USING iceberg
PARTITIONED BY (txn_date)
"""

SILVER_ALL_DDLS = [
    STAGE_DIM_CUSTOMERS_DDL,
    STAGE_DIM_ACCOUNTS_DDL,
    STAGE_FACT_TRANSACTIONS_DDL,
]
