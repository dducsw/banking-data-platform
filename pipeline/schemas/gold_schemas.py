"""
Apache Iceberg DDL schemas for Gold Curated Marts Layer.
"""

from pipeline.config.pipeline_config import CATALOG_NAME, NAMESPACE_GOLD

GOLD_DB_NAME = f"{CATALOG_NAME}.{NAMESPACE_GOLD}"

GOLD_DAILY_BRANCH_SUMMARY_DDL = f"""
CREATE TABLE IF NOT EXISTS {GOLD_DB_NAME}.daily_branch_summary (
    branch_code STRING,
    txn_date DATE,
    total_transactions BIGINT,
    total_inflow_amount DECIMAL(18,2),
    total_outflow_amount DECIMAL(18,2),
    total_gross_volume DECIMAL(18,2),
    fraud_txn_count BIGINT,
    disputed_txn_count BIGINT,
    net_flow_amount DECIMAL(18,2),
    _gold_processed_at TIMESTAMP,
    source_system STRING
)
USING iceberg
PARTITIONED BY (txn_date)
"""

GOLD_CUSTOMER_360_DDL = f"""
CREATE TABLE IF NOT EXISTS {GOLD_DB_NAME}.customer_360_summary (
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
    num_accounts BIGINT,
    num_active_accounts BIGINT,
    lifetime_txn_count BIGINT,
    lifetime_spend_amount DOUBLE,
    avg_ticket_size DOUBLE,
    last_active_date DATE,
    first_active_date DATE,
    _gold_processed_at TIMESTAMP,
    source_system STRING
)
USING iceberg
"""

GOLD_ALL_DDLS = [
    GOLD_DAILY_BRANCH_SUMMARY_DDL,
    GOLD_CUSTOMER_360_DDL,
]
