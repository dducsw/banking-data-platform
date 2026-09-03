-- ====================================================================
-- GOLD LAYER ICEBERG TABLES (lakehouse.gold)
-- ====================================================================

CREATE NAMESPACE IF NOT EXISTS lakehouse.gold;

CREATE TABLE IF NOT EXISTS lakehouse.gold.daily_branch_summary (
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
PARTITIONED BY (txn_date);

CREATE TABLE IF NOT EXISTS lakehouse.gold.customer_360_summary (
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
USING iceberg;
