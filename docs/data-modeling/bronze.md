# Bronze Layer (Raw Ingestion)

This document describes the **Bronze Layer** of the Banking Lakehouse Platform. The Bronze layer stores raw data from PostgreSQL and Apache Kafka without modifying values.

For the original system specification, see [`source.md`](./source.md).

---

## 1. Role & Ingestion Flow

The Bronze layer is our **raw, immutable landing zone**. It keeps the original data structure from source systems and adds audit metadata.

```mermaid
flowchart TD
    subgraph SOURCE["1. Sources"]
        PG[("PostgreSQL 16 OLTP<br/>Master & Snapshot Tables")]
        KAFKA[["Apache Kafka<br/>CDC & Event Topics")]
    end

    subgraph JOBS["2. Ingestion Jobs (Apache Spark)"]
        BATCH["Batch JDBC Jobs<br/>(Periodic Extract)"]
        STREAM["Structured Streaming<br/>(10-second micro-batch)"]
    end

    subgraph BRONZE_STORAGE["3. Bronze Tables (s3://lakehouse/bronze/*)"]
        M_TABLES["Master Tables (6 tables)<br/>customers, accounts, cards,<br/>loans, merchants, branches"]
        F_TABLES["Financial Tables (4 tables)<br/>transactions, account_ledger,<br/>account_balance_snapshots, loan_payments"]
        T_TABLES["Telemetry & CX (4 tables)<br/>login_events, notifications,<br/>complaints, feedback"]
    end

    PG -->|"JDBC"| BATCH
    KAFKA -->|"Streaming"| STREAM

    BATCH --> M_TABLES
    BATCH -->|"EOD Snapshot"| F_TABLES
    STREAM --> F_TABLES
    STREAM --> T_TABLES
```

### Key Principles:
- **Keep Raw Data**: No rows are removed, and column values are not modified.
- **Audit Columns**: Each table adds two metadata fields:
  - `_ingested_at (TIMESTAMP)`: Time when the record arrived in the Lakehouse.
  - `_source_system (STRING)`: Name of the origin system (e.g., `core_banking_postgres`, `kafka_cdc_stream`).
- **Storage Location**: Stored as Apache Iceberg Parquet files in `s3://lakehouse/bronze/<table_name>/`.
- **Fault Tolerance**: Kafka streaming jobs save progress in `s3://checkpoints/bronze_<stream>/`.

---

## 2. Ingestion Methods

The 14 tables enter the Bronze layer in two ways:

| Ingestion Method | Entity Tables | Frequency | Source Script |
| :--- | :--- | :--- | :--- |
| **Batch JDBC** | `customers`, `accounts`, `cards`, `loans`, `merchants`, `branches`, `account_balance_snapshots` | Hourly / Daily | [`ingest_postgres.py`](../../pipeline/jobs/bronze/ingest_postgres.py) |
| **Streaming CDC** | `transactions`, `account_ledger`, `loan_payments`, `login_events`, `notifications`, `complaints`, `feedback` | Continuous (every 10s) | [`ingest_kafka.py`](../../pipeline/jobs/bronze/ingest_kafka.py) |

---

## 3. Bronze Table Definitions

All tables belong to the catalog namespace **`lakehouse.bronze`**.

### 3.1 Master & Reference Tables

#### `bronze.merchants`
Stores merchant directories and merchant category codes (MCC).
```sql
CREATE TABLE IF NOT EXISTS lakehouse.bronze.merchants (
    merchant_id INT,
    merchant_name STRING,
    merchant_category STRING,
    mcc_code STRING,
    merchant_type STRING,
    is_online BOOLEAN,
    _ingested_at TIMESTAMP,
    _source_system STRING
)
USING iceberg;
```

#### `bronze.branches`
Stores bank branch locations and regions.
```sql
CREATE TABLE IF NOT EXISTS lakehouse.bronze.branches (
    branch_code STRING,
    branch_name STRING,
    city STRING,
    state STRING,
    region STRING,
    branch_type STRING,
    open_date STRING,
    closure_date STRING,
    customer_weight INT,
    _ingested_at TIMESTAMP,
    _source_system STRING
)
USING iceberg;
```

#### `bronze.customers`
Stores customer profile records from the core banking CIF system.
```sql
CREATE TABLE IF NOT EXISTS lakehouse.bronze.customers (
    customer_id BIGINT,
    cif_number STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    date_of_birth STRING,
    gender STRING,
    marital_status STRING,
    occupation STRING,
    employment_type STRING,
    annual_income DOUBLE,
    address STRING,
    city STRING,
    state STRING,
    zipcode STRING,
    country STRING,
    lat DOUBLE,
    lon DOUBLE,
    customer_since STRING,
    persona STRING,
    kyc_status STRING,
    is_active BOOLEAN,
    _ingested_at TIMESTAMP,
    _source_system STRING
)
USING iceberg;
```

#### `bronze.accounts`
Stores customer deposit accounts (Savings and Current accounts).
```sql
CREATE TABLE IF NOT EXISTS lakehouse.bronze.accounts (
    account_id BIGINT,
    customer_id BIGINT,
    branch_code STRING,
    account_type STRING,
    account_status STRING,
    account_currency STRING,
    salary_account_flag BOOLEAN,
    open_date STRING,
    account_close_date STRING,
    overdraft_limit DOUBLE,
    current_balance DOUBLE,
    available_balance DOUBLE,
    _ingested_at TIMESTAMP,
    _source_system STRING
)
USING iceberg;
```

#### `bronze.cards`
Stores debit and credit card contracts.
```sql
CREATE TABLE IF NOT EXISTS lakehouse.bronze.cards (
    card_id BIGINT,
    customer_id BIGINT,
    card_type STRING,
    network STRING,
    issue_date STRING,
    expiry_date STRING,
    card_status STRING,
    primary_card_flag BOOLEAN,
    credit_limit DOUBLE,
    rewards_program STRING,
    reward_tier STRING,
    _ingested_at TIMESTAMP,
    _source_system STRING
)
USING iceberg;
```

#### `bronze.loans`
Stores loan contracts (personal, auto, and home loans).
```sql
CREATE TABLE IF NOT EXISTS lakehouse.bronze.loans (
    loan_id BIGINT,
    customer_id BIGINT,
    branch_code STRING,
    loan_type STRING,
    sanctioned_amount DOUBLE,
    interest_rate DOUBLE,
    tenure_months INT,
    emi_amount DOUBLE,
    loan_purpose STRING,
    origination_channel STRING,
    loan_status STRING,
    disbursement_date STRING,
    maturity_date STRING,
    _ingested_at TIMESTAMP,
    _source_system STRING
)
USING iceberg;
```

---

### 3.2 Financial & Accounting Tables

#### `bronze.transactions`
Stores financial transactions from payment and transfer channels.
```sql
CREATE TABLE IF NOT EXISTS lakehouse.bronze.transactions (
    transaction_id BIGINT,
    account_id BIGINT,
    customer_id BIGINT,
    received_customer_id BIGINT,
    received_account_id BIGINT,
    merchant_id BIGINT,
    card_id BIGINT,
    branch_code STRING,
    txn_timestamp TIMESTAMP,
    txn_type STRING,
    direction STRING,
    channel STRING,
    amount DOUBLE,
    currency STRING,
    transaction_category STRING,
    transaction_description STRING,
    balance_after_txn DOUBLE,
    is_salary_credit BOOLEAN,
    is_fee BOOLEAN,
    is_reversal BOOLEAN,
    is_fraud BOOLEAN,
    is_disputed BOOLEAN,
    risk_score DOUBLE,
    device_id STRING,
    ip_address STRING,
    geolocation STRING,
    status STRING,
    _ingested_at TIMESTAMP,
    _source_system STRING
)
USING iceberg;
```

#### `bronze.account_ledger`
Stores double-entry accounting records for each balance movement.
```sql
CREATE TABLE IF NOT EXISTS lakehouse.bronze.account_ledger (
    entry_id BIGINT,
    transaction_id BIGINT,
    account_id BIGINT,
    customer_id BIGINT,
    entry_timestamp TIMESTAMP,
    entry_type STRING,
    debit_amount DOUBLE,
    credit_amount DOUBLE,
    amount DOUBLE,
    running_balance DOUBLE,
    reference_number STRING,
    _ingested_at TIMESTAMP,
    _source_system STRING
)
USING iceberg;
```

#### `bronze.account_balance_snapshots`
Stores end-of-day balances for each account.
```sql
CREATE TABLE IF NOT EXISTS lakehouse.bronze.account_balance_snapshots (
    snapshot_id BIGINT,
    account_id BIGINT,
    customer_id BIGINT,
    snapshot_date STRING,
    snapshot_month STRING,
    end_of_day_balance DOUBLE,
    is_month_end BOOLEAN,
    _ingested_at TIMESTAMP,
    _source_system STRING
)
USING iceberg;
```

#### `bronze.loan_payments`
Stores monthly loan payment installments and overdue days (DPD).
```sql
CREATE TABLE IF NOT EXISTS lakehouse.bronze.loan_payments (
    payment_id BIGINT,
    loan_id BIGINT,
    customer_id BIGINT,
    payment_date STRING,
    emi_due_amount DOUBLE,
    emi_paid_amount DOUBLE,
    principal_paid DOUBLE,
    interest_paid DOUBLE,
    outstanding_balance DOUBLE,
    dpd_days INT,
    loan_status STRING,
    is_delinquent BOOLEAN,
    _ingested_at TIMESTAMP,
    _source_system STRING
)
USING iceberg;
```

---

### 3.3 Telemetry & Customer Experience Tables

#### `bronze.login_events`
Stores user login attempts on mobile app and internet banking.
```sql
CREATE TABLE IF NOT EXISTS lakehouse.bronze.login_events (
    session_id BIGINT,
    customer_id BIGINT,
    login_timestamp TIMESTAMP,
    channel STRING,
    device_type STRING,
    session_duration_seconds INT,
    page_views INT,
    logout_type STRING,
    is_successful BOOLEAN,
    failed_attempt_count INT,
    otp_used BOOLEAN,
    biometric_used BOOLEAN,
    _ingested_at TIMESTAMP,
    _source_system STRING
)
USING iceberg;
```

#### `bronze.notifications`
Stores marketing and transaction alerts sent to customers.
```sql
CREATE TABLE IF NOT EXISTS lakehouse.bronze.notifications (
    notification_id BIGINT,
    customer_id BIGINT,
    sent_at TIMESTAMP,
    channel STRING,
    notification_type STRING,
    opened BOOLEAN,
    opened_at TIMESTAMP,
    _ingested_at TIMESTAMP,
    _source_system STRING
)
USING iceberg;
```

#### `bronze.complaints`
Stores customer service tickets and resolution tracking.
```sql
CREATE TABLE IF NOT EXISTS lakehouse.bronze.complaints (
    complaint_id BIGINT,
    customer_id BIGINT,
    complaint_date STRING,
    channel STRING,
    category STRING,
    severity STRING,
    resolution_days INT,
    resolved_flag BOOLEAN,
    escalated_flag BOOLEAN,
    csat_score INT,
    status STRING,
    _ingested_at TIMESTAMP,
    _source_system STRING
)
USING iceberg;
```

#### `bronze.feedback`
Stores customer satisfaction (CSAT) and Net Promoter Score (NPS) survey answers.
```sql
CREATE TABLE IF NOT EXISTS lakehouse.bronze.feedback (
    feedback_id BIGINT,
    customer_id BIGINT,
    feedback_date STRING,
    survey_channel STRING,
    survey_topic STRING,
    nps_score INT,
    csat_score INT,
    _ingested_at TIMESTAMP,
    _source_system STRING
)
USING iceberg;
```

---

## 4. Storage & Maintenance

- **Storage Path**: `s3://lakehouse/bronze/<table_name>/`
- **File Format**: Parquet with Snappy compression.
- **Compaction**: Streaming writes create small files every 10 seconds. We run periodic compaction jobs (`rewrite_data_files`) to combine small files into 128MB–256MB files.
- **Snapshot Retention**: Snapshots older than 14 days are removed to save storage space.
