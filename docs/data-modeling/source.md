# Source Data System Specification (`data-simulator`)

This document details the source data architecture, system characteristics, generation mechanisms, and relational contracts simulated by `data-simulator` for the **Banking Lakehouse Platform**.

---

## 1. System Role & Architecture Context

The `data-simulator` simulates a production-grade **Digital Core Banking OLTP System** and its peripheral digital channels. In our Lakehouse architecture, this component acts as the upstream operational system generating transactional operations and telemetry events.

```
┌─────────────────────────────────────────────────────────────────┐
│                    Core Banking Source System                   │
│                        (data-simulator)                         │
└───────────────┬─────────────────────────────────┬───────────────┘
                │ Bulk / Batch                    │ Change Data Capture (CDC)
                ▼                                 ▼
    ┌───────────────────────┐         ┌───────────────────────┐
    │     PostgreSQL 16     │         │   Debezium Connector  │
    │  (OLTP Relational DB) │         │    (WAL Logical Logs) │
    └───────────┬───────────┘         └───────────┬───────────┘
                │ JDBC Ingestion                  │ Event Streaming
                ▼                                 ▼
    ┌───────────────────────┐         ┌───────────────────────┐
    │     Apache Spark      │         │     Apache Kafka      │
    │    Batch Pipeline     │         │    (KRaft Cluster)    │
    └───────────┬───────────┘         └───────────┬───────────┘
                │                                 │ Spark Structured Streaming
                └───────────────┬─────────────────┘
                                ▼
                 ┌─────────────────────────────┐
                 │    Apache Iceberg Bronze    │
                 │   (MinIO Object Storage)    │
                 └─────────────────────────────┘
```

### Key Technical Properties:
- **Vectorized Generation Engine**: Implemented in Python using Polars and NumPy for memory-efficient and high-throughput data creation.
- **Pluggable Output Sinks (Strategy Pattern)**:
  - `PostgresSink`: Connects directly to PostgreSQL using binary `COPY FROM STDIN` streaming to simulate a live OLTP database.
  - `ParquetSink`: Generates partitioned Parquet files (`data-simulator/data/raw/`) for local offline development, testing, and cold bootstrapping.
- **Deterministic Simulation**: Governed by an immutable seed RNG, enabling 100% reproducible event flows across runs.

---

## 2. Ingestion Patterns & Downstream Interfaces

The simulated data flows into the lakehouse via two primary patterns:

1. **Batch Ingestion (Master & Snapshot Entities)**:
   - Tables such as `branches`, `merchants`, `customers`, `accounts`, `cards`, `loans`, and `account_balance_snapshots` are ingested via periodic batch runs ([`ingest_postgres.py`](../../pipeline/jobs/bronze/ingest_postgres.py)) using Spark JDBC.
2. **Streaming Ingestion & CDC (High-Velocity Operational Events & Telemetry)**:
   - Tables with continuous write traffic (`transactions`, `login_events`, `notifications`, `complaints`, `feedback`, `loan_payments`) are captured at transaction log level using **Debezium CDC** or streamed through **Apache Kafka** topics, then consumed into Bronze Iceberg tables via Spark Structured Streaming ([`ingest_kafka.py`](../../pipeline/jobs/bronze/ingest_kafka.py)).

---

## 3. Entity Domain Model & Data Groupings

The source system schema comprises **14 relational tables** categorized into 5 primary domains:

| Domain | Entity Tables | Update Cadence & Nature | Ingestion Pattern |
| :--- | :--- | :--- | :--- |
| **Core Party & Master Tables** | `customers`, `accounts`, `cards`, `loans`, `merchants`, `branches` | Operational reference and customer agreement profiles | Batch JDBC / CDC |
| **Financial Movements & Ledger** | `transactions`, `account_ledger`, `account_balance_snapshots` | High-frequency append-only financial journal | CDC / Kafka Streaming |
| **Loan Servicing** | `loan_payments` | Periodic monthly billing & settlement events | Batch / Streaming CDC |
| **Digital Channel Telemetry** | `login_events`, `notifications` | Append-only session logs and marketing dispatches | Kafka Streaming |
| **Customer Experience** | `complaints`, `feedback` | Transactional tickets, lifecycle status transitions, surveys | CDC / Event Streaming |

---

## 4. Entity-Relationship Schema Overview

```
                      ┌───────────────┐
                      │   branches    │
                      └───────┬───────┘
                              │ branch_code
                              ▼
┌───────────────┐      ┌───────────────┐
│   merchants   │      │   customers   │
└───────┬───────┘      └───────┬───────┘
        │                      │ customer_id
        │         ┌────────────┼────────────┐
        │         ▼            ▼            ▼
        │   ┌───────────┐ ┌─────────┐ ┌───────────┐
        │   │ accounts  │ │  cards  │ │   loans   │
        │   └─────┬─────┘ └─────────┘ └─────┬─────┘
        │         │ account_id              │ loan_id
        │         ├────────────────┐        ▼
        │         ▼                ▼  ┌───────────────┐
        │   ┌──────────────┐       │  │ loan_payments │
        │   │  acct_ledger │       │  └───────────────┘
        │   └──────────────┘       │
        ▼                          ▼
┌───────────────────┐      ┌───────────────────┐
│   transactions    │─────►│acct_bal_snapshots │
└───────────────────┘      └───────────────────┘

Customer Experience & Telemetry:
┌───────────────┐  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐
│ login_events  │  │ notifications │  │  complaints   │  │   feedback    │
└───────────────┘  └───────────────┘  └───────────────┘  └───────────────┘
```

---

## 5. Source Table Dictionary

### 5.1 Core Party & Master Tables

#### `merchants`
Normalized merchant directory with ISO 18245 Merchant Category Codes (MCC) for spending categorization.
- **Primary Key**: `merchant_id (INT)`
- **Grain**: One record per merchant establishment.
- **Key Fields**:
  - `merchant_name (VARCHAR(150))` — Merchant legal/trade name.
  - `merchant_category (VARCHAR(100))` — Industry category (e.g., Grocery, Dining, Travel).
  - `mcc_code (VARCHAR(4))` — Standard 4-digit MCC code.
  - `merchant_type (VARCHAR(20))` — `physical`, `online`, or `internal`.
  - `is_online (BOOLEAN)` — Indicator for e-commerce transactions.

#### `branches`
Physical and digital branch network directory.
- **Primary Key**: `branch_code (VARCHAR(20))`
- **Grain**: One record per bank branch.
- **Key Fields**: `branch_name`, `city`, `state`, `region`, `branch_type`, `open_date`, `closure_date`, `customer_weight`.

#### `customers`
Customer Information File (CIF) master holding customer profiles and demographic attributes.
- **Primary Key**: `customer_id (BIGINT)`
- **Grain**: One record per onboarded banking client.
- **Key Fields**:
  - `cif_number (VARCHAR(20))` — Unique business identifier.
  - Demographics: `first_name`, `last_name`, `date_of_birth`, `gender`, `marital_status`, `occupation`, `employment_type`.
  - Geographic/Contact: `address`, `city`, `state`, `zipcode`, `country`, `lat`, `lon`, `email`, `phone`.
  - Financial/Risk: `annual_income (NUMERIC(18,2))`, `customer_since (DATE)`, `persona (VARCHAR(50))`, `kyc_status (VARCHAR(20))`, `is_active (BOOLEAN)`.

#### `accounts`
Deposit accounts (CASA) owned by customers.
- **Primary Key**: `account_id (BIGINT)`
- **Foreign Keys**: `customer_id -> customers`, `branch_code -> branches`
- **Grain**: One record per opened deposit account.
- **Key Fields**:
  - `account_type (VARCHAR(50))` — `Savings` or `Current`.
  - `open_date (DATE)`, `account_close_date (DATE, NULL)`.
  - `account_status (VARCHAR(20))` — `Active`, `Dormant`, `Blocked`, `Closed`.
  - `account_currency (VARCHAR(3))` — Standard ISO currency code (e.g., `USD`, `VND`).
  - `salary_account_flag (BOOLEAN)` — Identifies designated payroll accounts.
  - `overdraft_limit (NUMERIC(18,2))` — Approved line of credit.

#### `cards`
Debit and credit cards issued to bank clients.
- **Primary Key**: `card_id (BIGINT)`
- **Foreign Keys**: `customer_id -> customers`
- **Grain**: One record per physical/virtual card cardholder relationship.
- **Key Fields**:
  - `card_type (VARCHAR(20))` — `Debit` or `Credit`.
  - `network (VARCHAR(20))` — `Visa`, `Mastercard`, `RuPay`, `Napas`.
  - `issue_date (DATE)`, `expiry_date (DATE)`.
  - `card_status (VARCHAR(20))` — `Active`, `Blocked`, `Expired`.
  - `primary_card_flag (BOOLEAN)` — Primary account card flag.
  - `credit_limit (NUMERIC(18,2))` — Total authorized revolving line.
  - `rewards_program (VARCHAR(50))`, `reward_tier (VARCHAR(20))`.

#### `loans`
Disbursed retail loans and term borrowing contracts.
- **Primary Key**: `loan_id (BIGINT)`
- **Foreign Keys**: `customer_id -> customers`, `branch_code -> branches`
- **Grain**: One record per credit facility contract.
- **Key Fields**:
  - `loan_type (VARCHAR(50))` — `Personal Loan`, `Home Loan`, `Auto Loan`.
  - `sanctioned_amount (NUMERIC(18,2))` — Total principal disbursed.
  - `interest_rate (NUMERIC(6,3))` — Annual percentage rate (APR).
  - `tenure_months (INT)`, `emi_amount (NUMERIC(18,2))`.
  - `loan_purpose (VARCHAR(100))`, `origination_channel (VARCHAR(50))`.
  - `loan_status (VARCHAR(20))` — `Active`, `Closed`, `Defaulted`.
  - `disbursement_date (DATE)`, `maturity_date (DATE)`.

---

### 5.2 Financial Movements & Accounting Records

#### `transactions`
Granular financial debit and credit transactions across all payment channels.
- **Primary Key**: `transaction_id (BIGINT)`
- **Foreign Keys**: `account_id -> accounts`, `customer_id -> customers`, `received_customer_id -> customers`, `received_account_id -> accounts`, `merchant_id -> merchants`
- **Partition Key**: `txn_date (DATE)` / `txn_month (DATE)`
- **Grain**: One row per atomic transaction attempt/execution.
- **Key Fields**:
  - `txn_timestamp (TIMESTAMP)`, `txn_type (VARCHAR(50))` (e.g., `Transfer`, `POS`, `ATM`, `Salary`, `Fee`).
  - `direction (VARCHAR(10))` — `Credit` or `Debit`.
  - `channel (VARCHAR(30))` — `Mobile App`, `Internet Banking`, `ATM`, `POS`, `Branch`.
  - `amount (NUMERIC(18,2))`, `currency (VARCHAR(3))`.
  - `transaction_category (VARCHAR(100))`, `transaction_description (VARCHAR(150))`.
  - Audit/Compliance: `is_salary_credit`, `is_fee`, `is_reversal`, `balance_after_txn`, `is_fraud`, `is_disputed`, `risk_score`.
  - Technical Trace: `device_id`, `ip_address`, `geolocation`.

#### `account_ledger`
Double-entry accounting journal tracking historical balance movements.
- **Primary Key**: `entry_id (BIGINT)`
- **Foreign Keys**: `transaction_id -> transactions`, `account_id -> accounts`, `customer_id -> customers`
- **Grain**: One entry per account ledger debit or credit.
- **Key Fields**: `entry_timestamp`, `entry_type`, `debit_amount`, `credit_amount`, `amount`, `running_balance`, `reference_number`.

#### `account_balance_snapshots`
End-of-day balances for point-in-time financial reporting.
- **Primary Key**: `snapshot_id (BIGINT)`
- **Foreign Keys**: `account_id -> accounts`, `customer_id -> customers`
- **Grain**: One snapshot record per account per calendar day.
- **Key Fields**: `snapshot_date`, `snapshot_month`, `end_of_day_balance`, `is_month_end`.

#### `loan_payments`
Periodic installment repayments and repayment delinquency tracking.
- **Primary Key**: `payment_id (BIGINT)`
- **Foreign Keys**: `loan_id -> loans`, `customer_id -> customers`
- **Grain**: One payment record per loan per monthly billing cycle.
- **Key Fields**: `payment_date`, `emi_due_amount`, `emi_paid_amount`, `principal_paid`, `interest_paid`, `outstanding_balance`, `dpd_days`, `loan_status`, `is_delinquent`.

---

### 5.3 Digital Telemetry & Experience Tables

#### `login_events`
Authentication and session access logs across mobile and web banking channels.
- **Primary Key**: `session_id (BIGINT)`
- **Foreign Keys**: `customer_id -> customers`
- **Grain**: One record per login session attempt.
- **Key Fields**: `login_timestamp`, `channel`, `device_type`, `session_duration_seconds`, `page_views`, `logout_type`, `is_successful`, `failed_attempt_count`, `otp_used`, `biometric_used`.

#### `notifications`
Direct marketing campaigns and transactional alert logs.
- **Primary Key**: `notification_id (BIGINT)`
- **Foreign Keys**: `customer_id -> customers`
- **Grain**: One record per dispatched alert/message.
- **Key Fields**: `sent_at`, `channel`, `notification_type`, `opened (BOOLEAN)`, `opened_at (TIMESTAMP)`.

#### `complaints`
Formal customer service tickets and resolution tracking.
- **Primary Key**: `complaint_id (BIGINT)`
- **Foreign Keys**: `customer_id -> customers`
- **Grain**: One record per submitted customer grievance.
- **Key Fields**: `complaint_date`, `channel`, `category`, `severity`, `resolution_days`, `resolved_flag`, `escalated_flag`, `csat_score`, `status`.

#### `feedback`
Customer sentiment feedback and periodic survey responses.
- **Primary Key**: `feedback_id (BIGINT)`
- **Foreign Keys**: `customer_id -> customers`
- **Grain**: One response per customer survey interaction.
- **Key Fields**: `feedback_date`, `survey_channel`, `survey_topic`, `nps_score (0-10)`, `csat_score (1-5)`.

---

## 6. Data Integrity & Contract Guarantees

The source data produced by `data-simulator` adheres to strict banking domain constraints:
1. **Mathematical Ledger Consistency**:
   $$\text{balance\_after\_txn} = \text{previous\_balance} \pm \text{amount}$$
   $$\text{running\_balance} \text{ in } \texttt{account\_ledger} \equiv \text{balance\_after\_txn} \text{ in } \texttt{transactions}$$
2. **Referential Integrity**: All transaction records reference valid, active `accounts` and `customers`. All card and loan products are bound to verified customers.
3. **Data Types & Temporal Boundaries**: Dates and timestamps conform to ISO 8601 UTC standard. Currency amounts are strictly numeric (`NUMERIC(18,2)`).
