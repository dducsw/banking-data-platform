# Banking Lakehouse Data Modeling

This folder contains the data modeling documents for our **Banking Data Platform**. The architecture follows the **Medallion Design** (Bronze, Silver, Gold) using **Apache Iceberg**, **Apache Spark**, **Apache Kafka**, and **MinIO S3** storage (`s3://lakehouse/`).

All models are based on the core banking system specified in [`source.md`](./source.md).

---

## 1. Architecture Overview

Data moves through three main layers:
- **Bronze**: Stores raw, unmodified data from databases and streams.
- **Silver**: Cleans data, removes duplicates, and organizes it into a standard Star Schema.
- **Gold**: Combines and aggregates data into business-ready marts for dashboards and reports.

```mermaid
flowchart TD
    subgraph SOURCE["1. Source Systems (data-simulator)"]
        SRC_PG[("PostgreSQL 16 OLTP<br/>Master & Snapshot Tables")]
        SRC_KAFKA[["Apache Kafka Cluster<br/>Streaming Topics & CDC")]
    end

    subgraph BRONZE["2. Bronze Layer (lakehouse.bronze.*)"]
        B_CORE["Master Entities (6 tables)<br/>customers, accounts, cards,<br/>loans, merchants, branches"]
        B_FIN["Financial Records (3 tables)<br/>transactions, account_ledger,<br/>account_balance_snapshots"]
        B_LOAN["Loan Payments (1 table)<br/>loan_payments"]
        B_TELEM["Telemetry & CX (4 tables)<br/>login_events, notifications,<br/>complaints, feedback"]
    end

    subgraph SILVER["3. Silver Layer (lakehouse.silver.*)"]
        S_DIMS["Dimensions (6 tables)<br/>dim_customers, dim_accounts,<br/>dim_cards, dim_loans,<br/>dim_merchants, dim_branches"]
        S_FACTS["Fact Tables (8 tables)<br/>fact_transactions, fact_account_ledger,<br/>fact_account_balance_daily, fact_loan_payments,<br/>fact_login_events, fact_notifications,<br/>fact_complaints, fact_feedback"]
    end

    subgraph GOLD["4. Gold Marts (lakehouse.gold.*)"]
        G_C360["customer_360_summary<br/>(Customer profile & totals)"]
        G_BRANCH["daily_branch_summary<br/>(Branch cashflow & operations)"]
        G_MCC["merchant_category_analytics<br/>(Spending by category & channel)"]
        G_LOAN["loan_portfolio_risk_mart<br/>(Overdue loans & risk buckets)"]
        G_DIGITAL["digital_channel_engagement<br/>(App active users & sessions)"]
    end

    subgraph CONSUMERS["5. Consumers"]
        BI["Trino + Apache Superset<br/>(Dashboards & SQL queries)"]
    end

    SRC_PG -->|"Batch JDBC"| B_CORE
    SRC_KAFKA -->|"Streaming CDC"| B_FIN
    SRC_KAFKA -->|"Streaming CDC"| B_LOAN
    SRC_KAFKA -->|"Streaming Events"| B_TELEM

    B_CORE -->|"Cleanse & Merge"| S_DIMS
    B_FIN -->|"Validate & Partition"| S_FACTS
    B_LOAN -->|"Cleanse & Partition"| S_FACTS
    B_TELEM -->|"Normalize & Partition"| S_FACTS

    S_DIMS -->|"Aggregate"| G_C360
    S_FACTS -->|"Aggregate"| G_C360
    S_DIMS -->|"Group by branch"| G_BRANCH
    S_FACTS -->|"Group by branch"| G_BRANCH
    S_FACTS -->|"Group by category"| G_MCC
    S_FACTS -->|"Calculate risk"| G_LOAN
    S_FACTS -->|"Calculate usage"| G_DIGITAL

    G_C360 --> BI
    G_BRANCH --> BI
    G_MCC --> BI
    G_LOAN --> BI
    G_DIGITAL --> BI
```

---

## 2. Documentation Links

| Document | Layer | Description |
| :--- | :--- | :--- |
| **[`source.md`](./source.md)** | **Source Systems** | Operational database tables, fields, relationships, and data generator rules. |
| **[`bronze.md`](./bronze.md)** | **Bronze (Raw)** | Raw Iceberg table schemas, Kafka CDC ingestion, and batch JDBC pipelines. |
| **[`silver.md`](./silver.md)** | **Silver (Cleansed)** | Conformed Star Schema, data types (`DECIMAL(18,2)`, `DATE`), deduplication, and upsert rules. |
| **[`gold.md`](./gold.md)** | **Gold (Marts)** | Curated business marts for customer analysis, branch performance, and risk tracking. |

---

## 3. Entity Mapping Across Layers

The table below shows how the **14 banking entities** move through the platform:

| Entity Domain | Source Table ([`source.md`](./source.md)) | Bronze Table (`lakehouse.bronze.*`) | Silver Table (`lakehouse.silver.*`) | Main Gold Mart (`lakehouse.gold.*`) |
| :--- | :--- | :--- | :--- | :--- |
| **Master Data** | `customers` | `bronze.customers` | `dim_customers` | `customer_360_summary` |
| **Master Data** | `accounts` | `bronze.accounts` | `dim_accounts` | `customer_360_summary`, `daily_branch_summary` |
| **Master Data** | `cards` | `bronze.cards` | `dim_cards` | `customer_360_summary` |
| **Master Data** | `loans` | `bronze.loans` | `dim_loans` | `loan_portfolio_risk_mart`, `customer_360_summary` |
| **Master Data** | `merchants` | `bronze.merchants` | `dim_merchants` | `merchant_category_analytics` |
| **Master Data** | `branches` | `bronze.branches` | `dim_branches` | `daily_branch_summary` |
| **Financial** | `transactions` | `bronze.transactions` | `fact_transactions` | `daily_branch_summary`, `merchant_category_analytics`, `customer_360_summary` |
| **Financial** | `account_ledger` | `bronze.account_ledger` | `fact_account_ledger` | Ledger reconciliation & audit |
| **Financial** | `account_balance_snapshots` | `bronze.account_balance_snapshots`| `fact_account_balance_daily` | Balance monitoring |
| **Loan Servicing** | `loan_payments` | `bronze.loan_payments` | `fact_loan_payments` | `loan_portfolio_risk_mart` |
| **Telemetry** | `login_events` | `bronze.login_events` | `fact_login_events` | `digital_channel_engagement` |
| **Telemetry** | `notifications` | `bronze.notifications` | `fact_notifications` | `digital_channel_engagement` |
| **Customer Exp.** | `complaints` | `bronze.complaints` | `fact_complaints` | `customer_360_summary` |
| **Customer Exp.** | `feedback` | `bronze.feedback` | `fact_feedback` | `customer_360_summary` |

---

## 4. Storage & Catalog Structure

All tables use the **Apache Iceberg REST Catalog** (under catalog name `lakehouse`):

```
lakehouse (Catalog)
├── bronze (Namespace)  ---> s3://lakehouse/bronze/<table_name>/
├── silver (Namespace)  ---> s3://lakehouse/silver/<table_name>/
├── gold (Namespace)    ---> s3://lakehouse/gold/<table_name>/
└── metadata (Namespace)---> s3://lakehouse/metadata/
```

### Storage Locations:
- **`s3://lakehouse/`**: Main bucket for all Iceberg Parquet files and metadata.
- **`s3://checkpoints/`**: Spark Streaming checkpoints for streaming Kafka topics.
- **`s3://spark-logs/`**: Spark event logs for monitoring and history tracking.

---

## 5. Key Engineering Rules

1. **ACID Transactions**: Every write commits cleanly through Apache Iceberg. Readers always see consistent snapshots.
2. **Safe Reruns (Idempotency)**: Pipelines use `MERGE INTO` or partition overwrites so running a job twice does not create duplicate rows.
3. **Audit Columns**:
   - Bronze: `_ingested_at (TIMESTAMP)`, `_source_system (STRING)`
   - Silver: `_silver_processed_at (TIMESTAMP)`, `source_system (STRING)`
   - Gold: `_gold_processed_at (TIMESTAMP)`, `source_system (STRING)`
4. **Data Accuracy**: Money values always use `DECIMAL(18,2)` to prevent rounding errors.
