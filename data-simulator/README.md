# Bank Data Simulator

High-throughput, deterministic synthetic core banking universe generator simulating a Digital Banking OLTP source system. Generates complex, correlated banking entities (customers, accounts, cards, loans, transactions, ledgers, events) optimized for downstream Big Data Lakehouse Analytics, CDC, and Machine Learning (Churn, Fraud, PFM).

---

## 🏛️ Architecture
- **Engine**: Python + Polars (vectorized generation).
- **Data Sinks (Strategy Pattern)**:
  - `ParquetSink`: Multi-core Hive-partitioned Parquet generation.
  - `PostgresSink`: High-performance bulk loading (`COPY FROM STDIN` binary/csv) with automated schema deployment.
- **Data Model**: Normalized Core Banking & Digital Channel schema (17 tables).
- **Determinism**: Seed-based RNG ensures 100% reproducible datasets.

---

## 📊 Key Entities & Tables

| Category | Tables |
| :--- | :--- |
| **Party & Core Dimensions** | `customers`, `accounts`, `cards`, `loans`, `merchants`, `branches` |
| **Transactions & Accounting** | `transactions`, `account_ledger`, `account_balance_snapshots` |
| **Digital Engagement Telemetry** | `login_events`, `notifications` |
| **Loan Servicing** | `loan_payments` |
| **Customer Experience** | `complaints`, `feedback` |
| **ML & Ground Truth State** | `churn_simulation_state`, `customer_churn_label`, `churn_feature_snapshot` |

---

## 🚀 Usage Guide

### 1. Prerequisites
- Python 3.10+
- Dependencies: `pip install -r requirements.txt` or `uv sync` (includes `polars`, `psycopg2-binary`, `faker`, `numpy`)

---

### 2. Generate Data into PostgreSQL

#### Option A: Direct load into k3d PostgreSQL (from Host)
If running against the k3d cluster PostgreSQL service:
```bash
python main.py \
  --n-customers 1000 \
  --sim-months 12 \
  --postgres-uri "postgresql://postgres:postgres123@localhost:5432/banking" \
  --init-db
```

> **Note for Windows Host**: If port `5432` is occupied by a local PostgreSQL service, port-forward k3d PostgreSQL to `5434`:
> ```powershell
> kubectl port-forward svc/postgres -n postgres 5434:5432
> python main.py --n-customers 1000 --sim-months 12 --postgres-uri "postgresql://postgres:postgres123@127.0.0.1:5434/banking" --init-db
> ```

#### Option B: Inside Kubernetes Cluster (Job / Pod)
```bash
python main.py \
  --n-customers 1000 \
  --sim-months 12 \
  --postgres-uri "postgresql://postgres:postgres123@postgres.postgres.svc.cluster.local:5432/banking" \
  --init-db
```

---

### 3. Generate Only Partitioned Parquet Files
```bash
python main.py --n-customers 5000 --sim-months 24 --jobs 4 --output-dir ./data/raw
```

---

### 4. CLI Arguments Reference

| Argument | Default | Description |
| :--- | :--- | :--- |
| `--n-customers` | `1000` | Total customer universe size |
| `--sim-months` | `24` | Number of simulation months |
| `--seed` | `42` | RNG seed for deterministic runs |
| `--output-dir` | `./data/raw` | Target directory for partitioned Parquet files |
| `--no-parquet` | `False` | Skip writing Parquet files to disk |
| `--postgres-uri` | `None` | PostgreSQL connection string (`postgresql://user:pass@host:port/db`) |
| `--init-db` | `False` | Execute DDL schema recreation ([`pipeline/schema.sql`](pipeline/schema.sql)) before ingest |
| `--jobs` | `1` | Number of parallel worker processes |
| `--streaming` | `Auto` | Memory streaming mode for large-scale runs |

---

## 📁 Directory Structure

```text
data-simulator/
├── config/             # Domain configs (personas, events, simulation limits)
├── generator/          # Core entity generators (customers, accounts, txns, loans, etc.)
├── sinks/              # Pluggable output layer (ParquetSink, PostgresSink)
│   ├── base.py
│   ├── parquet_sink.py
│   └── postgres_sink.py
├── pipeline/           # Orchestration & DDL schema
│   ├── simulate.py     # Simulation engine
│   └── schema.sql      # PostgreSQL DDL
├── assets/             # Demographic & location reference files
├── tests/              # Pytest test suite
└── main.py             # CLI entrypoint
```
