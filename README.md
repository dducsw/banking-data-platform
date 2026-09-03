# Big Data Platform for Banking

A hands-on project to design and implement an end-to-end **Big Data Platform** for banking transaction data, integrating modern Lakehouse table formats, real-time CDC streaming, distributed query processing, and workflow orchestration on **Kubernetes (k3d)**.

---

## 🛠️ Tech Stack & Platform Architecture

| Category | Technology | Version | Role & Responsibilities |
| :--- | :--- | :--- | :--- |
| **Infrastructure** | Kubernetes (k3d) | `v1.31` | Local multi-node cluster orchestration (`bigdata-dev`) |
| **Object Storage** | MinIO (S3-compatible) | `latest` | Scalable object storage for raw data & Iceberg Parquet files (`s3://lakehouse/`) |
| **Metadata Catalog** | Apache Gravitino | `1.3.0` | Central Iceberg REST Catalog replacing legacy Hive Metastore |
| **Lakehouse Format** | Apache Iceberg | `1.11.0` | ACID transactions, schema evolution, snapshot isolation, and time travel |
| **Compute Engine** | Apache Spark | `4.1` | Distributed batch & streaming compute with native `S3FileIO` |
| **Streaming & CDC** | Apache Kafka + Debezium | `4.1` (KRaft) / `3.2` | Event streaming broker and Change Data Capture from Core Banking DB |
| **Query Engine** | Trino | `480` | High-performance distributed SQL query engine across Iceberg and PostgreSQL |
| **Workflow Orchestration** | Apache Airflow | `3.1.3` | Pipeline scheduling via KubernetesExecutor and S3 remote task logging |
| **Observability** | Prometheus | `3.14.0` | Cluster-wide metric collection, pod service discovery, and monitoring |
| **Source Database** | PostgreSQL | `16-alpine` | Core Banking OLTP transactional database simulation |

---

## 🎯 Platform Objectives

1. **End-to-End Big Data Architecture:** Build a unified platform supporting both batch ETL and real-time streaming for high-volume banking workloads.
2. **Medallion Data Architecture:** Structure data pipelines into distinct layers:
   - **Bronze:** Raw, immutable event logs and transactional snapshots.
   - **Silver:** Cleaned, deduplicated, and conformed Dimension & Fact tables.
   - **Gold:** Curated, aggregated business marts for analytics, risk scoring, and reporting.
3. **Modern Iceberg REST Integration:** Adopt **Apache Gravitino 1.3.0** with **`S3FileIO`** to eliminate legacy Thrift metastore bottlenecks and improve metadata performance.
4. **Zero-Trust & Secrets Management:** Store credentials in Kubernetes Secrets and automate container resource allocations to run reliably within a local 16GB RAM environment.

---

## 📂 Repository Structure

```text
├── data-simulator/         # Synthetic core banking data generator (Faker)
├── infra/                  # Infrastructure as Code (K8s, Docker, k3d)
│   ├── docker/             # Custom Dockerfiles for Spark 4.1 and Airflow 3.1
│   ├── k3d/                # Cluster configuration and automated provisioning
│   ├── k8s/base/           # K8s manifests (Postgres, MinIO, Gravitino, Trino, Kafka...)
│   └── k8s/overlays/dev/   # Kustomize dev overlay with tuned local-path storage
├── src/                    # Data Pipelines & Business Logic (Medallion Architecture)
│   ├── config/             # Centralized settings and environment constants
│   ├── core/               # Spark session builder and reusable utilities
│   ├── orchestration/      # Airflow DAGs for automated scheduling
│   └── pipeline/
│       ├── bronze/         # Ingestion: Batch & CDC ingestion into Iceberg Bronze
│       ├── silver/         # Transformation: Data cleansing, deduplication, Dim/Fact modeling
│       ├── gold/           # Aggregation: Business marts and reporting aggregations
│       ├── quality/        # Data quality assertions and schema validation checks
│       └── maintenance/    # Table compaction and snapshot lifecycle management
└── tests/                  # Automated unit tests for configs and DAGs
```

---

## 🚀 Quick Start

### Prerequisites
- **Docker Desktop** (with WSL2 on Windows or native Linux/macOS)
- **k3d** (`v5.5+`) & **kubectl**
- Python 3.10+ (for synthetic data generation)

### 1. Provision Platform
```bash
# Automatically pull images, create k3d cluster, and deploy all services
make up
```

### 2. Generate Banking Data
```bash
# Seed 1,000 customers with 12 months of transactions into PostgreSQL
python data-simulator/main.py --n-customers 1000 --sim-months 12 --init-db
```

### 3. Verify Cluster Health
```bash
make status
```

---

## 🌐 Services & Endpoints

| Service | Local Endpoint | Default Credentials | Description |
| :--- | :--- | :--- | :--- |
| **PostgreSQL** | `localhost:5432` | `postgres` / `postgres123` | Core Banking database (`banking`) |
| **MinIO Console** | `http://localhost:9001` | `admin` / `password123` | Object Storage management |
| **Gravitino Admin**| `http://localhost:8090` | _(No auth required)_ | Central Iceberg REST Catalog UI |
| **Trino Coordinator**| `http://localhost:8080` | `trino` | Distributed SQL query interface |
| **Airflow Webserver**| `http://localhost:8088` | `admin` / `admin` | Pipeline orchestration UI |
| **Prometheus** | `http://localhost:9090` | `make forward-prometheus` | Real-time metrics & monitoring |

---

## 🔍 Useful Commands

- **Open Trino SQL CLI:**
  ```bash
  make cli-trino
  # Run query: SELECT * FROM iceberg.banking.customers LIMIT 5;
  ```

- **Run Sample Spark Workloads on K8s:**
  ```bash
  make run-job-postgres   # Read OLTP data via Spark JDBC
  make run-job-iceberg    # Write ACID Iceberg table to MinIO S3
  ```

- **Run Automated Test Suite:**
  ```bash
  pytest tests/unit/ -v
  ```

---

## 💡 Key Takeaways from Building This Platform

1. **Iceberg REST vs. Hive Metastore:** Switching to Gravitino Iceberg REST with `S3FileIO` eliminated Thrift bottlenecks and simplified metadata management across both Spark and Trino engines.
2. **Resource Management on Kubernetes:** Sizing JVM memory options (`-Xmx`/`-Xms`) and configuring container requests/limits properly was essential to keep 10+ distributed services running stably on local hardware without `OOMKilled` crashes.
3. **Decoupled Architecture:** Using Medallion layers (`bronze/`, `silver/`, `gold/`) with automated data quality checks ensures reliable schema evolution and high data trustworthiness for downstream analytics.
