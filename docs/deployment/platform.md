# Platform Architecture & Kubernetes Manifests

This document details the Kubernetes manifests and operational flow for the Big Data Banking Platform under [`infra/k8s/overlays/dev/`](../../infra/k8s/overlays/dev).

---

## 1. Namespaces & Segmentation

The platform isolates components into 4 distinct namespaces:

- `postgres`: Houses the relational database storing Hive Metastore tables.
- `minio`: Houses S3 object storage for data lake buckets (`warehouse`, `lakehouse`, `raw`, `processed`, `curated`).
- `metastore`: Houses Apache Hive Metastore Thrift service (port `9083`).
- `spark`: Houses compute workloads, service accounts, RBAC, and driver/executor pods.

---

## 2. Component Details

### A. PostgreSQL (Metadata Backend)
- **Manifests**: [`infra/k8s/base/postgres/`](../../infra/k8s/base/postgres/)
- **Deployment**: StatefulSet (`postgres-0`) with 5Gi persistent volume.
- **Initialization**: ConfigMap `postgres-init-scripts` creates database `metastore`, user `metastore`, and grants full privileges on schema `public`.
- **Service**: LoadBalancer exposing port `5432` to host.

### B. MinIO (S3 Object Storage)
- **Manifests**: [`infra/k8s/base/minio/`](../../infra/k8s/base/minio/)
- **Deployment**: StatefulSet (`minio-0`) with 20Gi persistent volume.
- **Bucket Initializer**: Job `minio-init-buckets` uses `minio/mc` client to auto-create standard data lake buckets upon startup:
  - `warehouse/`
  - `lakehouse/`
  - `raw/`
  - `processed/`
  - `curated/`
  - `checkpoints/`
  - `spark-logs/`
- **Service**: LoadBalancer exposing port `9000` (S3 API) and `9001` (Console Web UI).

### C. Hive Metastore
- **Manifests**: [`infra/k8s/base/hive/`](../../infra/k8s/base/hive/)
- **Deployment**: Deployment `hive-metastore` using `banking/hive-metastore:3.1.3`.
- **Init Containers**:
  1. `wait-for-postgres`: Waits for `postgres:5432` to be ready.
  2. `wait-for-minio`: Waits for `minio:9000` to be ready.
  3. `schema-init`: Executes `/opt/hive/bin/schematool -dbType postgres -info` or `-initSchema -verbose` to automatically initialize the schema if not already initialized.
- **Configuration**: ConfigMap `hive-metastore-config` (`hive-site.xml`) configures HikariCP connection pooling, S3A warehouse paths, and disabled schema verification.
- **Service**: LoadBalancer exposing port `9083` to host.

### D. Spark & Iceberg Configuration
- **Manifests**: [`infra/k8s/base/spark/`](../../infra/k8s/base/spark/)
- **Configuration**: ConfigMap `spark-defaults` (`spark-defaults.conf`) pre-configures:
  - **Catalog**: `lakehouse` (`org.apache.iceberg.spark.SparkCatalog`) using Hive Metastore type.
  - **Catalog URI**: `thrift://hive-metastore.metastore.svc.cluster.local:9083`
  - **Warehouse**: `s3a://warehouse/`
  - **S3A Settings**: MinIO endpoint, explicit numeric timeouts and keepalive settings (`threads.keepalivetime=60`, `multipart.purge.age=86400`, `fast.upload=true`).
  - **Extensions**: `org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions`.

---

## 3. Deploying Manifests

```bash
kubectl apply -k infra/k8s/overlays/dev
```

### Checking Deployment Status:
```bash
kubectl get pods -A
```

---

## 4. Kubernetes Spark Job Manifests

Spark jobs are packaged as standard Kubernetes `batch/v1` Job manifests in [`infra/k8s/jobs/`](../../infra/k8s/jobs/):

### A. Job: Read PostgreSQL & Analytical Aggregation
- **Manifest**: [`infra/k8s/jobs/job-spark-read-postgres.yaml`](../../infra/k8s/jobs/job-spark-read-postgres.yaml)
- **Execution**:
  ```bash
  # Via Makefile
  make run-job-postgres

  # Or via kubectl
  kubectl create configmap spark-job-read-postgres --from-file=spark_read_postgres.py=src/pipeline/example/spark_read_postgres.py -n spark --dry-run=client -o yaml | kubectl apply -f -
  kubectl apply -f infra/k8s/jobs/job-spark-read-postgres.yaml
  kubectl logs -n spark -l app=spark-read-postgres -f
  ```

### B. Job: Write & Query Apache Iceberg Tables
- **Manifest**: [`infra/k8s/jobs/job-spark-write-iceberg.yaml`](../../infra/k8s/jobs/job-spark-write-iceberg.yaml)
- **Execution**:
  ```bash
  # Via Makefile
  make run-job-iceberg

  # Or via kubectl
  kubectl create configmap spark-job-write-iceberg --from-file=spark_write_iceberg.py=src/pipeline/example/spark_write_iceberg.py -n spark --dry-run=client -o yaml | kubectl apply -f -
  kubectl apply -f infra/k8s/jobs/job-spark-write-iceberg.yaml
  kubectl logs -n spark -l app=spark-write-iceberg -f
  ```
