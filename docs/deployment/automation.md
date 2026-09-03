# Deployment Automation & Troubleshooting

This document outlines the available automation workflows and troubleshooting steps for common infrastructure issues.

---

## 1. Automation Workflows

### Makefile Cheat Sheet

| Command | Action |
| :--- | :--- |
| `make help` | Displays list of available Makefile targets |
| `make up` | Starts the k3d cluster via `infra/k3d/cluster-dev.sh` |
| `make down` | Deletes the `bigdata-dev` k3d cluster |
| `make build-all` | Builds both `banking/spark:4.1` and `banking/hive-metastore:3.1.3` |
| `make import-all` | Imports both Docker images into the k3d cluster |
| `make deploy-lakehouse` | Applies `infra/k8s/overlays/dev` via Kustomize |
| `make status` | Displays the status of all pods across all namespaces |
| `make logs-postgres` | Streams PostgreSQL logs |
| `make logs-minio` | Streams MinIO logs |
| `make logs-hive` | Streams Hive Metastore logs |

---

## 2. Common Troubleshooting Steps

### A. Windows `kubectl` Connection Timeout (`dial tcp: connectex`)
- **Root Cause**: `host.docker.internal` in kubeconfig does not route to localhost from Windows PowerShell.
- **Solution**:
  ```powershell
  $port = (kubectl config view -o jsonpath='{.clusters[?(@.name=="k3d-bigdata-dev")].cluster.server}').Split(':')[-1]
  kubectl config set-cluster k3d-bigdata-dev --server="https://127.0.0.1:$port"
  ```

### B. PostgreSQL `Authentication type 10 is not supported`
- **Root Cause**: PostgreSQL 15 uses SCRAM-SHA-256 by default. Old PostgreSQL JDBC drivers (e.g. 9.4 in Hive 3.1.3) only support MD5.
- **Solution**: Handled automatically in `banking/hive-metastore:3.1.3` by including `postgresql-42.7.3.jar`.

### C. Hadoop S3A `NumberFormatException: For input string: "60s" / "24h"`
- **Root Cause**: In Hadoop 3.4+, S3A time-based parameters (`fs.s3a.threads.keepalivetime`, `fs.s3a.multipart.purge.age`) expect raw integers, whereas Spark 4 defaults provide unit suffixes.
- **Solution**: Handled automatically in `infra/k8s/base/spark/configmap.yaml` by setting integer values (`threads.keepalivetime 60`, `multipart.purge.age 86400`).

### D. Spark 4 `ClassNotFoundException: software.amazon.awssdk.auth.credentials.AwsCredentialsProvider`
- **Root Cause**: Spark 4.1 runs on Hadoop 3.4.2 which uses AWS SDK v2 (`software.amazon.awssdk:bundle`) instead of AWS SDK v1 (`com.amazonaws:aws-java-sdk-bundle`).
- **Solution**: Handled automatically in `banking/spark:4.1` by bundling `software.amazon.awssdk:bundle:2.25.64`.
