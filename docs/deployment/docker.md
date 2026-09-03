# Custom Docker Images & Dependency Matrix

This guide details the Docker images built for the banking platform, the multi-stage build design, and the exact version matrix required to ensure complete compatibility across Spark 4.1, Iceberg 1.11.0, Hive Metastore 3.1.3, and PostgreSQL 15.

---

## 1. Version Compatibility Matrix

| Component / Library | Hive Metastore Image (`banking/hive-metastore:3.1.3`) | Spark Image (`banking/spark:4.1`) | Purpose & Notes |
| :--- | :--- | :--- | :--- |
| **Base Engine** | Apache Hive `3.1.3` | Apache Spark `4.1.1` (Python 3.10) | Core data processing engines |
| **Java Runtime** | Eclipse Temurin `17-jre` | OpenJDK `21` (Built into Spark 4.1) | LTS Java Runtimes |
| **Iceberg Runtime** | N/A | `iceberg-spark-runtime-4.1_2.13:1.11.0` | Spark-Iceberg catalog & writer |
| **Hadoop AWS** | `hadoop-aws-3.3.6.jar` | `hadoop-aws-3.4.2.jar` | S3A FileSystem connector |
| **AWS SDK** | `aws-java-sdk-bundle-1.12.367.jar` (v1) | `software.amazon.awssdk:bundle:2.25.64` (v2) | AWS S3 client SDKs |
| **PostgreSQL JDBC** | `postgresql-42.7.3.jar` | `postgresql-42.7.3.jar` | Metastore backend & Spark JDBC connector |
| **Guava** | `guava-27.0-jre.jar` | Built-in | Replaces old Guava 19 in Hive to avoid `NoSuchMethodError` |

---

## 2. Apache Hive Metastore Image (`banking/hive-metastore:3.1.3`)

### Dockerfile Location: [`infra/docker/hive/Dockerfile`](../../infra/docker/hive/Dockerfile)

### Multi-stage Build Highlights:
1. **Stage 1 (Builder)**:
   - Uses `apache/hive:3.1.3` as source for binaries.
   - Replaces the legacy PostgreSQL 9.4 driver with `postgresql-42.7.3.jar` to support **SCRAM-SHA-256** (Auth Type 10) on PostgreSQL 15+.
   - Downloads `hadoop-aws-3.3.6.jar` and `aws-java-sdk-bundle-1.12.367.jar` for direct S3A access.
   - Replaces `guava-19.0.jar` with `guava-27.0-jre.jar` to avoid classpath collision with Hadoop classes.
2. **Stage 2 (Runtime)**:
   - Minimal `eclipse-temurin:17-jre` with `curl`, `netcat`, and `procps`.
   - Entrypoint: `/opt/hive/bin/hive --service metastore` on port `9083`.

### Build Command:
```bash
docker build -t banking/hive-metastore:3.1.3 infra/docker/hive
```

---

## 3. Apache Spark 4.1 + Iceberg 1.11.0 Image (`banking/spark:4.1`)

### Dockerfile Location: [`infra/docker/spark/Dockerfile`](../../infra/docker/spark/Dockerfile)

### Key Configurations:
- Base image: `apache/spark:4.1.1-python3`
- Spark 4.1 runs on **Hadoop 3.4.2** internally. It requires `hadoop-aws-3.4.2.jar` and **AWS SDK v2** (`software.amazon.awssdk:bundle:2.25.64`).
- Bundles `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` into `/opt/spark/jars/`.
- Pre-installs Python packages from `requirements.txt` (`pyspark`, `pandas`, `pyarrow`, `fastparquet`, `boto3`).

### Build Command:
```bash
docker build -t banking/spark:4.1 infra/docker/spark
```

---

## 4. Importing Images into k3d

Because k3d uses its own containerd runtime inside Docker, locally built images must be loaded into the cluster:
```bash
k3d image import banking/hive-metastore:3.1.3 banking/spark:4.1 -c bigdata-dev
```
