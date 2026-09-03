# Deployment Guide

This directory contains the detailed deployment documentation for the Big Data Banking Platform.

## 📖 Table of Contents

1. [**k3d Cluster Setup (k3d.md)**](k3d.md)
   - Cluster topology (1 Server, 2 Agents, 1 LoadBalancer).
   - Port forwarding (5432, 9000, 9001, 9083, 4040).
   - Host networking, volume configurations, and Windows localhost setup.

2. [**Docker Images & Dependency Matrix (docker.md)**](docker.md)
   - Multi-stage Docker builds for **Apache Hive Metastore 3.1.3** and **Apache Spark 4.1.1**.
   - JAR dependency alignment (Iceberg 1.11.0, Hadoop 3.4.2, AWS SDK v2, PostgreSQL JDBC 42.7.3).
   - Guava collision fixes and SCRAM-SHA-256 PostgreSQL authentication compatibility.

3. [**Platform Architecture & Manifests (platform.md)**](platform.md)
   - Namespaces (postgres, minio, metastore, spark).
   - Kustomize dev overlay structure.
   - Component details: PostgreSQL StatefulSet, MinIO S3 bucket initializer, Hive Metastore deployment & schema migration, Spark default configs.
   - End-to-end Iceberg verification pipeline.

4. [**Deployment Automation (automation.md)**](automation.md)
   - Makefile workflow.
   - Shell bootstrap scripts (infra/k3d/cluster-dev.sh, scripts/init/).
   - Troubleshooting common environment errors.
