#!/usr/bin/env bash
# infra/helm/install-dev.sh
# Helm installer for Postgres, MinIO, and Spark Operator

set -euo pipefail

echo "Step 1: Adding Helm Repositories"
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update

echo "Step 2: Installing PostgreSQL"
helm upgrade --install postgres bitnami/postgresql \
  --namespace postgres --create-namespace \
  -f infra/helm/values/dev/postgres.yaml

echo "Step 3: Installing MinIO"
helm upgrade --install minio bitnami/minio \
  --namespace minio --create-namespace \
  -f infra/helm/values/dev/minio.yaml

echo "Step 4: Installing Spark on K8s Operator"
helm upgrade --install spark-operator spark-operator/spark-operator \
  --namespace spark-operator --create-namespace \
  -f infra/helm/values/dev/spark-operator.yaml

echo "Step 5: Installing Hive Metastore via Kustomize"
kubectl apply -k infra/k8s/base/hive

echo "Step 6: Configuring Spark RBAC and Defaults"
kubectl apply -k infra/k8s/base/spark

echo "Lakehouse Dev Stack successfully provisioned via Helm"
