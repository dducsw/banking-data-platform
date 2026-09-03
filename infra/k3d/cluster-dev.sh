#!/usr/bin/env bash
# infra/k3d/cluster-dev.sh
# Declarative k3d cluster creation and Lakehouse dev stack deployment

set -euo pipefail

CLUSTER_NAME="bigdata-dev"
CONFIG_FILE="infra/k3d/config.yaml"

# List of third-party images to pre-pull & import into k3d for fast bootstrapping
THIRD_PARTY_IMAGES=(
  "postgres:16-alpine"
  "minio/minio:latest"
  "apache/gravitino:1.3.0"
  "trinodb/trino:480"
  "apache/kafka:4.1.0"
  "quay.io/debezium/connect:3.2.0.Final"
  "prom/prometheus:v3.14.0"
  "clickhouse/clickhouse-server:24.3-alpine"
  "busybox:1.36"
)

echo "=== 1. Provisioning k3d Cluster: ${CLUSTER_NAME} ==="

if k3d cluster list 2>/dev/null | grep -q "^${CLUSTER_NAME}"; then
  echo "Cluster ${CLUSTER_NAME} already exists."
else
  echo "Creating cluster using config file ${CONFIG_FILE}..."
  mkdir -p ./data/k3d
  k3d cluster create --config "${CONFIG_FILE}"
fi

kubectl config use-context "k3d-${CLUSTER_NAME}"

echo "=== 2. Pre-pulling and importing images into k3d cluster ==="
for img in "${THIRD_PARTY_IMAGES[@]}"; do
  echo "Checking image: ${img}"
  if ! docker image inspect "${img}" >/dev/null 2>&1; then
    echo "Pulling ${img} on host..."
    docker pull "${img}"
  fi
  echo "Importing ${img} into k3d..."
  k3d image import "${img}" -c "${CLUSTER_NAME}" || true
done

echo "=== 3. Deploying Full Lakehouse Dev Stack ==="
kubectl apply -k infra/k8s/overlays/dev

echo "=== 4. Waiting for core services to become Ready ==="
echo "Waiting for PostgreSQL..."
kubectl wait --for=condition=Ready pod -l app=postgres -n postgres --timeout=120s || true

echo "Waiting for MinIO S3..."
kubectl wait --for=condition=Ready pod -l app=minio -n minio --timeout=120s || true

echo "Waiting for Apache Gravitino (Iceberg REST)..."
kubectl wait --for=condition=Available deploy/gravitino -n gravitino --timeout=120s || true

echo "Waiting for Trino..."
kubectl wait --for=condition=Available deploy/trino -n trino --timeout=180s || true

echo "Waiting for Kafka..."
kubectl wait --for=condition=Ready pod -l app=kafka -n kafka --timeout=120s || true

echo "Waiting for Debezium Connect..."
kubectl wait --for=condition=Available deploy/debezium-connect -n debezium --timeout=180s || true

echo "Waiting for Prometheus..."
kubectl wait --for=condition=Ready pod -l app=prometheus -n monitoring --timeout=120s || true

echo "=========================================================="
echo " Lakehouse & Real-Time Streaming Stack is READY on k3d!"
echo "=========================================================="
echo " Endpoints:"
echo " - MinIO S3 API      : http://localhost:9000"
echo " - MinIO Console     : http://localhost:9001"
echo " - PostgreSQL        : localhost:5432"
echo " - Gravitino Admin   : http://localhost:8090"
echo " - Iceberg REST API  : http://localhost:9001/iceberg/"
echo " - Trino Query Engine: http://localhost:8080"
echo " - Apache Kafka      : localhost:9092"
echo " - Debezium Connect  : http://localhost:8083"
echo " - Prometheus UI     : http://localhost:9090 (via port-forward)"
echo " - Airflow Webserver : http://localhost:8080 (in airflow ns)"
echo " - Spark UI          : http://localhost:4040"
echo "=========================================================="
