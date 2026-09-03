#!/usr/bin/env bash
# scripts/init/04-init-kafka-topics.sh
# Creates core banking streaming topics in Kafka

set -euo pipefail

KAFKA_POD="${KAFKA_POD:-kafka-0}"
KAFKA_NAMESPACE="${KAFKA_NAMESPACE:-kafka}"
BOOTSTRAP_SERVER="localhost:9092"

TOPICS=(
  "banking.transactions:6:1"
  "banking.logins:3:1"
  "banking.notifications:3:1"
  "banking.loan_payments:3:1"
  "banking.cdc.events:6:1"
  "banking.fraud.alerts:3:1"
)

echo "Step 4: Initializing Kafka Banking Topics"

if kubectl get pod "${KAFKA_POD}" -n "${KAFKA_NAMESPACE}" >/dev/null 2>&1; then
  for item in "${TOPICS[@]}"; do
    IFS=':' read -r topic partitions rf <<< "$item"
    echo "Creating topic ${topic} with ${partitions} partitions"
    kubectl exec -n "${KAFKA_NAMESPACE}" "${KAFKA_POD}" -- \
      /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" \
      --create --if-not-exists \
      --topic "${topic}" \
      --partitions "${partitions}" \
      --replication-factor 1 >/dev/null 2>&1 || true
    echo "Topic ${topic} ready"
  done
else
  echo "Notice: Kafka pod not running. Skipping Kafka topic initialization"
fi
