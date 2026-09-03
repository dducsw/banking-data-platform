#!/usr/bin/env bash
# scripts/init/bootstrap.sh
# Master initialization orchestrator for the Big Data Banking Platform

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Starting Big Data Platform Initialization"

bash "${SCRIPT_DIR}/01-init-minio-buckets.sh"
bash "${SCRIPT_DIR}/02-init-postgres-schema.sh"
bash "${SCRIPT_DIR}/04-init-kafka-topics.sh"
bash "${SCRIPT_DIR}/05-init-debezium-connector.sh"

echo "Platform Initialization Completed Successfully"
