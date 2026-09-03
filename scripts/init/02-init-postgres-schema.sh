#!/usr/bin/env bash
# scripts/init/02-init-postgres-schema.sh
# Initializes PostgreSQL with Core Banking schema and auxiliary databases

set -euo pipefail

PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-postgres}"
PG_PASSWORD="${PG_PASSWORD:-postgrespassword}"
PG_DB="${PG_DB:-banking}"

SCHEMA_FILE="bank-data-simulator/pipeline/schema.sql"

echo "Step 2: Initializing PostgreSQL Banking Schema and Sub-Databases"

if kubectl get pod postgres-0 -n postgres >/dev/null 2>&1; then
  echo "Applying schema inside postgres-0 pod"

  kubectl exec -n postgres postgres-0 -- psql -U "${PG_USER}" -d "${PG_DB}" -c "
    CREATE DATABASE metastore;
    CREATE DATABASE airflow;
  " 2>/dev/null || true

  kubectl exec -n postgres postgres-0 -- psql -U "${PG_USER}" -d "${PG_DB}" -c "
    ALTER SYSTEM SET wal_level = 'logical';
  " 2>/dev/null || true

  if [ -f "${SCHEMA_FILE}" ]; then
    echo "Applying ${SCHEMA_FILE}"
    kubectl exec -i -n postgres postgres-0 -- psql -U "${PG_USER}" -d "${PG_DB}" < "${SCHEMA_FILE}"
    echo "Banking tables created successfully in PostgreSQL"
  else
    echo "Warning: ${SCHEMA_FILE} not found at expected path"
  fi

else
  echo "Executing via local psql on ${PG_HOST}:${PG_PORT}"
  if command -v psql >/dev/null 2>&1; then
    export PGPASSWORD="${PG_PASSWORD}"
    psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}" -f "${SCHEMA_FILE}"
    echo "Banking tables created"
  else
    echo "Notice: psql not found locally. Ensure pod is accessible"
  fi
fi
