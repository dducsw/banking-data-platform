#!/usr/bin/env bash
# scripts/init/05-init-debezium-connector.sh
# Registers the PostgreSQL CDC Source Connector in Debezium Connect

set -euo pipefail

DEBEZIUM_HOST="${DEBEZIUM_HOST:-http://localhost:8083}"

echo "Step 5: Registering Debezium PostgreSQL CDC Connector"

CONNECTOR_CONFIG='{
  "name": "banking-postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "plugin.name": "pgoutput",
    "database.hostname": "postgres.postgres.svc.cluster.local",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgrespassword",
    "database.dbname": "banking",
    "database.server.name": "banking_db",
    "table.include.list": "public.transactions,public.account_balance_snapshots,public.login_events",
    "topic.prefix": "banking_cdc",
    "tombstones.on.delete": "false"
  }
}'

if curl -s "${DEBEZIUM_HOST}/connectors" >/dev/null 2>&1; then
  curl -s -X POST -H "Content-Type: application/json" \
    --data "${CONNECTOR_CONFIG}" \
    "${DEBEZIUM_HOST}/connectors" | grep -q "name" && echo "Debezium connector registered" || echo "Connector already registered"
else
  echo "Notice: Debezium REST API not reachable on ${DEBEZIUM_HOST}. Skipping"
fi
