"""
Centralized pipeline datasets, namespaces, and table definitions.
"""

from typing import Dict, Any

# Medallion Namespaces
CATALOG_NAME = "lakehouse"
NAMESPACE_BRONZE = "bronze"
NAMESPACE_SILVER = "silver"
NAMESPACE_GOLD = "gold"
NAMESPACE_METADATA = "metadata"

# Dataset table definitions
CORE_BANKING_TABLES = [
    "merchants",
    "branches",
    "customers",
    "accounts",
    "cards",
    "loans",
    "transactions",
    "account_ledger",
    "login_events",
    "notifications",
    "complaints",
    "feedback",
    "churn_simulation_state",
]

TABLE_CONFIGS: Dict[str, Dict[str, Any]] = {
    "dim_customers": {
        "primary_key": "customer_id",
        "source_table": f"{CATALOG_NAME}.{NAMESPACE_BRONZE}.customers",
        "target_table": f"{CATALOG_NAME}.{NAMESPACE_SILVER}.dim_customers",
        "partition_by": [],
    },
    "dim_accounts": {
        "primary_key": "account_id",
        "source_table": f"{CATALOG_NAME}.{NAMESPACE_BRONZE}.accounts",
        "target_table": f"{CATALOG_NAME}.{NAMESPACE_SILVER}.dim_accounts",
        "partition_by": [],
    },
    "fact_transactions": {
        "primary_key": "transaction_id",
        "source_table": f"{CATALOG_NAME}.{NAMESPACE_BRONZE}.transactions",
        "target_table": f"{CATALOG_NAME}.{NAMESPACE_SILVER}.fact_transactions",
        "partition_by": ["txn_date"],
    },
    "daily_branch_summary": {
        "source_table": f"{CATALOG_NAME}.{NAMESPACE_SILVER}.fact_transactions",
        "target_table": f"{CATALOG_NAME}.{NAMESPACE_GOLD}.daily_branch_summary",
        "partition_by": ["txn_date"],
    },
    "customer_360_summary": {
        "primary_key": "customer_id",
        "source_table": f"{CATALOG_NAME}.{NAMESPACE_SILVER}.dim_customers",
        "target_table": f"{CATALOG_NAME}.{NAMESPACE_GOLD}.customer_360_summary",
        "partition_by": [],
    },
}

# Storage Buckets
BUCKET_LAKEHOUSE = "lakehouse"
BUCKET_CHECKPOINTS = "checkpoints"
BUCKET_SPARK_LOGS = "spark-logs"

# Kafka Topics
TOPIC_TRANSACTIONS = "banking.public.transactions"
TOPIC_ACCOUNTS = "banking.public.accounts"
TOPIC_CUSTOMERS = "banking.public.customers"
TOPIC_LOGIN_EVENTS = "banking.public.login_events"

