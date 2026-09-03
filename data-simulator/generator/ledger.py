"""Account ledger generator (light version) — 1 entry per transaction.

Each transactions record maps to one ledger entry capturing:
- Which account was affected
- Debit/Credit direction
- Amount, running balance
- Human-readable description

The ledger is derived from transactions after balance calculation,
so running_balance is the authoritative post-transaction balance.

Useful for:
- ETL reconciliation: SUM(ledger amounts per account) == final balance
- Point-in-time balance queries
- Regulatory/compliance reporting patterns
- Data pipeline testing
"""

from datetime import date
from typing import List


def generate_account_ledger_from_transactions(
    transactions: List[dict],
    start_entry_id: int = 1,
) -> list[dict]:
    """Convert settled transactions rows into account_ledger entries.

    Must be called AFTER balance calculation (balance_after_txn must be set).
    Transactions should be pre-sorted by txn_timestamp ascending.

    Args:
        transactions: List of transactions dicts (with balance_after_txn populated).
        start_entry_id: Starting ledger entry ID.

    Returns:
        List of account_ledger dicts.
    """
    entries = []
    entry_id = start_entry_id

    for txn in transactions:
        amount = txn["amount"]
        direction = txn["direction"]

        # Build description
        txn_type = txn["txn_type"]
        merchant = txn.get("transaction_description", "")
        desc = f"{txn_type} — {merchant}" if merchant and merchant not in ("", "N/A") else txn_type

        entries.append({
            "entry_id": entry_id,
            "transaction_id": txn["transaction_id"],
            "account_id": txn["account_id"],
            "customer_id": txn["customer_id"],
            "entry_date": txn["txn_date"],
            "entry_month": txn["txn_month"],
            "entry_timestamp": txn["txn_timestamp"],
            "entry_type": direction,          # "Debit" or "Credit"
            "debit_amount": round(amount, 2) if direction == "Debit" else 0.0,
            "credit_amount": round(amount, 2) if direction == "Credit" else 0.0,
            "amount": round(amount, 2),
            "currency": txn.get("currency", "USD"),
            "running_balance": txn["balance_after_txn"],
            "description": desc[:200],
            "txn_type": txn_type,
            "channel": txn.get("channel", ""),
            "reference_number": f"TXN{txn['transaction_id']:012d}",
        })
        entry_id += 1

    return entries
