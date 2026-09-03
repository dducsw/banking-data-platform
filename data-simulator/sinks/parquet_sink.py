import os
from typing import Dict, Set
import polars as pl

from .base import BaseSink


class ParquetSink(BaseSink):
    """Saves generated DataFrames to single or Hive-partitioned Parquet files."""

    STATIC_TABLES: Set[str] = {
        "branches",
        "customers",
        "accounts",
        "cards",
        "loans",
        "churn_simulation_state",
        "merchants",
        "products",
    }

    PARTITION_COLS: Dict[str, str] = {
        "transactions": "txn_month",
        "login_events": "login_month",
        "notifications": "sent_month",
        "loan_payments": "payment_month",
        "complaints": "complaint_month",
        "feedback": "feedback_month",
        "account_ledger": "entry_month",
        "account_balance_snapshots": "snapshot_month",
    }

    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def write(self, tables: Dict[str, pl.DataFrame]) -> None:
        for name, df in tables.items():
            if df.is_empty():
                continue

            if name in self.STATIC_TABLES:
                file_path = os.path.join(self.output_dir, f"{name}.parquet")
                df.write_parquet(file_path)
            elif name in self.PARTITION_COLS:
                part_col = self.PARTITION_COLS[name]
                unique_vals = df[part_col].unique().sort().to_list()
                for val in unique_vals:
                    sub_df = df.filter(pl.col(part_col) == val)
                    val_str = (
                        val.strftime("%Y-%m-%d")
                        if hasattr(val, "strftime")
                        else str(val)
                    )
                    part_dir = os.path.join(self.output_dir, name, f"{part_col}={val_str}")
                    os.makedirs(part_dir, exist_ok=True)
                    file_path = os.path.join(part_dir, "part-0.parquet")
                    sub_df.write_parquet(file_path)
            else:
                file_path = os.path.join(self.output_dir, f"{name}.parquet")
                df.write_parquet(file_path)
