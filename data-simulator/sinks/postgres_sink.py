import io
import os
from typing import Dict, List, Optional
import polars as pl

from .base import BaseSink

try:
    import psycopg2
except ImportError:
    psycopg2 = None


class PostgresSink(BaseSink):
    """Bulk loads generated banking DataFrames into a PostgreSQL database using COPY FROM STDIN."""

    LOAD_ORDER: List[str] = [
        "merchants",
        "branches",
        "customers",
        "accounts",
        "cards",
        "loans",
        "churn_simulation_state",
        "transactions",
        "account_ledger",
        "account_balance_snapshots",
        "login_events",
        "notifications",
        "loan_payments",
        "complaints",
        "feedback",
        "customer_churn_label",
        "churn_feature_snapshot",
    ]

    def __init__(
        self,
        connection_uri: str,
        init_schema: bool = False,
        schema_file: Optional[str] = None,
    ):
        self.connection_uri = connection_uri
        self.init_schema = init_schema
        self.schema_file = schema_file or self._default_schema_path()
        self._conn = None

    def _default_schema_path(self) -> str:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return os.path.join(base_dir, "pipeline", "schema.sql")

    def _get_connection(self):
        if psycopg2 is None:
            raise ImportError(
                "psycopg2 is required for PostgresSink. Run: pip install psycopg2-binary"
            )
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self.connection_uri)
        return self._conn

    def initialize_schema(self) -> None:
        """Executes DDL schema against the target database."""
        if not os.path.exists(self.schema_file):
            print(f"Warning: Schema file not found at {self.schema_file}. Skipping DDL initialization.")
            return

        conn = self._get_connection()
        with conn.cursor() as cur:
            with open(self.schema_file, "r", encoding="utf-8") as f:
                ddl = f.read()
            cur.execute(ddl)
            conn.commit()
            print(f"[OK] Initialized database tables from {os.path.basename(self.schema_file)}.")

    def _get_table_columns(self, table_name: str) -> List[str]:
        """Fetch column names for a given table from PostgreSQL."""
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s 
                ORDER BY ordinal_position
                """,
                (table_name,),
            )
            return [row[0] for row in cur.fetchall()]

    def write(self, tables: Dict[str, pl.DataFrame]) -> None:
        if psycopg2 is None:
            print("Warning: psycopg2 is not installed. Skipping PostgreSQL load.")
            return

        try:
            conn = self._get_connection()
        except Exception as e:
            print(f"Warning: Could not connect to PostgreSQL ({e}). Skipping load.")
            return

        if self.init_schema:
            try:
                self.initialize_schema()
            except Exception as e:
                print(f"Error during schema initialization: {e}")
                return

        with conn.cursor() as cursor:
            for name in self.LOAD_ORDER:
                if name not in tables:
                    continue

                df = tables[name]
                if df.is_empty():
                    continue

                # Query database table columns to ensure exact column alignment
                try:
                    db_cols = self._get_table_columns(name)
                    if not db_cols:
                        cols_to_use = df.columns
                    else:
                        cols_to_use = [c for c in db_cols if c in df.columns]
                except Exception:
                    cols_to_use = df.columns

                # Handle nulls for boolean flags if any
                fill_exprs = []
                for col in ["is_fraud", "is_disputed", "is_salary_credit", "is_fee", "is_reversal"]:
                    if col in df.columns:
                        fill_exprs.append(pl.col(col).fill_null(False))
                for col in ["risk_score"]:
                    if col in df.columns:
                        fill_exprs.append(pl.col(col).fill_null(0.0))
                
                if fill_exprs:
                    df = df.with_columns(fill_exprs)

                df_to_load = df.select(cols_to_use)

                buffer = io.BytesIO()
                df_to_load.write_csv(buffer, include_header=False, separator=",", null_value="")
                buffer.seek(0)

                col_list_str = ", ".join(cols_to_use)
                copy_sql = f"COPY {name} ({col_list_str}) FROM STDIN WITH (FORMAT csv, HEADER false, NULL '')"
                try:
                    cursor.copy_expert(copy_sql, buffer)
                    conn.commit()
                    print(f"  [OK] Bulk loaded {df_to_load.height:,} rows into table [{name}].")
                except Exception as load_err:
                    conn.rollback()
                    print(f"  [ERROR] Error bulk loading table [{name}]: {load_err}")

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None
