import os
import shutil
import tempfile
import polars as pl
from datetime import date

from sinks.parquet_sink import ParquetSink
from sinks.postgres_sink import PostgresSink


def test_parquet_sink():
    temp_dir = tempfile.mkdtemp()
    try:
        sink = ParquetSink(output_dir=temp_dir)
        sample_df = pl.DataFrame({
            "branch_code": ["BR001"],
            "branch_name": ["Central Branch"],
            "city": ["Hanoi"],
            "state": ["HN"],
            "region": ["North"],
            "branch_type": ["Main"],
            "open_date": [date(2020, 1, 1)],
            "closure_date": [None],
            "customer_weight": [100],
        })
        partitioned_df = pl.DataFrame({
            "transaction_id": [1],
            "txn_month": [date(2024, 1, 1)],
            "amount": [100.0],
        })

        sink.write({"branches": sample_df, "transactions": partitioned_df})
        sink.close()

        assert os.path.exists(os.path.join(temp_dir, "branches.parquet"))
        assert os.path.exists(os.path.join(temp_dir, "transactions", "txn_month=2024-01-01", "part-0.parquet"))
    finally:
        shutil.rmtree(temp_dir)


def test_postgres_sink_unreachable():
    # Should fail gracefully without raising unhandled exception when DB is unreachable
    sink = PostgresSink(connection_uri="postgresql://fake_user:fake_pass@127.0.0.1:59999/non_existent_db")
    sample_df = pl.DataFrame({"id": [1]})
    sink.write({"customers": sample_df})
    sink.close()
