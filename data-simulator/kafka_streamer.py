"""Kafka Streaming Producer for Bank Data Simulator.

Replays generated Parquet data as a real-time Kafka event stream.

Topics produced:
  - bank.transactions      : one event per transactions row (ordered by txn_timestamp)
  - bank.customer_activity : one event per customer_monthly_activity row (ordered by snapshot_month)

Usage:
  # Dry run (no Kafka required) — prints event counts
  python kafka_streamer.py --data-dir ./data/raw --dry-run

  # Stream to Kafka at 1000 events/sec
  python kafka_streamer.py --data-dir ./data/raw --bootstrap-servers localhost:9092 --rate 1000

  # Stream only transactions, unlimited speed
  python kafka_streamer.py --data-dir ./data/raw --bootstrap-servers localhost:9092 --topics transactions --rate 0

Install producer dependency (not in pyproject.toml to keep it optional):
  pip install confluent-kafka
"""

import argparse
import json
import time
import sys
from datetime import date, datetime
from pathlib import Path


# ── helpers ──────────────────────────────────────────────────────────────────

def _serialize(obj):
    """JSON serializer for date/datetime objects."""
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def _to_json(record: dict) -> bytes:
    return json.dumps(record, default=_serialize).encode("utf-8")


def _load_transactions(data_dir: Path) -> list[dict]:
    """Load transactions Parquet (hive-partitioned) sorted by txn_timestamp."""
    try:
        import polars as pl
    except ImportError:
        sys.exit("polars not installed — run: pip install polars")

    txn_path = data_dir / "transactions"
    if not txn_path.exists():
        txn_path = data_dir / "transactions.parquet"
        if not txn_path.exists():
            sys.exit(f"transactions not found under {data_dir}")
        df = pl.read_parquet(txn_path)
    else:
        df = pl.read_parquet(str(txn_path / "**" / "*.parquet"), hive_partitioning=True)

    df = df.sort("txn_timestamp")
    return df.to_dicts()


def _load_activity(data_dir: Path) -> list[dict]:
    """Load customer_monthly_activity Parquet sorted by snapshot_month, customer_id."""
    try:
        import polars as pl
    except ImportError:
        sys.exit("polars not installed — run: pip install polars")

    act_path = data_dir / "customer_monthly_activity"
    if not act_path.exists():
        act_path = data_dir / "customer_monthly_activity.parquet"
        if not act_path.exists():
            sys.exit(f"customer_monthly_activity not found under {data_dir}")
        df = pl.read_parquet(act_path)
    else:
        df = pl.read_parquet(str(act_path / "**" / "*.parquet"), hive_partitioning=True)

    df = df.sort(["snapshot_month", "customer_id"])
    return df.to_dicts()



def _load_table(data_dir: Path, table_name: str, sort_col: str) -> list[dict]:
    """Generic partitioned Parquet loader."""
    try:
        import polars as pl
    except ImportError:
        sys.exit("polars not installed")
    p = data_dir / table_name
    if not p.exists():
        p = data_dir / f"{table_name}.parquet"
        if not p.exists():
            return []
        df = pl.read_parquet(p)
    else:
        df = pl.read_parquet(str(p / "**" / "*.parquet"), hive_partitioning=True)
    return df.sort(sort_col).to_dicts()


# ── producers ────────────────────────────────────────────────────────────────

def _dry_run(data_dir: Path, topics: list[str]):
    """Print stats without connecting to Kafka."""
    print("=== DRY RUN MODE (no Kafka connection) ===\n")
    if "transactions" in topics:
        txns = _load_transactions(data_dir)
        print(f"  bank.transactions        : {len(txns):,} events ready")
        if txns:
            print(f"    first : {txns[0]['txn_timestamp']}  txn_id={txns[0]['transaction_id']}")
            print(f"    last  : {txns[-1]['txn_timestamp']}  txn_id={txns[-1]['transaction_id']}")
            from collections import Counter
            types = Counter(t["txn_type"] for t in txns)
            for k, v in types.most_common():
                print(f"    {k:<30} {v:>8,}")
    if "login" in topics:
        logins = _load_table(data_dir, "login_events", "login_month")
        print(f"  bank.login_events        : {len(logins):,} events ready")
    if "loans" in topics:
        loans_ev = _load_table(data_dir, "loan_payments", "payment_month")
        print(f"  bank.loan_payments : {len(loans_ev):,} events ready")
    if "activity" in topics:
        acts = _load_activity(data_dir)
        print(f"  bank.customer_activity   : {len(acts):,} events ready (legacy)")
    print("\nDry run complete. Remove --dry-run to stream to Kafka.")


def _stream(data_dir, bootstrap_servers, topics, rate, verbose):
    """Stream events to Kafka."""
    try:
        from confluent_kafka import Producer
    except ImportError:
        sys.exit("confluent-kafka not installed.\nInstall: pip install confluent-kafka")

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    delay = (1.0 / rate) if rate > 0 else 0.0

    def _delivery_report(err, msg):
        if err and verbose:
            print(f"[ERROR] Delivery failed: {err}", file=sys.stderr)

    total_sent = 0
    start = time.monotonic()

    if "transactions" in topics:
        txns = _load_transactions(data_dir)
        print(f"Streaming {len(txns):,} events -> bank.transactions ...")
        for i, rec in enumerate(txns):
            producer.produce(
                topic="bank.transactions",
                key=str(rec["customer_id"]).encode(),
                value=_to_json(rec),
                callback=_delivery_report,
            )
            if delay > 0:
                time.sleep(delay)
            if i % 1000 == 0:
                producer.poll(0)
                print(f"  sent={i+1:>8,}  elapsed={time.monotonic()-start:.1f}s", end="\r")
        producer.flush()
        total_sent += len(txns)
        print(f"\n  OK bank.transactions: {len(txns):,} events")

    if "activity" in topics:
        acts = _load_activity(data_dir)
        print(f"Streaming {len(acts):,} events -> bank.customer_activity ...")
        for i, rec in enumerate(acts):
            producer.produce(
                topic="bank.customer_activity",
                key=str(rec["customer_id"]).encode(),
                value=_to_json(rec),
                callback=_delivery_report,
            )
            if delay > 0:
                time.sleep(delay)
            if i % 1000 == 0:
                producer.poll(0)
                print(f"  sent={i+1:>8,}  elapsed={time.monotonic()-start:.1f}s", end="\r")
        producer.flush()
        total_sent += len(acts)
        print(f"\n  OK bank.customer_activity: {len(acts):,} events")

    if "login" in topics:
        logins = _load_table(data_dir, "login_events", "login_month")
        print(f"Streaming {len(logins):,} events -> bank.login_events ...")
        for i, rec in enumerate(logins):
            producer.produce(topic="bank.login_events", key=str(rec["customer_id"]).encode(), value=_to_json(rec), callback=_delivery_report)
            if delay > 0: time.sleep(delay)
            if i % 1000 == 0: producer.poll(0)
        producer.flush()
        total_sent += len(logins)
        print(f"\n  OK bank.login_events: {len(logins):,} events")

    if "loans" in topics:
        loans = _load_table(data_dir, "loan_payments", "payment_month")
        print(f"Streaming {len(loans):,} events -> bank.loan_payments ...")
        for i, rec in enumerate(loans):
            producer.produce(topic="bank.loan_payments", key=str(rec["customer_id"]).encode(), value=_to_json(rec), callback=_delivery_report)
            if delay > 0: time.sleep(delay)
            if i % 1000 == 0: producer.poll(0)
        producer.flush()
        total_sent += len(loans)
        print(f"\n  OK bank.loan_payments: {len(loans):,} events")

    elapsed = time.monotonic() - start
    tp = total_sent / elapsed if elapsed > 0 else 0
    print(f"\nDone. {total_sent:,} events in {elapsed:.1f}s ({tp:,.0f} events/sec)")


def main():
    parser = argparse.ArgumentParser(description="Replay bank simulator data as Kafka stream.")
    parser.add_argument("--data-dir", default="./data/raw")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topics", nargs="+", choices=["transactions", "activity", "login", "loans"],
                        default=["transactions", "activity", "login", "loans"])
    parser.add_argument("--rate", type=float, default=0,
                        help="Max events/sec (0=unlimited)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        sys.exit(f"Data directory not found: {data_dir}\nRun main.py first.")

    if args.dry_run:
        _dry_run(data_dir, args.topics)
    else:
        _stream(data_dir, args.bootstrap_servers, args.topics, args.rate, args.verbose)


if __name__ == "__main__":
    main()

