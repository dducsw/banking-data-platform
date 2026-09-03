import argparse
import os
import shutil
import time
from datetime import date
from typing import List

from config.simulation import SimulationConfig
from pipeline.simulate import run_simulation
from sinks import BaseSink, ParquetSink, PostgresSink


def parse_args():
    parser = argparse.ArgumentParser(
        description="Banking Data Simulator CLI - Generates realistic synthetic core banking universe data."
    )
    parser.add_argument(
        "--n-customers",
        type=int,
        default=int(os.getenv("SIM_CUSTOMERS", "1000")),
        help="Number of customers to generate (default: 1000)",
    )
    parser.add_argument(
        "--sim-months",
        type=int,
        default=int(os.getenv("SIM_MONTHS", "24")),
        help="Number of simulation months (default: 24)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=int(os.getenv("SIM_SEED", "42")),
        help="RNG seed for reproducibility (default: 42)",
    )
    parser.add_argument(
        "--output-dir",
        default=os.getenv("OUTPUT_DIR", "./data/raw"),
        help="Output directory for Parquet files (default: ./data/raw)",
    )
    parser.add_argument(
        "--no-parquet",
        action="store_true",
        help="Disable writing Parquet files to disk",
    )
    parser.add_argument(
        "--postgres-uri",
        default=os.getenv("POSTGRES_URI", None),
        help="PostgreSQL connection URI (e.g. postgresql://postgres:postgres123@localhost:5432/banking)",
    )
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Execute DDL schema initialization against PostgreSQL before loading",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=int(os.getenv("SIM_JOBS", "1")),
        help="Number of parallel worker processes (default: 1)",
    )
    parser.add_argument(
        "--streaming",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable streaming memory mode for large runs (default: True if jobs > 1)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    config = SimulationConfig(
        n_customers=args.n_customers,
        sim_start=date(2024, 1, 1),
        sim_months=args.sim_months,
        seed=args.seed,
    )

    # 1. Clean output directory if writing Parquet
    if not args.no_parquet:
        if os.path.exists(args.output_dir):
            print(f"Cleaning old parquet directory: {args.output_dir}")
            shutil.rmtree(args.output_dir)
        os.makedirs(args.output_dir, exist_ok=True)

    # 2. Run simulation engine
    print(
        f"--> Running simulation for {config.n_customers:,} customers over {config.sim_months} months with {args.jobs} worker(s)..."
    )
    start_time = time.time()
    streaming = args.streaming if args.streaming is not None else (args.jobs > 1)

    results = run_simulation(
        config, streaming=streaming, output_dir=args.output_dir, jobs=args.jobs
    )
    sim_duration = time.time() - start_time
    print(f"[OK] Simulation completed in {sim_duration:.2f} seconds.\n")

    # 3. Setup and dispatch to Sinks
    sinks: List[BaseSink] = []

    if not args.no_parquet:
        sinks.append(ParquetSink(output_dir=args.output_dir))

    if args.postgres_uri:
        sinks.append(PostgresSink(connection_uri=args.postgres_uri, init_schema=args.init_db))

    for sink in sinks:
        sink_name = sink.__class__.__name__
        print(f"--> Writing data to [{sink_name}]...")
        start_sink = time.time()
        try:
            sink.write(results)
        finally:
            sink.close()
        sink_duration = time.time() - start_sink
        print(f"[OK] [{sink_name}] finished in {sink_duration:.2f} seconds.\n")


if __name__ == "__main__":
    main()
