"""Parquet writer wrapper around ParquetSink."""

from typing import Dict
import polars as pl
from sinks.parquet_sink import ParquetSink


def write_to_parquet(dataframes: Dict[str, pl.DataFrame], output_dir: str) -> None:
    """Saves generated Polars DataFrames to the output directory using ParquetSink."""
    sink = ParquetSink(output_dir=output_dir)
    sink.write(dataframes)
