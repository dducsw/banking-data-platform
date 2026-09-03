"""PostgreSQL bulk loader wrapper around PostgresSink."""

from typing import Dict
import polars as pl
from sinks.postgres_sink import PostgresSink


def load_to_postgres(dataframes: Dict[str, pl.DataFrame], connection_uri: str, init_schema: bool = True) -> None:
    """Loads final Polars DataFrames into PostgreSQL database."""
    sink = PostgresSink(connection_uri=connection_uri, init_schema=init_schema)
    try:
        sink.write(dataframes)
    finally:
        sink.close()
