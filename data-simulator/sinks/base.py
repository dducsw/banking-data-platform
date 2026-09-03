from abc import ABC, abstractmethod
from typing import Dict
import polars as pl


class BaseSink(ABC):
    """Abstract base class for all simulator data sinks."""

    @abstractmethod
    def write(self, tables: Dict[str, pl.DataFrame]) -> None:
        """Write generated table DataFrames to destination sink.

        Args:
            tables: Mapping of table names to Polars DataFrames.
        """
        pass

    def close(self) -> None:
        """Release any open resources, connections, or buffers."""
        pass
