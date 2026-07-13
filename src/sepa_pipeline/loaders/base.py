from abc import ABC, abstractmethod
from datetime import date

import polars as pl

from sepa_pipeline.utils.logger import get_logger

from ..config import SEPAConfig

logger = get_logger(__name__)

# Buffer precios appends so each Iceberg data file is closer to the recommended
# 128–256 MB range. ~2M rows is a practical tradeoff: far fewer UUID files than
# per-500k appends, without materializing the full ~12M-row day in RAM.
PRECIOS_APPEND_TARGET_ROWS = 2_000_000


class BaseLoader(ABC):
    """
    Abstract base class for all SEPA pipeline loaders.

    Each loader implementations (Postgres, Iceberg, etc.) must inherit from this class
    and implement the `load` method.
    """

    def __init__(self, config: SEPAConfig):
        """
        Initialize the loader with configuration.

        Args:
            config (SEPAConfig): The pipeline configuration object.
        """
        self.config = config
        self.stage_name = self.__class__.__name__
        self._precios_buffer: list[pl.DataFrame] = []
        self._precios_buffer_rows: int = 0
        self._precios_append_target_rows: int = PRECIOS_APPEND_TARGET_ROWS
        self._productos_buffer: list[pl.DataFrame] = []

    @abstractmethod
    def setup(self, fecha_vigencia: date) -> None:
        """
        Prepare the destination for data loading.

        This method is called once per execution for a given date, BEFORE any chunks are loaded.
        It should handle idempotent cleanup (e.g., truncating partitions or deleting existing data).

        Args:
            fecha_vigencia (date): The target business date.
        """
        pass

    @abstractmethod
    def load(self, df: pl.DataFrame, fecha_vigencia: date) -> None:
        """
        Load data into the destination system.

        Args:
            df (pl.DataFrame): The data to be loaded (Polars DataFrame).
            fecha_vigencia (date): The target business date for the data.

        Raises:
            Exception: If loading fails.
        """
        pass

    def load_comercios(self, df: pl.DataFrame, fecha_vigencia: date) -> None:
        """Load comercios dimension data. Default no-op."""
        pass

    def load_sucursales(self, df: pl.DataFrame, fecha_vigencia: date) -> None:
        """Load sucursales dimension data. Default no-op."""
        pass

    def load_productos(self, df: pl.DataFrame, fecha_vigencia: date) -> None:
        """Load productos dimension data. Default no-op."""
        pass

    def flush(self, fecha_vigencia: date) -> None:
        """Flush any buffered fact-table chunks. Default no-op."""
        pass

    def log_start(self, fecha_vigencia: date) -> None:
        """Log the start of a loading process."""
        logger.info(f"[{self.stage_name}] Starting load for {fecha_vigencia}")

    def log_success(self, fecha_vigencia: date, rows: int) -> None:
        """Log the successful completion of a loading process."""
        logger.info(
            f"[{self.stage_name}] Successfully loaded {rows} rows for {fecha_vigencia}"
        )
