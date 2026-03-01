from abc import ABC, abstractmethod
from datetime import date

import polars as pl

from sepa_pipeline.utils.logger import get_logger

from ..config import SEPAConfig

logger = get_logger(__name__)


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

    def log_start(self, fecha_vigencia: date):
        """Log the start of a loading process."""
        logger.info(f"[{self.stage_name}] Starting load for {fecha_vigencia}")

    def log_success(self, fecha_vigencia: date, rows: int):
        """Log the successful completion of a loading process."""
        logger.info(
            f"[{self.stage_name}] Successfully loaded {rows} rows for {fecha_vigencia}"
        )
