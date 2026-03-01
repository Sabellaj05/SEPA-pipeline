from datetime import date, datetime
import polars as pl
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.expressions import EqualTo
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform

from sepa_pipeline.utils.logger import get_logger
from sepa_pipeline.config import SEPAConfig
from .base import BaseLoader

logger = get_logger(__name__)


class IcebergLoader(BaseLoader):
    """
    Loader for Apache Iceberg tables stored in S3/MinIO using PyIceberg.
    """

    def __init__(self, config: SEPAConfig):
        super().__init__(config)
        self._table_identifier = "sepa.precios"
        try:
            self.catalog: Catalog | None = load_catalog(
                "default", **self.config.iceberg_catalog_config
            )
            self._iceberg_table: Table | None = None
        except Exception as e:
            logger.warning(f"Failed to initialize Iceberg Catalog: {e}")
            self.catalog = None

    def setup(self, fecha_vigencia: date) -> None:
        """
        Delete all data for a specific date partition to ensure idempotency.
        This enables a 'Overwrite (Delete + Append)' strategy for chunked loading.
        """
        if not self.catalog:
            logger.warning("[ICEBERG] Catalog not initialized, skipping setup.")
            return

        # Attempt to load the table if it exists
        if not self._iceberg_table:
            try:
                self._iceberg_table = self.catalog.load_table(self._table_identifier)
            except NoSuchTableError:
                logger.info(
                    f"Table {self._table_identifier} does not exist, skipping cleanup."
                )
                return

        logger.info(f"[ICEBERG] Cleaning up existing data for date: {fecha_vigencia}")
        try:
            # Use PyIceberg expression to delete matching rows idempotently
            self._iceberg_table.delete(
                delete_filter=EqualTo("fecha_vigencia", fecha_vigencia)
            )
            logger.info("[ICEBERG] Cleanup complete.")
        except Exception as e:
            logger.error(f"[ICEBERG] Failed to cleanup partition: {e}")
            raise

    def _ensure_iceberg_table(self, df: pl.DataFrame) -> None:
        """Ensure the Iceberg table exists, creating it if necessary."""
        if self._iceberg_table:
            return

        if not self.catalog:
            logger.error("[ICEBERG] Iceberg catalog not initialized, cannot create table")
            return

        try:
            self._iceberg_table = self.catalog.load_table(self._table_identifier)
            logger.info(f"[ICEBERG] Loaded existing Iceberg table: {self._table_identifier}")
        except NoSuchTableError:
            logger.info(f"[ICEBERG] Iceberg table {self._table_identifier} not found, creating from schema...")

            arrow_table = df.to_arrow()

            # Create namespace if it doesn't exist
            try:
                self.catalog.create_namespace("sepa")
            except NamespaceAlreadyExistsError:
                pass  # Ignore if it already exists implicitly

            # 1. Create unpartitioned table first
            self._iceberg_table = self.catalog.create_table(
                self._table_identifier,
                schema=arrow_table.schema,
            )
            logger.info(f"[ICEBERG] Created base Iceberg table: {self._table_identifier}")

            # 2. Update Partition Spec: Day(fecha_vigencia)
            try:
                with self._iceberg_table.update_spec() as update:
                    update.add_field(
                        "fecha_vigencia",
                        DayTransform(),
                        partition_field_name="fecha_vigencia_day",
                    )
                logger.info("[ICEBERG] Updated partition spec: Day(fecha_vigencia)")
            except Exception as e:
                logger.error(f"[ICEBERG] Failed to set partition spec: {e}")

    def load(self, df: pl.DataFrame, fecha_vigencia: date) -> None:
        """
        Append a chunk of data into the Iceberg table.
        """
        self.log_start(fecha_vigencia)

        if not self.catalog:
            logger.warning("[ICEBERG] Skipping Iceberg append (catalog not initialized)")
            return

        # Use the scraped_at defined in the dataframe if it's there, otherwise datetime.now()
        # Ensure it is naive (without timezone) as the Iceberg schema was created assuming naive timestamps
        if "scraped_at" not in df.columns:
            scraped_at_naive = datetime.now()
        else:
            # Grab the first element and parse it, or let polars handle timezone stripping
            # Typically pipeline.py will pass a timestamptz that might need to be naive.
            # Easiest way in polars: cast to Datetime without timezone
            # But normally we just inject it if missing, or strip it if present.
            pass

        # Ensure fecha_vigencia and scraped_at are present
        cols_to_add = []
        if "scraped_at" not in df.columns:
             cols_to_add.append(pl.lit(datetime.now()).alias("scraped_at"))
        else:
             # strip timezone from existing column just in case to match legacy behavior
             cols_to_add.append(pl.col("scraped_at").dt.replace_time_zone(None).alias("scraped_at"))

        if "fecha_vigencia" not in df.columns:
             cols_to_add.append(pl.lit(fecha_vigencia).alias("fecha_vigencia"))

        if cols_to_add:
            df = df.with_columns(cols_to_add)

        # Ensure table exists (lazy load via first chunk)
        self._ensure_iceberg_table(df)

        if self._iceberg_table:
            logger.info(f"[ICEBERG] Appending {len(df)} rows to Iceberg table...")
            self._iceberg_table.append(df.to_arrow())
            self.log_success(fecha_vigencia, len(df))
        else:
            logger.error("[ICEBERG] Failed to load/create Iceberg table, skipping append")
