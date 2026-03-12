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


class BigQueryLoader(BaseLoader):
    """
    Loader for Apache Iceberg tables stored in S3/MinIO and registered in
    Google BigQuery (BigLake).
    Relies on native write support for PyIceberg.
    """

    def __init__(self, config: SEPAConfig):
        super().__init__(config)

        # Workaround for BigQuery mapping null parent-snapshot-ids to strings instead of ints
        # BigQueryMetastoreCatalog checks the GLOBAL config for this property, not the catalog properties.
        import os
        os.environ["PYICEBERG_LEGACY_CURRENT_SNAPSHOT_ID"] = "True"

        # BigQuery datasets serve as namespaces. Use the one configured.
        self._namespace = self.config.gcp_dataset or "silver"
        self._table_name = "precios"
        self._table_identifier = f"{self._namespace}.{self._table_name}"

        try:
            self.catalog: Catalog | None = load_catalog(
                "default", **self.config.bigquery_catalog_config
            )
            self._iceberg_table: Table | None = None
            logger.info(
                f"[BIGQUERY] Initialized BigQuery Catalog for project "
                f"{self.config.gcp_project}"
            )
        except Exception as e:
            logger.warning(f"[BIGQUERY] Failed to initialize BigQuery Catalog: {e}")
            self.catalog = None

    def setup(self, fecha_vigencia: date) -> None:
        """
        Delete all data for a specific date partition to ensure idempotency.
        """
        if not self.catalog:
            logger.warning("[BIGQUERY] Catalog not initialized, skipping setup.")
            return

        if not self._iceberg_table:
            try:
                self._iceberg_table = self.catalog.load_table(self._table_identifier)
            except NoSuchTableError:
                logger.info(
                    f"[BIGQUERY] Table {self._table_identifier} does not exist, "
                    f"skipping cleanup."
                )
                return

        logger.info(f"[BIGQUERY] Cleaning up existing data for date: {fecha_vigencia}")
        try:
            self._iceberg_table.delete(
                delete_filter=EqualTo("fecha_vigencia", fecha_vigencia)
            )
            logger.info("[BIGQUERY] Cleanup complete.")
        except Exception as e:
            logger.error(f"[BIGQUERY] Failed to cleanup partition: {e}")
            raise

    def _ensure_iceberg_table(self, df: pl.DataFrame) -> None:
        """Ensure the BigLake Iceberg table exists, creating it if necessary."""
        if self._iceberg_table:
            return

        if not self.catalog:
            logger.error("[BIGQUERY] Catalog not initialized, cannot create table")
            return

        try:
            self._iceberg_table = self.catalog.load_table(self._table_identifier)
            logger.info(
                f"[BIGQUERY] Loaded existing BigQuery table: {self._table_identifier}"
            )
        except NoSuchTableError:
            logger.info(
                f"[BIGQUERY] Table {self._table_identifier} not found, "
                f"creating from schema..."
            )

            arrow_table = df.to_arrow()

            try:
                self.catalog.create_namespace(self._namespace)
            except NamespaceAlreadyExistsError:
                pass

            # Create unpartitioned table first
            self._iceberg_table = self.catalog.create_table(
                self._table_identifier,
                schema=arrow_table.schema,
            )
            logger.info(
                f"[BIGQUERY] Created base BigQuery table: {self._table_identifier}"
            )

            # Update Partition Spec
            try:
                with self._iceberg_table.update_spec() as update:
                    update.add_field(
                        "fecha_vigencia",
                        DayTransform(),
                        partition_field_name="fecha_vigencia_day",
                    )
                logger.info("[BIGQUERY] Updated partition spec: Day(fecha_vigencia)")
            except Exception as e:
                logger.error(f"[BIGQUERY] Failed to set partition spec: {e}")

        try:
            # Upgrade existing table to format version 2 if needed
            if str(self._iceberg_table.properties.get("format-version", "1")) == "1":
                with self._iceberg_table.transaction() as tx:
                    tx.set_properties({"format-version": "2"})
                logger.info("[BIGQUERY] Upgraded table to format-version: 2")
        except Exception as e:
            logger.warning(f"[BIGQUERY] Failed to upgrade format-version: {e}")

    def load(self, df: pl.DataFrame, fecha_vigencia: date) -> None:
        """
        Append a chunk of data into the BigQuery Iceberg table.
        """
        self.log_start(fecha_vigencia)

        if not self.catalog:
            logger.warning(
                "[BIGQUERY] Skipping BigQuery append (catalog not initialized)"
            )
            return

        # Ensure fecha_vigencia and scraped_at are present
        cols_to_add = []
        if "scraped_at" not in df.columns:
            cols_to_add.append(pl.lit(datetime.now()).alias("scraped_at"))
        else:
            cols_to_add.append(
                pl.col("scraped_at").dt.replace_time_zone(None).alias("scraped_at")
            )

        if "fecha_vigencia" not in df.columns:
            cols_to_add.append(pl.lit(fecha_vigencia).alias("fecha_vigencia"))

        if cols_to_add:
            df = df.with_columns(cols_to_add)

        self._ensure_iceberg_table(df)

        if self._iceberg_table:
            logger.info(f"[BIGQUERY] Appending {len(df)} rows to BigLake table...")
            self._iceberg_table.append(df.to_arrow())
            self.log_success(fecha_vigencia, len(df))
        else:
            logger.error(
                "[BIGQUERY] Failed to load/create BigQuery table, skipping append"
            )
