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
            self._dim_tables: dict[str, Table] = {}
            self._seen_productos: set[str] = set()
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

        # Cleanup dimension tables
        for dim_table in ["dim_comercios", "dim_sucursales", "dim_productos"]:
            self._cleanup_dimension_table(dim_table, fecha_vigencia)

        # Clear seen products for the new load
        self._seen_productos.clear()

    def _cleanup_dimension_table(self, table_name: str, fecha_vigencia: date) -> None:
        if not self.catalog:
            return
        identifier = f"{self._namespace}.{table_name}"
        try:
            table = self.catalog.load_table(identifier)
            table.delete(delete_filter=EqualTo("fecha_vigencia", fecha_vigencia))
            logger.info(f"[BIGQUERY] Cleanup complete for {identifier}.")
        except NoSuchTableError:
            pass
        except Exception as e:
            logger.warning(f"[BIGQUERY] Failed to cleanup {identifier}: {e}")

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

    def _ensure_dimension_table(self, table_name: str, df: pl.DataFrame) -> Table | None:
        if not self.catalog:
            return None
        
        identifier = f"{self._namespace}.{table_name}"
        if identifier in self._dim_tables:
            return self._dim_tables[identifier]

        try:
            table = self.catalog.load_table(identifier)
            self._dim_tables[identifier] = table
            # Upgrade existing table to format version 2 if needed
            if str(table.properties.get("format-version", "1")) == "1":
                with table.transaction() as tx:
                    tx.set_properties({"format-version": "2"})
            return table
        except NoSuchTableError:
            logger.info(f"[BIGQUERY] Creating dimension table {identifier}...")
            arrow_table = df.to_arrow()
            try:
                table = self.catalog.create_table(identifier, schema=arrow_table.schema)
                with table.update_spec() as update:
                    update.add_field(
                        "fecha_vigencia",
                        DayTransform(),
                        partition_field_name="fecha_vigencia_day",
                    )
                # Ensure format version 2
                with table.transaction() as tx:
                    tx.set_properties({"format-version": "2"})
                self._dim_tables[identifier] = table
                return table
            except Exception as e:
                logger.error(f"[BIGQUERY] Failed to create dimension table {identifier}: {e}")
                return None

    def _prepare_dim_df(self, df: pl.DataFrame, fecha_vigencia: date) -> pl.DataFrame:
        cols_to_add = []
        if "fecha_vigencia" not in df.columns:
            cols_to_add.append(pl.lit(fecha_vigencia).alias("fecha_vigencia"))
        if cols_to_add:
            df = df.with_columns(cols_to_add)
        return df

    def load_comercios(self, df: pl.DataFrame, fecha_vigencia: date) -> None:
        if not self.catalog or df.is_empty():
            return
        df = self._prepare_dim_df(df, fecha_vigencia)
        table = self._ensure_dimension_table("dim_comercios", df)
        if table:
            logger.info(f"[BIGQUERY] Appending {len(df)} rows to dim_comercios...")
            table.append(df.to_arrow())

    def load_sucursales(self, df: pl.DataFrame, fecha_vigencia: date) -> None:
        if not self.catalog or df.is_empty():
            return
        df = self._prepare_dim_df(df, fecha_vigencia)
        table = self._ensure_dimension_table("dim_sucursales", df)
        if table:
            logger.info(f"[BIGQUERY] Appending {len(df)} rows to dim_sucursales...")
            table.append(df.to_arrow())

    def load_productos(self, df: pl.DataFrame, fecha_vigencia: date) -> None:
        if not self.catalog or df.is_empty():
            return
        
        # Ensure the chunk itself only has unique products before checking against history
        df_unique = df.unique(subset=["id_producto"])
        
        # Filter out products we've already seen today
        new_products = df_unique.filter(~pl.col("id_producto").is_in(list(self._seen_productos)))
        
        if new_products.is_empty():
            logger.debug("[BIGQUERY] No new products to append to dim_productos in this chunk.")
            return

        # Update seen products
        new_ids = new_products["id_producto"].to_list()
        self._seen_productos.update(new_ids)

        new_products = self._prepare_dim_df(new_products, fecha_vigencia)
        table = self._ensure_dimension_table("dim_productos", new_products)
        if table:
            logger.info(f"[BIGQUERY] Appending {len(new_products)} rows to dim_productos...")
            table.append(new_products.to_arrow())
