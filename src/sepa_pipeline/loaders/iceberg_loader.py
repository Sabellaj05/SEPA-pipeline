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

# S3_ENDPOINT key as used by PyIceberg's FsspecFileIO
_S3_ENDPOINT_KEY = "s3.endpoint"

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
            self._dim_tables: dict[str, Table] = {}
            self._seen_productos: set[str] = set()
        except Exception as e:
            logger.warning(f"Failed to initialize Iceberg Catalog: {e}")
            self.catalog = None

    def _fix_io_endpoint(self, table: Table) -> None:
        """Override the S3 endpoint Nessie pushes into the table's FileIO properties.

        Nessie is configured to use 'http://minio:9000' internally (Docker network).
        It embeds this into the Iceberg REST catalog response as an 'override', so
        PyIceberg's FsspecFileIO picks it up. From the host OS, 'minio' is not
        resolvable. This method patches the endpoint to 'localhost:9000' BEFORE
        the first file operation, so the per-thread filesystem cache is built
        with the correct URL.
        StarRocks (running inside Docker) does NOT use this FileIO — it has its
        own Java S3 client configured via the catalog's 'aws.s3.endpoint' property.
        """
        s3_endpoint = self.config.minio_endpoint  # e.g. http://localhost:9000
        if hasattr(table, "_io") and hasattr(table._io, "properties"):
            if table._io.properties.get(_S3_ENDPOINT_KEY) != s3_endpoint:
                table._io.properties[_S3_ENDPOINT_KEY] = s3_endpoint
                # Clear the per-thread lru_cache so the next filesystem access
                # re-creates the S3FileSystem with the corrected endpoint.
                if hasattr(table._io, "_thread_locals") and hasattr(
                    table._io._thread_locals, "get_fs_cached"
                ):
                    table._io._thread_locals.get_fs_cached.cache_clear()
                logger.debug(
                    f"[ICEBERG] Overrode s3.endpoint to {s3_endpoint} for host-side FileIO"
                )

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
                self._fix_io_endpoint(self._iceberg_table)
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

        # Cleanup dimension tables
        for dim_table in ["dim_comercios", "dim_sucursales", "dim_productos"]:
            self._cleanup_dimension_table(dim_table, fecha_vigencia)

        # Clear seen products for the new load
        self._seen_productos.clear()

    def _cleanup_dimension_table(self, table_name: str, fecha_vigencia: date) -> None:
        if not self.catalog:
            return
        identifier = f"sepa.{table_name}"
        try:
            table = self.catalog.load_table(identifier)
            self._fix_io_endpoint(table)
            table.delete(delete_filter=EqualTo("fecha_vigencia", fecha_vigencia))
            logger.info(f"[ICEBERG] Cleanup complete for {identifier}.")
        except NoSuchTableError:
            pass
        except Exception as e:
            logger.warning(f"[ICEBERG] Failed to cleanup {identifier}: {e}")

    def _ensure_iceberg_table(self, df: pl.DataFrame) -> None:
        """Ensure the Iceberg table exists, creating it if necessary."""
        if self._iceberg_table:
            return

        if not self.catalog:
            logger.error("[ICEBERG] Iceberg catalog not initialized, cannot create table")
            return

        try:
            self._iceberg_table = self.catalog.load_table(self._table_identifier)
            self._fix_io_endpoint(self._iceberg_table)
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
            self._fix_io_endpoint(self._iceberg_table)
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

    def _ensure_dimension_table(self, table_name: str, df: pl.DataFrame) -> Table | None:
        if not self.catalog:
            return None
        
        identifier = f"sepa.{table_name}"
        if identifier in self._dim_tables:
            return self._dim_tables[identifier]

        try:
            table = self.catalog.load_table(identifier)
            self._fix_io_endpoint(table)
            self._dim_tables[identifier] = table
            # Upgrade existing table to format version 2 if needed
            if str(table.properties.get("format-version", "1")) == "1":
                with table.transaction() as tx:
                    tx.set_properties({"format-version": "2"})
            return table
        except NoSuchTableError:
            logger.info(f"[ICEBERG] Creating dimension table {identifier}...")
            arrow_table = df.to_arrow()
            try:
                self.catalog.create_namespace("sepa")
            except NamespaceAlreadyExistsError:
                pass

            try:
                table = self.catalog.create_table(identifier, schema=arrow_table.schema)
                self._fix_io_endpoint(table)
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
                logger.error(f"[ICEBERG] Failed to create dimension table {identifier}: {e}")
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
            logger.info(f"[ICEBERG] Appending {len(df)} rows to dim_comercios...")
            table.append(df.to_arrow())

    def load_sucursales(self, df: pl.DataFrame, fecha_vigencia: date) -> None:
        if not self.catalog or df.is_empty():
            return
        df = self._prepare_dim_df(df, fecha_vigencia)
        table = self._ensure_dimension_table("dim_sucursales", df)
        if table:
            logger.info(f"[ICEBERG] Appending {len(df)} rows to dim_sucursales...")
            table.append(df.to_arrow())

    def load_productos(self, df: pl.DataFrame, fecha_vigencia: date) -> None:
        if not self.catalog or df.is_empty():
            return
        
        # Ensure the chunk itself only has unique products before checking against history
        df_unique = df.unique(subset=["id_producto"])
        
        # Filter out products we've already seen today
        new_products = df_unique.filter(~pl.col("id_producto").is_in(list(self._seen_productos)))
        
        if new_products.is_empty():
            logger.debug("[ICEBERG] No new products to append to dim_productos in this chunk.")
            return

        # Update seen products
        new_ids = new_products["id_producto"].to_list()
        self._seen_productos.update(new_ids)

        new_products = self._prepare_dim_df(new_products, fecha_vigencia)
        table = self._ensure_dimension_table("dim_productos", new_products)
        if table:
            logger.info(f"[ICEBERG] Appending {len(new_products)} rows to dim_productos...")
            table.append(new_products.to_arrow())
