"""
SEPA Data Loader
Handles database loading and archiving.
"""
import logging
from datetime import date, datetime
from pathlib import Path

import pyarrow.parquet as pq
import polars as pl
import psycopg
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from pyiceberg.expressions import EqualTo

from sepa_pipeline.config import SEPAConfig

from sepa_pipeline.utils.logger import get_logger

logger = get_logger(__name__)

class SEPALoader:
    """Loads validated data into PostgreSQL and Parquet"""

    def __init__(self, config: SEPAConfig):
        self.config = config
        self._parquet_writer = None
        
        # Initialize Iceberg Catalog
        try:
            self.catalog = load_catalog("default", **self.config.iceberg_catalog_config)
            self._iceberg_table = None
        except Exception as e:
            logger.warning(f"Failed to initialize Iceberg Catalog: {e}")
            self.catalog = None

    def load_to_postgres(
        self,
        df_comercios: pl.DataFrame,
        df_sucursales: pl.DataFrame,
        df_productos: pl.DataFrame,
        scraped_at: datetime,
        fecha_vigencia: date,
    ) -> None:
        """Load data to PostgreSQL using COPY (fastest method)"""
        logger.info(f"Loading data to PostgreSQL for {fecha_vigencia}")

        # Create partition for this date
        with psycopg.connect(self.config.postgres_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT create_precios_partition(%s)", (fecha_vigencia,))
                conn.commit()

        # Upsert dimension tables
        self._upsert_comercios(df_comercios)
        self._upsert_sucursales(df_sucursales)
        self._upsert_productos_master(df_productos)

        # Bulk load precios
        self._bulk_load_precios(df_productos, scraped_at, fecha_vigencia)

        logger.info(f"✅ Loaded {len(df_productos):,} price records to PostgreSQL")

    def prepare_precios_partition(self, fecha_vigencia: date) -> None:
        """Create and truncate the partition for the given date."""
        partition_name = f"precios_{fecha_vigencia.strftime('%Y_%m_%d')}"
        logger.info(f"Preparing partition {partition_name}...")
        
        with psycopg.connect(self.config.postgres_dsn) as conn:
            with conn.cursor() as cur:
                # 1. Create partition
                cur.execute("SELECT create_precios_partition(%s)", (fecha_vigencia,))
                
                # 2. Truncate partition (idempotency)
                logger.info(f"Truncating partition {partition_name}...")
                try:
                    cur.execute(f"TRUNCATE TABLE {partition_name}")
                except psycopg.errors.UndefinedTable:
                    logger.warning(f"Partition {partition_name} does not exist (unexpected), skipping truncate")
                    
            conn.commit()

    def _ensure_iceberg_table(self, df: pl.DataFrame) -> None:
        """Ensure the Iceberg table exists, creating it if necessary."""
        if self._iceberg_table:
            return

        if not self.catalog:
            logger.error("Iceberg catalog not initialized, cannot create table")
            return

        table_identifier = "sepa.precios"
        try:
            self._iceberg_table = self.catalog.load_table(table_identifier)
            logger.info(f"Loaded existing Iceberg table: {table_identifier}")
        except NoSuchTableError:
            logger.info(f"Iceberg table {table_identifier} not found, creating from schema...")
            
            # Use the dataframe schema to create the table
            arrow_table = df.to_arrow()
            # Note: We pass the Arrow schema directly. PyIceberg assigns field IDs.
            
            # Create namespace if it doesn't exist (e.g. 'sepa')
            try:
                self.catalog.create_namespace("sepa")
            except Exception:
                pass # Namespace might exist or error ignored
            
            # 1. Create unpartitioned table first
            self._iceberg_table = self.catalog.create_table(
                table_identifier,
                schema=arrow_table.schema,
            )
            logger.info(f"Created base Iceberg table: {table_identifier}")

            # 2. Update Partition Spec: Day(fecha_vigencia)
            # This is safer than defining it at creation for fresh schemas without IDs
            try:
                with self._iceberg_table.update_spec() as update:
                    update.add_field("fecha_vigencia", DayTransform(), partition_field_name="fecha_vigencia_day")
                logger.info("Updated partition spec: Day(fecha_vigencia)")
            except Exception as e:
                logger.error(f"Failed to set partition spec: {e}")
                # We don't raise here to allow data loading to proceed (unpartitioned backup)

    def cleanup_iceberg_partition(self, fecha_vigencia: date) -> None:
        """
        Delete all data for a specific date partition to ensure idempotency.
        This enables a 'Overwrite (Delete + Append)' strategy for chunked loading.
        """
        table_identifier = "sepa.precios"
        if not self._iceberg_table:
             # Try to load if not already loaded, but don't create
            try:
                if self.catalog:
                    self._iceberg_table = self.catalog.load_table(table_identifier)
            except NoSuchTableError:
                logger.info(f"Table {table_identifier} does not exist, skipping cleanup.")
                return

        if self._iceberg_table:
            logger.info(f"Cleaning up existing data for date: {fecha_vigencia}")
            try:
                # Use PyIceberg expression to delete matching rows
                self._iceberg_table.delete(delete_filter=EqualTo("fecha_vigencia", fecha_vigencia))
                logger.info("Cleanup complete.")
            except Exception as e:
                logger.error(f"Failed to cleanup partition: {e}")
                raise

    def append_to_iceberg(self, df: pl.DataFrame, scraped_at: datetime, fecha_vigencia: date) -> None:
        """Append a chunk of data to the Iceberg table."""
        if not self.catalog:
             logger.warning("Skipping Iceberg append (catalog not init)")
             return
             
        # Enrich DataFrame with timestamps (same as bulk_load_precios)
        # Fix: The table schema expects 'timestamp' (naive), but Fecha().ahora returns 'timestamptz' (aware).
        # We assume the "local" time in AR is what we want to store as the naive timestamp value.
        scraped_at_naive = scraped_at.replace(tzinfo=None) if scraped_at.tzinfo else scraped_at
        
        df = df.with_columns([
            pl.lit(scraped_at_naive).alias("scraped_at"),
            pl.lit(fecha_vigencia).alias("fecha_vigencia"),
        ])

        # Ensure table exists (lazy load)
        self._ensure_iceberg_table(df)
        
        if self._iceberg_table:
            logger.info(f"Appending {len(df)} rows to Iceberg table...")
            self._iceberg_table.append(df.to_arrow())
        else:
            logger.error("Failed to load/create Iceberg table, skipping append")

    def upsert_comercios(self, df: pl.DataFrame) -> None:
        """Upsert comercios dimension table"""
        self._upsert_comercios(df) # Call internal method

    def _upsert_comercios(self, df: pl.DataFrame) -> None:
        """Upsert comercios dimension table (Internal)"""
        if df is None or df.height == 0:
            return
            
        with psycopg.connect(self.config.postgres_dsn) as conn:
            with conn.cursor() as cur:
                insert_sql = """
                    INSERT INTO comercios (
                        id_comercio, id_bandera, comercio_cuit, comercio_razon_social,
                        comercio_bandera_nombre, comercio_bandera_url, comercio_version_sepa,
                        updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (id_comercio, id_bandera)
                    DO UPDATE SET
                        comercio_razon_social = EXCLUDED.comercio_razon_social,
                        comercio_bandera_nombre = EXCLUDED.comercio_bandera_nombre,
                        comercio_bandera_url = EXCLUDED.comercio_bandera_url,
                        updated_at = NOW()
                """

                records = (
                    df.select(
                        [
                            "id_comercio",
                            "id_bandera",
                            "comercio_cuit",
                            "comercio_razon_social",
                            "comercio_bandera_nombre",
                            "comercio_bandera_url",
                            "comercio_version_sepa",
                        ]
                    ).rows()
                )

                cur.executemany(insert_sql, records)
                conn.commit()

        logger.info(f"Upserted {len(df)} comercio records")

    def upsert_sucursales(self, df: pl.DataFrame) -> None:
        self._upsert_sucursales(df)

    def _upsert_sucursales(self, df: pl.DataFrame) -> None:
        """Upsert sucursales dimension table."""
        if df is None or df.height == 0:
            logger.info("No sucursales to upsert")
            return

        # Keep the same column ordering as the DB table; provide defaults for missing optional columns
        cols = [
            "id_comercio",
            "id_bandera",
            "id_sucursal",
            "sucursales_nombre",
            "sucursales_tipo",
            "sucursales_calle",
            "sucursales_numero",
            "sucursales_latitud",
            "sucursales_longitud",
            "sucursales_observaciones",
            "sucursales_barrio",
            "sucursales_codigo_postal",
            "sucursales_localidad",
            "sucursales_provincia",
            "sucursales_lunes_horario_atencion",
            "sucursales_martes_horario_atencion",
            "sucursales_miercoles_horario_atencion",
            "sucursales_jueves_horario_atencion",
            "sucursales_viernes_horario_atencion",
            "sucursales_sabado_horario_atencion",
            "sucursales_domingo_horario_atencion",
        ]

        # Ensure all columns exist in df, if not add them with nulls
        for c in cols:
            if c not in df.columns:
                df = df.with_columns(pl.lit(None).alias(c))

        records = df.select(cols).rows()

        insert_sql = f"""
                INSERT INTO sucursales (
                    {", ".join(cols)}
                ) VALUES (
                    {", ".join(["%s"] * len(cols))}
                )
                ON CONFLICT (id_comercio, id_bandera, id_sucursal)
                DO UPDATE SET
                    sucursales_nombre = EXCLUDED.sucursales_nombre,
                    sucursales_tipo = EXCLUDED.sucursales_tipo,
                    sucursales_calle = EXCLUDED.sucursales_calle,
                    sucursales_numero = EXCLUDED.sucursales_numero,
                    sucursales_latitud = EXCLUDED.sucursales_latitud,
                    sucursales_longitud = EXCLUDED.sucursales_longitud,
                    sucursales_observaciones = EXCLUDED.sucursales_observaciones,
                    sucursales_barrio = EXCLUDED.sucursales_barrio,
                    sucursales_codigo_postal = EXCLUDED.sucursales_codigo_postal,
                    sucursales_localidad = EXCLUDED.sucursales_localidad,
                    sucursales_provincia = EXCLUDED.sucursales_provincia,
                    sucursales_lunes_horario_atencion = EXCLUDED.sucursales_lunes_horario_atencion,
                    sucursales_martes_horario_atencion = EXCLUDED.sucursales_martes_horario_atencion,
                    sucursales_miercoles_horario_atencion = EXCLUDED.sucursales_miercoles_horario_atencion,
                    sucursales_jueves_horario_atencion = EXCLUDED.sucursales_jueves_horario_atencion,
                    sucursales_viernes_horario_atencion = EXCLUDED.sucursales_viernes_horario_atencion,
                    sucursales_sabado_horario_atencion = EXCLUDED.sucursales_sabado_horario_atencion,
                    sucursales_domingo_horario_atencion = EXCLUDED.sucursales_domingo_horario_atencion,
                    updated_at = NOW()
            """

        with psycopg.connect(self.config.postgres_dsn) as conn:
            with conn.cursor() as cur:
                cur.executemany(insert_sql, records)
                conn.commit()

        logger.info(f"Upserted {len(records)} sucursal records")

    def _upsert_productos_master(self, df: pl.DataFrame) -> None:
        """Upsert productos_master table (populate basic master records)."""
        if df is None or df.height == 0:
            logger.info("No productos to upsert")
            return

        # reduce to unique products to upsert
        unique_products = df.select(
            [
                "id_producto",
                "productos_ean",
                "productos_descripcion",
                "productos_cantidad_presentacion",
                "productos_unidad_medida_presentacion",
                "productos_marca",
                "productos_cantidad_referencia",
                "productos_unidad_medida_referencia",
            ]
        ).unique(subset=["id_producto"])

        # Ensure columns exist
        expected_cols = [
            "id_producto",
            "productos_ean",
            "productos_descripcion",
            "productos_cantidad_presentacion",
            "productos_unidad_medida_presentacion",
            "productos_marca",
            "productos_cantidad_referencia",
            "productos_unidad_medida_referencia",
        ]
        for c in expected_cols:
            if c not in unique_products.columns:
                unique_products = unique_products.with_columns(pl.lit(None).alias(c))

        records = unique_products.select(expected_cols).rows()

        insert_sql = f"""
            INSERT INTO productos_master (
                id_producto,
                productos_ean,
                productos_descripcion,
                productos_cantidad_presentacion,
                productos_unidad_medida_presentacion,
                productos_marca,
                productos_cantidad_referencia,
                productos_unidad_medida_referencia,
                updated_at
            ) VALUES (
                {", ".join(["%s"] * len(expected_cols))},
                NOW()
            )
            ON CONFLICT (id_producto)
            DO UPDATE SET
                productos_descripcion = EXCLUDED.productos_descripcion,
                productos_marca = EXCLUDED.productos_marca,
                productos_cantidad_presentacion = EXCLUDED.productos_cantidad_presentacion,
                productos_unidad_medida_presentacion = EXCLUDED.productos_unidad_medida_presentacion,
                productos_cantidad_referencia = EXCLUDED.productos_cantidad_referencia,
                productos_unidad_medida_referencia = EXCLUDED.productos_unidad_medida_referencia,
                updated_at = NOW()
        """

        with psycopg.connect(self.config.postgres_dsn) as conn:
            with conn.cursor() as cur:
                cur.executemany(insert_sql, records)
                conn.commit()

        logger.info(f"Upserted {len(records)} unique products into productos_master")
    
    def upsert_productos_master(self, df: pl.DataFrame) -> None:
        self._upsert_productos_master(df)

    def bulk_load_precios(
        self, df: pl.DataFrame, scraped_at: datetime, fecha_vigencia: date
    ) -> None:
        return self._bulk_load_precios(df, scraped_at, fecha_vigencia)

    def _bulk_load_precios(
        self, df: pl.DataFrame, scraped_at: datetime, fecha_vigencia: date
    ) -> None:
        """Bulk load precios using PostgreSQL COPY"""
        df_precios = df.select(
            [
                "id_comercio",
                "id_bandera",
                "id_sucursal",
                "id_producto",
                "productos_precio_lista",
                "productos_precio_referencia",
                "productos_precio_unitario_promo1",
                "productos_leyenda_promo1",
                "productos_precio_unitario_promo2",
                "productos_leyenda_promo2",
                "productos_descripcion",
                "productos_marca",
            ]
        ).with_columns(
            [
                pl.lit(scraped_at).alias("scraped_at"),
                pl.lit(fecha_vigencia).alias("fecha_vigencia"),
            ]
        )

        # df_precios = df.with_columns(
        #     [
        #         pl.lit(scraped_at).alias("scraped_at"),
        #         pl.lit(fecha_vigencia).alias("fecha_vigencia"),
        #     ]
        # )

        temp_csv = Path(f"/tmp/precios_{fecha_vigencia}.csv")
        df_precios.write_csv(temp_csv, separator="|")

        with psycopg.connect(self.config.postgres_dsn) as conn:
            with conn.cursor() as cur:
                with open(temp_csv, "r") as f:
                    next(f)  # Skip header

                    copy_sql = """
                        COPY precios (
                            id_comercio, id_bandera, id_sucursal, id_producto,
                            productos_precio_lista, productos_precio_referencia,
                            productos_precio_unitario_promo1, productos_leyenda_promo1,
                            productos_precio_unitario_promo2, productos_leyenda_promo2,
                            productos_descripcion, productos_marca,
                            scraped_at, fecha_vigencia
                        ) FROM STDIN WITH (FORMAT CSV, DELIMITER '|', NULL '')
                    """

                    with cur.copy(copy_sql) as copy:
                        for line in f:
                            copy.write(line)

                conn.commit()

        temp_csv.unlink()

    def append_to_parquet(
        self, df: pl.DataFrame, fecha_vigencia: date
    ) -> None:
        """Append chunk to a single Parquet file in MinIO/S3"""
        from pyarrow import fs

        # Initialize S3 Filesystem
        s3 = fs.S3FileSystem(
            endpoint_override=self.config.minio_endpoint,
            access_key=self.config.minio_access_key,
            secret_key=self.config.minio_secret_key,
            scheme="http",  # Use https if configured
            region="us-east-1", # Required for MinIO compatibility
        )

        # Define path in bucket
        file_path = (
            f"{self.config.minio_bucket}/bronze/precios/"
            f"year={fecha_vigencia.year}/"
            f"month={fecha_vigencia.month:02d}/"
            f"day={fecha_vigencia.day:02d}/"
            "precios.parquet"
        )

        # Convert to Arrow Table
        table = df.to_arrow()

        if self._parquet_writer is None:
            logger.info(f"Creating new Parquet writer for s3://{file_path}")
            
            # Open output stream on S3
            # We need to keep the file open across chunks, so we store the writer.
            # PyArrow's ParquetWriter can take a filesystem object or an open file-like object.
            # Passing the path and filesystem is usually easiest.
            
            self._parquet_writer = pq.ParquetWriter(
                file_path,
                table.schema,
                compression="zstd",
                filesystem=s3
            )
        
        self._parquet_writer.write_table(table)
        logger.info(f"Appended {len(df)} rows to Parquet (S3)")

    def close_parquet_writer(self) -> None:
        """Close the Parquet writer if open"""
        if self._parquet_writer:
            self._parquet_writer.close()
            self._parquet_writer = None
            logger.info("Closed Parquet writer")
