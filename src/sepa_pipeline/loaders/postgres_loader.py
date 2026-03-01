from datetime import date, datetime
import polars as pl
import psycopg
from psycopg import sql

from .base import BaseLoader
from sepa_pipeline.utils.logger import get_logger

logger = get_logger(__name__)

class PostgresLoader(BaseLoader):
    """
    Loader for PostgreSQL database using range partitioning.
    Handles dimension upserts and efficient bulk loading of fact data.
    """

    def setup(self, fecha_vigencia: date) -> None:
        """
        Prepare the partition for the target date.
        
        Action:
            1. Creates the partition `precios_YYYY_MM_DD` if not exists.
            2. Truncates it to ensure a clean slate (idempotency).
        """
        partition_name = f"precios_{fecha_vigencia.strftime('%Y_%m_%d')}"
        logger.info(f"[POSTGRES] Setup: Preparing partition {partition_name}...")

        with psycopg.connect(self.config.postgres_dsn) as conn:
            with conn.cursor() as cur:
                # 1. Create partition
                cur.execute("SELECT create_precios_partition(%s)", (fecha_vigencia,))

                # 2. Truncate partition (idempotency)
                logger.info(f"[POSTGRES] Truncating partition {partition_name}...")
                try:
                    cur.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(partition_name)))
                except psycopg.errors.UndefinedTable:
                    # Should not happen because we just created it, but defensive coding
                    logger.warning(
                        f"[POSTGRES] Partition {partition_name} not found during truncate (unexpected)."
                    )
            conn.commit()

    def load(self, df: pl.DataFrame, fecha_vigencia: date) -> None:
        """
        Main execution flow for Postgres loading:
        1. Upsert Dimensions (Store, Branch, Product).
        2. Bulk Load Fact (Prices).
        """
        self.log_start(fecha_vigencia)

        # Extract dimension dataframes
        # We need to perform transformations or selections similar to the original loader

        # 1. Comercios (Stores)
        # Unique by (id_comercio, id_bandera)
        df_comercios = df.select([
            "id_comercio", "id_bandera", "comercio_cuit", "comercio_razon_social",
            "comercio_bandera_nombre", "comercio_bandera_url", "comercio_version_sepa"
        ]).unique(subset=["id_comercio", "id_bandera"])
        self._upsert_comercios(df_comercios)

        # 2. Sucursales (Branches)
        # Unique by (id_comercio, id_bandera, id_sucursal)
        # Note: We need all sucursal columns. The dataframe likely has them flattened.
        df_sucursales = df.unique(subset=["id_comercio", "id_bandera", "id_sucursal"])
        self._upsert_sucursales(df_sucursales)

        # 3. Productos (Products)
        # Unique by (id_producto)
        df_productos = df.unique(subset=["id_producto"])
        self._upsert_productos_master(df_productos)

        # 4. Precios (Facts)
        # We need a scraped_at timestamp for the fact table.
        # In current design, scraped_at comes from valid metadata or current time.
        # Since 'df' here is a chunk, we might assume scraped_at is already in it OR passed in.
        # Original loader logic: passed scraped_at explicitly.
        # FIX: The BaseLoader.load signature assumes df + date.
        # We should infer 'scraped_at' from the DataFrame if present, or use NOW().
        # However, purely relying on NOW() inside a chunk loop might give slightly different times.
        # Ideally, 'scraped_at' should be a column in the DF before it reaches here.

        # Let's check if 'scraped_at' is in columns, else use current UTC.
        if "scraped_at" not in df.columns:
            # Fallback to current time if missing (though pipeline should provide it)
            scraped_at = datetime.now()
        else:
            # Take the first value (assuming chunk is uniform) or it's per-row.
            # Best to just use the value if it's there.
            # For bulk load, we need to pass a single value or column.
            # The original logic expected a single datetime object.
            scraped_at = datetime.now() # Placeholder, revised below in _bulk_load

        self._bulk_load_precios(df, fecha_vigencia)

        self.log_success(fecha_vigencia, len(df))

    # --- Internal Helper Methods (Migrated from loader.py) ---

    def _upsert_comercios(self, df: pl.DataFrame) -> None:
        if df.is_empty():
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
                records = df.select([
                    "id_comercio", "id_bandera", "comercio_cuit", "comercio_razon_social",
                    "comercio_bandera_nombre", "comercio_bandera_url", "comercio_version_sepa"
                ]).unique(subset=["id_comercio", "id_bandera"]).rows()

                cur.executemany(insert_sql, records)
                conn.commit()

        logger.debug(f"[POSTGRES] Upserted {len(df)} comercios")

    def _upsert_sucursales(self, df: pl.DataFrame) -> None:
        if df.is_empty():
            return

        cols = [
            "id_comercio", "id_bandera", "id_sucursal", "sucursales_nombre", "sucursales_tipo",
            "sucursales_calle", "sucursales_numero", "sucursales_latitud", "sucursales_longitud",
            "sucursales_observaciones", "sucursales_barrio", "sucursales_codigo_postal",
            "sucursales_localidad", "sucursales_provincia", "sucursales_lunes_horario_atencion",
            "sucursales_martes_horario_atencion", "sucursales_miercoles_horario_atencion",
            "sucursales_jueves_horario_atencion", "sucursales_viernes_horario_atencion",
            "sucursales_sabado_horario_atencion", "sucursales_domingo_horario_atencion"
        ]

        # Add missing columns with None
        for c in cols:
            if c not in df.columns:
                df = df.with_columns(pl.lit(None).alias(c))

        records = df.select(cols).unique(subset=["id_comercio", "id_bandera", "id_sucursal"]).rows()

        # Dynamic SQL construction
        placeholders = ", ".join(["%s"] * len(cols))
        columns_list = ", ".join(cols)

        insert_sql = f"""
            INSERT INTO sucursales ({columns_list}) VALUES ({placeholders})
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

        logger.debug(f"[POSTGRES] Upserted {len(records)} sucursales")

    def _upsert_productos_master(self, df: pl.DataFrame) -> None:
        if df.is_empty():
            return

        expected_cols = [
            "id_producto", "productos_ean", "productos_descripcion",
            "productos_cantidad_presentacion", "productos_unidad_medida_presentacion",
            "productos_marca", "productos_cantidad_referencia",
            "productos_unidad_medida_referencia"
        ]

        # Handle missing columns
        for c in expected_cols:
            if c not in df.columns:
                df = df.with_columns(pl.lit(None).alias(c))

        records = df.select(expected_cols).unique(subset=["id_producto"]).rows()
        placeholders = ", ".join(["%s"] * len(expected_cols))
        columns_list = ", ".join(expected_cols)

        insert_sql = f"""
            INSERT INTO productos_master ({columns_list}, updated_at) 
            VALUES ({placeholders}, NOW())
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

        logger.debug(f"[POSTGRES] Upserted {len(records)} products")

    def _bulk_load_precios(self, df: pl.DataFrame, fecha_vigencia: date) -> None:
        """
        Bulk load precios using PostgreSQL COPY.
        Uses a temp CSV file to maximize throughput.
        """
        # Determine scraped_at
        # Assuming we want to maintain the original scan time if possible.
        # If 'scraped_at' column exists (from upstream), use it. Else default to now.
        if "scraped_at" not in df.columns:
            df = df.with_columns(pl.lit(datetime.now()).alias("scraped_at"))

        # Ensure target column exists
        if "fecha_vigencia" not in df.columns:
             df = df.with_columns(pl.lit(fecha_vigencia).alias("fecha_vigencia"))

        df_precios = df.select([
            "id_comercio", "id_bandera", "id_sucursal", "id_producto",
            "productos_precio_lista", "productos_precio_referencia",
            "productos_precio_unitario_promo1", "productos_leyenda_promo1",
            "productos_precio_unitario_promo2", "productos_leyenda_promo2",
            "productos_descripcion", "productos_marca",
            "scraped_at", "fecha_vigencia"
        ])

        temp_csv = self.config.temp_dir / f"precios_chunk_{fecha_vigencia}_{datetime.now().timestamp()}.csv"

        try:
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
                            
                            # test if performs better next time
                            # while True:
                            #     chunk = f.read(65536)
                            #     if not chunk:
                            #         break
                            #     copy.write(chunk)
                    conn.commit()
        except Exception as e:
            logger.error(f"[POSTGRES] Failed bulk load: {e}")
            raise
        finally:
            if temp_csv.exists():
                temp_csv.unlink()
