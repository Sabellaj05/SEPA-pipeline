"""
SEPA Serving Database Builder
Extracts the latest daily snapshot from the Iceberg Silver Lakehouse
and populates the PostgreSQL serving schema (serving.products and serving.store_prices)
with GIN trigram indexes for high-speed fuzzy recall.
"""

import logging
import os
import sys
import time

from dotenv import find_dotenv, load_dotenv

env_file = find_dotenv()
if env_file:
    load_dotenv(env_file)
load_dotenv("/home/saens/projects/SEPA-pipeline/.env")

from lakehouse_mcp.clients.duckdb import get_connection, init_duckdb  # noqa: E402

logger = logging.getLogger(__name__)

PG_HOST = os.getenv("SERVING_PG_HOST", os.getenv("POSTGRES_HOST", "localhost"))
PG_PORT = os.getenv("SERVING_PG_PORT", os.getenv("POSTGRES_PORT", "5432"))
PG_USER = os.getenv("SERVING_PG_USER", "polaris")
PG_PASSWORD = os.getenv("SERVING_PG_PASSWORD", "polaris")
PG_DB = os.getenv("SERVING_PG_DB", "polaris")


def build_serving_db(target_date: str | None = None) -> None:
    logger.info("Initializing connection to Iceberg catalog via DuckDB...")
    init_duckdb()
    conn = get_connection()

    if conn is None:
        raise RuntimeError("Failed to initialize DuckDB lakehouse connection")

    logger.info("Installing and loading DuckDB postgres extension...")
    conn.execute("INSTALL postgres; LOAD postgres;")

    logger.info(f"Attaching PostgreSQL ({PG_HOST}:{PG_PORT}/{PG_DB})...")
    conn.execute(
        f"ATTACH 'dbname={PG_DB} user={PG_USER} password={PG_PASSWORD} host={PG_HOST} port={PG_PORT}' AS pg (TYPE POSTGRES);"
    )

    # Determine date
    if not target_date:
        res = conn.execute("SELECT MAX(fecha_vigencia) FROM precios").fetchone()
        if not res or not res[0]:
            raise RuntimeError("No data found in precios table")
        target_date = str(res[0])

    logger.info(f"Targeting Silver snapshot date: {target_date}")

    # 1. Prepare schema and extensions in Postgres
    logger.info("Configuring PostgreSQL extensions and serving schema...")
    conn.execute("""
        CREATE SCHEMA IF NOT EXISTS pg.serving;
    """)

    # 2. Build serving.products (catalog aggregation across all branches)
    logger.info("Building serving.products catalog table...")
    t0 = time.time()
    conn.execute(f"""
        CREATE OR REPLACE TABLE pg.serving.products AS
        SELECT
            p.id_producto,
            ANY_VALUE(p.descripcion) AS descripcion,
            ANY_VALUE(p.marca) AS marca,
            ANY_VALUE(p.cantidad_presentacion) AS cantidad_presentacion,
            ANY_VALUE(p.unidad_medida_presentacion) AS unidad_medida_presentacion,
            MIN(p.precio_lista) AS precio_min,
            ROUND(AVG(p.precio_lista), 2) AS precio_avg,
            MAX(p.precio_lista) AS precio_max,
            COUNT(DISTINCT p.id_sucursal)::INTEGER AS sucursales_count,
            COUNT(*)::INTEGER AS quotes_count
        FROM precios p
        WHERE p.fecha_vigencia = '{target_date}'
        GROUP BY p.id_producto;
    """)
    prod_count = conn.execute("SELECT count(*) FROM pg.serving.products").fetchone()[0]
    logger.info(
        f"Loaded {prod_count:,} unique products into serving.products in {time.time() - t0:.2f}s"
    )

    # 3. Build serving.store_prices (store-level quotes with names and locations)
    logger.info("Building serving.store_prices quote table...")
    t1 = time.time()
    conn.execute(f"""
        CREATE OR REPLACE TABLE pg.serving.store_prices AS
        SELECT
            p.id_producto,
            p.id_sucursal,
            s.nombre AS sucursal_nombre,
            c.bandera_nombre AS cadena_nombre,
            s.localidad,
            s.provincia,
            p.precio_lista,
            p.precio_promo1,
            p.leyenda_promo1
        FROM precios p
        LEFT JOIN (
            SELECT id_comercio, id_sucursal, ANY_VALUE(nombre) as nombre, ANY_VALUE(localidad) as localidad, ANY_VALUE(provincia) as provincia
            FROM dim_sucursales
            WHERE fecha_vigencia = (SELECT MAX(fecha_vigencia) FROM dim_sucursales)
            GROUP BY id_comercio, id_sucursal
        ) s ON p.id_sucursal = s.id_sucursal AND p.id_comercio = s.id_comercio
        LEFT JOIN (
            SELECT id_comercio, id_bandera, ANY_VALUE(bandera_nombre) as bandera_nombre
            FROM dim_comercios
            WHERE fecha_vigencia = (SELECT MAX(fecha_vigencia) FROM dim_comercios)
            GROUP BY id_comercio, id_bandera
        ) c ON p.id_comercio = c.id_comercio AND p.id_bandera = c.id_bandera
        WHERE p.fecha_vigencia = '{target_date}';
    """)
    quotes_count = conn.execute(
        "SELECT count(*) FROM pg.serving.store_prices"
    ).fetchone()[0]
    logger.info(
        f"Loaded {quotes_count:,} store quotes into serving.store_prices in {time.time() - t1:.2f}s"
    )

    # 4. Create indexes in Postgres using raw connection
    logger.info("Creating GIN trigram and B-Tree indexes in PostgreSQL...")
    import psycopg2

    pg_conn = psycopg2.connect(
        host=PG_HOST,
        port=int(PG_PORT),
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB,
    )
    pg_cur = pg_conn.cursor()
    pg_cur.execute("""
        CREATE EXTENSION IF NOT EXISTS pg_trgm;
        CREATE EXTENSION IF NOT EXISTS unaccent;

        CREATE OR REPLACE FUNCTION public.immutable_unaccent(text)
          RETURNS text AS
        $func$
        SELECT public.unaccent('public.unaccent', $1)
        $func$ LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT;

        ALTER TABLE serving.products ADD PRIMARY KEY (id_producto);
        CREATE INDEX IF NOT EXISTS idx_products_trgm ON serving.products USING gin (public.immutable_unaccent(descripcion) gin_trgm_ops);
        CREATE INDEX IF NOT EXISTS idx_products_raw_trgm ON serving.products USING gin (descripcion gin_trgm_ops);
        CREATE INDEX IF NOT EXISTS idx_products_marca_trgm ON serving.products USING gin (marca gin_trgm_ops);

        CREATE INDEX IF NOT EXISTS idx_store_prices_prod ON serving.store_prices (id_producto);
        CREATE INDEX IF NOT EXISTS idx_store_prices_cadena ON serving.store_prices (cadena_nombre);
    """)
    pg_conn.commit()
    pg_conn.close()
    logger.info("Serving database build and indexing complete.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    target = sys.argv[1] if len(sys.argv) > 1 else None
    build_serving_db(target)
