import logging

import duckdb

from lakehouse_mcp.clients.duckdb import get_connection, init_duckdb

logger = logging.getLogger(__name__)


def build_sample() -> None:
    logger.info("Initializing connection to Iceberg catalog via DuckDB...")
    init_duckdb()
    conn = get_connection()

    if conn is None:
        raise RuntimeError("Failed to initialize DuckDB connection")

    logger.info("Creating persistent serving database (serving_sample.duckdb)...")
    conn.execute("ATTACH 'serving_sample.duckdb' AS serving (TYPE DUCKDB)")

    logger.info("Building current_prices table from latest Iceberg snapshot...")
    try:
        conn.execute("""
            CREATE OR REPLACE TABLE serving.current_prices AS
            SELECT
                row_number() over () as row_id,
                p.id_producto,
                p.descripcion,
                p.marca,
                p.precio_lista,
                p.precio_promo1,
                s.nombre AS sucursal_nombre,
                s.latitud,
                s.longitud
            FROM precios p
            JOIN dim_sucursales s ON p.id_sucursal = s.id_sucursal
            WHERE p.fecha_vigencia = (SELECT MAX(fecha_vigencia) FROM precios)
            LIMIT 50000;
        """)
        logger.info("Successfully created serving.current_prices table.")

        logger.info("Installing and Loading FTS extension...")
        conn.execute("INSTALL fts; LOAD fts;")

        logger.info("Building FTS index on descripcion and marca...")
        conn.execute("USE serving")
        conn.execute("""
            PRAGMA create_fts_index(
                'current_prices',
                'row_id',
                'descripcion',
                'marca',
                overwrite=1
            );
        """)
        logger.info("Successfully created FTS index.")
        print("Serving database created at serving_sample.duckdb")
    except duckdb.CatalogException as e:
        logger.error(
            "Failed to query Iceberg tables. Ensure you have loaded data into "
            f"the lakehouse: {e}"
        )
        print(f"Error: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    build_sample()
