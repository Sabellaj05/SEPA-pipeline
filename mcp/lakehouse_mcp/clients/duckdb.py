import logging

import duckdb

from lakehouse_mcp.clients.iceberg import get_catalog
from lakehouse_mcp.config import config

logger = logging.getLogger(__name__)

db_conn = None


def init_duckdb():
    global db_conn
    db_conn = duckdb.connect()
    db_conn.execute("INSTALL httpfs; LOAD httpfs;")

    endpoint = config.minio_endpoint
    if endpoint:
        if endpoint.startswith("http://"):
            endpoint = endpoint[7:]
        elif endpoint.startswith("https://"):
            endpoint = endpoint[8:]

    db_conn.execute(f"""
        CREATE SECRET (
            TYPE S3,
            KEY_ID '{config.minio_access_key}',
            SECRET '{config.minio_secret_key}',
            ENDPOINT '{endpoint}',
            URL_STYLE 'path',
            USE_SSL false
        );
    """)

    # Discover and register views
    try:
        catalog = get_catalog()
        tables = catalog.list_tables("sepa")
        for table_identifier in tables:
            table_name = table_identifier[-1]
            try:
                table = catalog.load_table(table_identifier)
                location = table.location()
                s3_path = f"{location}/data/**/*.parquet"
                db_conn.execute(
                    f"""
                    CREATE OR REPLACE VIEW {table_name} AS
                    SELECT *
                    FROM read_parquet(
                        '{s3_path}',
                        hive_partitioning = true,
                        union_by_name = true
                    )
                    """
                )
                logger.info(f"Registered DuckDB view for {table_name}")
            except Exception as ve:
                logger.warning(
                    "Could not register view for %s "
                    "(likely no parquet files exist yet): %s",
                    table_name,
                    ve,
                )
    except Exception as e:
        logger.warning(f"Could not list tables for DuckDB view initialization: {e}")


def close_duckdb():
    global db_conn
    if db_conn:
        db_conn.close()
        db_conn = None


def get_connection():
    return db_conn
