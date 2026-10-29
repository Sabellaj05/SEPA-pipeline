import logging
import os
from pathlib import Path

import psycopg2
from dotenv import find_dotenv, load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
env_path = find_dotenv()
if env_path and Path(env_path).parent.resolve() == PROJECT_ROOT.resolve():
    load_dotenv(env_path)
elif (PROJECT_ROOT / ".env").is_file():
    load_dotenv(PROJECT_ROOT / ".env")
else:
    load_dotenv()

logger = logging.getLogger(__name__)

_pg_conn = None


def get_pg_connection() -> psycopg2.extensions.connection:
    """Get or establish connection to the PostgreSQL serving database."""
    global _pg_conn
    if _pg_conn is None or _pg_conn.closed:
        host = os.getenv("SERVING_PG_HOST", os.getenv("POSTGRES_HOST", "localhost"))
        port = int(os.getenv("SERVING_PG_PORT", os.getenv("POSTGRES_PORT", 5432)))
        user = os.getenv("SERVING_PG_USER", "polaris")
        password = os.getenv("SERVING_PG_PASSWORD", "polaris")
        dbname = os.getenv("SERVING_PG_DB", "polaris")
        try:
            _pg_conn = psycopg2.connect(
                host=host, port=port, user=user, password=password, dbname=dbname
            )

            logger.info(
                f"Connected to PostgreSQL serving database at {host}:{port}/{dbname}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL serving database: {e}")
            raise
    return _pg_conn


def close_serving_connection() -> None:
    """Close active database connections."""
    global _pg_conn
    if _pg_conn and not _pg_conn.closed:
        _pg_conn.close()
        _pg_conn = None


# Alias for backward compatibility
get_serving_connection = get_pg_connection
