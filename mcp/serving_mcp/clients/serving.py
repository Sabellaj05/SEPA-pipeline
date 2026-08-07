import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

_serving_conn = None


def _resolve_db_path() -> Path:
    """Resolve the serving database, preferring mcp/serving_sample.duckdb."""
    db_path = Path(__file__).resolve().parent.parent.parent / "serving_sample.duckdb"
    if not db_path.exists():
        cwd_path = Path.cwd() / "serving_sample.duckdb"
        if cwd_path.exists():
            db_path = cwd_path
    return db_path


def get_serving_connection() -> duckdb.DuckDBPyConnection:
    global _serving_conn
    if _serving_conn is None:
        db_path = _resolve_db_path()
        if not db_path.exists():
            raise RuntimeError(
                f"Serving database not found at {db_path}. "
                "Please run: python -m serving_mcp.utils.build_serving_db"
            )
        try:
            _serving_conn = duckdb.connect(str(db_path), read_only=True)
            _serving_conn.execute("INSTALL fts; LOAD fts;")
            logger.info(f"Connected to serving database at {db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to serving DB: {e}")
            raise
    return _serving_conn


def close_serving_connection() -> None:
    global _serving_conn
    if _serving_conn:
        _serving_conn.close()
        _serving_conn = None
