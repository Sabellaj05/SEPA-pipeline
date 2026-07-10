import logging
import re
from typing import Any, Dict, List

from lakehouse_mcp.clients.duckdb import get_connection, get_registered_views

logger = logging.getLogger(__name__)

# Only SELECT statements are permitted through run_query.  This rejects COPY,
# LOAD, DROP, CREATE, INSERT, ATTACH, and any other write or meta-command
# before DuckDB ever sees them, providing a read-only enforcement layer on top
# of the in-memory connection (which cannot use duckdb.connect(read_only=True)
# because it has no file).
_SELECT_RE = re.compile(r"^\s*SELECT\b", re.IGNORECASE)

# Hard ceiling so an LLM cannot request unbounded result sets.
_MAX_LIMIT = 5000


def _assert_read_only(sql: str) -> None:
    """Raise ValueError if the statement is not a plain SELECT."""
    if not _SELECT_RE.match(sql):
        raise ValueError(
            f"Only SELECT statements are permitted. Got: {sql.strip()[:80]!r}"
        )


def _normalize_registered_table_names(sql: str) -> str:
    """Map Iceberg-style table names to DuckDB's registered in-memory views."""
    registered = get_registered_views()
    if not registered:
        return sql

    normalized = sql
    for table_name in sorted(registered, key=len, reverse=True):
        normalized = re.sub(
            rf"\bsepa\.{re.escape(table_name)}\b",
            table_name,
            normalized,
            flags=re.IGNORECASE,
        )
    return normalized


def run_query(sql: str, limit: int = 1000) -> List[Dict[str, Any]]:
    """
    Run a read-only SQL SELECT query over the Iceberg tables using DuckDB.

    Non-SELECT statements are rejected before execution.  The result set is
    capped at min(limit, 5000) rows.
    """
    try:
        _assert_read_only(sql)
        sql = _normalize_registered_table_names(sql)

        conn = get_connection()
        if conn is None:
            raise RuntimeError("DuckDB connection is not initialized.")

        limit = max(1, min(limit, _MAX_LIMIT))
        wrapped_sql = f"SELECT * FROM ({sql}) LIMIT {limit}"
        df = conn.execute(wrapped_sql).pl()

        if df.is_empty():
            return []

        return df.to_dicts()
    except (ValueError, RuntimeError) as e:
        logger.warning("Query rejected: %s", e)
        return [{"error": str(e)}]
    except Exception as e:
        logger.error("Error running DuckDB query: %s", e)
        return [{"error": str(e)}]


def preview_table(table_name: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Preview rows from a registered table view.

    table_name is validated against the set of views registered at startup
    before being interpolated into SQL, preventing injection via this path.
    """
    simple_name = table_name.split(".")[-1]
    registered = get_registered_views()

    if not registered:
        return [{"error": "No views are registered. DuckDB may not be initialized."}]

    if simple_name not in registered:
        return [{
            "error": (
                f"Unknown table '{simple_name}'. "
                f"Known tables: {sorted(registered)}"
            )
        }]

    # simple_name is now confirmed to be in the allowlist; safe to interpolate.
    return run_query(f"SELECT * FROM {simple_name}", limit=limit)
