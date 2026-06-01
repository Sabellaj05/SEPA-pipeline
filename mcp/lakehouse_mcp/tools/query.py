from typing import Any, Dict, List
import logging
from lakehouse_mcp.clients.duckdb import get_connection

logger = logging.getLogger(__name__)

def run_query(sql: str, limit: int = 1000) -> List[Dict[str, Any]]:
    try:
        conn = get_connection()
        if not conn:
            return [{"error": "DuckDB connection is not initialized"}]

        wrapped_sql = f"SELECT * FROM ({sql}) LIMIT {limit}"
        df = conn.execute(wrapped_sql).pl()

        if df.is_empty():
            return []

        return df.to_dicts()
    except Exception as e:
        logger.error(f"Error running DuckDB query: {e}")
        return [{"error": str(e)}]


def preview_table(table_name: str, limit: int = 50) -> List[Dict[str, Any]]:
    simple_name = table_name.split(".")[-1]
    return run_query(f"SELECT * FROM {simple_name}", limit=limit)
