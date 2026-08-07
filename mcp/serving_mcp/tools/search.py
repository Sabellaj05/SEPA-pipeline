import logging
from typing import Any, Dict, List

from serving_mcp.clients.serving import get_serving_connection

logger = logging.getLogger(__name__)


def search_products(query: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Fast Full-Text Search on the local serving database."""
    conn = get_serving_connection()
    # DuckDB FTS query
    sql = """
        SELECT
            row_id,
            id_producto,
            descripcion,
            marca,
            precio_lista,
            sucursal_nombre,
            fts_main_current_prices.match_bm25(row_id, ?) AS score
        FROM current_prices
        WHERE score > 0
        ORDER BY score DESC
        LIMIT ?
    """
    try:
        df = conn.execute(sql, [query, limit]).pl()
        if df.is_empty():
            return []
        return df.to_dicts()
    except Exception as e:
        logger.error(f"Error running DuckDB FTS query: {e}")
        return [{"error": str(e)}]
