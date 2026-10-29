from typing import Any, Dict, List

from mcp.server.fastmcp import FastMCP

from serving_mcp.tools import search

try:
    from langfuse import observe
except ImportError:

    def observe(*args: Any, **kwargs: Any) -> Any:
        def decorator(fn: Any) -> Any:
            return fn

        return decorator


mcp = FastMCP("sepa-serving")


@mcp.tool()
@observe(name="search_products_tool", as_type="tool")
def search_products_tool(
    search_query: str = "",
    query: str = "",
    limit: int = 10,
    use_jev: bool = True,
) -> List[Dict[str, Any]]:
    """
    Search for grocery products using PostgreSQL pg_trgm fuzzy recall (Stage 1),
    TypeSafe Jev semantic re-ranking (Stage 2), and store price quotes.

    Args:
        search_query: Search term (e.g. 'fideos', 'leche').
        query: Alternative parameter name for search_query.
        limit: Max products to return.
        use_jev: Whether to apply TypeSafe Jev semantic re-ranking.
    """
    term = search_query or query
    return search.search_products(query=term, limit=limit, use_jev=use_jev)
