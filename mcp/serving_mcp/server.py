from typing import Any, Dict, List

from mcp.server.fastmcp import FastMCP

from serving_mcp.tools import search

mcp = FastMCP("sepa-serving")


@mcp.tool()
def search_products_tool(search_query: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Search for grocery products by text (e.g. 'fideos', 'salsa') using Full-Text Search.
    Returns the most relevant products along with their prices and store locations.
    """
    return search.search_products(query=search_query, limit=limit)
