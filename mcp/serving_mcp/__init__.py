"""Serving MCP package — product-search server for the shopping agent.

Exposes a minimal toolset (``search_products_tool``) backed by the local
read-only serving DuckDB (Full-Text Search).  It deliberately does not
contain audit, catalog, or arbitrary-SQL tools; those live in
``lakehouse_mcp`` (the ops/analytics server).
"""

from serving_mcp.server import mcp

__all__ = ["mcp"]
