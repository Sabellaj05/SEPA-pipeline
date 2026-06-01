from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import FastMCP

from lakehouse_mcp.tools import catalog, query

mcp = FastMCP("lakehouse-mcp")


@mcp.tool()
def query_bronze_audit(
    limit: int = 10,
    fecha_vigencia: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Query the bronze audit table for ingestion stats (Raw ZIP to Parquet).

    Args:
        limit: Max number of records to return (default 10).
        fecha_vigencia: Filter by date (YYYY-MM-DD).
    """
    return catalog.query_bronze_audit(limit=limit, fecha_vigencia=fecha_vigencia)


@mcp.tool()
def query_silver_audit(
    limit: int = 10,
    fecha_vigencia: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Query the silver audit table for validation stats (validation drops, integrity).

    Args:
        limit: Max number of records to return (default 10).
        fecha_vigencia: Filter by date (YYYY-MM-DD).
    """
    return catalog.query_silver_audit(limit=limit, fecha_vigencia=fecha_vigencia)


@mcp.tool()
def list_namespaces() -> List[str]:
    """
    List all namespaces in the Iceberg catalog.
    """
    return catalog.list_namespaces()


@mcp.tool()
def list_tables(namespace: str = "sepa") -> List[str]:
    """
    List all tables in the specified Iceberg namespace.

    Args:
        namespace: Iceberg namespace (default 'sepa').
    """
    return catalog.list_tables(namespace=namespace)


@mcp.tool()
def get_table_schema(table_name: str) -> Dict[str, str]:
    """
    Get the schema (field names and types) of a table.

    Args:
        table_name: Fully qualified table name (e.g., 'sepa.dim_productos').
    """
    return catalog.get_table_schema(table_name=table_name)


@mcp.tool()
def get_table_snapshots(table_name: str) -> List[Dict[str, Any]]:
    """
    Get the snapshot history (commits/time travel references) of a table.

    Args:
        table_name: Fully qualified table name (e.g., 'sepa.dim_productos').
    """
    return catalog.get_table_snapshots(table_name=table_name)


@mcp.tool()
def run_query(sql: str, limit: int = 1000) -> List[Dict[str, Any]]:
    """
    Run an arbitrary read-only SQL query over the Iceberg tables using DuckDB.

    Args:
        sql: The SQL query to run.
        limit: Maximum number of rows to return (default 1000).
    """
    return query.run_query(sql=sql, limit=limit)


@mcp.tool()
def preview_table(table_name: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Preview the specified table using DuckDB read_parquet.

    Args:
        table_name: Table name (e.g., 'dim_productos' or 'sepa.dim_productos').
        limit: Number of rows to return (default 50).
    """
    return query.preview_table(table_name=table_name, limit=limit)
