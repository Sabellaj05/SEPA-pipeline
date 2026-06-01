import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import polars as pl
from pyiceberg.expressions import EqualTo

from lakehouse_mcp.clients.iceberg import get_table, get_catalog

logger = logging.getLogger(__name__)


def query_bronze_audit(
    limit: int = 10,
    fecha_vigencia: Optional[str] = None,
) -> List[Dict[str, Any]]:
    try:
        table = get_table("sepa.audit_bronze")
        scan = table.scan()

        if fecha_vigencia:
            val = date.fromisoformat(fecha_vigencia)
            scan = table.scan(row_filter=EqualTo("fecha_vigencia", val))

        arrow_table = scan.to_arrow()
        df = pl.from_arrow(arrow_table)

        if df.is_empty():
            return []

        result = df.sort("ingested_at", descending=True).head(limit)
        return result.to_dicts()
    except Exception as e:
        logger.error(f"Error querying bronze audit: {e}")
        return [{"error": str(e)}]


def query_silver_audit(
    limit: int = 10,
    fecha_vigencia: Optional[str] = None,
) -> List[Dict[str, Any]]:
    try:
        table = get_table("sepa.audit_silver")
        scan = table.scan()

        if fecha_vigencia:
            val = date.fromisoformat(fecha_vigencia)
            scan = table.scan(row_filter=EqualTo("fecha_vigencia", val))

        arrow_table = scan.to_arrow()
        df = pl.from_arrow(arrow_table)

        if df.is_empty():
            return []

        result = df.sort("ingested_at", descending=True).head(limit)
        return result.to_dicts()
    except Exception as e:
        logger.error(f"Error querying silver audit: {e}")
        return [{"error": str(e)}]


def list_namespaces() -> List[str]:
    try:
        catalog = get_catalog()
        namespaces = catalog.list_namespaces()
        return [".".join(ns) for ns in namespaces]
    except Exception as e:
        logger.error(f"Error listing namespaces: {e}")
        return [str(e)]


def list_tables(namespace: str = "sepa") -> List[str]:
    try:
        catalog = get_catalog()
        tables = catalog.list_tables(namespace)
        return [t[-1] for t in tables]
    except Exception as e:
        logger.error(f"Error listing tables for namespace {namespace}: {e}")
        return [str(e)]


def get_table_schema(table_name: str) -> Dict[str, str]:
    try:
        fqn = table_name if "." in table_name else f"sepa.{table_name}"
        table = get_table(fqn)
        schema = table.schema()
        return {field.name: str(field.field_type) for field in schema.fields}
    except Exception as e:
        logger.error(f"Error getting schema for {table_name}: {e}")
        return {"error": str(e)}


def get_table_snapshots(table_name: str) -> List[Dict[str, Any]]:
    try:
        fqn = table_name if "." in table_name else f"sepa.{table_name}"
        table = get_table(fqn)
        snapshots = table.snapshots()
        result = []
        for snap in snapshots:
            result.append({
                "snapshot_id": snap.snapshot_id,
                "timestamp_ms": snap.timestamp_ms,
                "datetime": str(datetime.fromtimestamp(snap.timestamp_ms / 1000.0)),
                "manifest_list": snap.manifest_list,
                "summary": snap.summary,
            })
        return result
    except Exception as e:
        logger.error(f"Error getting snapshots for {table_name}: {e}")
        return [{"error": str(e)}]
