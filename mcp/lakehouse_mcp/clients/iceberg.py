from lakehouse_mcp.config import config

from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table


def _fix_io_endpoint(table: Table) -> None:
    """Patch S3 endpoint for host-side access (as seen in IcebergLoader)."""
    s3_endpoint = config.minio_endpoint
    if hasattr(table, "_io") and hasattr(table._io, "properties"):
        if table._io.properties.get("s3.endpoint") != s3_endpoint:
            table._io.properties["s3.endpoint"] = s3_endpoint
            if hasattr(table._io, "_thread_locals") and hasattr(
                table._io._thread_locals, "get_fs_cached"
            ):
                table._io._thread_locals.get_fs_cached.cache_clear()


def get_catalog():
    return load_catalog("default")


def get_table(table_id: str) -> Table:
    catalog = get_catalog()
    table = catalog.load_table(table_id)
    _fix_io_endpoint(table)
    return table
