"""
Audit Tables — Tracks ingestion and validation metadata in Iceberg via Nessie.

Table: sepa.audit_bronze (unpartitioned, append-only)
Tracks per-date, per-table-type stats for CSV and Parquet building.

Table: sepa.audit_silver (unpartitioned, append-only)
Tracks per-date silver validation drop stats and load counts.
"""

from datetime import date, datetime
from typing import Dict

import polars as pl
import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError

from sepa_pipeline.config import SEPAConfig
from sepa_pipeline.utils.logger import get_logger

logger = get_logger(__name__)

BRONZE_AUDIT_SCHEMA = pa.schema(
    [
        pa.field("fecha_vigencia", pa.date32()),
        pa.field("table_type", pa.string()),
        pa.field("csv_row_count", pa.int64()),
        pa.field("csv_column_count", pa.int32()),
        pa.field("parquet_row_count", pa.int64()),
        pa.field("raw_zip_path", pa.string()),
        pa.field("parquet_path", pa.string()),
        pa.field("malformed_zips_count", pa.int64()),
        pa.field("stale_count", pa.int64()),
        pa.field("unknown_count", pa.int64()),
        pa.field("ingested_at", pa.timestamp("us")),
    ]
)

SILVER_AUDIT_SCHEMA = pa.schema(
    [
        pa.field("fecha_vigencia", pa.date32()),
        pa.field("table_type", pa.string()),
        pa.field("validation_dropped", pa.int64()),
        pa.field("integrity_dropped", pa.int64()),
        pa.field("negative_price_count", pa.int64()),
        pa.field("silver_loaded", pa.int64()),
        pa.field("ingested_at", pa.timestamp("us")),
    ]
)

# Maps internal ParquetLoader table keys → audit table_type values
_BRONZE_TABLE_TYPE_NAMES = {
    "comercio":   "bronze_comercio",
    "sucursales": "bronze_sucursales",
    "productos":  "bronze_productos",
}


class SEPAAuditWriter:
    """Writes ingestion audit rows to sepa.audit_bronze and sepa.audit_silver."""

    _BRONZE_TABLE_ID = "sepa.audit_bronze"
    _SILVER_TABLE_ID = "sepa.audit_silver"

    def __init__(self, config: SEPAConfig):
        self.config = config
        try:
            self.catalog: Catalog | None = load_catalog(
                "default", **config.iceberg_catalog_config
            )
        except Exception as e:
            logger.warning(f"[AUDIT] Failed to initialize Iceberg catalog: {e}")
            self.catalog = None

    def _fix_io_endpoint(self, table: object) -> None:
        """Patch S3 endpoint for host-side access (same as IcebergLoader)."""
        s3_endpoint = self.config.minio_endpoint
        if hasattr(table, "_io") and hasattr(table._io, "properties"):
            if table._io.properties.get("s3.endpoint") != s3_endpoint:
                table._io.properties["s3.endpoint"] = s3_endpoint
                if hasattr(table._io, "_thread_locals") and hasattr(
                    table._io._thread_locals, "get_fs_cached"
                ):
                    table._io._thread_locals.get_fs_cached.cache_clear()

    def _ensure_table(self, table_id: str, schema: pa.Schema) -> object | None:
        if not self.catalog:
            return None

        try:
            table = self.catalog.load_table(table_id)
            self._fix_io_endpoint(table)
            return table
        except NoSuchTableError:
            logger.info(f"[AUDIT] Creating {table_id}...")
            try:
                self.catalog.create_namespace("sepa")
            except NamespaceAlreadyExistsError:
                pass

            table = self.catalog.create_table(table_id, schema=schema)
            self._fix_io_endpoint(table)
            return table

    def write_bronze(
        self,
        fecha_vigencia: date,
        audit_data: Dict[str, Dict],
        raw_zip_path: str,
        parquet_prefix: str,
        malformed_zips_count: int = 0,
        stale_count: int = 0,
        unknown_count: int = 0,
    ) -> None:
        """
        Append audit rows to sepa.audit_bronze for a single pipeline date.
        """
        table = self._ensure_table(self._BRONZE_TABLE_ID, BRONZE_AUDIT_SCHEMA)
        if table is None:
            logger.warning("[AUDIT] Skipping bronze audit write (catalog not available)")
            return

        now = datetime.now()
        rows = []
        for internal_key, stats in audit_data.items():
            table_type = _BRONZE_TABLE_TYPE_NAMES.get(internal_key, f"bronze_{internal_key}")
            rows.append(
                {
                    "fecha_vigencia": fecha_vigencia,
                    "table_type": table_type,
                    "csv_row_count": stats.get("csv_rows", 0),
                    "csv_column_count": stats.get("csv_cols", 0),
                    "parquet_row_count": stats.get("parquet_rows", 0),
                    "raw_zip_path": raw_zip_path,
                    "parquet_path": f"{parquet_prefix}/{internal_key}.parquet",
                    "malformed_zips_count": malformed_zips_count,
                    "stale_count": stale_count,
                    "unknown_count": unknown_count,
                    "ingested_at": now,
                }
            )

        df = pl.DataFrame(rows, schema_overrides={
            "csv_row_count": pl.Int64,
            "csv_column_count": pl.Int32,
            "parquet_row_count": pl.Int64,
            "malformed_zips_count": pl.Int64,
            "stale_count": pl.Int64,
            "unknown_count": pl.Int64,
        })
        arrow_table = df.to_arrow().cast(BRONZE_AUDIT_SCHEMA)

        logger.info(f"[AUDIT] Writing {len(rows)} bronze audit rows for {fecha_vigencia}")
        table.append(arrow_table)
        logger.info("[AUDIT] Bronze audit write complete")

    def write_silver(
        self,
        fecha_vigencia: date,
        drop_stats: Dict[str, int],
    ) -> None:
        """
        Append a single 'silver_precios' audit row to sepa.audit_silver.
        """
        table = self._ensure_table(self._SILVER_TABLE_ID, SILVER_AUDIT_SCHEMA)
        if table is None:
            logger.warning("[AUDIT] Skipping silver audit write (catalog not available)")
            return

        row = {
            "fecha_vigencia": fecha_vigencia,
            "table_type": "silver_precios",
            "validation_dropped": drop_stats.get("validation_dropped", 0),
            "integrity_dropped": drop_stats.get("integrity_dropped", 0),
            "negative_price_count": drop_stats.get("negative_price_count", 0),
            "silver_loaded": drop_stats.get("silver_loaded", 0),
            "ingested_at": datetime.now(),
        }

        df = pl.DataFrame([row], schema_overrides={
            "validation_dropped": pl.Int64,
            "integrity_dropped": pl.Int64,
            "negative_price_count": pl.Int64,
            "silver_loaded": pl.Int64,
        })
        arrow_table = df.to_arrow().cast(SILVER_AUDIT_SCHEMA)

        logger.info(
            f"[AUDIT] Silver stats for {fecha_vigencia}: "
            f"loaded={row['silver_loaded']:,}, "
            f"validation_dropped={row['validation_dropped']:,}, "
            f"integrity_dropped={row['integrity_dropped']:,}, "
            f"negative_price_count={row['negative_price_count']:,}"
        )
        table.append(arrow_table)
        logger.info("[AUDIT] Silver audit write complete")
