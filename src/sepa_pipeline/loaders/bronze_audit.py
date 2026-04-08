"""
Bronze Audit Table — Tracks ingestion metadata in an Iceberg table via Nessie.

Records per-date, per-table-type stats (CSV row/column counts, parquet row
counts, timestamps) so the pipeline has observability over what has been
ingested into the bronze layer.

Table: sepa.bronze_audit (unpartitioned, append-only)
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

AUDIT_SCHEMA = pa.schema(
    [
        pa.field("fecha_vigencia", pa.date32()),
        pa.field("table_type", pa.string()),
        pa.field("csv_row_count", pa.int64()),
        pa.field("csv_column_count", pa.int32()),
        pa.field("parquet_row_count", pa.int64()),
        pa.field("raw_zip_path", pa.string()),
        pa.field("parquet_path", pa.string()),
        pa.field("ingested_at", pa.timestamp("us")),
        # Silver processing stats — only set for table_type='silver_precios'
        pa.field("validation_dropped", pa.int64()),
        pa.field("integrity_dropped", pa.int64()),
        pa.field("negative_price_count", pa.int64()),
        pa.field("silver_loaded", pa.int64()),
    ]
)

# Columns added in schema v2 — used to evolve existing tables
_SILVER_STAT_COLUMNS = {
    "validation_dropped",
    "integrity_dropped",
    "negative_price_count",
    "silver_loaded",
}

# Maps internal ParquetLoader table keys → audit table_type values
_BRONZE_TABLE_TYPE_NAMES = {
    "comercio":   "bronze_comercio",
    "sucursales": "bronze_sucursales",
    "productos":  "bronze_productos",
}


class BronzeAuditWriter:
    """Writes ingestion audit rows to the sepa.bronze_audit Iceberg table."""

    _TABLE_ID = "sepa.bronze_audit"

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

    def _evolve_schema_if_needed(self, table: object) -> None:
        """Add silver stat columns to existing tables that predate schema v2."""
        from pyiceberg.types import LongType

        existing = {field.name for field in table.schema().fields}
        missing = _SILVER_STAT_COLUMNS - existing
        if not missing:
            return

        logger.info(f"[AUDIT] Evolving schema: adding columns {sorted(missing)}")
        with table.update_schema() as update:
            for name in sorted(missing):
                update.add_column(name, LongType())

    def _ensure_table(self) -> object | None:
        if not self.catalog:
            return None

        try:
            table = self.catalog.load_table(self._TABLE_ID)
            self._fix_io_endpoint(table)
            self._evolve_schema_if_needed(table)
            return table
        except NoSuchTableError:
            logger.info(f"[AUDIT] Creating {self._TABLE_ID}...")
            try:
                self.catalog.create_namespace("sepa")
            except NamespaceAlreadyExistsError:
                pass

            table = self.catalog.create_table(self._TABLE_ID, schema=AUDIT_SCHEMA)
            self._fix_io_endpoint(table)
            return table

    def write(
        self,
        fecha_vigencia: date,
        audit_data: Dict[str, Dict],
        raw_zip_path: str,
        parquet_prefix: str,
    ) -> None:
        """
        Append audit rows for a single pipeline date.

        Args:
            fecha_vigencia: The business date.
            audit_data: Output from ParquetLoader.build() —
                        {"comercio": {"csv_rows": N, ...}, ...}
            raw_zip_path: S3 path to the raw ZIP file.
            parquet_prefix: S3 prefix for the parquet files.
        """
        table = self._ensure_table()
        if table is None:
            logger.warning("[AUDIT] Skipping audit write (catalog not available)")
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
                    "ingested_at": now,
                    # Silver stats are null for bronze-layer rows
                    "validation_dropped": None,
                    "integrity_dropped": None,
                    "negative_price_count": None,
                    "silver_loaded": None,
                }
            )

        df = pl.DataFrame(rows, schema_overrides={
            "validation_dropped": pl.Int64,
            "integrity_dropped": pl.Int64,
            "negative_price_count": pl.Int64,
            "silver_loaded": pl.Int64,
        })
        arrow_table = df.to_arrow().cast(AUDIT_SCHEMA)

        logger.info(f"[AUDIT] Writing {len(rows)} audit rows for {fecha_vigencia}")
        table.append(arrow_table)
        logger.info("[AUDIT] Audit write complete")

    def write_silver_stats(
        self,
        fecha_vigencia: date,
        drop_stats: Dict[str, int],
        parquet_prefix: str,
    ) -> None:
        """
        Append a single 'silver_precios' audit row with per-drop-reason counts.

        Args:
            fecha_vigencia: The business date.
            drop_stats: Output from SEPAValidator.get_drop_stats().
            parquet_prefix: S3 prefix for the bronze parquet files (for lineage).
        """
        table = self._ensure_table()
        if table is None:
            logger.warning("[AUDIT] Skipping silver audit write (catalog not available)")
            return

        row = {
            "fecha_vigencia": fecha_vigencia,
            "table_type": "silver_precios",
            "csv_row_count": 0,
            "csv_column_count": 0,
            "parquet_row_count": 0,
            "raw_zip_path": "",
            "parquet_path": f"{parquet_prefix}/productos.parquet",
            "ingested_at": datetime.now(),
            "validation_dropped": drop_stats.get("validation_dropped", 0),
            "integrity_dropped": drop_stats.get("integrity_dropped", 0),
            "negative_price_count": drop_stats.get("negative_price_count", 0),
            "silver_loaded": drop_stats.get("silver_loaded", 0),
        }

        df = pl.DataFrame([row], schema_overrides={
            "validation_dropped": pl.Int64,
            "integrity_dropped": pl.Int64,
            "negative_price_count": pl.Int64,
            "silver_loaded": pl.Int64,
        })
        arrow_table = df.to_arrow().cast(AUDIT_SCHEMA)

        logger.info(
            f"[AUDIT] Silver stats for {fecha_vigencia}: "
            f"loaded={row['silver_loaded']:,}, "
            f"validation_dropped={row['validation_dropped']:,}, "
            f"integrity_dropped={row['integrity_dropped']:,}, "
            f"negative_price_count={row['negative_price_count']:,}"
        )
        table.append(arrow_table)
        logger.info("[AUDIT] Silver audit write complete")
