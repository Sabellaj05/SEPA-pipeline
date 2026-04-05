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
    ]
)


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

    def _ensure_table(self) -> object | None:
        if not self.catalog:
            return None

        try:
            table = self.catalog.load_table(self._TABLE_ID)
            self._fix_io_endpoint(table)
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
        for table_type, stats in audit_data.items():
            rows.append(
                {
                    "fecha_vigencia": fecha_vigencia,
                    "table_type": table_type,
                    "csv_row_count": stats.get("csv_rows", 0),
                    "csv_column_count": stats.get("csv_cols", 0),
                    "parquet_row_count": stats.get("parquet_rows", 0),
                    "raw_zip_path": raw_zip_path,
                    "parquet_path": f"{parquet_prefix}/{table_type}.parquet",
                    "ingested_at": now,
                }
            )

        df = pl.DataFrame(rows)
        arrow_table = df.to_arrow().cast(AUDIT_SCHEMA)

        logger.info(f"[AUDIT] Writing {len(rows)} audit rows for {fecha_vigencia}")
        table.append(arrow_table)
        logger.info("[AUDIT] Audit write complete")
