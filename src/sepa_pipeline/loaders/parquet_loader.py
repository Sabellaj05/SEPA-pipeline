"""
Bronze Parquet Layer — Materializes raw CSV data as Parquet on MinIO.

Reads all child ZIP CSVs (comercio, sucursales, productos) extracted from the
daily master ZIP, concatenates each table type, and writes a single Parquet
file per table per date to bronze/parquet/.

Also provides read access so the silver pipeline can skip ZIP extraction
on subsequent runs.
"""

from datetime import date
from pathlib import Path
from typing import Dict, List

import polars as pl
import pyarrow.parquet as pq
from pyarrow import fs

from sepa_pipeline.config import SEPAConfig
from sepa_pipeline.schema import get_schema_dict
from sepa_pipeline.utils.logger import get_logger

logger = get_logger(__name__)

TABLE_TYPES = ("comercio", "sucursales", "productos")


class ParquetLoader:
    """
    Bronze Parquet layer: materializes extracted CSVs as Parquet on MinIO/S3
    and provides read access for downstream silver processing.
    """

    def __init__(self, config: SEPAConfig):
        self.config = config
        self._s3 = fs.S3FileSystem(
            endpoint_override=config.minio_endpoint,
            access_key=config.minio_access_key,
            secret_key=config.minio_secret_key,
            scheme="http",
            region="us-east-1",
        )

    def _parquet_prefix(self, fecha_vigencia: date) -> str:
        return (
            f"{self.config.minio_bucket}/bronze/parquet/"
            f"year={fecha_vigencia.year}/"
            f"month={fecha_vigencia.month:02d}/"
            f"day={fecha_vigencia.day:02d}"
        )

    def _parquet_path(self, fecha_vigencia: date, table_type: str) -> str:
        return f"{self._parquet_prefix(fecha_vigencia)}/{table_type}.parquet"

    def exists(self, fecha_vigencia: date) -> bool:
        """Check if all three bronze parquet files exist for a given date."""
        for table_type in TABLE_TYPES:
            path = self._parquet_path(fecha_vigencia, table_type)
            info = self._s3.get_file_info(path)
            if info.type == fs.FileType.NotFound:
                return False
        return True

    def build(
        self,
        all_csv_paths: List[Dict[str, Path]],
        fecha_vigencia: date,
    ) -> Dict[str, Dict]:
        """
        Read all extracted CSVs, concatenate by table type, write Parquet to MinIO.

        Returns audit metadata per table type:
            {
                "comercio":   {"csv_rows": N, "csv_cols": N, "parquet_rows": N},
                "sucursales": {"csv_rows": N, "csv_cols": N, "parquet_rows": N},
                "productos":  {"csv_rows": N, "csv_cols": N, "parquet_rows": N},
            }
        """
        logger.info(
            f"[BRONZE] Building parquet for {fecha_vigencia} "
            f"from {len(all_csv_paths)} child ZIPs"
        )

        audit: Dict[str, Dict] = {}

        for table_type in TABLE_TYPES:
            schema = get_schema_dict(table_type)

            frames: list[pl.DataFrame] = []
            total_csv_rows = 0
            csv_cols = 0

            for csv_paths in all_csv_paths:
                if table_type not in csv_paths:
                    continue
                try:
                    df = pl.read_csv(
                        csv_paths[table_type],
                        separator="|",
                        encoding="utf8-lossy",
                        has_header=True,
                        null_values=["", "NULL", "null"],
                        schema_overrides=schema,
                        truncate_ragged_lines=True,
                        ignore_errors=True,
                        quote_char=None,
                    )
                    df = df.rename(
                        {col: col.lstrip("\ufeff").strip() for col in df.columns}
                    )
                    total_csv_rows += df.height
                    csv_cols = len(df.columns)
                    frames.append(df)
                except Exception as e:
                    logger.warning(
                        f"[BRONZE] Failed to read {csv_paths[table_type]}: {e}"
                    )

            if not frames:
                logger.warning(
                    f"[BRONZE] No data for {table_type}, skipping parquet write"
                )
                audit[table_type] = {
                    "csv_rows": 0,
                    "csv_cols": 0,
                    "parquet_rows": 0,
                }
                continue

            df_concat = pl.concat(frames)
            parquet_path = self._parquet_path(fecha_vigencia, table_type)

            logger.info(
                f"[BRONZE] Writing {table_type}.parquet: "
                f"{df_concat.height:,} rows -> s3://{parquet_path}"
            )

            with self._s3.open_output_stream(parquet_path) as out:
                pq.write_table(df_concat.to_arrow(), out, compression="zstd")

            audit[table_type] = {
                "csv_rows": total_csv_rows,
                "csv_cols": csv_cols,
                "parquet_rows": df_concat.height,
            }

        logger.info(f"[BRONZE] Parquet build complete for {fecha_vigencia}")
        return audit

    def read_dimensions(
        self, fecha_vigencia: date
    ) -> Dict[str, pl.DataFrame]:
        """
        Read the small dimension tables (comercio, sucursales) eagerly.

        Returns:
            {"comercio": df, "sucursales": df}
        """
        result: Dict[str, pl.DataFrame] = {}

        for table_type in ("comercio", "sucursales"):
            path = self._parquet_path(fecha_vigencia, table_type)
            logger.info(f"[BRONZE] Reading s3://{path}")
            table = pq.read_table(path, filesystem=self._s3)
            result[table_type] = pl.from_arrow(table)

        return result

    def read_productos_batched(
        self,
        fecha_vigencia: date,
        batch_size: int = 500_000,
    ):
        """
        Yield productos parquet in batches to keep memory bounded.

        Args:
            fecha_vigencia: The business date.
            batch_size: Rows per batch (default 500k ~= 1-2 GB peak).

        Yields:
            pl.DataFrame chunks of productos data.
        """
        path = self._parquet_path(fecha_vigencia, "productos")
        logger.info(f"[BRONZE] Streaming s3://{path} in batches of {batch_size:,}")

        parquet_file = pq.ParquetFile(self._s3.open_input_file(path))

        for batch in parquet_file.iter_batches(batch_size=batch_size):
            df = pl.from_arrow(batch)
            if df.height > 0:
                yield df
