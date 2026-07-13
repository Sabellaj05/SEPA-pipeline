"""
Bronze Parquet Layer — Materializes raw CSV data as Parquet on MinIO.

Reads all child ZIP CSVs (comercio, sucursales, productos) extracted from the
daily master ZIP and writes a single Parquet file per table per date to
bronze/parquet/.

Productos (the large fact-like table) is staged with a streaming ParquetWriter:
each child CSV is read, written as a row group, then freed — avoiding a full
in-memory concat of ~12–15M rows.

Writes are staged under ``.staging/`` and committed only when all three tables
succeed, then a ``_SUCCESS`` marker is written. ``exists()`` requires the
marker plus all three files so incomplete days are never treated as cache hits.

Also provides read access so the silver pipeline can skip ZIP extraction
on subsequent runs.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterator, List

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs

from sepa_pipeline.config import SEPAConfig
from sepa_pipeline.schema import get_schema_dict
from sepa_pipeline.utils.logger import get_logger

logger = get_logger(__name__)

TABLE_TYPES = ("comercio", "sucursales", "productos")
STAGING_DIR = ".staging"
SUCCESS_MARKER = "_SUCCESS"
# Chunk size for staging → final stream copy (avoids pyarrow copy_file quirks).
_COPY_CHUNK_BYTES = 8 * 1024 * 1024


class ParquetLoader:
    """
    Bronze Parquet layer: materializes extracted CSVs as Parquet on MinIO/S3
    and provides read access for downstream silver processing.
    """

    def __init__(
        self,
        config: SEPAConfig,
        filesystem: fs.FileSystem | None = None,
    ):
        self.config = config
        # Injected filesystem is used in tests (LocalFileSystem / SubTreeFileSystem).
        self._s3: fs.FileSystem = filesystem or fs.S3FileSystem(
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

    def _staging_prefix(self, fecha_vigencia: date) -> str:
        return f"{self._parquet_prefix(fecha_vigencia)}/{STAGING_DIR}"

    def _staging_path(self, fecha_vigencia: date, table_type: str) -> str:
        return f"{self._staging_prefix(fecha_vigencia)}/{table_type}.parquet"

    def _success_path(self, fecha_vigencia: date) -> str:
        return f"{self._parquet_prefix(fecha_vigencia)}/{SUCCESS_MARKER}"

    def _file_exists(self, path: str) -> bool:
        info = self._s3.get_file_info(path)
        return bool(info.type != fs.FileType.NotFound)

    def _delete_if_exists(self, path: str) -> None:
        if self._file_exists(path):
            self._s3.delete_file(path)

    def _ensure_parent(self, path: str) -> None:
        """Create parent directories when the backend requires them (local FS)."""
        parent, sep, _ = path.rpartition("/")
        if sep and parent:
            self._s3.create_dir(parent, recursive=True)

    def _open_output(self, path: str) -> Any:
        """Open an output stream, ensuring parent dirs exist for local FS."""
        self._ensure_parent(path)
        return self._s3.open_output_stream(path)

    def _copy_object(self, src: str, dst: str) -> None:
        """Stream-copy one object (works on S3 and LocalFileSystem)."""
        with self._s3.open_input_stream(src) as inp:
            with self._open_output(dst) as out:
                while True:
                    chunk = inp.read(_COPY_CHUNK_BYTES)
                    if not chunk:
                        break
                    out.write(chunk)

    def _cleanup_staging(self, fecha_vigencia: date) -> None:
        for table_type in TABLE_TYPES:
            self._delete_if_exists(self._staging_path(fecha_vigencia, table_type))

    def exists(self, fecha_vigencia: date) -> bool:
        """
        Return True only for a fully committed bronze day.

        Requires ``_SUCCESS`` plus all three parquet files so partial writes
        and mid-commit rebuilds are never treated as cache hits.
        """
        if not self._file_exists(self._success_path(fecha_vigencia)):
            return False
        for table_type in TABLE_TYPES:
            if not self._file_exists(self._parquet_path(fecha_vigencia, table_type)):
                return False
        return True

    def _commit_day(self, fecha_vigencia: date) -> None:
        """
        Promote staged files to final paths and write ``_SUCCESS``.

        ``_SUCCESS`` is removed first so a mid-commit failure cannot leave a
        cache hit with mixed old/new tables.
        """
        for table_type in TABLE_TYPES:
            staging = self._staging_path(fecha_vigencia, table_type)
            if not self._file_exists(staging):
                raise RuntimeError(
                    f"Cannot commit bronze day {fecha_vigencia}: "
                    f"missing staged {table_type}.parquet"
                )

        # Invalidate cache for the duration of the commit.
        self._delete_if_exists(self._success_path(fecha_vigencia))

        for table_type in TABLE_TYPES:
            staging = self._staging_path(fecha_vigencia, table_type)
            final = self._parquet_path(fecha_vigencia, table_type)
            self._copy_object(staging, final)
            logger.info(f"[BRONZE] Committed {table_type}.parquet -> s3://{final}")

        with self._open_output(self._success_path(fecha_vigencia)) as out:
            out.write(b"")

        self._cleanup_staging(fecha_vigencia)
        logger.info(
            f"[BRONZE] Day {fecha_vigencia} committed ({SUCCESS_MARKER} written)"
        )

    def _read_csv_frame(
        self, path: Path, schema: Dict[str, type[pl.DataType]]
    ) -> pl.DataFrame | None:
        """Read one pipe-delimited SEPA CSV; return None on failure."""
        try:
            df = pl.read_csv(
                path,
                separator="|",
                encoding="utf8-lossy",
                has_header=True,
                null_values=["", "NULL", "null"],
                schema_overrides=schema,
                truncate_ragged_lines=True,
                ignore_errors=True,
                quote_char=None,
            )
            return df.rename({col: col.lstrip("\ufeff").strip() for col in df.columns})
        except Exception as e:
            logger.warning(f"[BRONZE] Failed to read {path}: {e}")
            return None

    def _iter_table_frames(
        self,
        all_csv_paths: List[Dict[str, Path]],
        table_type: str,
    ) -> Iterator[pl.DataFrame]:
        """Yield one DataFrame per child-ZIP CSV for ``table_type``."""
        schema = get_schema_dict(table_type)
        for csv_paths in all_csv_paths:
            if table_type not in csv_paths:
                continue
            df = self._read_csv_frame(csv_paths[table_type], schema)
            if df is not None and df.height > 0:
                yield df

    @staticmethod
    def _align_to_columns(df: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
        """Reorder / pad columns so every batch matches the first frame's schema."""
        missing = [c for c in columns if c not in df.columns]
        if missing:
            df = df.with_columns([pl.lit(None).alias(c) for c in missing])
        return df.select(columns)

    def _stage_table_streamed(
        self,
        table_type: str,
        all_csv_paths: List[Dict[str, Path]],
        fecha_vigencia: date,
    ) -> Dict[str, int]:
        """
        Stream child CSVs into a single staged parquet via ParquetWriter.

        Peak memory is roughly one child-ZIP frame (not the full day concat).
        Still produces one unified ``{table_type}.parquet`` after commit.
        """
        staging_path = self._staging_path(fecha_vigencia, table_type)
        writer: pq.ParquetWriter | None = None
        output: Any | None = None
        column_names: list[str] | None = None
        arrow_schema: pa.Schema | None = None
        total_csv_rows = 0
        csv_cols = 0
        frames_written = 0

        try:
            for df in self._iter_table_frames(all_csv_paths, table_type):
                if writer is None:
                    column_names = list(df.columns)
                    csv_cols = len(column_names)
                    arrow_table = df.to_arrow()
                    arrow_schema = arrow_table.schema
                    output = self._open_output(staging_path)
                    writer = pq.ParquetWriter(
                        output,
                        arrow_schema,
                        compression="zstd",
                    )
                    writer.write_table(arrow_table)
                else:
                    assert column_names is not None
                    aligned = self._align_to_columns(df, column_names)
                    # Cast to the writer's schema so column types stay stable.
                    table = aligned.to_arrow()
                    if arrow_schema is not None:
                        table = table.cast(arrow_schema)
                    writer.write_table(table)

                total_csv_rows += df.height
                frames_written += 1
                del df

            if writer is None:
                logger.warning(
                    f"[BRONZE] No data for {table_type}, skipping parquet write"
                )
                return {"csv_rows": 0, "csv_cols": 0, "parquet_rows": 0}

            logger.info(
                f"[BRONZE] Staging {table_type}.parquet (streamed): "
                f"{total_csv_rows:,} rows from {frames_written} CSV(s) "
                f"-> s3://{staging_path}"
            )
            return {
                "csv_rows": total_csv_rows,
                "csv_cols": csv_cols,
                "parquet_rows": total_csv_rows,
            }
        finally:
            if writer is not None:
                writer.close()
            elif output is not None:
                output.close()

    def build(
        self,
        all_csv_paths: List[Dict[str, Path]],
        fecha_vigencia: date,
    ) -> Dict[str, Dict]:
        """
        Read all extracted CSVs and write Parquet to MinIO (streamed staging).

        Files are written under ``.staging/`` first. Only when all three tables
        stage successfully are they promoted to the day prefix and ``_SUCCESS``
        written. On partial failure, any prior committed day is left intact.

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

        # Drop leftovers from a previous failed attempt before staging again.
        self._cleanup_staging(fecha_vigencia)

        audit: Dict[str, Dict] = {}
        staged: list[str] = []

        try:
            for table_type in TABLE_TYPES:
                meta = self._stage_table_streamed(
                    table_type, all_csv_paths, fecha_vigencia
                )
                audit[table_type] = meta
                if meta["parquet_rows"] > 0:
                    staged.append(table_type)

            if set(staged) == set(TABLE_TYPES):
                self._commit_day(fecha_vigencia)
            else:
                missing = [t for t in TABLE_TYPES if t not in staged]
                logger.warning(
                    f"[BRONZE] Incomplete day {fecha_vigencia}: "
                    f"missing {missing}; not committing "
                    f"(prior committed day left intact if any)"
                )
                self._cleanup_staging(fecha_vigencia)
        except Exception:
            # Leave final paths + _SUCCESS untouched; drop partial staging.
            logger.error(
                f"[BRONZE] Build failed for {fecha_vigencia}; "
                "cleaning staging, prior committed day preserved"
            )
            self._cleanup_staging(fecha_vigencia)
            raise

        logger.info(f"[BRONZE] Parquet build complete for {fecha_vigencia}")
        return audit

    def read_dimensions(self, fecha_vigencia: date) -> Dict[str, pl.DataFrame]:
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
            result[table_type] = pl.DataFrame(pl.from_arrow(table))

        return result

    def read_productos_batched(
        self,
        fecha_vigencia: date,
        batch_size: int = 500_000,
    ) -> Iterator[pl.DataFrame | pl.Series]:
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
            ## added pl.DataFrame() wrapper to avoid typing issues
            ## as stated in pl.from_arrow docstrings
            df = pl.DataFrame(pl.from_arrow(batch))
            if df.height > 0:
                yield df
