from datetime import date
import uuid
import polars as pl
from pyarrow import fs
import pyarrow.parquet as pq

from sepa_pipeline.utils.logger import get_logger
from .base import BaseLoader

logger = get_logger(__name__)


class ParquetLoader(BaseLoader):
    """
    Loader for writing immutable Parquet chunks to MinIO/S3 (Bronze Layer).
    Each chunk gets its own file to avoid S3 multipart upload complexity.
    """

    def setup(self, fecha_vigencia: date) -> None:
        """
        No-op for ParquetLoader because writing to Parquet is a stateless append operation
        that generates a new file (UUID based) each time. Idempotency is naturally handled
        by creating unique files per chunk upon load.
        """
        pass

    def load(self, df: pl.DataFrame, fecha_vigencia: date) -> None:
        """
        Write a chunk to a unique Parquet file in MinIO/S3 (Stateless).
        """
        self.log_start(fecha_vigencia)

        # Initialize S3 Filesystem
        s3 = fs.S3FileSystem(
            endpoint_override=self.config.minio_endpoint,
            access_key=self.config.minio_access_key,
            secret_key=self.config.minio_secret_key,
            scheme="http",  # Use https if configured
            region="us-east-1",  # Required for MinIO compatibility
        )

        chunk_id = uuid.uuid4().hex

        # Define path in bucket (hive-style partitioning)
        file_path = (
            f"{self.config.minio_bucket}/bronze/parquet/"
            f"year={fecha_vigencia.year}/"
            f"month={fecha_vigencia.month:02d}/"
            f"day={fecha_vigencia.day:02d}/"
            f"chunk_{chunk_id}.parquet"
        )

        logger.info(f"[BRONZE] Writing Parquet chunk -> {file_path}")
        try:
            with s3.open_output_stream(file_path) as out_stream:
                pq.write_table(df.to_arrow(), out_stream, compression="zstd")
            self.log_success(fecha_vigencia, len(df))
        except Exception as e:
            logger.error(f"Failed to write Parquet chunk to S3: {e}")
            raise
