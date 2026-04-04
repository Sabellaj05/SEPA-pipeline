"""
SEPA Pipeline Configuration — Infrastructure and Environment

Owns service connections, credentials, and runtime settings loaded from
environment variables. Data model definitions live in schema.py.
"""

import os
from pathlib import Path
from typing import Dict

import polars as pl
from dotenv import load_dotenv

# Re-export raw CSV schemas so existing callers of
# `from sepa_pipeline.config import get_schema_dict` keep working.
from sepa_pipeline.schema import get_schema_dict  # noqa: F401

load_dotenv()


class SEPAConfig:
    """
    Runtime configuration for the SEPA pipeline.
    Reads all settings from environment variables at instantiation time.
    """

    def __init__(self):
        # PostgreSQL
        self.sepa_user: str | None = os.getenv("POSTGRES_USER")
        self.sepa_password: str | None = os.getenv("POSTGRES_PASSWORD")
        self.sepa_db: str | None = os.getenv("POSTGRES_DB")
        self.sepa_host: str | None = os.getenv("POSTGRES_HOST", "localhost")
        self.sepa_port: str | None = os.getenv("POSTGRES_PORT")

        # MinIO / S3
        self.minio_endpoint: str | None = os.getenv("MINIO_ENDPOINT")
        self.minio_access_key: str | None = os.getenv(
            "MINIO_ACCESS_KEY", os.getenv("MINIO_USER")
        )
        self.minio_secret_key: str | None = os.getenv(
            "MINIO_SECRET_KEY", os.getenv("MINIO_PASSWORD")
        )
        self.minio_bucket: str | None = os.getenv("MINIO_BUCKET")
        self.minio_region: str | None = os.getenv("MINIO_REGION")

        # Pipeline tuning
        self.retention_days_postgres: int = int(os.getenv("RETENTION_DAYS_POSTGRES", "90"))
        self.max_workers: int = int(os.getenv("MAX_WORKERS_POSTGRES", "8"))

        self._validate()

        # Local directories
        self.temp_dir: Path = Path(os.getenv("SEPA_TEMP_DIR", "/tmp"))
        self.raw_data_dir: Path = Path("data")
        self.archive_dir: Path = Path("data/archive")

    def _validate(self) -> None:
        # GCP is optional for local-only runs but required for BigQuery loader
        self.gcp_project: str | None = os.getenv("GCP_PROJECT", "sepa-lakehouse42")
        self.gcp_dataset: str | None = os.getenv("GCP_DATASET", "silver")
        self.gcp_bucket: str | None = os.getenv("GCP_BUCKET", "sepa-lakehouse-silver-74dbadf7")
        self.gcp_dataset_gold: str | None = os.getenv("GCP_DATASET_GOLD", "gold")
        self.gcp_location: str | None = os.getenv("GCP_LOCATION", "US")

        required = {
            "POSTGRES_USER":     self.sepa_user,
            "POSTGRES_PASSWORD": self.sepa_password,
            "POSTGRES_DB":       self.sepa_db,
            "POSTGRES_HOST":     self.sepa_host,
            "POSTGRES_PORT":     self.sepa_port,
            "MINIO_ENDPOINT":    self.minio_endpoint,
            "MINIO_BUCKET":      self.minio_bucket,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.sepa_user}:{self.sepa_password}"
            f"@{self.sepa_host}:{self.sepa_port}/{self.sepa_db}"
        )

    @property
    def iceberg_catalog_config(self) -> dict:
        """PyIceberg REST Catalog config for Nessie (local MinIO)."""
        endpoint = self.minio_endpoint
        assert endpoint is not None
        if not endpoint.startswith("http"):
            endpoint = f"http://{endpoint}"
        return {
            "type": "rest",
            "uri": "http://localhost:19120/iceberg/main",
            "warehouse": f"s3://{self.minio_bucket}/silver/iceberg",
            "s3.endpoint": endpoint,
            "s3.access-key-id": self.minio_access_key,
            "s3.secret-access-key": self.minio_secret_key,
            "s3.region": self.minio_region or "us-east-1",
            "s3.path-style-access": "true",
        }

    @property
    def bigquery_catalog_config(self) -> dict:
        """PyIceberg BigQuery Catalog config for GCS/BigLake."""
        return {
            "type": "bigquery",
            "gcp.project-id": self.gcp_project,
            "gcp.bigquery.project-id": self.gcp_project,
            # Must be gs:// so BigLake can access the Iceberg metadata natively.
            "warehouse": f"gs://{self.gcp_bucket}/warehouse",
        }

    def get_schema(self, table_type: str) -> Dict[str, type[pl.DataType]]:
        """Convenience accessor — delegates to schema.get_schema_dict."""
        return get_schema_dict(table_type)
