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

    def __init__(self) -> None:
        # RustFS / S3 object storage
        self.rustfs_endpoint: str | None = os.getenv("RUSTFS_ENDPOINT")
        self.rustfs_access_key: str | None = os.getenv("RUSTFS_ACCESS_KEY")
        self.rustfs_secret_key: str | None = os.getenv("RUSTFS_SECRET_KEY")
        self.rustfs_bucket: str | None = os.getenv("RUSTFS_BUCKET")
        self.rustfs_region: str | None = os.getenv("RUSTFS_REGION", "us-east-1")

        if self.rustfs_access_key and "AWS_ACCESS_KEY_ID" not in os.environ:
            os.environ["AWS_ACCESS_KEY_ID"] = self.rustfs_access_key
        if self.rustfs_secret_key and "AWS_SECRET_ACCESS_KEY" not in os.environ:
            os.environ["AWS_SECRET_ACCESS_KEY"] = self.rustfs_secret_key

        # Apache Polaris REST catalog
        self.polaris_uri: str | None = os.getenv(
            "POLARIS_URI", "http://localhost:8181/api/catalog"
        )
        self.polaris_realm: str | None = os.getenv("POLARIS_REALM", "default")
        self.polaris_client_id: str | None = os.getenv("POLARIS_CLIENT_ID", "polaris")
        self.polaris_client_secret: str | None = os.getenv(
            "POLARIS_CLIENT_SECRET", "polaris"
        )

        self._validate()

        # Local directories
        self.temp_dir: Path = Path(os.getenv("SEPA_TEMP_DIR", "/tmp"))
        self.raw_data_dir: Path = Path("data")
        self.archive_dir: Path = Path("data/archive")

    def _validate(self) -> None:
        # GCP is optional for local-only runs but required for BigQuery loader
        self.gcp_project: str | None = os.getenv("GCP_PROJECT", "sepa-lakehouse42")
        self.gcp_dataset: str | None = os.getenv("GCP_DATASET", "silver")
        self.gcp_bucket: str | None = os.getenv(
            "GCP_BUCKET", "sepa-lakehouse-silver-74dbadf7"
        )
        self.gcp_dataset_gold: str | None = os.getenv("GCP_DATASET_GOLD", "gold")
        self.gcp_location: str | None = os.getenv("GCP_LOCATION", "US")

        required = {
            "RUSTFS_ENDPOINT": self.rustfs_endpoint,
            "RUSTFS_BUCKET": self.rustfs_bucket,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

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
