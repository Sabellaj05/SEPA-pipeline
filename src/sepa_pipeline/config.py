"""
SEPA Pipeline Configuration
"""
import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

@dataclass
class SEPAConfig:
    """Configuration for SEPA pipeline"""

    sepa_user = os.getenv("POSTGRES_USER")
    sepa_password = os.getenv("POSTGRES_PASSWORD")
    sepa_db = os.getenv("POSTGRES_DB")
    sepa_host = os.getenv("POSTGRES_HOST", "localhost") # Default to localhost for host-running scripts
    sepa_port = os.getenv("POSTGRES_PORT")
    raw_data_dir: Path = Path("data")
    archive_dir: Path = Path("data/archive")

    # MinIO / S3 Configuration
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT")
    # Fallback to MINIO_USER/PASSWORD if specific keys aren't set
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_USER"))
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_PASSWORD"))
    minio_bucket: str = os.getenv("MINIO_BUCKET")

    @property
    def postgres_dsn(self) -> str:
        """Construct PostgreSQL DSN from environment variables."""
        # Note: The original instruction used self.db_user etc.
        # Assuming sepa_user etc. are the intended attributes for the DSN.
        return f"postgresql://{self.sepa_user}:{self.sepa_password}@{self.sepa_host}:{self.sepa_port}/{self.sepa_db}"

    retention_days_postgres: int = 90
    max_workers: int = 8

    @property
    def iceberg_catalog_config(self) -> dict:
        """Configuration for PyIceberg SQL Catalog"""
        # SQLAlchemy URI for the catalog (metadata)
        db_uri = f"postgresql+psycopg2://{self.sepa_user}:{self.sepa_password}@{self.sepa_host}:{self.sepa_port}/{self.sepa_db}"
        
        # Ensure endpoint has scheme
        endpoint = self.minio_endpoint
        # If running locally on host, ensure we target localhost if env is internal
        # if "minio" in endpoint and not "localhost" in endpoint and "127.0.0.1" not in endpoint:
        #      # Heuristic: if we are running the script outside docker but env var is 'minio:9000'
        #      endpoint = endpoint.replace("minio", "localhost")

        if not endpoint.startswith("http"):
            endpoint = f"http://{endpoint}"

        return {
            "type": "sql",
            "uri": db_uri,
            "warehouse": f"s3://{self.minio_bucket}/silver/iceberg",
            "s3.endpoint": endpoint,
            "s3.access-key-id": self.minio_access_key,
            "s3.secret-access-key": self.minio_secret_key,
            "s3.region": "us-east-1",
            # PyIceberg/PyArrow should handle scheme from endpoint, but path-style is usually needed for MinIO
            # "s3.path-style-access": "true", # PyArrow might default correctly with custom endpoint, but let's try WITHOUT explicit first if PyArrow script worked without it.
            # actually, PyArrow script didn't set it. let's comment it out to match.
            "s3.signer": "s3v4", # Force S3v4 signing for MinIO compatibility
        }
