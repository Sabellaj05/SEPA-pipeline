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

    sepa_user = os.getenv("TEST_POSTGRES_USER")
    sepa_password = os.getenv("TEST_POSTGRES_PASSWORD")
    sepa_db = os.getenv("TEST_POSTGRES_DB")
    sepa_host = os.getenv("TEST_POSTGRES_HOST")
    sepa_port = os.getenv("TEST_POSTGRES_PORT")
    raw_data_dir: Path = Path("data")
    archive_dir: Path = Path("data/archive_test")

    # Production env vars (commented out as in original)
    # sepa_user = os.getenv("POSTGRES_USER")
    # sepa_password = os.getenv("POSTGRES_PASSWORD")
    # sepa_db = os.getenv("POSTGRES_DB")
    # sepa_host = os.getenv("POSTGRES_HOST")
    # sepa_port = os.getenv("POSTGRES_PORT")
    # raw_data_dir: Path = Path("data")
    # archive_dir: Path = Path("data/archive")

    # MinIO / S3 Configuration
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY")
    minio_bucket: str = os.getenv("MINIO_BUCKET")

    @property
    def postgres_dsn(self) -> str:
        """Construct PostgreSQL DSN from environment variables."""
        # Note: The original instruction used self.db_user etc.
        # Assuming sepa_user etc. are the intended attributes for the DSN.
        return f"postgresql://{self.sepa_user}:{self.sepa_password}@{self.sepa_host}:{self.sepa_port}/{self.sepa_db}"

    retention_days_postgres: int = 90
    max_workers: int = 8
