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

    postgres_dsn: str = (
        f"postgresql://{sepa_user}:{sepa_password}@{sepa_host}:{sepa_port}/{sepa_db}"
    )
    retention_days_postgres: int = 90
    max_workers: int = 8
