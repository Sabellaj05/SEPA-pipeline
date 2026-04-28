import argparse
import logging
import time
from datetime import timedelta
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from sepa_pipeline.config import SEPAConfig
from sepa_pipeline.utils.logger import get_logger

logger = get_logger(__name__)

ICEBERG_TABLES = [
    "sepa.precios",
    "sepa.dim_comercios",
    "sepa.dim_sucursales",
    "sepa.dim_productos",
    # Audit table is typically bigquery or not strictly managed here, but we can add if it exists
    # "sepa.bronze_audit"
]

class IcebergManager:
    def __init__(self, config: SEPAConfig):
        self.config = config
        try:
            self.catalog = load_catalog("default", **self.config.iceberg_catalog_config)
        except Exception as e:
            logger.error(f"Failed to initialize Iceberg Catalog: {e}")
            self.catalog = None

    def _fix_io_endpoint(self, table) -> None:
        """Override S3 endpoint (same as IcebergLoader) to ensure FileIO works correctly."""
        s3_endpoint = self.config.minio_endpoint
        _S3_ENDPOINT_KEY = "s3.endpoint"
        if hasattr(table, "_io") and hasattr(table._io, "properties"):
            if table._io.properties.get(_S3_ENDPOINT_KEY) != s3_endpoint:
                table._io.properties[_S3_ENDPOINT_KEY] = s3_endpoint
                if hasattr(table._io, "_thread_locals") and hasattr(table._io._thread_locals, "get_fs_cached"):
                    table._io._thread_locals.get_fs_cached.cache_clear()

    def expire_table_snapshots(self, table_identifier: str, retain_days: int = 5) -> None:
        if not self.catalog:
            return

        try:
            table = self.catalog.load_table(table_identifier)
            self._fix_io_endpoint(table)
            
            logger.info(f"Expiring snapshots for {table_identifier} (older than {retain_days} days)...")
            
            if hasattr(table, 'maintenance'):
                # Calculate timestamp for X days ago
                from datetime import datetime, timezone
                cutoff_dt = datetime.now(timezone.utc) - timedelta(days=retain_days)
                table.maintenance.expire_snapshots().older_than(cutoff_dt).commit()
                logger.info(f"✅ Successfully expired snapshots for {table_identifier}.")
            else:
                logger.warning(f"Maintenance API not available on table {table_identifier}.")
                
        except NoSuchTableError:
            logger.info(f"Table {table_identifier} does not exist, skipping.")
        except Exception as e:
            if "Nessie Catalog does not allow" in str(e):
                logger.info(f"⏭️  Skipping {table_identifier}: Nessie Catalog manages its own snapshot garbage collection.")
            else:
                logger.error(f"Failed to expire snapshots for {table_identifier}: {e}")

    def clean_all_tables(self, retain_days: int = 5) -> None:
        logger.info(f"Starting maintenance for Iceberg tables (retaining {retain_days} days of history)...")
        for table_identifier in ICEBERG_TABLES:
            self.expire_table_snapshots(table_identifier, retain_days=retain_days)
        logger.info("Iceberg maintenance completed.")

def main():
    parser = argparse.ArgumentParser(description="SEPA Pipeline Iceberg Maintenance")
    parser.add_argument(
        "--retain-days",
        type=int,
        default=5,
        help="Number of days of history to retain per table (default: 5)"
    )
    args = parser.parse_args()

    config = SEPAConfig()
    manager = IcebergManager(config)
    manager.clean_all_tables(retain_days=args.retain_days)

if __name__ == "__main__":
    main()
