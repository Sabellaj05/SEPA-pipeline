"""
SEPA Data Ingestion Pipeline
Orchestrates the extraction, validation, and loading process.
"""

import argparse
import sys
from datetime import date, datetime

import polars as pl

from sepa_pipeline.config import SEPAConfig
from sepa_pipeline.extractor import SEPAExtractor
from sepa_pipeline.loaders.postgres_loader import PostgresLoader
from sepa_pipeline.loaders.iceberg_loader import IcebergLoader
from sepa_pipeline.loaders.parquet_loader import ParquetLoader
from sepa_pipeline.loaders.bigquery_loader import BigQueryLoader
from sepa_pipeline.schema import (
    get_schema_dict,
    to_silver_precios,
    to_silver_sucursales,
    to_silver_comercios,
    to_silver_productos,
)
from sepa_pipeline.utils.fecha import Fecha
from sepa_pipeline.utils.logger import get_logger
from sepa_pipeline.validator import SEPAValidator

logger = get_logger(__name__)


def process_daily_data(
    target_date: date, config: SEPAConfig, stages: list[str] | None = None
) -> None:
    """
    Main pipeline: Extract → Validate → Load → Archive

    Args:
        target_date: Date to process
        config: Configuration object
        stages: List of stages to run ['postgres', 'iceberg', 'parquet', 'bigquery']. If None, runs all.
    """
    if stages is None:
        stages = ["postgres", "iceberg", "parquet", "bigquery"]

    logger.info(f"Starting SEPA pipeline for {target_date} | Stages: {stages}")

    # AR timezone now
    scraped_at = Fecha().ahora

    # Step 1: Fetch and Extract (Cloud Native Flow)
    logger.info(f"Starting SEPA pipeline for {target_date}")
    extractor = SEPAExtractor()

    import shutil

    # Fetch Master ZIP from Bronze (MinIO)
    raw_zip_dir = None
    try:
        try:
            raw_zip_dir = extractor.fetch_from_bronze(target_date, config)
            if raw_zip_dir is None:
                logger.warning(
                    f"Skipping pipeline execution: Source data not available for {target_date}."
                )
                return
        except Exception as e:
            logger.error(f"Failed to fetch from Bronze Layer: {e}")
            # Build robustness: In a real scenario, we might trigger the scraper here if missing,
            # or fail. For now, we raise to ensure the architecture is respected.
            raise

        # 2. Extract child ZIPs
        all_csv_paths = extractor.extract_all_zips(raw_zip_dir)

        # Initialize loaders
        postgres_loader = PostgresLoader(config) if "postgres" in stages else None
        iceberg_loader = IcebergLoader(config) if "iceberg" in stages else None
        parquet_loader = ParquetLoader(config) if "parquet" in stages else None
        bigquery_loader = BigQueryLoader(config) if "bigquery" in stages else None

        # --- Phase 1: Prepare Partition (Idempotent Setup) ---
        logger.info("Phase 1: Preparing Partitions")

        if postgres_loader:
            postgres_loader.setup(target_date)

        if iceberg_loader:
            # Ensure Iceberg idempotency (overwrite strategy)
            iceberg_loader.setup(target_date)

        if parquet_loader:
            parquet_loader.setup(target_date)

        if bigquery_loader:
            bigquery_loader.setup(target_date)

        # --- Phase 2: Load Dimensions (Comercios & Sucursales) ---
        # Only if Postges is enabled or we decide dims are needed for both
        # Currently referential integrity uses dims, so we might need to load valid dims into memory regardless.
        # But upserting to DB should be gated.
        logger.info("Phase 2: Loading Dimensions (Comercios & Sucursales)")

        all_comercios = []
        all_sucursales = []

        # Get schemas
        comercio_schema = get_schema_dict("comercio")
        sucursales_schema = get_schema_dict("sucursales")
        productos_schema = get_schema_dict("productos")

        validator = SEPAValidator()

        if any(s in stages for s in ["postgres", "iceberg", "bigquery"]):
            for idx, csv_paths in enumerate(all_csv_paths):
                logger.info(
                    f"Dimensions Scan: Processing file {idx + 1}/{len(all_csv_paths)}"
                )

                # Read comercio.csv
                try:
                    df_comercio = pl.read_csv(
                        csv_paths["comercio"],
                        separator="|",
                        encoding="utf8-lossy",
                        has_header=True,
                        null_values=["", "NULL", "null"],
                        schema_overrides=comercio_schema,
                        truncate_ragged_lines=True,
                        ignore_errors=True,
                        quote_char=None,  # Fix for malformed CSVs
                    )
                    # Clean BOM
                    df_comercio = df_comercio.rename(
                        {
                            col: col.lstrip("\ufeff").strip()
                            for col in df_comercio.columns
                        }
                    )

                    # VALIDATE IMMEDIATELY
                    df_comercio = validator.validate_comercio(df_comercio)

                    if df_comercio.height > 0:
                        all_comercios.append(df_comercio)

                except Exception as e:
                    logger.warning(
                        f"Failed to read/validate comercio {csv_paths['comercio']}: {e}"
                    )

                # Read sucursales.csv
                try:
                    df_sucursal = pl.read_csv(
                        csv_paths["sucursales"],
                        separator="|",
                        encoding="utf8-lossy",
                        has_header=True,
                        null_values=["", "NULL", "null"],
                        schema_overrides=sucursales_schema,
                        truncate_ragged_lines=True,
                        ignore_errors=True,
                        quote_char=None,  # Fix for malformed CSVs
                    )
                    # Clean BOM
                    df_sucursal = df_sucursal.rename(
                        {
                            col: col.lstrip("\ufeff").strip()
                            for col in df_sucursal.columns
                        }
                    )

                    # VALIDATE IMMEDIATELY
                    df_sucursal = validator.validate_sucursales(df_sucursal)

                    if df_sucursal.height > 0:
                        all_sucursales.append(df_sucursal)

                except Exception as e:
                    logger.warning(
                        f"Failed to read/validate sucursales {csv_paths['sucursales']}: {e}"
                    )

            # Concatenate and Validate Dimensions
            logger.info("Concatenating dimensions...")
            if all_comercios:
                df_comercios = pl.concat(all_comercios).unique()
            else:
                df_comercios = pl.DataFrame(schema=comercio_schema)

            if all_sucursales:
                df_sucursales = pl.concat(all_sucursales).unique()
            else:
                df_sucursales = pl.DataFrame(schema=sucursales_schema)

        else:
            # If skipping everything that needs dimensions, initialize empty DFs
            df_comercios = pl.DataFrame(schema=comercio_schema)
            df_sucursales = pl.DataFrame(schema=sucursales_schema)

        df_comercios = validator.validate_comercio(df_comercios)
        df_sucursales = validator.validate_sucursales(df_sucursales)

        # Validate Referential Integrity for Dimensions (clean orphaned sucursales)
        # We pass an empty products DF as we haven't loaded them yet
        empty_products = pl.DataFrame(schema=productos_schema)
        df_sucursales, _ = validator.validate_referential_integrity(
            df_comercios, df_sucursales, empty_products
        )

        # Load Dimensions
        if postgres_loader:
            postgres_loader._upsert_comercios(df_comercios)
            postgres_loader._upsert_sucursales(df_sucursales)

        if iceberg_loader:
            iceberg_loader.load_comercios(to_silver_comercios(df_comercios), target_date)
            iceberg_loader.load_sucursales(to_silver_sucursales(df_sucursales), target_date)

        if bigquery_loader:
            bigquery_loader.load_comercios(to_silver_comercios(df_comercios), target_date)
            bigquery_loader.load_sucursales(to_silver_sucursales(df_sucursales), target_date)

        # Free memory
        del all_comercios
        del all_sucursales
        # We keep df_comercios and df_sucursales for referential integrity checks

        # --- Phase 3: Chunked Product & Price Loading ---
        logger.info("Phase 3: Loading Products and Prices (Chunked)")

        total_prices_loaded = 0

        for idx, csv_paths in enumerate(all_csv_paths):
            logger.info(f"Processing chunk {idx + 1}/{len(all_csv_paths)}")

            try:
                df_producto = pl.read_csv(
                    csv_paths["productos"],
                    separator="|",
                    encoding="utf8-lossy",
                    has_header=True,
                    null_values=["", "NULL", "null"],
                    schema_overrides=productos_schema,
                    truncate_ragged_lines=True,
                    ignore_errors=True,
                    quote_char=None,  # Fix for malformed CSVs
                )
                df_producto = df_producto.rename(
                    {col: col.lstrip("\ufeff").strip() for col in df_producto.columns}
                )

                # Validate Schema
                df_producto = validator.validate_productos(df_producto)

                # Validate Referential Integrity (against loaded dimensions)
                # Note: We don't need to update sucursales here, just filter products
                _, df_producto = validator.validate_referential_integrity(
                    df_comercios, df_sucursales, df_producto
                )

                if df_producto.height > 0:
                    # Postgres uses raw column names — no transform needed
                    if postgres_loader:
                        postgres_loader._upsert_productos_master(df_producto)
                        postgres_loader._bulk_load_precios(df_producto, target_date)

                    # Transform to Silver schema for all Lakehouse targets
                    df_silver = to_silver_precios(df_producto)
                    df_silver_dim = to_silver_productos(df_producto)

                    # Archive to Iceberg (Silver Layer)
                    if iceberg_loader:
                        iceberg_loader.load_productos(df_silver_dim, target_date)
                        iceberg_loader.load(df_silver, target_date)

                    # Archive to Parquet (Bronze Layer)
                    if parquet_loader:
                        parquet_loader.load(df_silver, target_date)

                    # Export to BigQuery Data Lakehouse
                    if bigquery_loader:
                        bigquery_loader.load_productos(df_silver_dim, target_date)
                        bigquery_loader.load(df_silver, target_date)

                    total_prices_loaded += df_producto.height

            except Exception as e:
                logger.error(f"Failed to process chunk {idx + 1}: {e}")
                # Continue to next chunk instead of crashing entire pipeline?
                # For now, let's log and continue.

    finally:
        # Cleanup Temporary Bronze Directory
        if raw_zip_dir and raw_zip_dir.exists():
            logger.info(f"Cleaning up temporary directory: {raw_zip_dir}")
            try:
                # Check if it looks like our temp dir
                if "sepa_bronze_" in str(raw_zip_dir):
                    # Find the root of our temp dir
                    cleanup_path = raw_zip_dir
                    while (
                        "sepa_bronze_" not in cleanup_path.name
                        and len(cleanup_path.parts) > 1
                    ):
                        cleanup_path = cleanup_path.parent

                    if cleanup_path.exists():
                        shutil.rmtree(cleanup_path)
                        logger.info(f"Deleted {cleanup_path}")
                else:
                    # Fallback
                    shutil.rmtree(raw_zip_dir)
            except Exception as e:
                logger.warning(f"Failed to cleanup {raw_zip_dir}: {e}")

    logger.info(
        f"✅ Pipeline completed successfully for {target_date}. Total prices loaded: {total_prices_loaded:,}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SEPA Pipeline Runner")
    parser.add_argument(
        "--date",
        type=str,
        help="Single target date in YYYY-MM-DD format (default: today)",
        default=None,
    )
    parser.add_argument(
        "--date-from",
        type=str,
        help="Start of date range in YYYY-MM-DD format (inclusive). Use with --date-to.",
        default=None,
    )
    parser.add_argument(
        "--date-to",
        type=str,
        help="End of date range in YYYY-MM-DD format (inclusive). Use with --date-from.",
        default=None,
    )
    parser.add_argument(
        "--stages",
        type=str,
        help="Comma-separated stages to run: postgres,iceberg,parquet,bigquery (default: all)",
        default="postgres,iceberg,parquet,bigquery",
    )
    return parser.parse_args()


if __name__ == "__main__":
    from datetime import timedelta

    config = SEPAConfig()
    args = parse_args()
    stages = [s.strip().lower() for s in args.stages.split(",")]

    def _parse_date(value: str) -> date:
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            logger.error(f"Invalid date format '{value}'. Use YYYY-MM-DD.")
            sys.exit(1)

    if args.date_from or args.date_to:
        if not (args.date_from and args.date_to):
            logger.error("--date-from and --date-to must be used together.")
            sys.exit(1)
        date_from = _parse_date(args.date_from)
        date_to = _parse_date(args.date_to)
        if date_from > date_to:
            logger.error("--date-from must be earlier than or equal to --date-to.")
            sys.exit(1)
        dates = [date_from + timedelta(days=i) for i in range((date_to - date_from).days + 1)]
    elif args.date:
        dates = [_parse_date(args.date)]
    else:
        dates = [Fecha().ahora.date()]

    logger.info(f"Arguments -> Dates: {dates[0]} to {dates[-1]} ({len(dates)} day(s)), Stages: {stages}")

    for target_date in dates:
        process_daily_data(target_date, config, stages)
