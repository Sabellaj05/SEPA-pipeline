"""
SEPA Data Ingestion Pipeline
Orchestrates the extraction, validation, and loading process.
"""
import logging
import argparse
import sys
from datetime import date, datetime

import polars as pl

from sepa_pipeline.config import SEPAConfig
from sepa_pipeline.extractor import SEPAExtractor
from sepa_pipeline.loader import SEPALoader
from sepa_pipeline.utils.logger import get_logger
from sepa_pipeline.utils.fecha import Fecha
from sepa_pipeline.validator import SEPAValidator, get_schema_dict

logger = get_logger(__name__)


def process_daily_data(target_date: date, config: SEPAConfig, stages: list[str] = None) -> None:
    """
    Main pipeline: Extract → Validate → Load → Archive
    
    Args:
        target_date: Date to process
        config: Configuration object
        stages: List of stages to run ['postgres', 'iceberg']. If None, runs all.
    """
    if stages is None:
        stages = ["postgres", "iceberg"]
        
    logger.info(f"Starting SEPA pipeline for {target_date} | Stages: {stages}")
    
    # AR timezone now
    scraped_at = Fecha().ahora

    # Step 1: Fetch and Extract (Cloud Native Flow)
    logger.info(f"Starting SEPA pipeline for {target_date}")
    extractor = SEPAExtractor()
    
    # Fetch Master ZIP from Bronze (MinIO)
    try:
        raw_zip_dir = extractor.fetch_from_bronze(target_date, config)
    except Exception as e:
        logger.error(f"Failed to fetch from Bronze Layer: {e}")
        # Build robustness: In a real scenario, we might trigger the scraper here if missing,
        # or fail. For now, we raise to ensure the architecture is respected.
        raise

    # 2. Extract child ZIPs
    all_csv_paths = extractor.extract_all_zips(raw_zip_dir)

    # Initialize loader
    loader = SEPALoader(config)

    # --- Phase 1: Load Dimensions (Comercios & Sucursales) ---
    # Only if Postges is enabled or we decide dims are needed for both
    # Currently referential integrity uses dims, so we might need to load valid dims into memory regardless.
    # But upserting to DB should be gated.
    logger.info("Phase 1: Loading Dimensions (Comercios & Sucursales)")
    
    all_comercios = []
    all_sucursales = []

    # Get schemas
    comercio_schema = get_schema_dict("comercio")
    sucursales_schema = get_schema_dict("sucursales")
    productos_schema = get_schema_dict("productos")

    validator = SEPAValidator()

    if "postgres" in stages:
        for idx, csv_paths in enumerate(all_csv_paths):
            logger.info(f"Dimensions Scan: Processing file {idx + 1}/{len(all_csv_paths)}")
            
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
                     quote_char=None, # Fix for malformed CSVs
                )
                # Clean BOM
                df_comercio = df_comercio.rename(
                    {col: col.lstrip("\ufeff").strip() for col in df_comercio.columns}
                )
                
                # VALIDATE IMMEDIATELY
                df_comercio = validator.validate_comercio(df_comercio)
                
                if df_comercio.height > 0:
                     all_comercios.append(df_comercio)

            except Exception as e:
                logger.warning(f"Failed to read/validate comercio {csv_paths['comercio']}: {e}")

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
                     quote_char=None, # Fix for malformed CSVs
                )
                # Clean BOM
                df_sucursal = df_sucursal.rename(
                    {col: col.lstrip("\ufeff").strip() for col in df_sucursal.columns}
                )

                # VALIDATE IMMEDIATELY
                df_sucursal = validator.validate_sucursales(df_sucursal)

                if df_sucursal.height > 0:
                     all_sucursales.append(df_sucursal)

            except Exception as e:
                logger.warning(f"Failed to read/validate sucursales {csv_paths['sucursales']}: {e}")

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
        # If skipping postgres, initialize empty DFs
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
    if "postgres" in stages:
        loader.upsert_comercios(df_comercios)
        loader.upsert_sucursales(df_sucursales)
    
    # Free memory
    del all_comercios
    del all_sucursales
    # We keep df_comercios and df_sucursales for referential integrity checks

    # --- Phase 2: Prepare Partition ---
    logger.info("Phase 2: Preparing Partitions")
    
    if "postgres" in stages:
        loader.prepare_precios_partition(target_date)
    
    if "iceberg" in stages:
        # Ensure Iceberg idempotency (overwrite strategy)
        loader.cleanup_iceberg_partition(target_date)

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
                quote_char=None, # Fix for malformed CSVs
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
                # Upsert Products Master
                if "postgres" in stages:
                    loader.upsert_productos_master(df_producto)
                    
                    # Load Prices
                    loader.bulk_load_precios(df_producto, scraped_at, target_date)
                
                # Archive to Iceberg (Silver Layer)
                if "iceberg" in stages:
                    loader.append_to_iceberg(df_producto, scraped_at, target_date)
                
                # Archive to Parquet (Bronze Layer)
                # loader.append_to_parquet(df_producto, target_date)
                
                total_prices_loaded += df_producto.height
            
        except Exception as e:
            logger.error(f"Failed to process chunk {idx + 1}: {e}")
            # Continue to next chunk instead of crashing entire pipeline?
            # For now, let's log and continue.

    loader.close_parquet_writer()
    logger.info(f"✅ Pipeline completed successfully for {target_date}. Total prices loaded: {total_prices_loaded:,}")


def parse_args():
    parser = argparse.ArgumentParser(description="SEPA Pipeline Runner")
    parser.add_argument(
        "--date", 
        type=str, 
        help="Target date in YYYY-MM-DD format (default: today)",
        default=None
    )
    parser.add_argument(
        "--stages",
        type=str,
        help="Comma-separated stages to run: postgres,iceberg (default: all)",
        default="postgres,iceberg"
    )
    return parser.parse_args()


if __name__ == "__main__":
    
    # If explicit arguments are provided via CLI, use them
    # Otherwise check if the user is running it without args (legacy behavior maybe?)
    # But since we are adding argparse, any args will be parsed.
    
    config = SEPAConfig()
    
    # Determine target date from CLI or default
    # But wait, we need to call parse_args() first.
    if len(sys.argv) > 1:
        args = parse_args()
        if args.date:
             try:
                 target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
             except ValueError:
                 logger.error("Invalid date format. Use YYYY-MM-DD")
                 sys.exit(1)
        else:
             target_date = Fecha().ahora.date()
             
        stages = [s.strip().lower() for s in args.stages.split(",")]
    else:
        # Default behavior if no args provided (backward compatible-ish)
        # But we want to encourage CLI usage.
        # Let's verify if current date is 2026-01-05
        target_date = Fecha().ahora.date()
        stages = ["postgres", "iceberg"]

    logger.info(f"Arguments -> Date: {target_date}, Stages: {stages}")

    process_daily_data(target_date, config, stages)
