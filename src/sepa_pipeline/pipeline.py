"""
SEPA Data Ingestion Pipeline
Orchestrates the extraction, validation, and loading process.
"""
import logging
from datetime import date, datetime

import polars as pl

from sepa_pipeline.config import SEPAConfig
from sepa_pipeline.extractor import SEPAExtractor
from sepa_pipeline.loader import SEPALoader
from sepa_pipeline.utils.logger import get_logger
from sepa_pipeline.validator import SEPAValidator, get_schema_dict

logger = get_logger(__name__)


def process_daily_data(target_date: date, config: SEPAConfig) -> None:
    """Main pipeline: Extract → Validate → Load → Archive"""
    scraped_at = datetime.now()

    # Step 1: Extract all ZIP files
    logger.info(f"Starting SEPA pipeline for {target_date}")
    extractor = SEPAExtractor()
    all_csv_paths = extractor.extract_all_zips(config.raw_data_dir, target_date)

    # Initialize loader
    loader = SEPALoader(config)

    # --- Phase 1: Load Dimensions (Comercios & Sucursales) ---
    logger.info("Phase 1: Loading Dimensions (Comercios & Sucursales)")
    all_comercios = []
    all_sucursales = []

    # Get schemas
    comercio_schema = get_schema_dict("comercio")
    sucursales_schema = get_schema_dict("sucursales")
    productos_schema = get_schema_dict("productos")

    validator = SEPAValidator()

    for idx, csv_paths in enumerate(all_csv_paths):
        logger.info(f"Processing file {idx + 1}/{len(all_csv_paths)}")

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
            # Clean BOM from column names
            df_comercio = df_comercio.rename(
                {col: col.lstrip("\ufeff").strip() for col in df_comercio.columns}
            )
            all_comercios.append(df_comercio)
        except Exception as e:
            logger.warning(f"Failed to read comercio: {e}")

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
            df_sucursal = df_sucursal.rename(
                {col: col.lstrip("\ufeff").strip() for col in df_sucursal.columns}
            )
            all_sucursales.append(df_sucursal)
        except Exception as e:
            logger.warning(f"Failed to read sucursales: {e}")

    # Concatenate and Validate Dimensions
    logger.info("Concatenating and validating dimensions...")
    df_comercios = pl.concat(all_comercios).unique()
    df_sucursales = pl.concat(all_sucursales).unique()

    df_comercios = validator.validate_comercio(df_comercios)
    df_sucursales = validator.validate_sucursales(df_sucursales)

    # Validate Referential Integrity for Dimensions (clean orphaned sucursales)
    # We pass an empty products DF as we haven't loaded them yet
    empty_products = pl.DataFrame(schema=productos_schema)
    df_sucursales, _ = validator.validate_referential_integrity(
        df_comercios, df_sucursales, empty_products
    )

    # Load Dimensions
    loader.upsert_comercios(df_comercios)
    loader.upsert_sucursales(df_sucursales)
    
    # Free memory
    del all_comercios
    del all_sucursales
    # We keep df_comercios and df_sucursales for referential integrity checks

    # --- Phase 2: Prepare Partition ---
    logger.info("Phase 2: Preparing Precios Partition")
    loader.prepare_precios_partition(target_date)

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
                loader.upsert_productos_master(df_producto)
                
                # Load Prices
                loader.bulk_load_precios(df_producto, scraped_at, target_date)
                
                # Archive to Parquet (Chunked)
                loader.append_to_parquet(df_producto, target_date)
                
                total_prices_loaded += df_producto.height
            
        except Exception as e:
            logger.error(f"Failed to process chunk {idx + 1}: {e}")
            # Continue to next chunk instead of crashing entire pipeline?
            # For now, let's log and continue.

    loader.close_parquet_writer()
    logger.info(f"✅ Pipeline completed successfully for {target_date}. Total prices loaded: {total_prices_loaded:,}")


if __name__ == "__main__":
    config = SEPAConfig()
    # Should be the current date
    target_date = date(2025, 11, 23)

    process_daily_data(target_date, config)
