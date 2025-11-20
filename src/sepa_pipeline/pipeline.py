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
from sepa_pipeline.validator import SEPAValidator, get_schema_dict

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def process_daily_data(target_date: date, config: SEPAConfig) -> None:
    """Main pipeline: Extract → Validate → Load → Archive"""
    scraped_at = datetime.now()

    # Step 1: Extract all ZIP files
    logger.info(f"Starting SEPA pipeline for {target_date}")
    extractor = SEPAExtractor()
    all_csv_paths = extractor.extract_all_zips(config.raw_data_dir, target_date)

    # Step 2: Load and concatenate all CSVs
    logger.info("Loading and concatenating CSV files")
    all_comercios = []
    all_sucursales = []
    all_productos = []

    # Get schemas
    comercio_schema = get_schema_dict("comercio")
    sucursales_schema = get_schema_dict("sucursales")
    productos_schema = get_schema_dict("productos")

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
            all_comercios.append(pl.DataFrame(schema=comercio_schema))

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
            all_sucursales.append(pl.DataFrame(schema=sucursales_schema))

        # Read productos.csv
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
            all_productos.append(df_producto)
        except Exception as e:
            logger.warning(f"Failed to read productos: {e}")
            all_productos.append(pl.DataFrame(schema=productos_schema))

    # Concatenate all dataframes
    logger.info("Concatenating dataframes")
    df_comercios = pl.concat(all_comercios).unique()
    df_sucursales = pl.concat(all_sucursales).unique()
    df_productos = pl.concat(all_productos)

    logger.info(
        f"Loaded {len(df_productos):,} price records from {len(all_csv_paths)} comercios"
    )

    # Step 3: Validate
    logger.info("Validating data")
    validator = SEPAValidator()
    df_comercios = validator.validate_comercio(df_comercios)
    df_sucursales = validator.validate_sucursales(df_sucursales)
    df_productos = validator.validate_productos(df_productos)
    
    df_sucursales, df_productos = validator.validate_referential_integrity(
        df_comercios, df_sucursales, df_productos
    )

    # Step 4: Load to PostgreSQL and Parquet
    loader = SEPALoader(config)
    loader.load_to_postgres(
        df_comercios, df_sucursales, df_productos, scraped_at, target_date
    )
    loader.archive_to_parquet(df_productos, target_date)

    logger.info(f"✅ Pipeline completed successfully for {target_date}")


if __name__ == "__main__":
    config = SEPAConfig()
    # Should be the current date
    target_date = date(2025, 11, 14)

    process_daily_data(target_date, config)
