"""
SEPA Data Ingestion Pipeline

Unified orchestrator for the full pipeline flow:
  Scrape -> Bronze Parquet -> Validate -> Silver Load

CLI examples:
  uv run pipeline.py                            # full pipeline, today
  uv run pipeline.py --scrape-only              # scrape today only
  uv run pipeline.py --scrape-only --date 2026-03-15
  uv run pipeline.py --target iceberg
  uv run pipeline.py --target iceberg,bigquery --date-from X --date-to Y
"""

import argparse
import asyncio
import shutil
import sys
from datetime import date, datetime, timedelta

import polars as pl

from sepa_pipeline.config import SEPAConfig
from sepa_pipeline.extractor import SEPAExtractor
from sepa_pipeline.loaders.bigquery_loader import BigQueryLoader
from sepa_pipeline.loaders.bronze_audit import BronzeAuditWriter
from sepa_pipeline.loaders.iceberg_loader import IcebergLoader
from sepa_pipeline.loaders.parquet_loader import ParquetLoader
from sepa_pipeline.schema import (
    get_schema_dict,
    to_silver_comercios,
    to_silver_precios,
    to_silver_productos,
    to_silver_sucursales,
)
from sepa_pipeline.scraper import SepaScraper
from sepa_pipeline.utils.fecha import Fecha
from sepa_pipeline.utils.logger import get_logger
from sepa_pipeline.validator import SEPAValidator

logger = get_logger(__name__)

SCRAPER_URL = "https://datos.produccion.gob.ar/dataset/sepa-precios"
SCRAPER_DATA_DIR = "data"


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------

async def _scrape_date(target_date: date) -> bool:
    """Run the scraper for a single date. Returns True on success."""
    logger.info(f"Scraping data for {target_date}")
    async with SepaScraper(
        url=SCRAPER_URL,
        data_dir=SCRAPER_DATA_DIR,
        target_date=target_date,
    ) as scraper:
        return await scraper.hurtar_datos()


def _raw_zip_s3_path(config: SEPAConfig, target_date: date) -> str:
    """Return the S3 path where the raw ZIP should live."""
    filename = f"sepa_precios_{target_date.strftime('%Y-%m-%d')}.zip"
    return (
        f"{config.minio_bucket}/bronze/raw/"
        f"year={target_date.year}/"
        f"month={target_date.month:02d}/"
        f"day={target_date.day:02d}/"
        f"{filename}"
    )


def _raw_zip_exists(config: SEPAConfig, target_date: date) -> bool:
    """Check if the raw ZIP already exists in the bronze layer."""
    from pyarrow import fs

    s3 = fs.S3FileSystem(
        endpoint_override=config.minio_endpoint,
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key,
        scheme="http",
        region="us-east-1",
    )
    info = s3.get_file_info(_raw_zip_s3_path(config, target_date))
    return info.type != fs.FileType.NotFound


# ---------------------------------------------------------------------------
# Daily processing
# ---------------------------------------------------------------------------

def process_daily_data(
    target_date: date,
    config: SEPAConfig,
    targets: list[str],
) -> None:
    """
    Full pipeline for one date:
      1. Check if bronze data exists (parquet or raw ZIP)
      2. Scrape if needed
      3. Build bronze parquet (if not cached) + write audit
      4. Read from bronze parquet, validate, transform to silver
      5. Load to targets (iceberg, bigquery)
    """
    logger.info(f"Starting SEPA pipeline for {target_date} | Targets: {targets}")

    parquet_loader = ParquetLoader(config)
    audit_writer = BronzeAuditWriter(config)

    # --- Step 1–2: Ensure bronze data exists ---
    has_parquet = parquet_loader.exists(target_date)

    if has_parquet:
        logger.info(
            f"[BRONZE] Parquet cache hit for {target_date}, "
            "skipping extraction"
        )
    else:
        # Need raw ZIP → check if it exists, scrape if not
        if not _raw_zip_exists(config, target_date):
            logger.info(f"No bronze data for {target_date}, scraping...")
            success = asyncio.run(_scrape_date(target_date))
            if not success:
                logger.warning(
                    f"Scraping failed for {target_date}, skipping pipeline execution"
                )
                return

        # --- Step 3: Extract ZIPs → build bronze parquet ---
        extractor = SEPAExtractor()
        raw_zip_dir = None
        try:
            raw_zip_dir = extractor.fetch_from_bronze(target_date, config)
            if raw_zip_dir is None:
                logger.warning(
                    f"Source data not available for {target_date} after scrape attempt"
                )
                return

            all_csv_paths = extractor.extract_all_zips(raw_zip_dir)

            # Build parquet + audit
            audit_data = parquet_loader.build(all_csv_paths, target_date)

            audit_writer.write(
                fecha_vigencia=target_date,
                audit_data=audit_data,
                raw_zip_path=_raw_zip_s3_path(config, target_date),
                parquet_prefix=parquet_loader._parquet_prefix(target_date),
            )

        except Exception as e:
            logger.error(f"Failed to build bronze parquet for {target_date}: {e}")
            raise
        finally:
            if raw_zip_dir and raw_zip_dir.exists():
                cleanup_path = raw_zip_dir
                while (
                    "sepa_bronze_" not in cleanup_path.name
                    and len(cleanup_path.parts) > 1
                ):
                    cleanup_path = cleanup_path.parent
                if cleanup_path.exists() and "sepa_bronze_" in cleanup_path.name:
                    shutil.rmtree(cleanup_path)
                    logger.info(f"Cleaned up temp directory: {cleanup_path}")

    # --- Step 4: Read dimensions from bronze parquet (small, eager) ---
    logger.info(f"Reading bronze parquet for {target_date}")
    dims = parquet_loader.read_dimensions(target_date)

    # --- Step 5a: Validate dimensions ---
    logger.info("Validating dimensions")
    validator = SEPAValidator()

    df_comercios = validator.validate_comercio(dims["comercio"])
    df_sucursales = validator.validate_sucursales(dims["sucursales"])

    # Referential integrity between dims only (empty productos stub)
    df_sucursales, _ = validator.validate_referential_integrity(
        df_comercios,
        df_sucursales,
        pl.DataFrame(schema=get_schema_dict("productos")),
    )

    # --- Step 6: Initialize loaders + load dimensions ---
    logger.info(f"Loading silver layer | Targets: {targets}")

    iceberg_loader = IcebergLoader(config) if "iceberg" in targets else None
    bigquery_loader = BigQueryLoader(config) if "bigquery" in targets else None

    if iceberg_loader:
        iceberg_loader.setup(target_date)
    if bigquery_loader:
        bigquery_loader.setup(target_date)

    if iceberg_loader:
        iceberg_loader.load_comercios(
            to_silver_comercios(df_comercios), target_date
        )
        iceberg_loader.load_sucursales(
            to_silver_sucursales(df_sucursales), target_date
        )
    if bigquery_loader:
        bigquery_loader.load_comercios(
            to_silver_comercios(df_comercios), target_date
        )
        bigquery_loader.load_sucursales(
            to_silver_sucursales(df_sucursales), target_date
        )

    # --- Step 7: Stream productos in batches ---
    logger.info("Processing productos in batches")
    total_loaded = 0

    for idx, chunk in enumerate(
        parquet_loader.read_productos_batched(target_date)
    ):
        logger.info(
            f"Processing batch {idx + 1}: {chunk.height:,} rows"
        )

        chunk = validator.validate_productos(chunk)
        _, chunk = validator.validate_referential_integrity(
            df_comercios, df_sucursales, chunk
        )

        if chunk.height == 0:
            continue

        df_silver_precios = to_silver_precios(chunk)
        df_silver_productos = to_silver_productos(chunk)

        if iceberg_loader:
            iceberg_loader.load_productos(df_silver_productos, target_date)
            iceberg_loader.load(df_silver_precios, target_date)
        if bigquery_loader:
            bigquery_loader.load_productos(df_silver_productos, target_date)
            bigquery_loader.load(df_silver_precios, target_date)

        total_loaded += chunk.height

    drop_stats = validator.get_drop_stats()
    drop_stats["silver_loaded"] = total_loaded

    logger.info(
        f"Pipeline completed for {target_date} | "
        f"Products: {total_loaded:,} rows | "
        f"Dropped — validation: {drop_stats['validation_dropped']:,}, "
        f"integrity: {drop_stats['integrity_dropped']:,}, "
        f"negative_price: {drop_stats['negative_price_count']:,}"
    )

    audit_writer.write_silver_stats(
        fecha_vigencia=target_date,
        drop_stats=drop_stats,
        parquet_prefix=parquet_loader._parquet_prefix(target_date),
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

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
        help="Start of date range (inclusive). Use with --date-to.",
        default=None,
    )
    parser.add_argument(
        "--date-to",
        type=str,
        help="End of date range (inclusive). Use with --date-from.",
        default=None,
    )
    parser.add_argument(
        "--target",
        type=str,
        help="Comma-separated targets: iceberg,bigquery (default: all)",
        default="iceberg,bigquery",
    )
    parser.add_argument(
        "--scrape-only",
        action="store_true",
        help="Only scrape and upload to bronze, do not process",
    )

    return parser.parse_args()


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        logger.error(f"Invalid date format '{value}'. Use YYYY-MM-DD.")
        sys.exit(1)


def _resolve_dates(args: argparse.Namespace) -> list[date]:
    """Resolve the target dates from CLI arguments."""
    if args.date_from or args.date_to:
        if not (args.date_from and args.date_to):
            logger.error("--date-from and --date-to must be used together.")
            sys.exit(1)
        date_from = _parse_date(args.date_from)
        date_to = _parse_date(args.date_to)
        if date_from > date_to:
            logger.error("--date-from must be earlier than or equal to --date-to.")
            sys.exit(1)
        return [
            date_from + timedelta(days=i)
            for i in range((date_to - date_from).days + 1)
        ]
    elif args.date:
        return [_parse_date(args.date)]
    else:
        return [Fecha().ahora.date()]


if __name__ == "__main__":
    config = SEPAConfig()
    args = parse_args()
    dates = _resolve_dates(args)
    targets = [t.strip().lower() for t in args.target.split(",")]

    if args.scrape_only:
        logger.info(
            f"Scrape-only mode | Dates: {dates[0]} to {dates[-1]} "
            f"({len(dates)} day(s))"
        )
        for target_date in dates:
            success = asyncio.run(_scrape_date(target_date))
            if success:
                logger.info(f"Scrape successful for {target_date}")
            else:
                logger.error(f"Scrape failed for {target_date}")
    else:
        logger.info(
            f"Full pipeline | Dates: {dates[0]} to {dates[-1]} "
            f"({len(dates)} day(s)) | Targets: {targets}"
        )
        for target_date in dates:
            process_daily_data(target_date, config, targets)
