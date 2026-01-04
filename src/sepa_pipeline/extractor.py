"""
SEPA Data Extractor
Handles ZIP file extraction.
"""
import logging
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Dict, List
from sepa_pipeline.utils.logger import get_logger
from sepa_pipeline.utils.fecha import Fecha
from pyarrow import fs
from sepa_pipeline.config import SEPAConfig

logger = get_logger(__name__)

class SEPAExtractor:
    """Extracts and validates SEPA ZIP files"""

    EXPECTED_FILES = {"comercio.csv", "sucursales.csv", "productos.csv"}

    @staticmethod
    def extract_zip(zip_path: Path, extract_to: Path) -> Dict[str, Path]:
        """Extract a single ZIP file and return paths to CSVs"""
        logger.info(f"Extracting {zip_path.name}")

        csv_paths = {}
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_files = set(zip_ref.namelist())
            missing_files = SEPAExtractor.EXPECTED_FILES - zip_files
            if missing_files:
                raise ValueError(f"{zip_path.name} missing files: {missing_files}")

            extract_dir = extract_to / zip_path.stem
            extract_dir.mkdir(parents=True, exist_ok=True)
            zip_ref.extractall(extract_dir)

            for csv_name in SEPAExtractor.EXPECTED_FILES:
                csv_type = csv_name.replace(".csv", "")
                csv_paths[csv_type] = extract_dir / csv_name

        return csv_paths

    @staticmethod
    def fetch_from_bronze(target_date: date, config: SEPAConfig) -> Path:
        """
        Download the Master ZIP from MinIO (Bronze Layer) and unzip it to a temp dir.
        Returns the path to the directory containing the child ZIPs.
        """
        logger.info(f"Fetching Bronze Layer data for {target_date} from MinIO...")
        
        # Initialize S3 Filesystem
        s3 = fs.S3FileSystem(
            endpoint_override=config.minio_endpoint,
            access_key=config.minio_access_key,
            secret_key=config.minio_secret_key,
            scheme="http",
            region="us-east-1",
        )
        
        # Construct S3 Path
        # The stored file is usually: sepa_precios_YYYY-MM-DD.zip
        # Path: bucket/bronze/raw/year=YYYY/month=MM/day=DD/filename.zip
        # But wait, we need the exact filename or list the directory.
        # Scraper saves as: sepa_precios_YYYY-MM-DD.zip
        
        filename = f"sepa_precios_{target_date.strftime('%Y-%m-%d')}.zip"
        s3_path = (
            f"{config.minio_bucket}/bronze/raw/"
            f"year={target_date.year}/"
            f"month={target_date.month:02d}/"
            f"day={target_date.day:02d}/"
            f"{filename}"
        )
        
        # Local Temp Path
        temp_dir = Path(f"/tmp/sepa_bronze_{target_date}")
        temp_dir.mkdir(parents=True, exist_ok=True)
        local_zip_path = temp_dir / filename
        
        logger.info(f"Downloading s3://{s3_path} -> {local_zip_path}")
        try:
            # Manually stream from S3 to local file to avoid API issues with copy_file
            with s3.open_input_stream(s3_path) as source:
                with open(local_zip_path, "wb") as dest:
                    dest.write(source.read())
        except Exception as e:
            logger.error(f"Failed to download from Bronze: {e}")
            raise

        # Unzip Master ZIP
        # This will create a folder (usually with the same name as the zip stem) containing child zips
        master_extract_dir = temp_dir / "master_extracted"
        master_extract_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Unzipping Master ZIP to {master_extract_dir}")
        with zipfile.ZipFile(local_zip_path, "r") as zip_ref:
            zip_ref.extractall(master_extract_dir)
            
        # The content is usually a single folder like 'precios_20240520' containing the zips.
        # Or sometimes just the zips at root. We need to find where the child zips are.
        # Let's search for any .zip file inside master_extract_dir recursively.
        child_zips = list(master_extract_dir.rglob("sepa_*.zip"))
        
        if not child_zips:
            raise ValueError("No child ZIPs found in the downloaded Master ZIP")
            
        # Return the parent directory of the found zips (assuming they are in one dir)
        return child_zips[0].parent

    @staticmethod
    def extract_all_zips(source_dir: Path, target_date: date = None) -> List[Dict[str, Path]]:
        """
        Extract all ZIP files from a source directory in parallel.
        If target_date is None, source_dir is treated as the directory containing zips.
        """
        if target_date:
            # Legacy/Local mode: construct path from data_dir + date
            date_dir = source_dir / target_date.isoformat()
            if not date_dir.exists():
                # Allow for the case where source_dir IS the date dir
                if source_dir.name == target_date.isoformat():
                     date_dir = source_dir
                else: 
                     raise FileNotFoundError(f"No data directory for {target_date} in {source_dir}")
        else:
            # Cloud mode: source_dir IS the directory containing zips
            date_dir = source_dir

        if not date_dir.exists():
             raise FileNotFoundError(f"Source directory does not exist: {date_dir}")

        zip_files = sorted(date_dir.glob("sepa_*.zip"))
        logger.info(f"Found {len(zip_files)} ZIP files in {date_dir}")

        extract_dir = date_dir / "extracted_csvs"
        extract_dir.mkdir(parents=True, exist_ok=True)

        all_csv_paths = []
        with ProcessPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(
                    SEPAExtractor.extract_zip, zip_path, extract_dir
                ): zip_path
                for zip_path in zip_files
            }

            for future in as_completed(futures):
                zip_path = futures[future]
                try:
                    csv_paths = future.result()
                    all_csv_paths.append(csv_paths)
                except Exception as e:
                    logger.error(f"Failed to extract {zip_path.name}: {e}")
                    raise

        return all_csv_paths
