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
    def extract_all_zips(data_dir: Path, target_date: date) -> List[Dict[str, Path]]:
        """Extract all ZIP files for a given date in parallel"""
        date_dir = data_dir / target_date.isoformat()
        if not date_dir.exists():
            raise FileNotFoundError(f"No data directory for {target_date}")

        zip_files = sorted(date_dir.glob("sepa_*.zip"))
        logger.info(f"Found {len(zip_files)} ZIP files for {target_date}")

        extract_dir = data_dir / "temp" / target_date.isoformat()
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
