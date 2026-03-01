"""SEPA Precios data scraper"""

import re
from pathlib import Path
from types import TracebackType
from typing import Optional, Self

import httpx
from bs4 import BeautifulSoup
from pyarrow import fs
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

from sepa_pipeline.config import SEPAConfig

import zipfile
import io

from .utils.fecha import Fecha
from .utils.logger import get_logger

logger = get_logger(__name__)


class SepaScraper:
    """Class to scrape SEPA precios."""

    def __init__(self, url: str, data_dir: str):
        """
        Initializes the Scraper.

        args:
            url: The base URL to scrape from.
            data_dir: The directory to save downloaded files.
        """
        self.url = url
        self.data_dir = Path(data_dir)
        self.fecha = Fecha()
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> Self:
        self._client = httpx.AsyncClient(timeout=30.0)
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional[TracebackType] = None,
    ) -> None:
        if self._client:
            await self._client.aclose()

    def _scraped_filename(self) -> str:
        """Return (weekday_based) filename pattern expected on the SEPA site"""
        return f"sepa_{self.fecha.nombre_weekday}.zip"

    def _storage_filename(self) -> str:
        """Return (date-based) filename used when storing the downladed file"""
        return f"sepa_precios_{self.fecha.hoy}.zip"

    @property
    def client(self) -> httpx.AsyncClient:
        """Get the HTTP client, error if not initialized"""
        if self._client is None:
            raise RuntimeError(
                "Client not initialized, use 'async with SepaScraper(...)' pattern"
            )
        return self._client

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def _connect_to_source(self) -> Optional[httpx.Response]:
        """
        Handles connection to the page.
        """
        logger.info(f"Attempting connection to {self.url}")
        try:
            response = await self.client.get(self.url)
            response.raise_for_status()

            if not response.text:
                raise ValueError("No content on the Response")

            logger.info("Successfully connected to source")
            return response

        # let tenacity handle the error with 'raise'
        except httpx.RequestError as exc:
            logger.error(f"Error while requesting {exc.request.url!r}: {exc}")
            raise
        except httpx.HTTPStatusError as exc:
            logger.error(
                f"HTTP {exc.response.status_code} errror for {exc.request.url!r}"
            )
            raise

    def _parse_html(self, response: httpx.Response) -> Optional[str]:
        """
        Parses the HTML to find the download link for today's day of the week.
        The site now uses Spanish day names (e.g., sepa_jueves.zip for Thursday).
        """
        try:
            logger.info("Starting HTML parsing")
            soup = BeautifulSoup(response.text, "html.parser")

            # Get today's Spanish day name
            day_name = self.fecha.nombre_weekday
            iso_date = self.fecha.hoy
            logger.info(f"Today is: {day_name} {iso_date}")

            # Iterate over all package containers to find the one for today
            logger.info(f"Scanning for package matching date: {iso_date}")

            # Find all containers that hold package info and actions
            pkg_containers = soup.find_all("div", class_="pkg-container")
            logger.info(f"Found {len(pkg_containers)} package containers")

            for container in pkg_containers:
                # check date in this container
                package_info = container.find("div", class_="package-info")
                if not package_info:
                    continue

                p_tag = package_info.find("p")
                if not p_tag:
                    continue

                text_content = p_tag.get_text().strip()
                match = re.search(r"(\d{4}-\d{2}-\d{2})", text_content)

                if match:
                    found_date = match.group(1)
                    if found_date == iso_date:
                        logger.info(f"Found matching date container: {found_date}")

                        # get the download link from THIS container
                        # The actions div is usually a sibling or child in the same container
                        actions_div = container.find("div", class_="pkg-actions")
                        if actions_div:
                            # Look for the download button/link
                            # Usually the second link or the one with button "DESCARGAR"
                            links = actions_div.find_all("a", href=True)
                            for link in links:
                                href = link.get("href", "")
                                # Check if it looks like a zip download or contains "download"
                                if (
                                    "download" in str(href).lower()
                                    or ".zip" in str(href).lower()
                                ):
                                    logger.info(f"Found download link: {href}")
                                    return str(href)

                        # Fallback: Searching recursively in this container if logic above failed
                        link = container.find(
                            "a", href=True, string=lambda t: t and "DESCARGAR" in t
                        )
                        if link:
                            return str(link.get("href"))

                # If date doesn't match, continue to next container

            logger.error(
                f"No package found for date {iso_date}. The site might not be updated yet."
            )
            return None

        except Exception as e:
            logger.error(f"Error parsing HTML: {e}")
            return None

    async def _download_data(
        self, download_link: str, min_file_size_mb: int = 150
    ) -> bool:
        """
        Downloads and saves the data from the provided link.

        Args:
            download_link: URL to download the file from
            min_file_size_mb: Minimum file size in MB to consider the download
                successful
        """
        if not download_link:
            logger.error("No download link provided")
            return False

        try:
            self.data_dir.mkdir(exist_ok=True)

            file_name = self._storage_filename()
            file_path = self.data_dir / file_name

            logger.info(f"Downloading file: {file_name}")
            logger.info(f"Destination path: {file_path}")
            logger.info(f"Source link: {download_link}")

            async with self.client.stream("GET", download_link) as response:
                response.raise_for_status()
                total = int(response.headers.get("content-length", 0))

                if total > 0:
                    total_mb = total / (1024 * 1024)
                    logger.info(f"Expected file size: {total_mb:.2f} MB")

                    if total_mb < min_file_size_mb * 0.69:
                        logger.warning(
                            f"Expected size ({total_mb:.2f} MB) smaller than "
                            f"minimum ({min_file_size_mb}) MB"
                        )
                # Downlaod wit progressbar
                with tqdm(
                    total=total, unit="iB", unit_scale=True, desc=file_name
                ) as pbar:
                    with open(file_path, "wb") as f:
                        async for chunk in response.aiter_bytes():
                            f.write(chunk)
                            pbar.update(len(chunk))

            # Validate file size after download
            file_size_bytes = file_path.stat().st_size
            file_size_mb = file_size_bytes / (1024 * 1024)

            logger.info(f"Downloaded file size: {file_size_mb:.2f} MB")

            if file_size_mb < min_file_size_mb:
                logger.error(
                    f"Downloaded file is too small: {file_size_mb:.2f} MB "
                    f"(minimum expected: {min_file_size_mb} MB). "
                    f"The data source may not have updated data for today."
                )
                # Remove the invalid file
                file_path.unlink(missing_ok=True)
                return False

            # Inherent Data Validation Inside the ZIP
            if not self._validate_zip_date(file_path):
                file_path.unlink(missing_ok=True)
                return False

            logger.info("File downloaded successfully and size validated")
            return True
        except httpx.RequestError as exc:
            logger.error(f"Error downloading the file: {exc}")
            return False
        except httpx.HTTPStatusError as exc:
            logger.error(
                f"HTTP {exc.response.status_code} error downloading: {download_link}"
            )
            if file_path and file_path.exists():
                file_path.unlink(missing_ok=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error downloading the file: {e}")
            if 'file_path' in locals() and file_path and file_path.exists():
                file_path.unlink(missing_ok=True)
            return False

    def _validate_zip_date(self, zip_path: Path) -> bool:
        """
        Extracts the ISO date from the 'comercio.csv' footer inside the downloaded ZIP.
        If the intrinsic date is older than today ('target_date'), returns False to abort.
        """
        logger.info("Performing intrinsic date validation inside downloaded Master ZIP...")
        try:
            with zipfile.ZipFile(zip_path, 'r') as master_zf:
                # Find the first inner zip (e.g., sepa_coto.zip)
                inner_zips = [f for f in master_zf.namelist() if f.endswith('.zip')]
                
                if not inner_zips:
                    logger.warning("No nested '.zip' files found inside Master ZIP to validate date.")
                    return True # Fail open if structure unexpected
                    
                inner_zip_name = inner_zips[0]
                logger.info(f"Inspecting nested ZIP: {inner_zip_name}")
                
                # Open the inner ZIP in memory
                with master_zf.open(inner_zip_name, 'r') as inner_file:
                    with zipfile.ZipFile(io.BytesIO(inner_file.read())) as inner_zf:
                        # Find comercio.csv inside the nested zip
                        comercio_files = [f for f in inner_zf.namelist() if f.endswith('comercio.csv')]
                        if not comercio_files:
                            logger.warning(f"No 'comercio.csv' found inside {inner_zip_name}.")
                            return True
                            
                        comercio_filename = comercio_files[0]
                        
                        # Read comercio.csv block by block
                        with inner_zf.open(comercio_filename, 'r') as f:
                            wrapper = io.TextIOWrapper(f, encoding='utf-8-sig', errors='replace')
                            
                            for line in wrapper:
                                if "ltima actualizaci" in line.lower():
                                    logger.info(f"Found footer metadata row: '{line.strip()}'")
                                    
                                    # Extract the YYYY-MM-DD
                                    date_match = re.search(r"(\d{4}-\d{2}-\d{2})", line)
                                    if date_match:
                                        extracted_date_str = date_match.group(1)
                                        target_date_str = self.fecha.hoy
                                        
                                        from datetime import datetime, timedelta
                                        extracted_d = datetime.strptime(extracted_date_str, "%Y-%m-%d").date()
                                        target_d = datetime.strptime(target_date_str, "%Y-%m-%d").date()
                                        
                                        # Data is valid if it's from today or yesterday. 
                                        # If it's older than yesterday, it's stale.
                                        if extracted_d < (target_d - timedelta(days=1)):
                                            logger.error(
                                                f"Intrinsic validation failed! Portal claims {target_date_str} "
                                                f"but actual data inside {inner_zip_name} is stale (up to {extracted_date_str})."
                                            )
                                            return False
                                        else:
                                            logger.info(f"Intrinsic validation passed: {extracted_date_str} is fresh enough for {target_date_str}")
                                            return True
                                    
                            logger.warning("Footer timestamp 'Última actualización' not found in comercio.csv. Attempting to proceed without intrinsic date validation.")
                            return True

        except Exception as e:
            logger.error(f"Error during intrinsic ZIP validation: {e}")
            return True # Fail open on unexpected ZIP parsing errors

    async def hurtar_datos(self, min_file_size_mb: int = 150) -> bool:
        """
        Main async function that orchestrates the scraping process.
        Returns True if scraping and download were successful, False otherwise.

        Args:
            min_file_size_mb: Minimum file size in MB to consider the download
                successful
        """
        try:
            response = await self._connect_to_source()
        except RetryError:
            logger.error(
                "Failed to connect to source after multiple attempts. Aborting."
            )
            return False

        if not response:
            logger.error("Failed to connect to source, aborting.")
            return False

        download_link = self._parse_html(response)
        if not download_link:
            day_name = self.fecha.nombre_weekday
            logger.error(f"No download link found for {day_name} ({self.fecha.hoy})")
            return False

        success = await self._download_data(download_link, min_file_size_mb)

        if success:
            # Upload to Bronze Layer (MinIO)
            try:
                file_name = self._storage_filename()
                local_path = self.data_dir / file_name
                self.upload_to_bronze(local_path)
            except Exception as e:
                logger.error(f"Failed to upload to Bronze layer: {e}")
                # We don't return False here because the download itself was successful,
                # and for local dev we might continue. In strict cloud, this might be fatal.

        return success

    def upload_to_bronze(self, local_path: Path) -> None:
        """Upload the raw ZIP file to MinIO (Bronze Layer)."""
        logger.info(f"Uploading {local_path} to Bronze Layer (MinIO)...")

        config = SEPAConfig()

        # Initialize S3 Filesystem
        s3 = fs.S3FileSystem(
            endpoint_override=config.minio_endpoint,
            access_key=config.minio_access_key,
            secret_key=config.minio_secret_key,
            scheme="http",
            region="us-east-1",
        )

        # Define path: bronze/raw/YYYY/MM/DD/filename.zip
        # We use the date from 'self.fecha.ahora' which is a datetime object.
        date_path = self.fecha.ahora

        s3_path = (
            f"{config.minio_bucket}/bronze/raw/"
            f"year={date_path.year}/"
            f"month={date_path.month:02d}/"
            f"day={date_path.day:02d}/"
            f"{local_path.name}"
        )

        # Ensure directory structure exists (S3 doesn't strictly need this but good for some clients)
        logger.info(f"Destination: s3://{s3_path}")

        # Manually stream the file to avoid API version issues with fs.copy_file
        try:
            with open(local_path, "rb") as source:
                with s3.open_output_stream(s3_path) as dest:
                    dest.write(source.read())

            logger.info("Upload to Bronze Layer successful")
        except Exception as e:
            logger.warning(f"Error uploading data to MinIO: {e}")
