"""SEPA Precios data scraper"""

from pathlib import Path
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

from .utils.fecha import Fecha
from .utils.logger import logger


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
        self.client = httpx.AsyncClient(timeout=20)

    def _scraped_filename(self) -> str:
        """Return (weekday_based) filename pattern expected on the SEPA site"""
        return f"sepa_{self.fecha.nombre_weekday}.zip"

    def _storage_filename(self) -> str:
        """Return (date-based) filename used when storing the downladed file"""
        return f"sepa_precios{self.fecha.hoy}.zip"

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
            logger.info("Successfully connected to source")
            return response
        except httpx.RequestError as exc:
            logger.error(f"Error while requesting {exc.request.url!r}: {exc}")
            return None
        except httpx.HTTPStatusError as exc:
            logger.error(
                f"Error response {exc.response.status_code} while requesting "
                f"{exc.request.url!r}"
            )
            return None

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
            logger.info(f"Today is: {day_name}")

            # Find all links on the page
            all_links = soup.find_all("a", href=True)
            logger.info(f"Found {len(all_links)} total links on page")

            # Look for link containing "sepa_" + day name + ".zip"
            # target_filename = f"sepa_{day_name}.zip"
            target_filename = self._scraped_filename()
            logger.info(f"Searching for file: {target_filename}")

            for link in all_links:
                href = link.get("href", "")  # type: ignore[union-attr]

                # ignore 'mypy error' because the code is fine
                # A _QueryResults (or ResultSet) is what you get when you call methods
                # like find_all() or select()—it is essentially a list-like container
                # of Tag objects, so you must access elements within it before you can
                # call methods like get().
                #
                # links = soup.find_all('a', class_='product-link')
                # hrefs = [link.get('href') for link in links]
                # .get() works on each Tag, not on the ResultSet

                # Check if this link contains the target filename
                if target_filename in str(href).lower():
                    logger.info("Found matching download link!")
                    logger.info(f"Link: {href}")

                    # The link should already be absolute from the CKAN dataset page
                    return str(href)

            logger.warning(f"No download link found for {target_filename}")
            logger.debug(f"Searched all {len(all_links)} links")
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

            logger.info(f"Downloading file: {file_name} to : {file_path}")

            async with self.client.stream("GET", download_link) as response:
                response.raise_for_status()
                total = int(response.headers.get("content-length", 0))

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
                file_path.unlink()
                return False

            logger.info("File downloaded successfully and size validated")
            return True
        except httpx.RequestError as exc:
            logger.error(f"Error downloading the file: {exc}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error downloading the file: {e}")
            return False

    async def hurtar_datos(self, min_file_size_mb: int = 150) -> bool:
        """
        Main async function that orchestrates the scraping process.
        Returns True if scraping and download were successful, False otherwise.

        Args:
            min_file_size_mb: Minimum file size in MB to consider the download
                successful
        """
        response = await self._connect_to_source()
        if not response:
            logger.error("Failed to connect to source, aborting.")
            return False

        download_link = self._parse_html(response)
        if not download_link:
            day_name = self.fecha.nombre_weekday
            logger.error(f"No download link found for {day_name} ({self.fecha.hoy})")
            return False

        return await self._download_data(download_link, min_file_size_mb)
