"""SEPA Precios data scraper"""

from datetime import datetime
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

    # Spanish day names mapping
    SPANISH_DAYS = {
        0: "lunes",      # Monday
        1: "martes",     # Tuesday
        2: "miercoles",  # Wednesday (no accent in filename)
        3: "jueves",     # Thursday
        4: "viernes",    # Friday
        5: "sabado",     # Saturday (no accent in filename)
        6: "domingo",    # Sunday
    }

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

    def _get_spanish_day_name(self) -> str:
        """
        Get the Spanish day name for today.
        Returns: Spanish day name in lowercase (e.g., 'jueves')
        """
        today = datetime.now()
        day_index = today.weekday()
        return self.SPANISH_DAYS[day_index]

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
            day_name = self._get_spanish_day_name()
            logger.info(f"Today is: {day_name}")
            
            # Find all links on the page
            all_links = soup.find_all("a", href=True)
            logger.info(f"Found {len(all_links)} total links on page")
            
            # Look for link containing "sepa_" + day name + ".zip"
            target_filename = f"sepa_{day_name}.zip"
            logger.info(f"Searching for file: {target_filename}")
            
            for link in all_links:
                href = link.get("href", "")
                
                # Check if this link contains the target filename
                if target_filename in str(href).lower():
                    logger.info(f"Found matching download link!")
                    logger.info(f"Link: {href}")
                    
                    # The link should already be absolute from the CKAN dataset page
                    return str(href)
            
            logger.warning(f"No download link found for {target_filename}")
            logger.debug(f"Searched all {len(all_links)} links")
            return None
            
        except Exception as e:
            logger.error(f"Error parsing HTML: {e}")
            return None

    async def _download_data(self, download_link: str) -> bool:
        """
        Downloads and saves the data from the provided link.
        """
        if not download_link:
            logger.error("No download link provided")
            return False

        try:
            self.data_dir.mkdir(exist_ok=True)

            today_date = self.fecha.hoy
            file_name = f"sepa_precios_{today_date}.zip"
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

            logger.info("File downloaded successfully")
            return True
        except httpx.RequestError as exc:
            logger.error(f"Error downloading the file: {exc}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error downloading the file: {e}")
            return False

    async def hurtar_datos(self) -> bool:
        """
        Main async function that orchestrates the scraping process.
        Returns True if scraping and download were successful, False otherwise.
        """
        response = await self._connect_to_source()
        if not response:
            logger.error("Failed to connect to source, aborting.")
            return False

        download_link = self._parse_html(response)
        if not download_link:
            day_name = self._get_spanish_day_name()
            logger.error(f"No download link found for {day_name} ({self.fecha.hoy})")
            return False

        return await self._download_data(download_link)
