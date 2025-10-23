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
        Parses the HTML to find the download link for today's date.
        """
        try:
            logger.info("Starting HTML parsing")
            soup = BeautifulSoup(response.text, "html.parser")
            pkg_containers = soup.select("div.pkg-container")
            logger.info(f"Found {len(pkg_containers)} package containers")
            if not pkg_containers:
                logger.warning("No 'pkg-container' elements found in the HTML")
                return None
        except Exception as e:
            logger.error(f"Error parsing 'pkg-containers' in HTML: {e}")
            return None

        today_str = self.fecha.hoy
        logger.info(f"Searching for packages matching date: {today_str}")

        for pkg in pkg_containers:
            package_info = pkg.find("div", class_="package-info")
            if not package_info:
                continue

            description_tag = package_info.find("p")  # type: ignore
            if not description_tag:
                continue

            description = description_tag.get_text(strip=True)
            if today_str in description:
                logger.info(f"Found matching package for date {today_str}")
                download_button = pkg.find("a")
                if download_button:
                    button = download_button.find("button")  # type: ignore
                    if button and "DESCARGAR" in button.get_text():
                        pass  # Found the right button
                    else:
                        download_button = None
                if download_button and download_button.get("href"):  # type: ignore
                    download_link = str(download_button["href"])  # type: ignore
                    logger.info(f"Found download link: {download_link}")
                    return download_link
                else:
                    logger.warning("Matching package found, but no download link.")
                    return None

        logger.info(f"No package found for date: {today_str}")
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
            logger.error(f"No download link found for date: {self.fecha.hoy}")
            return False

        return await self._download_data(download_link)
