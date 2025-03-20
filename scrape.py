"""SEPA Precios data scraper"""

from pathlib import Path
from typing import Optional
from utils.logger import logger
from utils.fecha import Fecha
import asyncio
import httpx
from tqdm import tqdm
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass


async def connect_to_source(URL: str) -> Optional[httpx.Response]:
    """
    Handle connection to the page
    args:
        URL: url of the page
    returns:
        Optional[httpx.Response]: A response object from the page or None if connection failed
    """
    logger.info(f"Attempting connection to {URL}")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(URL)
            response.raise_for_status()
            logger.info("Successfully connected to source")
            return response
    except httpx.RequestError as exc:
        logger.error(
            f"Error while requesting {exc.request.url!r}: {exc}", exc_info=True
        )
        return None
    except httpx.HTTPStatusError as exc:
        logger.error(
            f"Error response {exc.response.status_code} while requesting {exc.request.url!r}",
            exc_info=True
        )
        return None


def parse_html(response: httpx.Response, ar_date) -> Optional[str]:
    """
    Parses the html on the page and fetches the download link
    args:
        response: Response from the connection function
        ar_date: AR date in (YYYY-MM-DD) format
    returns:
        Optional[str]: The download link or None if not found
    """
    try:
        logger.info("Starting HTML parsing")
        soup = BeautifulSoup(response.text, "html.parser")
        logger.debug(f"HTML Content: {soup.prettify()[:500]}")
        pkg_containers = soup.find_all("div", class_="pkg-container")
        logger.info(f"Found {len(pkg_containers)} package containers")
        if not pkg_containers:
            logger.warning("No 'pkg-container' elements in the HTML")
            return None
    except AttributeError as e:
        logger.error(f"AttributeError while parsing HTML: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Error parsing 'pkg-containers' in HTML {e}", exc_info=True)
        return None
    
    for pkg in pkg_containers:
        try:
            package_info = pkg.find("div", class_="package-info")
            if package_info is None:
                logger.warning("No 'package-info' found in 'pkg-container")
                continue
            description_tag = package_info.find("p")
            if description_tag is None:
                logger.warning("No 'description-tag' found in 'package-info'")
                continue
            description = description_tag.get_text(strip=True)
            logger.info(f"'description' content: {description}")
        except Exception as e:
            logger.error(f"Error extracting package info from 'pkg-container' {e}", exc_info=True)
            continue

        hoy = ar_date 
        logger.info(f"Searching for packages matching date: {hoy}")

        if hoy in description:
            download_link = None
            try:
                for a in pkg.find_all("a", href=True):
                    button = a.find("button")
                    if button and "DESCARGAR" in button.get_text(strip=True):
                        download_link = str(a["href"])
                        break
            except Exception as e:
                logger.error(f"Error extracting the download link from 'description' {e}", exc_info=True)

            if download_link:
                logger.info(f"Found matching package for date {hoy}")
                return download_link
            else:
                logger.info(f"No download link found for date {hoy}")
                return None
    return None


async def download_data(download_link: str, fecha: Fecha) -> bool:
    """
    Downloads and extract the data from the provided link
    args:
        download_link: URL to download the files
        fecha: AR date in (YYYY-MM-DD) format
    returns:
        bool: True if download was successful, False otherwise
    """

    if not download_link:
        logger.error("No download link provided")
        return False
    
    try:
        # Cretaes dowwnload directory
        download_dir = Path(__file__).parent / "data"
        try:
            download_dir.mkdir(exist_ok=True)
        except OSError as e:
            logger.error(f"Could not create data directory, error: {e}")

        # Extract the filename from the URL
        file_name = str(download_link.split('/')[-1]).lower()   # grab the last element in the split
        today_date = fecha.hoy
        # We know is a zip file
        if file_name.endswith('.zip'):
            file_name = f"sepa_precios_{today_date}.zip"
            logger.info(f"The data to download is a zip file")
        elif file_name.endswith('.csv'):
            file_name = f"sepa_precios_{today_date}.csv"
            logger.info(f"The data to download is a csv file")
        else:
            extension = Path(file_name).suffix         # grab the file type
            logger.warning(f"The file type is of type {extension} and it is not handled")
            return False

        file_path = download_dir / file_name
        logger.info(f"Downloading file: {file_name} to : {file_path}")
        
        # download the file in a stream manner, good for large files
        async with httpx.AsyncClient() as client:
            async with client.stream('GET', download_link) as response:
                response.raise_for_status()

                total = int(response.headers.get("content-length", 0))
                chunk_size = 8192

                with tqdm(total=total, unit='iB', unit_scale=True, desc=file_name) as pbar:
                    with open(file_path, 'wb') as f:
                        async for chunk in response.aiter_bytes(chunk_size=chunk_size):
                            size = len(chunk)
                            f.write(chunk)
                            # logger.info(f"Downloading file in chunks of {chunk} bytes")
                            pbar.update(size)

        logger.info("File downloaded successfully")
        return True

    except httpx.RequestError as exc:
        logger.error(f"Error downloading the file: {exc}")
        return False
    except Exception as e:
        logger.error(f"Error downloading the file: {e}")
        return False

async def scrape_async() -> bool:
    """
    Main async function that orchestrates the scraping process
    
    returns:
        bool: True if scraping and download were successful
    """
    URL = "https://datos.produccion.gob.ar/dataset/sepa-precios"
    fecha = Fecha()
    ar_date = fecha.hoy
    
    # Connect to source system
    response = await connect_to_source(URL)
    if not response:
        logger.error("Failed to connect to source")
        return False
        
    # Parse HTML and get download link
    download_link = parse_html(response, ar_date)
    if not download_link:
        logger.info(f"No download link found for date: {ar_date}")
        return False
        
    # Download the data
    logger.info(f"Found download link: {download_link}")
    downloaded_data = await download_data(download_link, fecha)
    return downloaded_data

def main() -> None:

    """
    Main entry point for the script
    """
    logger.info("=== Starting new scraping session ===")
    
    success = asyncio.run(scrape_async())
    
    status = "successfully" if success else "unsuccessfully"
    logger.info(f"=== Scraping session completed {status} ===")

if __name__ == "__main__":
    main()
