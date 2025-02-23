"""SEPA Precios data scraper"""

from pathlib import Path
from typing import Union
import logging
import asyncio
import httpx
from tqdm import tqdm
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass

@dataclass
class Fecha:
    @property
    def _now(self) -> datetime:
        """ Returns current time in date in AR timezone"""
        timezone_ar = timezone(timedelta(hours=-3))
        return datetime.now(timezone_ar)

    @property
    def hoy(self) -> str:
        """ Returns the current date in YYYY-MM-DD format """
        return self._now.strftime("%Y-%m-%d")
    
    @property
    def hoy_full(self) -> str:
        """ Returns the current date in YYYY-MM-DD_HH:MM:SS format"""
        return self._now.strftime("%Y-%m-%d_%H:%M:%S")

def logger_setup() -> logging.Logger:
    """
    Setup logging configuration
    returns:
        logging.Logger: Logger object
    """
    # create log dir if doesn't exists
    current_dir = Path(__file__).parent
    log_dir = current_dir / "logs"
    try:
        log_dir.mkdir(exist_ok=True)
    except OSError as e:
        print(f"Could not create the logs directory: {e}")

    log_file_name = f"logging_{datetime.now().strftime('%Y-%m-%d')}.log"
    log_file_path = log_dir / log_file_name
    # Logging 
    # create the logger
    logger = logging.getLogger("mylog")
    logger.setLevel(logging.DEBUG)
    file_handler = logging.FileHandler(log_file_path)
    ## file handler, only INFO and up (WARNING, ERROR, CRITICAL)
    ## file_handler.setLevel(logging.INFO)
    # file haldner set to DEBUG
    file_handler.setLevel(logging.DEBUG)
    # console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    # format the logger, (time format, log level, message itself)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S")
    # attach format to handler
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

async def connect_to_source(URL: str, logger: logging.Logger) -> Union[httpx.Response, None]:
    """
    Handle connection to the page
    args:
        URL: url of the page
        logger: a logging instance
    returns:
        httpx.Response: A response object from the page
    """
    logger.info(f"Attempting connection to {URL}")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(URL)
            response.raise_for_status()
            logger.info("Successfully connected to source")
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

    return response

def parse_html(response: httpx.Response, logger: logging.Logger) -> Union[str, None]:
    """
    Parses the html on the page and fetches the download link
    args:
        response: Response from the connection function
    returns:
        str:The download link
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
        

        # hoy = datetime.today().strftime("%Y-%m-%d")
        fecha = Fecha()
        hoy = fecha.hoy
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


async def download_data(download_link: str, logger: logging.Logger) -> bool:
    """
    Downloads and extract the data from the provided link
    args:
        download_link: URL to download the files
        logger: Logger instance
    returns:
        bool: True if download was successful, False otherwise
    """

    if not download_link:
        logger.error("No download link provided")
        return False
    
    try:
        # cretaes dowwnload directory
        download_dir = Path(__file__).parent / "data"
        try:
            download_dir.mkdir(exist_ok=True)
        except OSError as e:
            logger.error(f"Could not create data directory, error: {e}")

        # extract the filename from the URL
        file_name = str(download_link.split('/')[-1]).lower()   # grab the last element in the split
        extension = Path(file_name).suffix         # grab the file type
        # we know is a zip file
        if file_name.endswith('.zip'):
            file_name = f"sepa_precios_{datetime.now().strftime('%Y-%m-%d')}.zip"
            logger.info(f"The data to download is a zip file")
        elif file_name.endswith('.csv'):
            file_name = f"sepa_precios_{datetime.now().strftime('%Y-%m-%d')}.csv"
            logger.info(f"The data to download is a csv file")
        else:
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

def main() -> None:
    logger = logger_setup()

    URL = "https://datos.produccion.gob.ar/dataset/sepa-precios"
    logger.info("=== Starting new scraping session ===")
    logger.info(f"Scraping URL: {URL}")
    
    response = asyncio.run(connect_to_source(URL, logger))
    if response is not None:
        download_link = parse_html(response, logger)
        if download_link:
            logger.info(f"Successfully found download link: {download_link}")
            asyncio.run(download_data(download_link,logger))
        else:
            logger.info("No download link found for today's date")
    else:
        logger.info("Cannot retrieve response from the URL")
    
    logger.info("=== Scraping session finished ===")

if __name__ == "__main__":
    main()
