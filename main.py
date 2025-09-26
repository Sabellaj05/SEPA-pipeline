from utils.logger import logger
from scraper import scrape_async
import asyncio


def main() -> None:
    """
    Main entry point for the script
    """

    URL = "https://datos.produccion.gob.ar/dataset/sepa-precios"

    logger.info("=== Starting new scraping session ===")

    success = asyncio.run(scrape_async(URL))

    status = "successfully" if success else "unsuccessfully"
    logger.info(f"=== Scraping session completed {status} ===")


if __name__ == "__main__":
    main()
