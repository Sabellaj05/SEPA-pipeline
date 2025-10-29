import asyncio

from .scraper import SepaScraper
from .utils.logger import logger


URL = "https://datos.produccion.gob.ar/dataset/sepa-precios"
DATA_DIR = "data"


async def main() -> int:
    """
    Main entry point
    Initializes and runs the scraper, then logs the outcome.
    """
    logger.info("=== Starting new scraping session ===")

    # Le damos caña mamahuevo
    async with SepaScraper(url=URL, data_dir=DATA_DIR) as scraper:
        success = await scraper.hurtar_datos()

        if success:
            logger.info("=== Scraping completed successfully ===")
            return 0
        else:
            logger.info("=== Scraping failed ===")
            return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
