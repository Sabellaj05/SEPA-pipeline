import asyncio

from .scraper import SepaScraper
from .utils.logger import get_logger

logger = get_logger(__name__)


URL = "https://datos.produccion.gob.ar/dataset/sepa-precios"
DATA_DIR = "data"


async def main() -> None:
    """
    Initializes and runs the scraper, uploads the raw files to the bronze layer
    then logs the outcome.
    """
    logger.info("=== Starting new scraping session ===")

    # Le damos caña mamahuevo
    async with SepaScraper(url=URL, data_dir=DATA_DIR) as scraper:
        success = await scraper.hurtar_datos()

        if success:
            logger.info("=== Scraping completed successfully ===")
        else:
            logger.info("=== Scraping failed ===")


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
