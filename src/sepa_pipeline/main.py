import asyncio
from sepa_pipeline.scraper import SepaScraper
from sepa_pipeline.utils.logger import logger


URL = "https://www.se.gob.ar/datosupstream/procesamiento-de-gas/"
DATA_DIR = "data"


def main() -> None:
    """
    Main entry point
    Initializes and runs the scraper, then logs the outcome.
    """
    logger.info("=== Starting new scraping session ===")

    # Instanciamos el scraper
    scraper = SepaScraper(url=URL, data_dir=DATA_DIR)
    
    # Le damos caña mamahuevo
    success = asyncio.run(scraper.hurtar_datos())

    status = "successfully" if success else "unsuccessfully"
    logger.info(f"=== Scraping session completed {status} ===")


if __name__ == "__main__":
    main()
