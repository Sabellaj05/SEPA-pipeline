import argparse
import asyncio
from datetime import datetime

from .scraper import SepaScraper
from .utils.logger import get_logger

logger = get_logger(__name__)


URL = "https://datos.produccion.gob.ar/dataset/sepa-precios"
DATA_DIR = "data"


async def main(target_date: str | None = None) -> int:
    """
    Initializes and runs the scraper, uploads the raw files to the bronze layer
    then logs the outcome.
    """
    logger.info("=== Starting new scraping session ===")

    # Le damos caña mamahuevo
    async with SepaScraper(url=URL, data_dir=DATA_DIR, target_date=target_date) as scraper:
        success = await scraper.hurtar_datos()

        if success:
            logger.info("=== Scraping completed successfully ===")
            return 0
        else:
            logger.info("=== Scraping failed ===")
            return 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SEPA Pipeline Scraper Extractor")
    parser.add_argument(
        "--date",
        type=str,
        help="Target date in YYYY-MM-DD format (default: today)",
        default=None,
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    
    if args.date:
        try:
            # Simple validation
            datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            logger.error("Invalid date format. Use YYYY-MM-DD.")
            exit(1)

    exit_code = asyncio.run(main(target_date=args.date))
    exit(exit_code)
