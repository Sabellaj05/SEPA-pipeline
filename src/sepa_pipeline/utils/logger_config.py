import logging
from datetime import datetime
from pathlib import Path


def logger_setup(name: str) -> logging.Logger:
    """
    Setup logging configuration
    returns:
        logging.Logger: Logger object
    """
    # create log dir if doesn't exists
    # Navigate to project root (three levels up from utils/)
    project_root = Path(__file__).parent.parent.parent.parent
    log_dir = project_root / "logs"
    try:
        log_dir.mkdir(exist_ok=True)
    except OSError as e:
        print(f"Could not create the logs directory: {e}")

    log_file_name = f"logging_{datetime.now().strftime('%Y-%m-%d')}.log"
    log_file_path = log_dir / log_file_name

    # Logging
    # create the logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(log_file_path)
    # file handler, only INFO and up (WARNING, ERROR, CRITICAL)
    # file_handler.setLevel(logging.INFO)
    # file handler set to DEBUG
    file_handler.setLevel(logging.DEBUG)

    # console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    # format the logger, (time format, log level, message itself)
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    # attach format to handler
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
