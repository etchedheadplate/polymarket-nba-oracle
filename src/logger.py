import logging
from logging.handlers import RotatingFileHandler

from src.config import settings

LOG_DIR = settings.LOG_DIR
LOG_DIR.mkdir(exist_ok=True)

formatter = logging.Formatter("[%(levelname)s] %(asctime)s - %(name)s - %(message)s")

logger = logging.getLogger("service")
logger.setLevel(logging.DEBUG)

if not logger.hasHandlers():
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    info_file_handler = RotatingFileHandler(
        LOG_DIR / "service.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    info_file_handler.setLevel(logging.INFO)
    info_file_handler.setFormatter(formatter)

    debug_file_handler = RotatingFileHandler(
        LOG_DIR / "debug.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    debug_file_handler.setLevel(logging.DEBUG)
    debug_file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(info_file_handler)
    logger.addHandler(debug_file_handler)

logging.getLogger("celery").propagate = False
