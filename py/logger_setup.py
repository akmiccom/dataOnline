# ============================
# logger_setup.py
# ============================
import logging
from colorlog import ColoredFormatter

def setup_logger(name="scraper", log_file=None, level=logging.DEBUG):
    formatter = ColoredFormatter(
        "%(log_color)s[%(asctime)s] [%(levelname)s]%(reset)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "bold_red",
        },
    )

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.handlers = []  # avoid duplicate logs

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s'))
        logger.addHandler(file_handler)

    return logger


if __name__ == "__main__":
    setup_logger()