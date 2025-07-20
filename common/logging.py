import sys

from loguru import logger


def init_logger():
    logger.remove()

    # Sink for INFO logs - just the message
    logger.add(
        sys.stdout,
        level="INFO",
        filter=lambda record: record["level"].name == "INFO",
        format="<white>{message}</white>",
    )

    # Sink for other levels - full formatting
    logger.add(
        sys.stderr,
        level="DEBUG",
        filter=lambda record: record["level"].name not in ("INFO", "DEBUG"),
        format="<green>{time:HH:mm:ss.SSS}</green> | <level>{level}</level> | <cyan>{message}</cyan>",
    )

    logger.add(
        "app.log",
        level="DEBUG",
        format="{time} | {level} | {function}:{line} | {message}",
        mode="a",
        enqueue=True,
    )
