import logging
import time


def setup_logger() -> logging.Logger:
    logging.Formatter.converter = time.gmtime

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        handlers=[logging.FileHandler("orderbook.log"), logging.StreamHandler()],
    )
    return logging.getLogger("OrderBookLogger")
