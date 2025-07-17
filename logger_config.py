import logging


def setup_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        handlers=[logging.FileHandler("orderbook.log"), logging.StreamHandler()],
    )
    return logging.getLogger("OrderBookLogger")
