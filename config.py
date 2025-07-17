import enum


class OrderBookSavingMethods(enum.StrEnum):
    FULL_CONSTRUCTED = "FULL-CONSTRUCTED"
    ESSENTIAL_UPDATES = "ESSENTIAL-UPDATES"


SYMBOLS: list[str] = ["btcusdt", "ethusdt", "solusdt", "bnbusdt"]
ORDERBOOK_SAVING_METHOD = OrderBookSavingMethods.ESSENTIAL_UPDATES
SAVE_DIR: str = "orderbook_data"

DEPTH_URL_TEMPLATE: str = "wss://fstream.binance.com/stream?streams={}@depth"
QUEUE_MAXSIZE: int = 1000
FLUSH_INTERVAL: int = 5  # seconds
MAX_BATCH_SIZE: int = 500
