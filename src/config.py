import enum
import dotenv
import os

dotenv.load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")


class OrderBookSavingMethods(enum.StrEnum):
    FULL_CONSTRUCTED = "FULL-CONSTRUCTED"
    ESSENTIAL_UPDATES = "ESSENTIAL-UPDATES"


SYMBOLS: list[str] = ["btcusdt", "ethusdt", "solusdt", "bnbusdt"]
ORDERBOOK_SAVING_METHOD = OrderBookSavingMethods.ESSENTIAL_UPDATES
SAVE_DIR: str = "orderbook_data"

TELEGRAM_API_URL_TEMPLATE = (
    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/" + "{METHOD_NAME}"
)
TELEGRAM_CHANNEL_CHAT_ID = "-1002569093160"
TELEGRAM_MESSAGE_MAX_LENGTH = 4000

MAX_ZIP_SIZE_MB = 30
MAX_ZIP_SIZE_BYTES = MAX_ZIP_SIZE_MB * 1024 * 1024

QUEUE_MAXSIZE: int = 1000
FLUSH_INTERVAL: int = 5  # seconds
MAX_BATCH_SIZE: int = 500
