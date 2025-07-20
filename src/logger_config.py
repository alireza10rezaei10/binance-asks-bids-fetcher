import logging
import time
import asyncio
import telegram
import config


telegram_log_queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1000)


async def telegram_log_sender():
    while True:
        message = await telegram_log_queue.get()

        await telegram.send_message(
            chat_id=config.TELEGRAM_LOGGER_CHAT_ID, text=message
        )
        await asyncio.sleep(2)


class TelegramLogHandler(logging.Handler):
    def __init__(self, chat_id: str):
        super().__init__()
        self.chat_id: str = chat_id

    def emit(self, record: logging.LogRecord):
        if record.name == "telegram" and "SendMessage" in record.getMessage():
            return

        message = self.format(record)
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(telegram_log_queue.put(message))
        # for sync versions telegram handler not working
        except Exception:
            return


def setup_logger() -> None:
    logging.Formatter.converter = time.gmtime

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        handlers=[
            logging.FileHandler(filename="orderbook.log"),
            logging.StreamHandler(),
            TelegramLogHandler(chat_id=config.TELEGRAM_CHANNEL_CHAT_ID),
        ],
    )
