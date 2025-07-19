import aiohttp
import enum
from typing import TypedDict
import os
import config
import logging

logger = logging.getLogger(__name__)


class Urls(enum.StrEnum):
    SEND_MESSAGE = config.TELEGRAM_API_URL_TEMPLATE.format(METHOD_NAME="sendMessage")
    SEND_DOCUMENT = config.TELEGRAM_API_URL_TEMPLATE.format(METHOD_NAME="sendDocument")


class SendMessagePayload(TypedDict):
    chat_id: str
    text: str


async def send_message(chat_id: str, text: str):
    payload: SendMessagePayload = {
        "chat_id": chat_id,
        "text": text[: config.TELEGRAM_MESSAGE_MAX_LENGTH],
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(Urls.SEND_MESSAGE, json=payload) as response:
                if response.status != 200:
                    error = await response.json()
                    logger.error(
                        f"[SendMessage] Message: {text} could not send to telegram channel. {error}"
                    )
    except Exception as e:
        logger.error(f"[SendMessage] Error: {e}")


async def send_document_from_disk(chat_id: str, file_path: str, caption: str = ""):
    if not os.path.exists(file_path):
        logger.error(f"[SendDocument] Error: {file_path} file not found.")
        return

    try:
        form = aiohttp.FormData()

        form.add_field("chat_id", chat_id)

        if caption:
            form.add_field("caption", caption)

        with open(file_path, "rb") as file:
            form.add_field(
                "document",
                file,
                filename=os.path.basename(file_path),
                content_type="application/octet-stream",
            )

        async with aiohttp.ClientSession() as session:
            async with session.post(Urls.SEND_DOCUMENT, data=form) as response:
                if response.status == 200:
                    logger.info(
                        f"[SendDocument] File: {file_path} sent to telegram channel."
                    )
                else:
                    error = await response.json()
                    logger.error(
                        f"[SendDocument] File: {file_path} could not send to telegram channel. {error}"
                    )

    except Exception as e:
        logger.error(f"[SendDocument] Error: {e}")
