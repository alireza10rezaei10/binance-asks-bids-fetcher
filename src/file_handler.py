import os
import ujson as json
import aiofiles
from datetime import datetime, timezone
import logging
import asyncio
import config
import zipfile
import telegram

os.makedirs(config.SAVE_DIR, exist_ok=True)

logger = logging.getLogger(__name__)

current_files: dict[
    tuple[str, datetime], aiofiles.threadpool.text.AsyncTextIOWrapper
] = {}

zip_queue: asyncio.Queue[tuple[str, datetime]] = asyncio.Queue()

QueueType = asyncio.Queue[dict[str, object]]


def get_filename_for_hour(symbol: str, dt: datetime) -> str:
    return os.path.join(config.SAVE_DIR, f"{symbol}_{dt.strftime('%Y-%m-%d_%H')}.jsonl")


def get_hour_key(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def split_and_zip_file(input_path: str, output_zip_base: str) -> list[str]:
    part_paths: list[str] = []
    part_index: int = 1
    with open(input_path, "rb") as f:
        while True:
            chunk = f.read(config.MAX_ZIP_SIZE_BYTES)
            if not chunk:
                break
            part_zip_path = f"{output_zip_base}.part{part_index}.zip"
            with zipfile.ZipFile(
                part_zip_path, "w", compression=zipfile.ZIP_DEFLATED
            ) as zf:
                zf.writestr(os.path.basename(input_path), chunk)
            part_paths.append(part_zip_path)
            part_index += 1
    return part_paths


async def zip_worker():
    while True:
        try:
            symbol, dt = await zip_queue.get()
            input_path = get_filename_for_hour(symbol, dt)
            zip_base_path = input_path.replace(".jsonl", "")

            part_paths = await asyncio.to_thread(
                split_and_zip_file, input_path, zip_base_path
            )
            logger.info(f"[Zipper:{symbol}] Created {len(part_paths)} zip parts.")

            for i, part_path in enumerate(part_paths):
                caption = f"{symbol} | {dt} | Part {i + 1}/{len(part_paths)}"
                is_sent: bool | None = await telegram.send_document_from_disk(
                    chat_id=config.TELEGRAM_CHANNEL_CHAT_ID,
                    file_path=part_path,
                    caption=caption,
                )
                if is_sent:
                    os.remove(part_path)
                    logger.info(f"[Zipper:{symbol}] Removed part file: {part_path}")
                else:
                    logger.warning(f"[Zipper:{symbol}] Failed to send: {part_path}")
                await asyncio.sleep(2)

            os.remove(input_path)
            logger.info(f"[Zipper:{symbol}] Removed original file: {input_path}")

        except Exception as e:
            logger.warning(f"[Zipper] Exception: {e}")


async def write_batch_to_file(symbol: str, batch: list[dict[str, object]]) -> None:
    now = get_hour_key(datetime.now(timezone.utc))
    file_key = (symbol, now)

    if file_key not in current_files:
        for key, f in list(current_files.items()):
            if key[0] == symbol and key[1] != now:
                await f.close()
                logger.info(f"[Writer:{symbol}] Closed file for {key[1]}")

                await zip_queue.put((symbol, key[1]))

                del current_files[key]

        current_files[file_key] = await aiofiles.open(
            get_filename_for_hour(symbol, now), "a"
        )
        logger.info(f"[Writer:{symbol}] Opened new file for {now}")

    current_file = current_files[file_key]
    lines = [json.dumps(entry) + "\n" for entry in batch]
    await current_file.writelines(lines)
    await current_file.flush()


async def writer_task(symbol: str, queue: QueueType) -> None:
    while True:
        batch: list[dict[str, object]] = []

        try:
            item: dict[str, object] = await asyncio.wait_for(
                queue.get(), timeout=config.FLUSH_INTERVAL
            )
            batch.append(item)
        except asyncio.TimeoutError:
            pass

        while not queue.empty() and len(batch) < config.MAX_BATCH_SIZE:
            batch.append(queue.get_nowait())

        if batch:
            try:
                await write_batch_to_file(symbol, batch)
            except Exception as e:
                logger.error(f"[Writer:{symbol}] Error writing batch: {e}")


async def close_all_files() -> None:
    for f in current_files.values():
        await f.close()
    logger.info("[Shutdown] All files closed.")
