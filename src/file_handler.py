import os
import ujson as json
import aiofiles
from datetime import datetime, timezone
import logging
import asyncio
import config
import zipfile

os.makedirs(config.SAVE_DIR, exist_ok=True)

logger = logging.getLogger(__name__)

current_files: dict[
    tuple[str, datetime], aiofiles.threadpool.text.AsyncTextIOWrapper
] = {}

QueueType = asyncio.Queue[dict[str, object]]


def get_filename_for_hour(symbol: str, dt: datetime) -> str:
    return os.path.join(config.SAVE_DIR, f"{symbol}_{dt.strftime('%Y-%m-%d_%H')}.jsonl")


def get_hour_key(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


async def write_batch_to_file(symbol: str, batch: list[dict[str, object]]) -> None:
    now = get_hour_key(datetime.now(timezone.utc))
    file_key = (symbol, now)

    if file_key not in current_files:
        for key, f in list(current_files.items()):
            if key[0] == symbol and key[1] != now:
                await f.close()
                logger.info(f"[Writer:{symbol}] Closed file for {key[1]}")

                # Zip the file
                input_path = get_filename_for_hour(symbol, key[1])
                output_zip_path = input_path + ".zip"
                try:
                    zip_single_file(input_path, output_zip_path)
                    logger.info(f"[Writer:{symbol}] Zipped file: {output_zip_path}")
                    os.remove(input_path)
                    logger.info(
                        f"[Writer:{symbol}] Removed original file: {input_path}"
                    )
                except Exception as e:
                    logger.warning(f"[Writer:{symbol}] Failed to remove file: {e}")

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


def zip_single_file(input_path: str, output_zip_path: str):
    with zipfile.ZipFile(output_zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(input_path, arcname=input_path.split("/")[-1])


async def close_all_files() -> None:
    for f in current_files.values():
        await f.close()
    logger.info("[Shutdown] All files closed.")
