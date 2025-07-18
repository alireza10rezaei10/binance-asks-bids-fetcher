import os
import ujson as json
import aiofiles
from datetime import datetime, timezone
from typing import Dict, List, Tuple
import logging
from aiofiles.threadpool.text import AsyncTextIOWrapper
import asyncio
from config import SAVE_DIR, FLUSH_INTERVAL, MAX_BATCH_SIZE

os.makedirs(SAVE_DIR, exist_ok=True)

logger = logging.getLogger("OrderBookLogger")

current_files: Dict[Tuple[str, datetime], AsyncTextIOWrapper] = {}

QueueType = asyncio.Queue[Dict[str, object]]


def get_filename_for_hour(symbol: str, dt: datetime) -> str:
    return os.path.join(SAVE_DIR, f"{symbol}_{dt.strftime('%Y-%m-%d_%H')}.jsonl")


def get_hour_key(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


async def write_batch_to_file(symbol: str, batch: List[Dict[str, object]]) -> None:
    now = get_hour_key(datetime.now(timezone.utc))
    file_key = (symbol, now)

    if file_key not in current_files:
        for key, f in list(current_files.items()):
            if key[0] == symbol and key[1] != now:
                await f.close()
                logger.info(f"[Writer:{symbol}] Closed file for {key[1]}")
                del current_files[key]

        current_files[file_key] = await aiofiles.open(
            get_filename_for_hour(symbol, now), "a"
        )
        logger.info(f"[Writer:{symbol}] Opened new file for {now}")

    current_file = current_files[file_key]
    lines = [json.dumps(entry) + "\n" for entry in batch]
    await current_file.writelines(lines)
    await current_file.flush()
    # logger.info(f"[Writer:{symbol}] Flushed {len(batch)} messages to disk.")


async def writer_task(symbol: str, queue: QueueType) -> None:
    while True:
        batch: List[Dict[str, object]] = []

        try:
            item: Dict[str, object] = await asyncio.wait_for(
                queue.get(), timeout=FLUSH_INTERVAL
            )
            batch.append(item)
        except asyncio.TimeoutError:
            pass

        while not queue.empty() and len(batch) < MAX_BATCH_SIZE:
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
