import websockets
import ujson as json
import asyncio
import aiohttp
import logging
from typing import Any
import config
import data_processors

QueueType = asyncio.Queue[dict[str, object]]

logger = logging.getLogger("OrderBookLogger")


async def get_snapshot(symbol: str) -> dict[str, Any]:
    url = f"https://api.binance.com/api/v3/depth?symbol={symbol.upper()}&limit=5000"
    async with aiohttp.ClientSession() as session:
        while True:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(
                        f"[Fetcher:{symbol}] Successfully fetched snapshot for {symbol}."
                    )
                    return data
                else:
                    logger.error(
                        f"Failed to fetch snapshot for {symbol}, with status code: {response.status} retrying in 1 second..."
                    )
                    await asyncio.sleep(1)


def orderbook_is_not_usable(
    new_data: dict[str, Any], orderbook: dict[str, Any]
) -> bool:
    if len(orderbook) == 0 or orderbook["lastUpdateId"] + 1 < new_data["U"]:
        return True
    return False


def data_is_usable(orderbook: dict[str, Any], new_data: dict[str, Any]):
    return new_data["u"] >= orderbook["lastUpdateId"]


async def put_orderbook_updates_to_the_queue(
    symbol: str,
    depth_updates_queue: QueueType,
    orderbook_updates_queue: QueueType,
    saving_method: config.OrderBookSavingMethods,
):
    """method is one of the: essential-updates and full-order-book"""
    orderbook: dict[str, Any] = {}

    while True:
        new_data: dict[str, Any] = await depth_updates_queue.get()

        if orderbook_is_not_usable(new_data=new_data, orderbook=orderbook):
            while orderbook_is_not_usable(new_data=new_data, orderbook=orderbook):
                logger.info(
                    f"[WebSocket:{symbol}] orderbook is not found or too old... trying to get new one..."
                )
                orderbook: dict[str, Any] = await get_snapshot(symbol)

            if saving_method == config.OrderBookSavingMethods.ESSENTIAL_UPDATES:
                await orderbook_updates_queue.put(orderbook)

        if data_is_usable(new_data=new_data, orderbook=orderbook):
            if saving_method == config.OrderBookSavingMethods.FULL_CONSTRUCTED:
                orderbook = await asyncio.to_thread(
                    data_processors.update_orderbook,
                    new_data=new_data,
                    orderbook=orderbook,
                )
                await orderbook_updates_queue.put(orderbook)
            else:
                await orderbook_updates_queue.put(new_data)
                orderbook["lastUpdateId"] = new_data["u"]


async def put_depth_updates_to_the_queue(symbol: str, depth_updates_queue: QueueType):
    url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@depth"

    while True:
        try:
            async with websockets.connect(url) as ws:
                logger.info(f"[WebSocket:{symbol}] Connected.")
                while True:
                    message = await ws.recv()
                    data: dict[str, object] = json.loads(message)
                    await depth_updates_queue.put(data)
        except Exception as e:
            logger.error(f"[WebSocket:{symbol}] Error: {e}. Reconnecting in 1s...")
            await asyncio.sleep(1)
