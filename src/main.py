import asyncio
import uvloop
import websocket_handler
import config
import logging
import logger_config
import file_handler


logger_config.setup_logger()
logger = logging.getLogger(__name__)

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

QueueType = asyncio.Queue[dict[str, object]]


async def store_order_book(symbol: str, saving_method: config.OrderBookSavingMethods):
    depth_updates_queue: QueueType = asyncio.Queue(maxsize=config.QUEUE_MAXSIZE)
    orderbook_updates_queue: QueueType = asyncio.Queue(maxsize=config.QUEUE_MAXSIZE)

    await asyncio.gather(
        websocket_handler.put_depth_updates_to_the_queue(symbol, depth_updates_queue),
        websocket_handler.put_orderbook_updates_to_the_queue(
            symbol,
            depth_updates_queue,
            orderbook_updates_queue,
            saving_method=saving_method,
        ),
        file_handler.writer_task(symbol, orderbook_updates_queue),
        file_handler.zip_worker(),
    )


async def main() -> None:
    tasks = [
        store_order_book(symbol, saving_method=config.ORDERBOOK_SAVING_METHOD)
        for symbol in config.SYMBOLS
    ]
    tasks.append(logger_config.telegram_log_sender())

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError as ce:
        logger.info(f"[Shutdown] Tasks cancelled. details: {ce}")
    finally:
        await file_handler.close_all_files()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("[Shutdown] Interrupted by user.")
