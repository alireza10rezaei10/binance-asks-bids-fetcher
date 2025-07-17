import pandas as pd
import json


DATA_PATH = "./orderbook_data/btcusdt_2025-07-17_02.jsonl"

with open(DATA_PATH, "r") as reader:
    all_data = [json.loads(line) for line in reader]

orderbook = all_data[0]

writer = open("reconstructed.jsonl", "w")


def update_orderbook(new_data, orderbook):
    if len(orderbook) == 0 or orderbook["lastUpdateId"] + 1 < new_data["U"]:
        raise "Error"
    if new_data["u"] < orderbook["lastUpdateId"]:
        raise "Error"

    # get copy to prevent changing orderbooks in queue
    new_orderbook = orderbook.copy()

    # new_orderbook["symbol"] = new_data["s"]
    new_orderbook["time"] = new_data["E"]
    new_orderbook["lastUpdateId"] = new_data["u"]

    for side in ["asks", "bids"]:
        book = {p: q for p, q in orderbook[side]}

        # in data we have 'a' for 'asks' and 'b' for 'bids'
        updates = new_data.get(side[0], [])

        for price_str, qty_str in updates:
            if float(qty_str) == 0:
                book.pop(price_str, None)
            else:
                book[price_str] = qty_str

        # # Rebuild sorted list for order book
        # reverse = side == "bids"  # bids are sorted high to low, asks low to high
        # self.order_book[side] = sorted(book.items(), reverse=reverse)

        new_orderbook[side] = list(book.items())

    return new_orderbook


for data in all_data[1:]:
    orderbook = update_orderbook(new_data=data, orderbook=orderbook)
    writer.writelines([json.dumps(orderbook) + "\n"])
