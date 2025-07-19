from typing import Any


def update_orderbook(
    new_data: dict[str, Any], orderbook: dict[str, Any]
) -> dict[str, Any]:
    # get copy to prevent changing orderbooks in queue
    new_orderbook = orderbook.copy()

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

        new_orderbook[side] = list(book.items())

    return new_orderbook
