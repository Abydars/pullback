import asyncio
import os

os.environ["BINANCE_API_KEY"] = "b8jOlmhpO3olvEa9WC4HncwTyjLBNT8N6QSG7jRTRxbBmuCzHHY6qEoOwGmBm7Ia"
os.environ["BINANCE_PRIVATE_KEY"] = """-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIHvaHXdrLWwfh6mr1CtVASCkgxy1P4H4mBNvtUNcFs/X
-----END PRIVATE KEY-----"""

import binance_client as bc

async def test():
    symbol = "TRXUSDT"
    print("Testing REST ALGO order SL ...")
    params = {
        "symbol": symbol,
        "side": "SELL",
        "type": "STOP_MARKET",
        "stopPrice": 0.3000,
        "closePosition": "true",
        "positionSide": "LONG",
        "workingType": "MARK_PRICE",
    }
    try:
        # Maybe it's /fapi/v1/algo/order? Or /fapi/v1/algo/futures/newOrderAlgo? Let's just try /fapi/TBD
        res = await bc._post("/fapi/v1/algo/order", params=params)
        print("Success:", res)
    except Exception as e:
        print("Failed:", e)

if __name__ == "__main__":
    asyncio.run(test())
