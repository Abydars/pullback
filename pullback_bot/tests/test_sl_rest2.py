import asyncio
import os

os.environ["BINANCE_API_KEY"] = "b8jOlmhpO3olvEa9WC4HncwTyjLBNT8N6QSG7jRTRxbBmuCzHHY6qEoOwGmBm7Ia"
os.environ["BINANCE_PRIVATE_KEY"] = """-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIHvaHXdrLWwfh6mr1CtVASCkgxy1P4H4mBNvtUNcFs/X
-----END PRIVATE KEY-----"""

import binance_client as bc

async def test():
    symbol = "TRXUSDT"
    print("Testing REST order SL without closePosition...")
    params = {
        "symbol": symbol,
        "side": "SELL",
        "type": "STOP_MARKET",
        "stopPrice": 0.3000,
        "quantity": 10.0,
        "positionSide": "LONG",
        "reduceOnly": "true"
    }
    try:
        res = await bc._post("/fapi/v1/order", params=params)
        print("Success:", res)
    except Exception as e:
        print("Failed:", e)
        
    print("-- without reduceOnly either --")
    params.pop("reduceOnly", None)
    try:
        res = await bc._post("/fapi/v1/order", params=params)
        print("Success:", res)
    except Exception as e:
        print("Failed:", e)

if __name__ == "__main__":
    asyncio.run(test())
