import asyncio
import os
import time
import pprint

os.environ["BINANCE_API_KEY"] = "b8jOlmhpO3olvEa9WC4HncwTyjLBNT8N6QSG7jRTRxbBmuCzHHY6qEoOwGmBm7Ia"
os.environ["BINANCE_PRIVATE_KEY"] = """-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIHvaHXdrLWwfh6mr1CtVASCkgxy1P4H4mBNvtUNcFs/X
-----END PRIVATE KEY-----"""

import binance_client as bc

async def test():
    symbol = "TRXUSDT"
    print("Testing WebSocket order.place SL ...")
    params = {
        "symbol": symbol,
        "side": "SELL",
        "type": "STOP_MARKET",
        "stopPrice": 0.3000,
        "closePosition": "true",
        "positionSide": "LONG",
        "workingType": "MARK_PRICE",
    }
    
    await bc.start_ws_api_client()
    time.sleep(1)
    
    try:
        res = await bc._ws_request("order.place", params=params)
        print("WS Success:", res)
    except Exception as e:
        print("WS Failed:", e)

if __name__ == "__main__":
    asyncio.run(test())
