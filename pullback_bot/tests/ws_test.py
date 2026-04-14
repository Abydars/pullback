import asyncio
import os
import json
import logging
from pprint import pprint

# Initialize environment for config
os.environ["BINANCE_API_KEY"] = "b8jOlmhpO3olvEa9WC4HncwTyjLBNT8N6QSG7jRTRxbBmuCzHHY6qEoOwGmBm7Ia"
os.environ["BINANCE_PRIVATE_KEY"] = """-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIHvaHXdrLWwfh6mr1CtVASCkgxy1P4H4mBNvtUNcFs/X
-----END PRIVATE KEY-----"""
os.environ["BINANCE_API_SECRET"] = ""
os.environ["MODE"] = "live"

# Setup logging
logging.basicConfig(level=logging.INFO)

import db
import config
import binance_client as bc

async def main():
    await db.init_db()
    await config.load_from_db()
    # ensure it uses live mode
    config.MODE = "live"
    print("[*] Config loaded.")
    
    # 1. Start WS Client in background
    ws_task = asyncio.create_task(bc.start_ws_api_client())
    await asyncio.sleep(2)  # wait for connection
    
    # 2. Get Exchange Info to find precision
    print("\n[*] Fetching exchange info...")
    info = await bc.get_exchange_info()
    sym = "XRPUSDT"
    rules = bc._exchange_info_cache.get(sym, {})
    if not rules:
        print(f"{sym} not found in exchange info!")
        ws_task.cancel()
        return
        
    prices = [f for f in rules["filters"] if f["filterType"] == "PRICE_FILTER"][0]
    lots = [f for f in rules["filters"] if f["filterType"] == "LOT_SIZE"][0]
    
    print(f"{sym} Tick Size: {prices['tickSize']}, Step Size: {lots['stepSize']}")
    
    # 3. Test REST API (Leverage)
    print("\n[*] Testing REST leverage (HMAC/Ed25519)...")
    res = await bc.set_leverage(sym, 2)
    print(f"Leverage Response: {res}")
    
    # 4. Get Current Price via REST
    price_res = await bc._get("/fapi/v1/premiumIndex", params={"symbol": sym}, signed=False)
    mark_price = float(price_res["markPrice"])
    print(f"\n[*] {sym} Mark Price: {mark_price}")
    
    price_prec = rules.get("pricePrecision", 2)
    qty_prec = rules.get("quantityPrecision", 2)

    # Calculate a safe limit price 50% below current price
    safe_price = round(mark_price * 0.5, price_prec)
    # Notional value should be at least 5 USDT. At $0.5, we need 15 units.
    qty = round(30.0, qty_prec)
    
    print(f"[*] Planning safe LIMIT LONG order: Qty={qty}, Price={safe_price}")
    
    # 5. Place WS LIMIT Order
    order_id = None
    try:
        print("\n[*] Placing LIMIT order via WS API...")
        order_res = await bc._ws_request("order.place", {
            "symbol": sym,
            "side": "BUY",
            "positionSide": "LONG",
            "type": "LIMIT",
            "quantity": qty,
            "timeInForce": "GTC",
            "price": safe_price
        })
        print(f"Order Success!: {json.dumps(order_res, indent=2)}")
        order_id = order_res.get("orderId")
    except Exception as e:
        print(f"WS Order Failed: {e}")
        
    # 6. Cancel the Order
    if order_id:
        await asyncio.sleep(1)
        print(f"\n[*] Cancelling order {order_id}...")
        try:
            cancel_res = await bc._ws_request("order.cancel", {
                "symbol": sym,
                "orderId": order_id
            })
            print(f"Cancel Success!: {json.dumps(cancel_res, indent=2)}")
        except Exception as e:
            print(f"WS Cancel Failed: {e}")

    # Shutdown
    ws_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
