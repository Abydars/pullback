import asyncio
import os
import json
import logging
import time

# Initialize environment for config
os.environ["BINANCE_API_KEY"] = "b8jOlmhpO3olvEa9WC4HncwTyjLBNT8N6QSG7jRTRxbBmuCzHHY6qEoOwGmBm7Ia"
os.environ["BINANCE_PRIVATE_KEY"] = """-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIHvaHXdrLWwfh6mr1CtVASCkgxy1P4H4mBNvtUNcFs/X
-----END PRIVATE KEY-----"""
os.environ["BINANCE_API_SECRET"] = ""
os.environ["MODE"] = "live"

logging.basicConfig(level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)

import config
import db
import binance_client as bc
from order_manager import order_manager
from user_data_stream import user_data_stream

async def main():
    await db.init_db()
    await config.load_from_db()
    getattr(config, "HEDGE_MODE_ENABLED", True) # Safe hedge mode default
    
    # ── 1. Stream Boot ────────────────────────────────────────────────────────
    print("[*] Starting Binance WebSocket User Data Stream...")
    asyncio.create_task(bc.start_ws_api_client())
    await user_data_stream.start()
    await asyncio.sleep(2)
    
    # ── 2. Data Prepare ────────────────────────────────────────────────────────
    symbol = "TRXUSDT"
    
    # Clean previous test ghost data
    open_trades = await db.get_open_trades()
    for t in open_trades:
        if t["symbol"] == symbol:
            await db._execute("UPDATE trades SET status='CLOSED' WHERE id=?", (t["id"],))
            
    # Fetch exchange info for precision rules
    info = await bc.get_exchange_info()
    rules = bc._exchange_info_cache.get(symbol, {})
    if not rules:
        print(f"Could not find rules for {symbol}!")
        return

    price_res = await bc._get("/fapi/v1/premiumIndex", params={"symbol": symbol}, signed=False)
    mark_price = float(price_res["markPrice"])
    
    # Need $6 notional to safely clear Binance $5 minimum order threshold.
    # At 1x leverage, 6 / mark_price
    qty_prec = rules.get("quantityPrecision", 0)
    qty = round(6.0 / mark_price, qty_prec)
    
    print(f"\n=======================================================")
    print(f"[*] Placing REAL LIVE ORDER on BINANCE: {symbol} LONG")
    print(f"[*] Qty: {qty} ({symbol}) at Market Price ~{mark_price}")
    print(f"=======================================================")
    
    # ── 3. Live Entry! ────────────────────────────────────────────────────────
    success, msg = await order_manager._live_open(
        symbol=symbol,
        direction="LONG",
        entry=mark_price,
        sl=mark_price * 0.90, # 10% drop, very safe
        tp1=mark_price * 1.10,
        tp2=mark_price * 1.20,
        qty=qty,
        leverage=2,
        now_ms=int(time.time() * 1000),
        score=100,
        signal_type="TEST"
    )
    
    if not success:
        print(f"❌ Failed to open live test order! Error: {msg}")
        return
        
    print("\n✅ ORDER EXECUTED ON BINANCE!")
    
    # Fetch trade from DB to verify accurate Fill Price detection
    open_trades = await db.get_open_trades()
    trade = next((t for t in open_trades if t["symbol"] == symbol), None)
    
    if not trade:
        print("❌ DB Insert Failed!")
        return
        
    print(f"🎯 YOUR DB ENTRY PRICE IS (Exact Binance Avg_Fill): {trade['entry_price']}")
    
    print(f"\n=======================================================")
    print(f"⏳ WAITING FOR YOU TO CLOSE THE TRADE ON BINANCE UI...")
    print(f"👉 INSTRUCTION: Go to Binance -> Close the LONG position for {symbol}.")
    print(f"=======================================================\n")
    
    # ── 4. Await streaming closure ──────────────────────────────────────────
    trade_id = trade["id"]
    while True:
        await asyncio.sleep(1)
        trades = await db.get_open_trades()
        if not any(t["id"] == trade_id for t in trades):
            print(f"\n🎉 🎯 SUCCESS! Pullback caught the Manual Closure!")
            async with db._get_connection() as conn:
                async with conn.execute("SELECT close_price, close_reason, pnl_usdt, pnl_pct FROM trades WHERE id=?", (trade_id,)) as cursor:
                    row = await cursor.fetchone()
                    print(f"📈 CLOSE PRICE:  {row[0]}")
                    print(f"🔖 REASON:       {row[1]}")
                    print(f"💰 PNL USDT:      {row[2]}")
                    print(f"📊 PNL PCT:       {row[3]}%")
            break

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting.")
