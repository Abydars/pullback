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

logging.basicConfig(level=logging.INFO)

import config
import db
from user_data_stream import user_data_stream
import binance_client as bc

async def main():
    await db.init_db()
    await config.load_from_db()
    config.MODE = "live"
    
    # 1. Start User Data Stream
    print("[*] Starting Binance WebSocket User Data Stream...")
    await user_data_stream.start()
    await asyncio.sleep(2)
    
    # 2. Setup Fake Open Trade in Database
    print("\n[*] Registering a local ONLY tracking trade in SQLite for TRXUSDT")
    open_trades = await db.get_open_trades()
    for t in open_trades:
        if t["symbol"] == "TRXUSDT":
            print("Found existing TRXUSDT trade. Deleting/Closing to refresh...")
            await db._execute("UPDATE trades SET status='CLOSED' WHERE id=?", (t["id"],))
            
    # Insert Fake LIVE Trade
    entry_price = 0.2000
    trade_qty = 100
    trade_id = await db.insert_trade(
        symbol="TRXUSDT",
        direction="LONG",
        entry_price=entry_price,
        sl_price=0.1500,
        tp1_price=0.2500,
        tp2_price=0.2800,
        qty=trade_qty,
        mode="live",
        entry_time=1700000000,
        signal_score=90,
        leverage=5,
        binance_order_id="fake_system_id",
        signal_type="PULLBACK",
    )
    
    print(f"\n=======================================================")
    print(f"✅ DB Tracking trade created with ID {trade_id}")
    print(f"BOT IS NOW MONITORING FOR TRXUSDT CLOSURES...")
    print(f"=======================================================")
    print(f"👉 INSTRUCTION FOR USER:")
    print(f"1. Open Binance App or Web / Binance Futures.")
    print(f"2. Note: Bot assumes you have a LONG position of TRXUSDT.")
    print(f"3. Place a REAL MARKET SELL (Short) order for TRXUSDT (Quantity: minimum allowed, e.g. 5 USD worth).")
    print(f"4. Once that Market Sell executes, the script below should detect the manual fill and auto-close the trade in DB.")
    print(f"=======================================================\n")
    
    # 3. Wait for database update
    print("[*] Waiting for Binance 'ORDER_TRADE_UPDATE' via stream...")
    
    while True:
        await asyncio.sleep(1)
        trades = await db.get_open_trades()
        if not any(t["id"] == trade_id for t in trades):
            print(f"\n🎉 🎯 SUCCESS! The User Data Stream recognized your Binance manual order execution!")
            # Fetch closed trade info
            async with db._get_connection() as conn:
                async with conn.execute("SELECT close_price, close_reason, pnl_usdt FROM trades WHERE id=?", (trade_id,)) as cursor:
                    row = await cursor.fetchone()
                    print(f"Trade Closed in DB! Fill Price: {row[0]}, Reason: {row[1]}, PNL: {row[2]}")
            break

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting.")
