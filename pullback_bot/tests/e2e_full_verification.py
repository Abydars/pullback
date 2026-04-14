import asyncio
import os
import time
import logging

os.environ["BINANCE_API_KEY"] = "b8jOlmhpO3olvEa9WC4HncwTyjLBNT8N6QSG7jRTRxbBmuCzHHY6qEoOwGmBm7Ia"
os.environ["BINANCE_PRIVATE_KEY"] = """-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIHvaHXdrLWwfh6mr1CtVASCkgxy1P4H4mBNvtUNcFs/X
-----END PRIVATE KEY-----"""
os.environ["BINANCE_API_SECRET"] = ""
os.environ["MODE"] = "live"

logging.basicConfig(level=logging.ERROR)
logging.getLogger("order_manager").setLevel(logging.INFO)
logging.getLogger("user_data_stream").setLevel(logging.INFO)

import config
import db
import binance_client as bc
from order_manager import order_manager
from user_data_stream import user_data_stream

async def main():
    await db.init_db()
    # Mock config flags for the test
    getattr(config, "HEDGE_MODE_ENABLED", True)
    config.USE_STOP_LOSS = True
    
    # 1. Start Stream API
    print("===================================[ BOOT ]===================================")
    print("[*] Connecting WS User Data Stream & Binance Client...")
    asyncio.create_task(bc.start_ws_api_client())
    await user_data_stream.start()
    await asyncio.sleep(2)
    
    # 2. Setup Symbol
    symbol = "TRXUSDT"
    await bc.cancel_all_orders(symbol)
    
    # Purge old local test trades
    open_trades = await db.get_open_trades()
    for t in open_trades:
        if t["symbol"] == symbol:
            await db._execute("UPDATE trades SET status='CLOSED' WHERE id=?", (t["id"],))
            
    info = await bc.get_exchange_info()
    rules = bc._exchange_info_cache.get(symbol, {})
    price_res = await bc._get("/fapi/v1/premiumIndex", params={"symbol": symbol}, signed=False)
    mark_price = float(price_res["markPrice"])
    
    qty_prec = rules.get("quantityPrecision", 0)
    qty = round(6.0 / mark_price, qty_prec) # ~$6 notional

    print("===================================[ ENTRY ]===================================")
    print(f"[*] Firing `order_manager._live_open` for {qty} {symbol} LONG @ ~{mark_price}")
    # 3. Use Class Method for Entry
    success, msg = await order_manager._live_open(
        symbol=symbol,
        direction="LONG",
        entry=mark_price,
        sl=mark_price * 0.90, # 10% dump (catastrophic safety)
        tp1=mark_price * 1.10,
        tp2=mark_price * 1.20,
        qty=qty,
        leverage=2,
        now_ms=int(time.time() * 1000),
        score=100,
        signal_type="E2E_TEST"
    )
    
    if not success:
        print(f"❌ Entry Failed: {msg}")
        return
        
    print(f"✅ Class method returned successfully. Waiting 3 seconds for entry_fee background hook to settle...")
    await asyncio.sleep(3.5)
    
    open_trades = await db.get_open_trades()
    active_trade = next((t for t in open_trades if t["symbol"] == symbol), None)
    
    if not active_trade:
        print("❌ Could not locate trade in Database!")
        return

    print("===================================[ VERIFICATION ]===================================")
    print(f"🎯 DB Entry Price: {active_trade['entry_price']}")
    print(f"💸 DB native Entry Fee extracted: {active_trade.get('entry_fee', 0.0)} USDT")
    
    # 4. Verify API for Stop Loss
    oorders = await bc._get("/fapi/v1/openOrders", params={"symbol": symbol}, signed=True)
    sl_order = next((o for o in oorders if o["type"] == "STOP_MARKET"), None)
    if sl_order:
        print(f"🛡️ Verified Binance API: Catastrophic Stop Loss active at Price {sl_order['stopPrice']} (Order ID: {sl_order['orderId']})")
    else:
        print(f"⚠️ Could not find Catastrophic SL on Binance API!")

    print("===================================[ CLOSURE ]===================================")
    print("[*] Placing an opposing MARKET order manually to trigger closure...")
    
    # Place opposite trade to close (Hedge Mode tracking natively requires PositionSide)
    await bc.place_market_order(symbol=symbol, side="SELL", qty=qty, position_side="LONG")
    
    print("[*] Simulated manual close submitted! Waiting 3.5 seconds for Data Stream listener to calculate everything...")
    await asyncio.sleep(3.5) # Allow WS stream to catch up
    
    # Cleanup trailing Open Orders from the closing process
    await bc.cancel_all_orders(symbol)
    
    print("===================================[ FINAL METRICS ]===================================")
    # Fetch final database record
    active_trade_id = active_trade["id"]
    import aiosqlite
    async with aiosqlite.connect(db.DB_PATH) as conn:
        async with conn.execute("SELECT close_price, entry_fee, close_fee, pnl_usdt, qty, leverage, entry_price FROM trades WHERE id=?", (active_trade_id,)) as cursor:
            row = await cursor.fetchone()
            print(f"===================================[ INTEGRATION DATA ]===================================")
            live_wallet = await bc.get_balance()
            print(f"🏦 Live Wallet Capital: {live_wallet} USDT")
            print(f"🛒 Native DB Qty:       {row[4]} (Should match Binance Executed Qty)")
            print(f"⚙️  Native DB Leverage:  {row[5]}x (Should match Binance returned Leverge)")
            print(f"📏 Native DB Size:      {round(row[4] * row[6], 4)} Notional USDT")
            
            print("===================================[ FINAL METRICS ]===================================")
            print(f"📈 Exit Fill Price:  {row[0]}")
            print(f"💵 Entry Fee Deducted: {row[1]} USDT")
            print(f"💴 Close Fee Deducted: {row[2]} USDT")
            print(f"💰 EXACT NET PNL:      {row[3]} USDT")
            print("=======================================================================================")
            print("👉 MATCH THESE NUMBERS WITH YOUR BINANCE TRADE HISTORY!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting.")
