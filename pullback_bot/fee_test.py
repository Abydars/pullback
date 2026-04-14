import asyncio
import os
import json
import logging

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

async def main():
    await db.init_db()
    
    # Get the last closed trade (the one we just ran earlier)!
    trades = await db.get_closed_trades(limit=1)
    if not trades:
        print("No closed trades in DB.")
        return
        
    last_trade = trades[0]
    symbol = last_trade["symbol"]
    order_id = last_trade.get("binance_order_id")
    
    if not order_id:
        print(f"Last trade for {symbol} has no Binance Order ID.")
        return
        
    print(f"[*] Fetching ENTRY trade history from Binance for Order ID: {order_id} ({symbol})")
    
    # Binance REST API for retrieving account trade history
    trades_resp = await bc._get("/fapi/v1/userTrades", params={"symbol": symbol, "orderId": order_id}, signed=True)
    
    total_commission = 0.0
    commission_asset = ""
    
    for t in trades_resp:
        comm = float(t.get("commission", 0))
        asset = t.get("commissionAsset", "")
        # Realized profit for the trade portion
        realized_pnl = float(t.get("realizedPnl", 0))
        
        total_commission += comm
        commission_asset = asset
        
        print(f"  - Fill ID: {t['id']}")
        print(f"    Qty: {t['qty']} @ Price: {t['price']}")
        print(f"    Commission: {comm} {asset}")
        print(f"    Realized PnL: {realized_pnl}")
        
    print(f"\n==========================================")
    print(f"✅ Total Binance Fee extracted for Entry: {total_commission} {commission_asset}")
    print(f"==========================================\n")
    
    # Can also fetch the LAST FEW trades entirely for the symbol to find the CLOSING fee
    print(f"[*] Fetching LAST 2 execution receipts for {symbol} to see closing fees...")
    recent_all = await bc._get("/fapi/v1/userTrades", params={"symbol": symbol, "limit": 2}, signed=True)
    
    close_commission = 0.0
    close_asset = ""
    for t in recent_all:
        if str(t.get("orderId")) != str(order_id): # If it's the other side
            comm = float(t.get("commission", 0))
            asset = t.get("commissionAsset", "")
            close_commission += comm
            close_asset = asset
            print(f"  - (Close) Fill ID: {t['id']} | Qty: {t['qty']} | Commission: {comm} {asset}")
            
    print(f"✅ Total Binance Fee extracted for Close: {close_commission} {close_asset}")

if __name__ == "__main__":
    asyncio.run(main())
