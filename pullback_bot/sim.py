import asyncio
import binance_client
import signal_engine

async def main():
    sym = "BTCUSDT"
    k15 = await binance_client.get_klines(sym, "15m", 100)
    k5 = await binance_client.get_klines(sym, "5m", 100)
    k4 = await binance_client.get_klines(sym, "4h", 100)

    def to_dict(data):
        return [{"open_time": int(k[0]), "open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4]), "volume": float(k[5])} for k in data]
    
    k15d = to_dict(k15)
    k5d = to_dict(k5)
    k4d = to_dict(k4)
    
    # Actually, check_breakout currently traps exceptions and returns None!
    # Let's call _check_breakout_impl directly!
    try:
        signal_engine._check_breakout_impl(sym, k15d, k5d, k4d, [])
        print("Success")
    except Exception as e:
        import traceback
        traceback.print_exc()

asyncio.run(main())
