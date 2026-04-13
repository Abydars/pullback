import signal_engine
import binance_client
import asyncio

async def test():
    k15 = await binance_client.get_klines("BTCUSDT", "15m", 100)
    k5 = await binance_client.get_klines("BTCUSDT", "5m", 100)
    k4 = await binance_client.get_klines("BTCUSDT", "4h", 100)
    try:
        print(signal_engine.check_breakout("BTCUSDT", k15, k5, k4, []))
        print("SUCCESS")
    except Exception as e:
        import traceback
        traceback.print_exc()

asyncio.run(test())
