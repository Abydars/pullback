import binance_client
import asyncio
import pandas as pd
async def run():
    k = await binance_client.get_klines("BTCUSDT", "4h", 100)
    df = pd.DataFrame(k)
    print(df.columns)
asyncio.run(run())
