import asyncio
import os
import config
from order_manager import OrderManager
import binance_client as bc

async def test():
    config.MODE = "live"
    config.CAPITAL = 1000.0
    config.RISK_PCT = 2.0
    config.HEDGE_MODE_ENABLED = False
    
    # Start WS
    ws_task = asyncio.create_task(bc.start_ws_api_client())
    await asyncio.sleep(2)
    
    om = OrderManager()
    res, msg = await om._live_open(
        symbol="ADAUSDT",
        direction="LONG",
        entry=1.2,
        sl=1.1,
        tp1=1.3,
        tp2=1.4,
        qty=10,
        leverage=2,
        now_ms=123456789,
        score=99,
        atr=0.1
    )
    print("Result:", res, msg)
    ws_task.cancel()

asyncio.run(test())
