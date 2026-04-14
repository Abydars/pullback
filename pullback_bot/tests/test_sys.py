import signal_engine
import pandas as pd

# Mock klines
kl = [{"open": 100, "high": 105, "low": 95, "close": 102, "volume": 1000, "open_time": 12345, "time": 12345} for _ in range(60)]

try:
    signal_engine.check_breakout("BTCUSDT", kl, kl, kl, [])
    print("SUCCESS")
except Exception as e:
    import traceback
    traceback.print_exc()
