import os
import time
import logging
from typing import Optional

import requests
import pandas as pd
import numpy as np
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ml_trainer")

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "models")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_historical_klines(symbol: str, interval: str = "15m", limit: int = 1500) -> pd.DataFrame:
    """Download klines from Binance Futures API."""
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
    logger.info(f"Downloading {limit} candles for {symbol} ({interval})...")
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    
    df = pd.DataFrame(data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    
    return df

def _ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(span=period, adjust=False).mean()
    avg_loss = loss.ewm(span=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def _stoch_rsi(series: pd.Series, rsi_period=14, stoch_period=14, smooth_k=3, smooth_d=3):
    rsi = _rsi(series, rsi_period)
    min_rsi = rsi.rolling(stoch_period).min()
    max_rsi = rsi.rolling(stoch_period).max()
    denom = (max_rsi - min_rsi).replace(0, np.nan)
    k = (100 * (rsi - min_rsi) / denom).rolling(smooth_k).mean()
    d = k.rolling(smooth_d).mean()
    return k, d

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Appends identical features to those used in signal_engine."""
    df = df.copy()
    close = df["close"]
    
    df["ema_50"] = _ema(close, 50)
    df["ema_200"] = _ema(close, 200)
    
    df["dist_50"] = (close - df["ema_50"]) / df["ema_50"]
    df["dist_200"] = (close - df["ema_200"]) / df["ema_200"]
    
    df["stoch_k"], df["stoch_d"] = _stoch_rsi(close)
    
    fast_ema = _ema(close, 12)
    slow_ema = _ema(close, 26)
    macd_line = fast_ema - slow_ema
    signal_line = _ema(macd_line, 9)
    df["macd_hist"] = macd_line - signal_line
    
    df["roc_3"] = close.pct_change(3)
    df["vol_ratio"] = df["volume"] / df["volume"].rolling(20).mean()
    
    df.dropna(inplace=True)
    return df

def build_targets(df: pd.DataFrame, target_pct: float = 0.015, stop_pct: float = 0.01) -> pd.DataFrame:
    """
    Creates directional target variable: 
      1 if LONG succeeds (hits target_pct before stop_pct)
     -1 if SHORT succeeds (hits -target_pct before +stop_pct)
      0 if price hits SL for both sides or chops (fails)
    Looks up to 8 candles ahead.
    """
    df = df.copy()
    target_hit = []
    
    closes = df["close"].values
    highs = df["high"].values
    lows = df["low"].values
    
    for i in range(len(df) - 8):
        c = closes[i]
        
        # LONG constraints
        l_target = c * (1 + target_pct)
        l_stop = c * (1 - stop_pct)
        
        # SHORT constraints
        s_target = c * (1 - target_pct)
        s_stop = c * (1 + stop_pct)
        
        l_success = False
        s_success = False
        
        for j in range(1, 9):
            h = highs[i+j]
            l = lows[i+j]
            
            # Evaluate LONG: If it hits stop-loss first, it's dead. Else if it hits target, it succeeds.
            if l <= l_stop:
                break
            if h >= l_target:
                l_success = True
                break
                
        for j in range(1, 9):
            h = highs[i+j]
            l = lows[i+j]
            
            # Evaluate SHORT: If it hits stop-loss (high) first, it's dead. Else if it hits target (low), it succeeds.
            if h >= s_stop:
                break
            if l <= s_target:
                s_success = True
                break
                
        if l_success and not s_success:
            target_hit.append(1)
        elif s_success and not l_success:
            target_hit.append(-1)
        else:
            # Either both succeeded (massive volatility chop, unsafe) or neither did
            target_hit.append(0)
        
    for _ in range(8):
        target_hit.append(np.nan)
        
    df["target"] = target_hit
    df.dropna(inplace=True)
    return df

def train_for_symbol(symbol: str):
    df = fetch_historical_klines(symbol, limit=1500)
    df = build_features(df)
    df = build_targets(df)
    
    features = ["dist_50", "dist_200", "stoch_k", "stoch_d", "macd_hist", "roc_3", "vol_ratio"]
    X = df[features]
    y = df["target"]
    
    if len(X) < 100:
        logger.warning(f"Not enough clean data for {symbol}.")
        return

    # Train-test split by time (80/20)
    split_idx = int(len(df) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
    
    # class_weight="balanced" prevents the model from heavily defaulting to 0 (Chop / Failure)
    model = RandomForestClassifier(n_estimators=200, max_depth=5, class_weight="balanced", random_state=42)
    model.fit(X_train, y_train)
    
    train_acc = model.score(X_train, y_train)
    test_acc = model.score(X_test, y_test)
    
    logger.info(f"[{symbol}] Train Acc: {train_acc:.2f} | Test Acc: {test_acc:.2f}")
    
    model_path = os.path.join(OUTPUT_DIR, f"{symbol}_model.pkl")
    joblib.dump(model, model_path)
    logger.info(f"Saved {model_path}")

def get_active_symbols() -> list[str]:
    """Fetch all USDT-M perpetual symbols passing the config's 24h volume and price change threshold."""
    logger.info("Fetching eligible symbols from Binance Futures...")
    # Use Futures endpoint to match the bot's scanner exactly
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    
    min_vol = getattr(config, "MIN_VOLUME_24H", 20000000)
    min_chg = getattr(config, "MIN_PRICE_CHANGE_PCT", 0.5)
    
    usdt_pairs = []
    for d in data:
        if d["symbol"].endswith("USDT"):
            vol = float(d.get("quoteVolume", 0))
            chg = abs(float(d.get("priceChangePercent", 0)))
            if vol >= min_vol and chg >= min_chg:
                usdt_pairs.append((d["symbol"], vol))
            
    # Sort descending by volume
    usdt_pairs.sort(key=lambda x: x[1], reverse=True)
    return [p[0] for p in usdt_pairs]

if __name__ == "__main__":
    logger.info("Starting ML Training Pipeline...")
    # Fetch all symbols meeting scanner requirements
    symbols = get_active_symbols()
    logger.info(f"Training on {len(symbols)} eligible symbols...")
    
    for s in symbols:
         try:
             train_for_symbol(s)
         except Exception as e:
             logger.error(f"Failed to train {s}: {e}")
         time.sleep(1)
    
    logger.info("Training complete.")
