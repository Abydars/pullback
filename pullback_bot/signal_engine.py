"""
signal_engine.py — Pullback detection logic with 0-100 scoring.

Indicators implemented manually (no pandas-ta dependency):
  - EMA (exponential moving average)
  - ATR (average true range)
  - StochRSI (K/D lines)
  - MACD histogram

All candle data is passed in as a list of dicts with keys:
  open, high, low, close, volume  (float values)
"""
from __future__ import annotations

import logging
import time
from typing import Optional

import numpy as np
import pandas as pd

import config

logger = logging.getLogger(__name__)

# ── Indicator helpers ──────────────────────────────────────────────────────────

def _ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def _atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high = df["high"]
    low = df["low"]
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1
    ).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()


def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(span=period, adjust=False).mean()
    avg_loss = loss.ewm(span=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _stoch_rsi(
    series: pd.Series,
    rsi_period: int = 14,
    stoch_period: int = 14,
    smooth_k: int = 3,
    smooth_d: int = 3,
) -> tuple[pd.Series, pd.Series]:
    """Return (K, D) lines scaled 0-100."""
    rsi = _rsi(series, rsi_period)
    min_rsi = rsi.rolling(stoch_period).min()
    max_rsi = rsi.rolling(stoch_period).max()
    denom = (max_rsi - min_rsi).replace(0, np.nan)
    raw_k = 100 * (rsi - min_rsi) / denom
    k = raw_k.rolling(smooth_k).mean()
    d = k.rolling(smooth_d).mean()
    return k, d


def _macd(
    series: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Return (macd_line, signal_line, histogram)."""
    fast_ema = _ema(series, fast)
    slow_ema = _ema(series, slow)
    macd_line = fast_ema - slow_ema
    signal_line = _ema(macd_line, signal)
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def _swing_highs(series: pd.Series, window: int = 5) -> pd.Series:
    """Boolean series: True where value is local max over window."""
    return (series == series.rolling(window, center=True).max()) & (
        series.shift(1) < series
    )


def _swing_lows(series: pd.Series, window: int = 5) -> pd.Series:
    return (series == series.rolling(window, center=True).min()) & (
        series.shift(1) > series
    )


# ── Signal Engine ──────────────────────────────────────────────────────────────

def check_pullback(
    symbol: str,
    klines_15m: list[dict],
    klines_5m: list[dict],
) -> Optional[dict]:
    """
    Run the full pullback scoring model.

    Parameters
    ----------
    symbol       : e.g. "BTCUSDT"
    klines_15m   : list of candle dicts (at least 210 candles)
    klines_5m    : list of candle dicts (at least 50 candles)

    Returns None if no valid signal, else a signal dict.
    """
    if len(klines_15m) < 210 or len(klines_5m) < 50:
        return None

    df15 = pd.DataFrame(klines_15m).astype(float)
    df5 = pd.DataFrame(klines_5m).astype(float)

    score = 0
    reasons: list[str] = []

    # ── 1. Trend direction (15m) ──────────────────────────────────────────────
    close15 = df15["close"]
    ema50 = _ema(close15, 50)
    ema200 = _ema(close15, 200)

    last_close = close15.iloc[-1]
    last_ema50 = ema50.iloc[-1]
    last_ema200 = ema200.iloc[-1]

    if last_close > last_ema50 and last_ema50 > last_ema200:
        direction = "LONG"
        score += 25
        reasons.append("trend:LONG")
    elif last_close < last_ema50 and last_ema50 < last_ema200:
        direction = "SHORT"
        score += 25
        reasons.append("trend:SHORT")
    else:
        # Ranging — skip
        return None

    # ── 2. Pullback zone (15m) ────────────────────────────────────────────────
    atr15 = _atr(df15, 14).iloc[-1]
    ema50_zone_pct = abs(last_close - last_ema50) / last_ema50

    in_ema50_zone = ema50_zone_pct <= 0.005  # within 0.5% of EMA50

    # Swing high/low zones (last 20 candles)
    recent = df15.tail(21)
    if direction == "LONG":
        swing_level = recent["low"].min()
        in_swing_zone = abs(last_close - swing_level) <= atr15 * 1.5
    else:
        swing_level = recent["high"].max()
        in_swing_zone = abs(last_close - swing_level) <= atr15 * 1.5

    if in_ema50_zone or in_swing_zone:
        score += 25
        reasons.append("pullback_zone")

    # ── 3. Momentum reversal (5m) ─────────────────────────────────────────────
    close5 = df5["close"]
    k, d = _stoch_rsi(close5)
    _, _, macd_hist = _macd(close5)

    # Use last 2 values for crossover detection
    k_prev, k_last = k.iloc[-2], k.iloc[-1]
    d_prev, d_last = d.iloc[-2], d.iloc[-1]
    hist_prev = macd_hist.iloc[-2]
    hist_last = macd_hist.iloc[-1]

    stoch_signal = False
    macd_signal = False

    if direction == "LONG":
        # K crosses above D from oversold
        if k_prev < d_prev and k_last > d_last and k_prev < 20:
            stoch_signal = True
        # MACD hist turning positive
        if hist_prev < 0 and hist_last > hist_prev:
            macd_signal = True
    else:
        # K crosses below D from overbought
        if k_prev > d_prev and k_last < d_last and k_prev > 80:
            stoch_signal = True
        # MACD hist turning negative
        if hist_prev > 0 and hist_last < hist_prev:
            macd_signal = True

    if stoch_signal:
        score += 20
        reasons.append("stochrsi")
    if macd_signal:
        score += 15
        reasons.append("macd")

    # ── 4. Volume spike ───────────────────────────────────────────────────────
    vol15 = df15["volume"]
    avg_vol = vol15.iloc[-21:-1].mean()
    last_vol = vol15.iloc[-1]
    if avg_vol > 0 and last_vol > avg_vol * 1.5:
        score += 15
        reasons.append("volume_spike")

    # ── Score gate ────────────────────────────────────────────────────────────
    if score < config.SIGNAL_SCORE_THRESHOLD:
        return None

    # ── Compute entry / SL / TP ───────────────────────────────────────────────
    entry_price = last_close

    # Use the TIGHTER of (recent swing level, 1.0×ATR) to keep risk:reward sane.
    # Wide SLs push TP1/TP2 far away, making them unlikely to be reached.
    if direction == "LONG":
        sl_price = max(
            recent["low"].min(),
            entry_price - atr15 * 1.0,
        )
        sl_price = round(sl_price, 8)
    else:
        sl_price = min(
            recent["high"].max(),
            entry_price + atr15 * 1.0,
        )
        sl_price = round(sl_price, 8)

    risk = abs(entry_price - sl_price)
    if risk <= 0:
        return None

    # Trail arm: price level at which trailing take-profit activates (1:1 RR).
    # Once mark crosses this level the position tracker starts trailing;
    # there is no fixed TP2 — tp2_price reuses the same value for DB compat.
    if direction == "LONG":
        trail_arm = round(entry_price + risk, 8)
    else:
        trail_arm = round(entry_price - risk, 8)

    signal: dict = {
        "symbol": symbol,
        "direction": direction,
        "score": score,
        "entry_price": round(entry_price, 8),
        "sl_price": sl_price,
        "tp1_price": trail_arm,   # trail arm activation price
        "tp2_price": trail_arm,   # kept for DB schema compat (same value)
        "timeframe": "15m",
        "timestamp": int(time.time()),
        "reasons": reasons,
    }
    logger.info(
        "Signal: %s %s score=%d reasons=%s",
        symbol, direction, score, reasons,
    )
    return signal
