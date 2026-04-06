"""
signal_engine.py — Signal strategies with 0-100 scoring.

Strategies:
  - check_pullback : trend-following pullback to EMA50/swing zone
  - check_breakout : price breaks above resistance (LONG) or below support (SHORT)

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

    # Trail arm: price at which trailing activates.
    # Distance = risk × TRAIL_ARM_RR (configurable; default 1.5 → 1.5:1 RR).
    arm_rr = max(0.5, config.TRAIL_ARM_RR)
    if direction == "LONG":
        trail_arm = round(entry_price + risk * arm_rr, 8)
    else:
        trail_arm = round(entry_price - risk * arm_rr, 8)

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


# ── Breakout / Breakdown ───────────────────────────────────────────────────────

def check_breakout(
    symbol: str,
    klines_15m: list[dict],
    klines_5m: list[dict],
) -> Optional[dict]:
    """
    Breakout / Breakdown detector.

    Looks for a 15m candle that closes outside the prior 20-candle range
    (above resistance for LONG, below support for SHORT) with volume and
    candle-strength confirmation.

    Scoring (max 100):
      +40  price breaks out of 20-candle consolidation range
      +25  volume surge (last candle > 1.5 × 20-bar average)
      +20  strong candle body (close within top/bottom 40 % of range)
      +15  EMA200 alignment (above for LONG, below for SHORT)

    SL placement:
      LONG  : max(resistance - 0.3 × ATR,  entry - 1.5 × ATR)
      SHORT : min(support   + 0.3 × ATR,   entry + 1.5 × ATR)
      → SL sits just beyond the broken level, capped at 1.5 ATR.
    """
    if len(klines_15m) < 50:
        return None

    df15 = pd.DataFrame(klines_15m).astype(float)

    last        = df15.iloc[-1]
    last_close  = float(last["close"])
    last_high   = float(last["high"])
    last_low    = float(last["low"])
    last_vol    = float(last["volume"])

    # 20 confirmed candles before the breakout candle
    lookback = df15.iloc[-21:-1]
    resistance = float(lookback["high"].max())   # breakout level  (LONG)
    support    = float(lookback["low"].min())    # breakdown level (SHORT)

    atr15   = float(_atr(df15, 14).iloc[-1])
    avg_vol = float(df15["volume"].iloc[-21:-1].mean())

    ema200_val = float(_ema(df15["close"], 200).iloc[-1]) if len(df15) >= 200 else None

    score: int = 0
    reasons: list[str] = []
    direction: Optional[str] = None

    # ── Determine breakout direction ──────────────────────────────────────────
    if last_close > resistance:
        direction = "LONG"
        score += 40
        reasons.append("breakout")
    elif last_close < support:
        direction = "SHORT"
        score += 40
        reasons.append("breakdown")
    else:
        return None   # no breakout on this candle

    # ── Volume surge ──────────────────────────────────────────────────────────
    if avg_vol > 0 and last_vol > avg_vol * 1.5:
        score += 25
        reasons.append("volume_surge")

    # ── Candle-body strength ──────────────────────────────────────────────────
    candle_range = last_high - last_low
    if candle_range > 0:
        if direction == "LONG":
            close_position = (last_close - last_low) / candle_range
            if close_position >= 0.6:        # closes in upper 40 % of candle
                score += 20
                reasons.append("strong_close")
        else:
            close_position = (last_high - last_close) / candle_range
            if close_position >= 0.6:        # closes in lower 40 % of candle
                score += 20
                reasons.append("strong_close")

    # ── EMA200 alignment ──────────────────────────────────────────────────────
    if ema200_val is not None:
        if direction == "LONG" and last_close > ema200_val:
            score += 15
            reasons.append("above_ema200")
        elif direction == "SHORT" and last_close < ema200_val:
            score += 15
            reasons.append("below_ema200")

    # ── Score gate ────────────────────────────────────────────────────────────
    if score < config.SIGNAL_SCORE_THRESHOLD:
        return None

    # ── Entry / SL / TP ───────────────────────────────────────────────────────
    entry_price = last_close

    if direction == "LONG":
        # SL: just below the broken resistance, capped at 1.5 × ATR from entry
        sl_price = round(max(resistance - atr15 * 0.3,
                             entry_price - atr15 * 1.5), 8)
    else:
        # SL: just above the broken support, capped at 1.5 × ATR from entry
        sl_price = round(min(support + atr15 * 0.3,
                             entry_price + atr15 * 1.5), 8)

    risk = abs(entry_price - sl_price)
    if risk <= 0:
        return None

    arm_rr = max(0.5, config.TRAIL_ARM_RR)
    trail_arm = round(
        entry_price + risk * arm_rr if direction == "LONG" else entry_price - risk * arm_rr, 8
    )

    signal: dict = {
        "symbol":       symbol,
        "direction":    direction,
        "score":        score,
        "entry_price":  round(entry_price, 8),
        "sl_price":     sl_price,
        "tp1_price":    trail_arm,
        "tp2_price":    trail_arm,
        "timeframe":    "15m",
        "timestamp":    int(time.time()),
        "reasons":      reasons,
        "signal_type":  "BREAKOUT",
    }
    logger.info(
        "Breakout signal: %s %s score=%d reasons=%s",
        symbol, direction, score, reasons,
    )
    return signal
