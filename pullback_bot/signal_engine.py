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


def _adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculate Average Directional Index (ADX) using Wilder's smoothing."""
    high = df['high']
    low = df['low']
    close = df['close']
    
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    
    up_move = high - high.shift(1)
    down_move = low.shift(1) - low
    
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    
    plus_dm = pd.Series(plus_dm, index=df.index)
    minus_dm = pd.Series(minus_dm, index=df.index)
    
    # Wilder's smoothing equates roughly to EMA with alpha=1/period
    tr_smooth = tr.ewm(alpha=1/period, adjust=False).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1/period, adjust=False).mean() / tr_smooth)
    minus_di = 100 * (minus_dm.ewm(alpha=1/period, adjust=False).mean() / tr_smooth)
    
    dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di)).fillna(0)
    return dx.ewm(alpha=1/period, adjust=False).mean()


# ── BTC Regime ─────────────────────────────────────────────────────────────────

def get_btc_regime(klines_15m: list[dict]) -> str:
    """
    Classify the current BTC market regime using a 3-candle (45-minute)
    rate-of-change on the 15m timeframe.

    15m single-candle noise on BTC ≈ 0.3–0.5%.  A sustained 3-candle move
    of ≥ 0.8% (BTC_BREAKOUT_ROC default) is a genuine breakout, not noise.

    Returns
    -------
    "BULL_BREAKOUT"  — BTC moved up ≥ threshold in the last 45 min
    "BEAR_BREAKDOWN" — BTC moved down ≥ threshold in the last 45 min
    "NEUTRAL"        — within normal noise, or filter is disabled
    """
    try:
        if not config.BTC_REGIME_FILTER:
            return "NEUTRAL"
        if len(klines_15m) < 5:
            return "NEUTRAL"
        closes = [float(c["close"]) for c in klines_15m]
        btc_roc = (closes[-1] - closes[-4]) / closes[-4]
        threshold = config.BTC_BREAKOUT_ROC
        if btc_roc > threshold:
            return "BULL_BREAKOUT"
        elif btc_roc < -threshold:
            return "BEAR_BREAKDOWN"
        else:
            return "NEUTRAL"
    except Exception:
        return "NEUTRAL"


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

    # ── Parse Candles ──
    close15 = df15["close"]
    high15  = df15["high"]
    low15   = df15["low"]
    open15  = df15["open"]
    
    ema50 = _ema(close15, 50)
    ema200 = _ema(close15, 200)

    last_close = float(close15.iloc[-1])
    last_high  = float(high15.iloc[-1])
    last_low   = float(low15.iloc[-1])
    last_open  = float(open15.iloc[-1])

    prev_close = float(close15.iloc[-2])
    prev_high  = float(high15.iloc[-2])
    prev_low   = float(low15.iloc[-2])
    prev_open  = float(open15.iloc[-2])

    last_ema50  = float(ema50.iloc[-1])
    last_ema200 = float(ema200.iloc[-1])

    # Indicators
    adx15      = float(_adx(df15, 14).iloc[-1])
    atr_series = _atr(df15, 14)
    atr15      = float(atr_series.iloc[-1])

    # ── 1. Trend Filter & Strength ────────────────────────────────────────────
    if last_close > last_ema50 and last_ema50 > last_ema200:
        direction = "LONG"
    elif last_close < last_ema50 and last_ema50 < last_ema200:
        direction = "SHORT"
    else:
        return None

    ema200_slope_up = float(ema200.iloc[-1]) > float(ema200.iloc[-21])
    if direction == "LONG" and not ema200_slope_up:
        return None
    if direction == "SHORT" and ema200_slope_up:
        return None

    score += 40
    reasons.append(f"trend:{direction}")

    if adx15 > 22.0:
        score += 20
        reasons.append("adx_momentum")
    else:
        # Weak chopped trend; very risky to trade pullback.
        return None

    # ── 2. Dynamic Volatility Zone Mapping ────────────────────────────────────
    ema50_upper_band = last_ema50 + (atr15 * 1.0)
    ema50_lower_band = last_ema50 - (atr15 * 1.0)

    touched_zone = False
    if direction == "LONG":
        if last_low <= ema50_upper_band and last_close >= ema50_lower_band:
            touched_zone = True
    else:
        if last_high >= ema50_lower_band and last_close <= ema50_upper_band:
            touched_zone = True

    if not touched_zone:
        return None

    score += 20
    reasons.append("dynamic_value_zone")

    # ── 3. Strict Candlestick Rejection Filter ────────────────────────────────
    candle_range = last_high - last_low
    if candle_range <= 0:
        return None

    pa_valid = False

    if direction == "LONG":
        # Check Pinbar (Lower wick must be > 40% of the entire candle structure)
        lower_wick = min(last_open, last_close) - last_low
        if lower_wick / candle_range >= 0.40:
            pa_valid = True
            reasons.append("pinbar_rejection")
            score += 20
        # Check Bullish Engulfing (Current Green body swallows previous Red body)
        elif last_close > last_open and prev_close < prev_open and last_close >= prev_open and last_open <= prev_close:
            pa_valid = True
            reasons.append("bullish_engulfing")
            score += 20
    else:
        # Check Inverse Pinbar (Upper wick must be > 40% of the entire candle structure)
        upper_wick = last_high - max(last_open, last_close)
        if upper_wick / candle_range >= 0.40:
            pa_valid = True
            reasons.append("inverse_pinbar_rejection")
            score += 20
        # Check Bearish Engulfing (Current Red body swallows previous Green body)
        elif last_close < last_open and prev_close > prev_open and last_close <= prev_open and last_open >= prev_close:
            pa_valid = True
            reasons.append("bearish_engulfing")
            score += 20

    if pa_valid is False:
        # Falling Knife caught. Kill trade mathematically.
        return None

    # ── Score gate ────────────────────────────────────────────────────────────
    if score < config.SIGNAL_SCORE_THRESHOLD:
        return None

    # ── 4. Structural Stop Loss Anchoring ─────────────────────────────────────
    entry_price = float(last_close)

    atr_avg20 = float(atr_series.iloc[-21:-1].mean()) if len(atr_series) > 21 else atr15
    atr_ratio = atr15 / atr_avg20 if atr_avg20 > 0 else 1.0

    if direction == "LONG":
        # Anchor explicitly below the structural fractal bottom + 0.2 ATR buffer
        sl_price = round(last_low - (atr15 * 0.2), 8)
        # Structural disaster cap (1.5 ATR Max)
        sl_price = max(sl_price, entry_price - (atr15 * 1.5))
        
        trail_arm = round(entry_price + atr15 * 1.0, 8)
    else:
        # Anchor explicitly above the structural fractal top + 0.2 ATR buffer
        sl_price = round(last_high + (atr15 * 0.2), 8)
        # Structural disaster cap (1.5 ATR Max)
        sl_price = min(sl_price, entry_price + (atr15 * 1.5))
        
        trail_arm = round(entry_price - atr15 * 1.0, 8)

    if sl_price <= 0:
        return None

    signal: dict = {
        "symbol":      symbol,
        "direction":   direction,
        "score":       score,
        "entry_price": round(entry_price, 8),
        "sl_price":    sl_price,
        "tp1_price":   trail_arm,   # trail arm activation price
        "tp2_price":   trail_arm,   # kept for DB schema compat
        "atr":         round(atr15, 8),
        "atr_ratio":   round(atr_ratio, 3),
        "timeframe":   "15m",
        "timestamp":   int(time.time()),
        "reasons":     reasons,
        "signal_type": "PULLBACK",
    }
    logger.info(
        "V2 Pullback Signal: %s %s score=%d sl=%.6f arm=%.6f atr=%.6f reasons=%s",
        symbol, direction, score, sl_price, trail_arm, atr15, reasons,
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

    Scoring (max ~115):
      +50  price breaks out of 20-candle consolidation range
      +25  volume surge (last candle > 1.5 × 20-bar average)
      +20  strong candle body (close within top/bottom 40 % of range)
      +15  squeeze (20-candle range < 3.5 × avg ATR — genuine compression)
      +5   EMA200 alignment (above for LONG, below for SHORT)

    Squeeze rationale: over 20 candles a random walk produces an expected
    high-to-low range of √20 × ATR ≈ 4.5 × ATR.  A range below 3.5 × ATR
    is meaningfully tighter than random noise — price was coiling — making
    the breakout more likely to be sustained.  At the default threshold of
    70, base+squeeze alone scores 65 (does not fire), so the bonus rewards
    already-valid breakouts rather than creating new ones from nothing.

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

    _atr_series = _atr(df15, 14)
    atr15       = float(_atr_series.iloc[-1])
    _atr_avg20  = float(_atr_series.iloc[-21:-1].mean()) if len(_atr_series) > 21 else atr15
    atr_ratio   = atr15 / _atr_avg20 if _atr_avg20 > 0 else 1.0
    avg_vol = float(df15["volume"].iloc[-21:-1].mean())

    ema200_val = float(_ema(df15["close"], 200).iloc[-1]) if len(df15) >= 200 else None
    rsi_val    = float(_rsi(df15["close"], 14).iloc[-1]) if len(df15) >= 14 else 50.0

    score: int = 0
    reasons: list[str] = []
    direction: Optional[str] = None

    # ── Determine breakout direction ──
    if last_close > resistance:
        if rsi_val > 70.0:
            logger.info("Breakout rejected: %s LONG fakeout risk (RSI=%.1f)", symbol, rsi_val)
            return None
        direction = "LONG"
        score += 50
        reasons.append("breakout")
    elif last_close < support:
        if rsi_val < 30.0:
            logger.info("Breakdown rejected: %s SHORT fakeout risk (RSI=%.1f)", symbol, rsi_val)
            return None
        direction = "SHORT"
        score += 50
        reasons.append("breakdown")
    else:
        return None   # no breakout on this candle

    # ── Volume surge mandate ──
    if avg_vol > 0:
        has_volume = False
        if last_vol > avg_vol * 1.5:
            has_volume = True
        elif len(klines_5m) > 1:
            recent_5m_vol = max(float(klines_5m[-1]["volume"]), float(klines_5m[-2]["volume"]))
            if recent_5m_vol > (avg_vol / 3) * 1.5:
                has_volume = True
                
        if has_volume:
            score += 25
            reasons.append("volume_surge")
        else:
            score -= 20
            reasons.append("low_volume_penalty")

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
            score += 5
            reasons.append("above_ema200")
        elif direction == "SHORT" and last_close < ema200_val:
            score += 5
            reasons.append("below_ema200")

    # ── Squeeze bonus ─────────────────────────────────────────────────────────
    # A 20-candle range smaller than 3.5 × avg ATR is below the ~4.5 × ATR
    # expected from random noise, indicating genuine price compression.
    # Breakouts from tight ranges tend to be stronger and more sustained.
    range_size = resistance - support
    if range_size < _atr_avg20 * 3.5:
        score += 15
        reasons.append("squeeze")

    # ── Score gate ────────────────────────────────────────────────────────────
    if score < config.SIGNAL_SCORE_THRESHOLD:
        return None

    # ── Early Distance Cap (Reject overextended late breakouts) ───────────────
    # Reject the trade if price has already traveled more than 1.0 ATR from the breakout line
    distance = (last_close - resistance) if direction == "LONG" else (support - last_close)
    if distance > atr15 * 1.0:
        logger.info("Breakout signal rejected: %s %s is overextended (dist: %.5f, atr: %.5f)", symbol, direction, distance, atr15)
        return None

    # ── Entry / SL / Trail Arm ────────────────────────────────────────────────
    entry_price = last_close

    # Structural SL: just beyond the broken level (resistance for LONG,
    # support for SHORT), capped at 1.5×ATR so risk stays bounded when the
    # breakout candle travelled far from the consolidation range.
    #   LONG  : max(resistance - 0.3×ATR,  entry - 1.5×ATR)  → closer to entry
    #   SHORT : min(support   + 0.3×ATR,   entry + 1.5×ATR)  → closer to entry
    if direction == "LONG":
        sl_price  = round(max(resistance - atr15 * 0.3, entry_price - atr15 * 1.5), 8)
        trail_arm = round(entry_price + atr15 * 1.0, 8)
    else:
        sl_price  = round(min(support + atr15 * 0.3, entry_price + atr15 * 1.5), 8)
        trail_arm = round(entry_price - atr15 * 1.0, 8)

    if sl_price <= 0:
        return None

    signal: dict = {
        "symbol":       symbol,
        "direction":    direction,
        "score":        score,
        "entry_price":  round(entry_price, 8),
        "sl_price":     sl_price,
        "tp1_price":    trail_arm,
        "tp2_price":    trail_arm,
        "atr":          round(atr15, 8),
        "atr_ratio":    round(atr_ratio, 3),
        "timeframe":    "15m",
        "timestamp":    int(time.time()),
        "reasons":      reasons,
        "signal_type":  "BREAKOUT",
    }
    logger.info(
        "Breakout signal: %s %s score=%d sl=%.6f arm=%.6f atr=%.6f reasons=%s",
        symbol, direction, score, sl_price, trail_arm, atr15, reasons,
    )
    return signal
