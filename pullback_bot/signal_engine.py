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

    # EMA200 slope — proxy for higher-timeframe alignment.
    # If EMA200 has been declining over the last 20 bars (≈5 hours on 15m),
    # the medium-term structure opposes a LONG; rising EMA200 opposes a SHORT.
    # A 15m EMA cross during a counter-trend bounce would otherwise pass the
    # trend check above while the broader structure remains against the trade.
    ema200_slope_up = ema200.iloc[-1] > ema200.iloc[-21]
    if direction == "LONG" and not ema200_slope_up:
        return None
    if direction == "SHORT" and ema200_slope_up:
        return None

    # ── 2. Pullback zone (15m) ────────────────────────────────────────────────
    atr_series = _atr(df15, 14)
    atr15      = atr_series.iloc[-1]
    # ATR regime: how elevated is current volatility vs recent 20-bar average?
    atr_avg20  = float(atr_series.iloc[-21:-1].mean()) if len(atr_series) > 21 else atr15
    atr_ratio  = atr15 / atr_avg20 if atr_avg20 > 0 else 1.0
    ema50_zone_pct = abs(last_close - last_ema50) / last_ema50

    in_ema50_zone = ema50_zone_pct <= 0.005  # within 0.5% of EMA50

    # Swing high/low zones — exclude the current candle so the zone is always
    # a *prior* level.  Including it would make in_swing_zone trivially true
    # whenever price makes a new 21-bar low (LONG) or high (SHORT), which is
    # a breakdown/breakout, not a pullback to support/resistance.
    recent = df15.iloc[-22:-1]
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
    if avg_vol > 0:
        if last_vol > avg_vol * 1.5:
            score += 15
            reasons.append("volume_spike")
        elif len(klines_5m) > 1:
            recent_5m_vol = max(float(klines_5m[-1]["volume"]), float(klines_5m[-2]["volume"]))
            if recent_5m_vol > (avg_vol / 3) * 1.5:
                score += 15
                reasons.append("volume_spike")

    # ── Score gate ────────────────────────────────────────────────────────────
    if score < config.SIGNAL_SCORE_THRESHOLD:
        return None

    # ── Compute entry / SL / Trail Arm ───────────────────────────────────────
    entry_price = last_close

    # SL: 1.5×ATR from entry — sits outside normal 15m noise.
    # Trail arm: 1×ATR from entry — activates trailing once the trade confirms.
    if direction == "LONG":
        sl_price   = round(entry_price - atr15 * 1.5, 8)
        trail_arm  = round(entry_price + atr15 * 1.0, 8)
    else:
        sl_price   = round(entry_price + atr15 * 1.5, 8)
        trail_arm  = round(entry_price - atr15 * 1.0, 8)

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
        "Signal: %s %s score=%d sl=%.6f arm=%.6f atr=%.6f reasons=%s",
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
