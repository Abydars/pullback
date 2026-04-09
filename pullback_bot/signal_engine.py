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
    V2 Structural Breakout / Breakdown detector.

    Scans for explosive momentum breaking out of a proven consolidation Box,
    aligned explicitly with macro-trend flow, printing shaved-head block candles.
    """
    if len(klines_15m) < 50:
        return None

    df15 = pd.DataFrame(klines_15m).astype(float)

    last        = df15.iloc[-1]
    last_close  = float(last["close"])
    last_high   = float(last["high"])
    last_low    = float(last["low"])
    last_vol    = float(last["volume"])
    last_open   = float(last["open"])

    # 20 confirmed candles before the breakout candle
    lookback = df15.iloc[-21:-1]
    resistance = float(lookback["high"].max())   # breakout level  (LONG)
    support    = float(lookback["low"].min())    # breakdown level (SHORT)

    _atr_series = _atr(df15, 14)
    atr15       = float(_atr_series.iloc[-1])
    _atr_avg20  = float(_atr_series.iloc[-21:-1].mean()) if len(_atr_series) > 21 else atr15
    atr_ratio   = atr15 / _atr_avg20 if _atr_avg20 > 0 else 1.0
    avg_vol = float(df15["volume"].iloc[-21:-1].mean())

    ema50_val = float(_ema(df15["close"], 50).iloc[-1]) if len(df15) >= 50 else None
    ema200_val = float(_ema(df15["close"], 200).iloc[-1]) if len(df15) >= 200 else None
    rsi_val    = float(_rsi(df15["close"], 14).iloc[-1]) if len(df15) >= 14 else 50.0

    score: int = 0
    reasons: list[str] = []
    direction: Optional[str] = None

    # ── 1. The Box Mandate (Consolidation Filter) ───────────────────────────
    range_size = resistance - support
    if range_size > (_atr_avg20 * 3.5):
        # Range was too wide and noisy; not a coiled spring.
        logger.info("Breakout rejected: %s range too wide (%f vs %f max)", symbol, range_size, _atr_avg20 * 3.5)
        return None
    else:
        score += 30
        reasons.append("structural_consolidation")

    # ── Determine breakout direction ─────────────────────────────────────────
    if last_close > resistance:
        if rsi_val > 70.0:
            logger.info("Breakout rejected: %s LONG fakeout risk (RSI=%.1f)", symbol, rsi_val)
            return None
        direction = "LONG"
        score += 30
        reasons.append("breakout")
    elif last_close < support:
        if rsi_val < 30.0:
            logger.info("Breakdown rejected: %s SHORT fakeout risk (RSI=%.1f)", symbol, rsi_val)
            return None
        direction = "SHORT"
        score += 30
        reasons.append("breakdown")
    else:
        return None   # no breakout on this candle

    # ── 2. Macro-Trend Cohesion ──────────────────────────────────────────────
    if ema50_val and ema200_val:
        if direction == "LONG":
            if last_close < ema50_val or last_close < ema200_val:
                # Fighting down-trend
                return None
        else:
            if last_close > ema50_val or last_close > ema200_val:
                # Fighting up-trend
                return None

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
            score += 20
            reasons.append("volume_surge")
        else:
            score -= 20
            reasons.append("low_volume_penalty")

    # ── 3. Ultra-Strict Close Conviction ─────────────────────────────────────
    candle_range = last_high - last_low
    if candle_range > 0:
        if direction == "LONG":
            close_position = (last_close - last_low) / candle_range
            if close_position >= 0.8:        # closes in extreme upper 20%
                score += 20
                reasons.append("shaved_conviction")
            else:
                # Wick rejected resistance; fail trade.
                return None
        else:
            close_position = (last_high - last_close) / candle_range
            if close_position >= 0.8:        # closes in extreme lower 20%
                score += 20
                reasons.append("shaved_conviction")
            else:
                # Wick rejected support; fail trade.
                return None

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
        "V2 Breakout Signal: %s %s score=%d sl=%.6f arm=%.6f atr=%.6f reasons=%s",
        symbol, direction, score, sl_price, trail_arm, atr15, reasons,
    )
    return signal

def check_micro_scalp(
    symbol: str, 
    k1: list[dict],
) -> dict | None:
    """
    V2 MICRO_SCALP HFT ENGINE (1m timeframe)
    Strict Volatility requirements:
      1) Explosive Volume Spike: The 1m closed candle has > 400% volume over SMA14.
      2) Shaved Momentum: Candle closed in the top/bottom 20% of its range (0 wick fade).
      3) Aggressive SL: The stop-loss is placed strictly beneath/above the impulse candle low/high.
    """
    if len(k1) < 20:
        return None

    c = k1[-1]
    closes = [float(x["close"]) for x in k1]
    vols = [float(x["volume"]) for x in k1]
    
    # 1. Volume Delta Spike (>400% over SMA14)
    vol_sma_14 = sum(vols[-15:-1]) / 14 if len(vols) >= 15 else sum(vols[:-1]) / len(vols[:-1])
    if vol_sma_14 <= 0:
        return None
    
    current_vol = vols[-1]
    vol_ratio = current_vol / vol_sma_14
    if vol_ratio < 4.0:
        return None
        
    # 2. Shaved Momentum Logic
    c_open = c["open"]
    c_close = c["close"]
    c_high = c["high"]
    c_low = c["low"]
    
    c_range = c_high - c_low
    if c_range == 0:
        return None
        
    direction = "LONG" if c_close > c_open else "SHORT"
    
    if direction == "LONG":
        close_pos = (c_close - c_low) / c_range
        if close_pos < 0.80:
            return None  # Needs to close in top 20%, massive conviction
            
        sl_price = round(c_low * 0.9995, 8)  # Stop loss tightly hugging the bottom of the impulse + 0.05% safety
        trail_arm = round(c_close + c_range * 1.5, 8)  # Aggressive 1.5R 
        
    else:  # SHORT
        close_pos = (c_high - c_close) / c_range
        if close_pos < 0.80:
            return None  # Needs to close in bottom 20%
            
        sl_price = round(c_high * 1.0005, 8) 
        trail_arm = round(c_close - c_range * 1.5, 8)
        
    # Score is 100 for micro-scalps as they are hard boolean logic grids.
    score = 100
    atr1 = c_range  # Local ATR proxy
    atr_ratio = 1.0
    
    reasons = [
        f"VolSpike:{vol_ratio:.1f}x",
        f"ShavedConviction:{close_pos:.2f}",
        "1m_HFT"
    ]
    
    signal = {
        "symbol":       symbol,
        "direction":    direction,
        "score":        score,
        "entry_price":  round(c_close, 8),
        "sl_price":     sl_price,
        "tp1_price":    trail_arm,
        "tp2_price":    trail_arm,
        "atr":          round(atr1, 8),
        "atr_ratio":    atr_ratio,
        "timeframe":    "1m",
        "timestamp":    int(time.time()),
        "reasons":      reasons,
        "signal_type":  "MICRO_SCALP",
    }
    
    logger.info(
        "V2 Micro-Scalp Signal: %s %s score=%d sl=%.6f arm=%.6f reasons=%s",
        symbol, direction, score, sl_price, trail_arm, reasons,
    )
    return signal

def check_funding_predator(
    symbol: str, 
    funding_rate: float,
    mark_price: float,
) -> dict | None:
    """
    V2 FUNDING_PREDATOR HFT SQUEEZE ENGINE (Temporal Payload)
    Generates a LONG signal strictly exploiting the post-funding short-squeeze.
    Triggered natively by the chronological cron scanner at exactly 00:00:01, 08:00:01, 16:00:01 UTC.
    """
    direction = "LONG"
    
    # Aggressively tight SL because the squeeze must be instantaneous. If it slips downward natively, abort immediately.
    sl_price = round(mark_price * 0.995, 8)  # 0.5% Hard Stop
    # Target 1% bounce immediately trailing upward
    trail_arm = round(mark_price * 1.01, 8) 
    
    score = 100
    atr1 = mark_price * 0.005 # Baseline standard representation
    
    reasons = [
        f"FundingSqueeze",
        f"Yield:{funding_rate*100:.2f}%",
        "08H_Tick_Predator"
    ]
    
    import time
    signal = {
        "symbol":       symbol,
        "direction":    direction,
        "score":        score,
        "entry_price":  round(mark_price, 8),
        "sl_price":     sl_price,
        "tp1_price":    trail_arm,
        "tp2_price":    trail_arm,
        "atr":          round(atr1, 8),
        "atr_ratio":    1.0,
        "timeframe":    "tick",
        "timestamp":    int(time.time()),
        "reasons":      reasons,
        "signal_type":  "FUNDING_PREDATOR",
    }
    
    import logging
    logger = logging.getLogger(__name__)
    logger.info(
        "V2 FUNDING PREDATOR EXECUTED: %s %s score=%d sl=%.6f arm=%.6f reasons=%s",
        symbol, direction, score, sl_price, trail_arm, reasons,
    )
    return signal
