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
import os
import time
from typing import Optional

import joblib
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


def _compute_daily_vwap(df: pd.DataFrame) -> float:
    """Calculates Daily Volume Weighted Average Price (VWAP) matching Binance 00:00 UTC resets."""
    import datetime
    now_utc = datetime.datetime.utcnow()
    start_of_day = datetime.datetime(now_utc.year, now_utc.month, now_utc.day, tzinfo=datetime.timezone.utc).timestamp()
    
    if "time" not in df.columns:
        return 0.0
        
    day_df = df[df["time"] >= start_of_day]
    if len(day_df) == 0:
        return 0.0
        
    typical_price = (day_df["high"] + day_df["low"] + day_df["close"]) / 3.0
    cum_vol = day_df["volume"].sum()
    if cum_vol == 0:
        return 0.0
        
    vwap = (typical_price * day_df["volume"]).sum() / cum_vol
    return float(vwap)


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


# ── ML Filter Integration ──────────────────────────────────────────────────────

_ML_MODELS = {}

def _get_ml_model(symbol: str):
    if not getattr(config, "ML_FILTER_ENABLED", False):
        return None
        
    if symbol in _ML_MODELS:
        return _ML_MODELS[symbol]
    
    model_path = os.path.join(os.path.dirname(__file__), "models", f"{symbol}_model.pkl")
    if os.path.exists(model_path):
        try:
            model = joblib.load(model_path)
            _ML_MODELS[symbol] = model
            return model
        except Exception as e:
            logger.error(f"Failed to load ML model for {symbol}: {e}")
            _ML_MODELS[symbol] = None
    else:
        _ML_MODELS[symbol] = None
    return None

def _run_ml_filter(symbol: str, df: pd.DataFrame, direction: str) -> tuple[bool, float, str]:
    """
    Returns (passed, confidence, reason).
    """
    if not getattr(config, "ML_FILTER_ENABLED", False):
        return True, 1.0, "ml_disabled"
        
    model = _get_ml_model(symbol)
    if not model:
        # Fallback to true if no model is trained yet
        return True, 1.0, "ml_no_model"
        
    try:
        df = df.copy()
        close = df["close"]
        ema_50 = _ema(close, 50)
        ema_200 = _ema(close, 200)
        
        dist_50 = (close - ema_50) / ema_50
        dist_200 = (close - ema_200) / ema_200
        
        stoch_k, stoch_d = _stoch_rsi(close)
        
        fast_ema = _ema(close, 12)
        slow_ema = _ema(close, 26)
        macd_line = fast_ema - slow_ema
        signal_line = _ema(macd_line, 9)
        macd_hist = macd_line - signal_line
        
        roc_3 = close.pct_change(3)
        vol_ratio = df["volume"] / df["volume"].rolling(20).mean()
        
        features = pd.DataFrame({
            "dist_50": dist_50,
            "dist_200": dist_200,
            "stoch_k": stoch_k,
            "stoch_d": stoch_d,
            "macd_hist": macd_hist,
            "roc_3": roc_3,
            "vol_ratio": vol_ratio
        })
        
        # Take the very last row for current inference
        current_features = features.iloc[[-1]]
        
        # Check for NaNs
        if current_features.isnull().values.any():
            return True, 1.0, "ml_nan_inputs"
            
        proba = model.predict_proba(current_features)[0]
        classes = list(model.classes_)
        
        target_class = 1 if direction == "LONG" else -1
        
        if target_class in classes:
            idx = classes.index(target_class)
            success_prob = float(proba[idx])
        else:
            # If the model fundamentally didn't see enough of this class in training, fail safe
            success_prob = 0.0
        
        threshold = getattr(config, "ML_CONFIDENCE_THRESHOLD", 0.70)
        if success_prob >= threshold:
            return True, success_prob, f"ml_pass_{int(success_prob*100)}"
        else:
            return False, success_prob, f"ml_reject_{int(success_prob*100)}"
            
    except Exception as e:
        logger.error(f"ML Filter Error on {symbol}: {e}")
        return True, 1.0, "ml_error"


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
    klines_4h: list[dict] = [],
    oi_hist: list[dict] = [],
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

    # ── 15m Macro Indicators ──
    close15 = df15["close"]
    ema50 = _ema(close15, 50)
    ema200 = _ema(close15, 200)
    
    last_ema50  = float(ema50.iloc[-1])
    last_ema200 = float(ema200.iloc[-1])

    # ── 5m Execution Parameters ──
    close5 = df5["close"]
    high5  = df5["high"]
    low5   = df5["low"]
    open5  = df5["open"]

    last_close = float(close5.iloc[-1])
    last_high  = float(high5.iloc[-1])
    last_low   = float(low5.iloc[-1])
    last_open  = float(open5.iloc[-1])

    prev_close = float(close5.iloc[-2])
    prev_high  = float(high5.iloc[-2])
    prev_low   = float(low5.iloc[-2])
    prev_open  = float(open5.iloc[-2])

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

    # ── MTF Alignment (4-Hour Macro Trend) ────────────────────────────────────
    if config.FILTER_MTF_ENABLED and len(klines_4h) >= 200:
        df4h = pd.DataFrame(klines_4h).astype(float)
        ema200_4h = _ema(df4h["close"], 200)
        current_4h_close = float(df4h["close"].iloc[-1])
        current_4h_ema = float(ema200_4h.iloc[-1])
        
        if direction == "LONG" and current_4h_close < current_4h_ema:
            logger.debug("MTF Guard: Blocked LONG on %s (Below 4H EMA200)", symbol)
            return None
        if direction == "SHORT" and current_4h_close > current_4h_ema:
            logger.debug("MTF Guard: Blocked SHORT on %s (Above 4H EMA200)", symbol)
            return None
            
        score += 20
        reasons.append("mtf_aligned")

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

    # ── Open Interest (OI) Spike Detection ────────────────────────────────────
    if config.FILTER_OI_ENABLED and oi_hist and len(oi_hist) > 5:
        try:
            oi_start = float(oi_hist[0].get("sumOpenInterest", 0))
            oi_end = float(oi_hist[-1].get("sumOpenInterest", 0))
            if oi_start > 0:
                oi_change = (oi_end - oi_start) / oi_start
                if oi_change > 0.015:  # 1.5% genuine liquidity injection
                    score += 20
                    reasons.append("oi_spike")
                elif oi_change < -0.015: # -1.5% liquidity drying up / squeeze trap
                    logger.debug("OI Guard: Blocked %s %s (OI dropping by %.2f%%)", symbol, direction, oi_change * 100)
                    return None
        except Exception as e:
            logger.debug("OI parsing error %s: %s", symbol, e)
            pass

    # ── Daily VWAP Bounce ───────────────────────────────────────────────────
    if config.FILTER_VWAP_ENABLED:
        vwap = _compute_daily_vwap(df15)
        if vwap > 0:
            dist_to_vwap = abs(last_close - vwap)
            if dist_to_vwap <= (atr15 * 0.25):
                score += 30
                reasons.append("daily_vwap_bounce")

    # ── RSI Divergence ──────────────────────────────────────────────────────
    if config.FILTER_RSI_ENABLED:
        df_rsi = _rsi(df15["close"], 14)
        if len(df_rsi) > 20:
            recent_price_low = float(df15["low"].iloc[-10:].min())
            recent_rsi_low = float(df_rsi.iloc[-10:].min())
            prev_price_low = float(df15["low"].iloc[-20:-10].min())
            prev_rsi_low = float(df_rsi.iloc[-20:-10].min())
            
            recent_price_high = float(df15["high"].iloc[-10:].max())
            recent_rsi_high = float(df_rsi.iloc[-10:].max())
            prev_price_high = float(df15["high"].iloc[-20:-10].max())
            prev_rsi_high = float(df_rsi.iloc[-20:-10].max())
            
            if direction == "LONG" and recent_price_low < prev_price_low and recent_rsi_low > prev_rsi_low:
                score += 20
                reasons.append("bullish_rsi_div")
            elif direction == "SHORT" and recent_price_high > prev_price_high and recent_rsi_high < prev_rsi_high:
                score += 20
                reasons.append("bearish_rsi_div")

    # ── Score gate ────────────────────────────────────────────────────────────
    if score < config.SIGNAL_SCORE_THRESHOLD:
        return None

    # ── 4. Structural Stop Loss Anchoring ─────────────────────────────────────
    entry_price = float(last_close)

    atr_avg20 = float(atr_series.iloc[-21:-1].mean()) if len(atr_series) > 21 else atr15
    atr_ratio = atr15 / atr_avg20 if atr_avg20 > 0 else 1.0

    # Identify the true 5-candle 5m structural swing high/low for tight execution anchoring
    recent_low = float(df5["low"].iloc[-5:].min())
    recent_high = float(df5["high"].iloc[-5:].max())

    if direction == "LONG":
        # Anchor explicitly below the 5-candle structural fractal bottom + 0.75 ATR buffer (widened to absorb wicks)
        sl_price = round(recent_low - (atr15 * 0.75), 8)
        # Structural disaster cap (2.5 ATR Max)
        sl_price = max(sl_price, entry_price - (atr15 * 2.5))
        
        trail_arm = round(entry_price + atr15 * 1.0, 8)
    else:
        # Anchor explicitly above the 5-candle structural fractal top + 0.75 ATR buffer (widened to absorb wicks)
        sl_price = round(recent_high + (atr15 * 0.75), 8)
        # Structural disaster cap (2.5 ATR Max)
        sl_price = min(sl_price, entry_price + (atr15 * 2.5))
        
        trail_arm = round(entry_price - atr15 * 1.0, 8)

    if sl_price <= 0:
        return None

    # --- ML Smart Filter ---
    # Strip the last unclosed 15m live candle to match historical training perfectly
    ml_passed, ml_conf, ml_reason = _run_ml_filter(symbol, df15[:-1], direction)
    if not ml_passed:
        logger.info(f"[{symbol}] ML Filter rejected pullback ({ml_conf:.2f} < threshold).")
        
    reasons.append(ml_reason)
    if "pass" in ml_reason:
        score += 10

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
        "ml_passed":   ml_passed,
        "ml_confidence": ml_conf,
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
    klines_4h: list[dict] = [],
    oi_hist: list[dict] = [],
) -> Optional[dict]:
    """
    V2 Structural Breakout / Breakdown detector.

    Scans for explosive momentum breaking out of a proven consolidation Box,
    aligned explicitly with macro-trend flow, printing shaved-head block candles.
    """
    if len(klines_15m) < 50:
        return None

    df15 = pd.DataFrame(klines_15m).astype(float)
    df5 = pd.DataFrame(klines_5m).astype(float)

    # ── 5m Execution Parameters ──
    last        = df5.iloc[-1]
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
    if range_size > (_atr_avg20 * 8.0):
        # Range was too wide and noisy; not a coiled spring.
        logger.info("Breakout rejected: %s range too wide (%f vs %f max)", symbol, range_size, _atr_avg20 * 8.0)
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

    # ── MTF Alignment (4-Hour Macro Trend) ────────────────────────────────────
    if config.FILTER_MTF_ENABLED and len(klines_4h) >= 200:
        df4h = pd.DataFrame(klines_4h).astype(float)
        ema200_4h = _ema(df4h["close"], 200)
        current_4h_close = float(df4h["close"].iloc[-1])
        current_4h_ema = float(ema200_4h.iloc[-1])
        
        if direction == "LONG" and current_4h_close < current_4h_ema:
            logger.debug("MTF Guard: Blocked LONG Breakout on %s (Below 4H EMA)", symbol)
            return None
        if direction == "SHORT" and current_4h_close > current_4h_ema:
            logger.debug("MTF Guard: Blocked SHORT Breakdown on %s (Above 4H EMA)", symbol)
            return None
            
        score += 20
        reasons.append("mtf_aligned")

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
        # last_vol is now the 5m closed volume. Compare to 5m average equivalent (15m_avg / 3)
        if last_vol > (avg_vol / 3) * 1.5:
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

    # ── Daily VWAP Bounce ───────────────────────────────────────────────────
    if config.FILTER_VWAP_ENABLED:
        vwap = _compute_daily_vwap(df15)
        if vwap > 0:
            dist_to_vwap = abs(last_close - vwap)
            if dist_to_vwap <= (atr15 * 0.25):
                score += 30
                reasons.append("daily_vwap_bounce")

    # ── RSI Divergence ──────────────────────────────────────────────────────
    if config.FILTER_RSI_ENABLED:
        df_rsi = _rsi(df15["close"], 14)
        if len(df_rsi) > 20:
            recent_price_low = float(df15["low"].iloc[-10:].min())
            recent_rsi_low = float(df_rsi.iloc[-10:].min())
            prev_price_low = float(df15["low"].iloc[-20:-10].min())
            prev_rsi_low = float(df_rsi.iloc[-20:-10].min())
            
            recent_price_high = float(df15["high"].iloc[-10:].max())
            recent_rsi_high = float(df_rsi.iloc[-10:].max())
            prev_price_high = float(df15["high"].iloc[-20:-10].max())
            prev_rsi_high = float(df_rsi.iloc[-20:-10].max())
            
            if direction == "LONG" and recent_price_low < prev_price_low and recent_rsi_low > prev_rsi_low:
                score += 20
                reasons.append("bullish_rsi_div")
            elif direction == "SHORT" and recent_price_high > prev_price_high and recent_rsi_high < prev_rsi_high:
                score += 20
                reasons.append("bearish_rsi_div")

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
        sl_price  = round(max(resistance - atr15 * 0.5, entry_price - atr15 * 2.5), 8)
        trail_arm = round(entry_price + atr15 * 1.0, 8)
    else:
        sl_price  = round(min(support + atr15 * 0.5, entry_price + atr15 * 2.5), 8)
        trail_arm = round(entry_price - atr15 * 1.0, 8)

    if sl_price <= 0:
        return None

    # ── Open Interest (OI) Spike Detection ────────────────────────────────────
    if config.FILTER_OI_ENABLED and oi_hist and len(oi_hist) > 5:
        try:
            oi_start = float(oi_hist[0].get("sumOpenInterest", 0))
            oi_end = float(oi_hist[-1].get("sumOpenInterest", 0))
            if oi_start > 0:
                oi_change = (oi_end - oi_start) / oi_start
                if oi_change > 0.015:  # 1.5% genuine liquidity injection
                    score += 20
                    reasons.append("oi_spike")
                elif oi_change < -0.015: # -1.5% liquidity drying up / squeeze trap
                    logger.debug("OI Guard: Blocked %s %s (OI dropping by %.2f%%)", symbol, direction, oi_change * 100)
                    return None
        except Exception as e:
            logger.debug("OI parsing error %s: %s", symbol, e)
            pass

    # --- ML Smart Filter ---
    # Strip the last unclosed 15m live candle to match historical training perfectly
    ml_passed, ml_conf, ml_reason = _run_ml_filter(symbol, df15[:-1], direction)
    if not ml_passed:
        logger.info(f"[{symbol}] ML Filter rejected BREAKOUT ({ml_conf:.2f} < threshold).")
        
    reasons.append(ml_reason)
    if "pass" in ml_reason:
        score += 10

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
        "ml_passed":    ml_passed,
        "ml_confidence": ml_conf,
    }
    logger.info(
        "V2 Breakout Signal: %s %s score=%d sl=%.6f arm=%.6f atr=%.6f reasons=%s",
        symbol, direction, score, sl_price, trail_arm, atr15, reasons,
    )
    return signal

