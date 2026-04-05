"""
signal_engine.py — Daily Sweep (ICT/SMC) strategy.

Three phases — ALL must pass for a valid signal:
  Phase 1: Daily Bias  (computed once/day from daily candles)
  Phase 2: Liquidity Sweep detection on 1h chart within NY open window
  Phase 3: Fair Value Gap (FVG) confirmation on 1h chart after the sweep

Public API:
  compute_daily_bias(daily_klines)  -> {"bias", "pdh", "pdl"}
  check_daily_sweep(symbol, klines_1h, daily_bias, current_price, utc_now) -> signal | None
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, time as dtime
from typing import Optional

import numpy as np
import pandas as pd

import config

logger = logging.getLogger(__name__)


# ── Indicator helpers ──────────────────────────────────────────────────────────

def _atr(candles: list[dict], period: int = 14) -> float:
    """Return ATR of the last `period` candles."""
    if len(candles) < period + 1:
        # Fallback: simple range average
        ranges = [c["high"] - c["low"] for c in candles[-period:]]
        return sum(ranges) / len(ranges) if ranges else 0.0
    highs  = [c["high"]  for c in candles[-(period + 1):]]
    lows   = [c["low"]   for c in candles[-(period + 1):]]
    closes = [c["close"] for c in candles[-(period + 1):]]
    trs = []
    for i in range(1, len(highs)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        )
        trs.append(tr)
    # Simple moving average of TR
    return sum(trs[-period:]) / period


# ── NY Open window check ───────────────────────────────────────────────────────

def _parse_hhmm(s: str) -> dtime:
    h, m = map(int, s.split(":"))
    return dtime(h, m)


def _in_ny_window(utc_now: datetime) -> bool:
    t = utc_now.time().replace(second=0, microsecond=0)
    start = _parse_hhmm(config.NY_OPEN_START_UTC)
    end   = _parse_hhmm(config.NY_OPEN_END_UTC)
    return start <= t <= end


# ── Phase 1: Daily Bias ────────────────────────────────────────────────────────

def compute_daily_bias(daily_klines: list[dict]) -> dict:
    """
    Determine today's directional bias from the last 3 daily candles.

    Requires at least 3 candles (index -3, -2, -1).
      - candle[-2] = previous day  → pdh, pdl
      - candle[-1] = today (or most recent closed day)

    BULLISH:  high[-1] > high[-2] AND low[-1] > low[-2]
    BEARISH:  high[-1] < high[-2] AND low[-1] < low[-2]
    NEUTRAL:  otherwise (inside bar / indecision)

    Returns: {"bias": str, "pdh": float, "pdl": float}
    """
    result = {"bias": "NEUTRAL", "pdh": 0.0, "pdl": 0.0}
    if len(daily_klines) < 3:
        return result

    c_prev = daily_klines[-2]   # previous day
    c_cur  = daily_klines[-1]   # most recent day

    pdh = float(c_prev["high"])
    pdl = float(c_prev["low"])
    result["pdh"] = pdh
    result["pdl"] = pdl

    hh = float(c_cur["high"]) > pdh   # higher high
    hl = float(c_cur["low"])  > pdl   # higher low
    lh = float(c_cur["high"]) < pdh   # lower high
    ll = float(c_cur["low"])  < pdl   # lower low

    if hh and hl:
        result["bias"] = "LONG"
    elif lh and ll:
        result["bias"] = "SHORT"
    # else NEUTRAL

    logger.debug(
        "Daily bias: %s | pdh=%.6f pdl=%.6f | cur_high=%.6f cur_low=%.6f",
        result["bias"], pdh, pdl,
        float(c_cur["high"]), float(c_cur["low"]),
    )
    return result


# ── Phase 2: Liquidity Sweep detection ────────────────────────────────────────

def _find_sweep(candles: list[dict], direction: str, lookback: int) -> Optional[dict]:
    """
    For LONG bias (direction="LONG") → look for a BEARISH sweep:
      - Find swing LOW in last `lookback` candles
      - Find a candle whose LOW broke below that swing low
        AND whose CLOSE recovered above it  (fake out)

    For SHORT bias (direction="SHORT") → look for a BULLISH sweep:
      - Find swing HIGH in last `lookback` candles
      - Candle HIGH broke above swing high AND CLOSE came back below it

    Checks the last 3 candles (most recent) for the sweep candle.
    Returns the sweep candle dict with added "sweep_level" key, or None.
    """
    if len(candles) < 2:
        return None

    # Split into reference window (older, for swing level) and search window (recent).
    # Use the last 2*lookback candles; first half is reference, second half is searched.
    # ref_end is the split point (at least 1 candle on each side).
    pool    = candles[-(2 * lookback):] if len(candles) >= 2 * lookback else candles
    ref_end = max(1, len(pool) - lookback)
    ref_w   = pool[:ref_end]    # reference candles for swing level
    srch_w  = pool[ref_end:]    # candidate sweep candles (searched newest-first)

    if not ref_w or not srch_w:
        return None

    if direction == "LONG":
        swing_low = min(c["low"] for c in ref_w)
        for candle in reversed(srch_w):
            if candle["low"] < swing_low and candle["close"] > swing_low:
                return {**candle, "sweep_level": swing_low}
    else:  # SHORT
        swing_high = max(c["high"] for c in ref_w)
        for candle in reversed(srch_w):
            if candle["high"] > swing_high and candle["close"] < swing_high:
                return {**candle, "sweep_level": swing_high}

    return None


# ── Phase 3: Fair Value Gap detection ─────────────────────────────────────────

def _find_fvg(
    candles: list[dict],
    direction: str,
    sweep_time: int,
    atr: float,
) -> Optional[dict]:
    """
    Look for an FVG in the candles AFTER the sweep candle.

    Bullish FVG (for LONG):  candle[i+2].low > candle[i].high
    Bearish FVG (for SHORT): candle[i+2].high < candle[i].low

    Checks the 3 candles immediately after the sweep.
    FVG size must be >= ATR * FVG_MIN_ATR_MULT.

    Returns {"fvg_high", "fvg_low", "fvg_mid"} or None.
    """
    min_size = atr * config.FVG_MIN_ATR_MULT

    # Find index of sweep candle in the list
    sweep_idx = None
    for i, c in enumerate(candles):
        if c.get("time") == sweep_time:
            sweep_idx = i
            break

    if sweep_idx is None or sweep_idx + 2 >= len(candles):
        # Sweep is too recent — not enough candles after it yet
        return None

    # Look at 3 consecutive candle triplets starting right after the sweep
    for i in range(sweep_idx + 1, min(sweep_idx + 4, len(candles) - 2)):
        c0 = candles[i]
        c2 = candles[i + 2]
        if direction == "LONG":
            fvg_low  = c0["high"]
            fvg_high = c2["low"]
            if fvg_high > fvg_low and (fvg_high - fvg_low) >= min_size:
                return {
                    "fvg_low":  fvg_low,
                    "fvg_high": fvg_high,
                    "fvg_mid":  (fvg_low + fvg_high) / 2,
                }
        else:  # SHORT
            fvg_high = c0["low"]
            fvg_low  = c2["high"]
            if fvg_high > fvg_low and (fvg_high - fvg_low) >= min_size:
                return {
                    "fvg_low":  fvg_low,
                    "fvg_high": fvg_high,
                    "fvg_mid":  (fvg_low + fvg_high) / 2,
                }

    return None


# ── Main signal function ───────────────────────────────────────────────────────

def check_daily_sweep(
    symbol: str,
    klines_1h: list[dict],
    daily_bias: dict,
    current_price: float,
    utc_now: datetime,
    atr_mult: float = None,
) -> Optional[dict]:
    """
    Run all 3 phases of the Daily Sweep strategy.

    Returns a signal dict if all phases pass, else None.
    """
    if atr_mult is None:
        atr_mult = config.FVG_MIN_ATR_MULT

    # ── Gate: NY open window ─────────────────────────────────────────────────
    if not _in_ny_window(utc_now):
        return None

    # ── Gate: daily bias must be directional ────────────────────────────────
    bias = daily_bias.get("bias", "NEUTRAL")
    if bias == "NEUTRAL":
        return None

    pdh = float(daily_bias.get("pdh", 0))
    pdl = float(daily_bias.get("pdl", 0))

    if len(klines_1h) < config.SWEEP_LOOKBACK + 3:
        return None

    direction = bias  # "LONG" or "SHORT"

    # ── Phase 2: Sweep ───────────────────────────────────────────────────────
    sweep = _find_sweep(klines_1h, direction, config.SWEEP_LOOKBACK)
    if sweep is None:
        logger.debug("%s: no sweep detected", symbol)
        return None

    # ── ATR on 1h candles ────────────────────────────────────────────────────
    atr = _atr(klines_1h, period=14)
    if atr <= 0:
        return None

    # ── Phase 3: FVG ─────────────────────────────────────────────────────────
    fvg = _find_fvg(klines_1h, direction, sweep.get("time", 0), atr)
    if fvg is None:
        logger.debug("%s: sweep found but no FVG yet", symbol)
        return None

    # ── Entry trigger: price inside FVG zone ─────────────────────────────────
    fvg_low  = fvg["fvg_low"]
    fvg_high = fvg["fvg_high"]
    if not (fvg_low <= current_price <= fvg_high):
        logger.debug(
            "%s: FVG [%.6f-%.6f] found but price %.6f not inside yet",
            symbol, fvg_low, fvg_high, current_price,
        )
        return None

    # ── SL / TP calculation ──────────────────────────────────────────────────
    sl_buffer = atr * 0.1
    if direction == "LONG":
        sl_price  = round(sweep["low"] - sl_buffer, 8)
        tp1_price = round(pdh, 8)                           # Previous Day High
        tp2_price = round(pdh + (pdh - current_price) * 0.5, 8)
    else:
        sl_price  = round(sweep["high"] + sl_buffer, 8)
        tp1_price = round(pdl, 8)                           # Previous Day Low
        tp2_price = round(pdl - (current_price - pdl) * 0.5, 8)

    if sl_price <= 0 or tp1_price <= 0:
        return None

    signal = {
        "symbol":      symbol,
        "direction":   direction,
        "entry_price": round(current_price, 8),
        "sl_price":    sl_price,
        "tp1_price":   tp1_price,
        "tp2_price":   tp2_price,
        "fvg_high":    round(fvg_high, 8),
        "fvg_low":     round(fvg_low,  8),
        "sweep_price": round(sweep.get("low" if direction == "LONG" else "high", 0), 8),
        "sweep_time":  sweep.get("time", 0),
        "daily_bias":  bias,
        "pdh":         round(pdh, 8),
        "pdl":         round(pdl, 8),
        "timestamp":   int(time.time()),
        "score":       100,   # all 3 phases passed
    }
    logger.info(
        "Daily Sweep signal: %s %s entry=%.6f sl=%.6f tp1=%.6f fvg=[%.6f-%.6f]",
        symbol, direction, current_price, sl_price, tp1_price, fvg_low, fvg_high,
    )
    return signal


# ── Inline unit test ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    def _c(t, o, h, l, c, v=1000):
        return {"time": t, "open": o, "high": h, "low": l, "close": c, "volume": v}

    print("=== Test 1: compute_daily_bias ===")
    daily = [
        _c(0, 100, 105, 98,  103),   # candle -3
        _c(1, 103, 108, 101, 106),   # candle -2 → pdh=108, pdl=101
        _c(2, 106, 110, 103, 109),   # candle -1 → hh (110>108) AND hl (103>101) → LONG
    ]
    bias = compute_daily_bias(daily)
    assert bias["bias"] == "LONG",  f"Expected LONG got {bias['bias']}"
    assert bias["pdh"] == 108.0
    assert bias["pdl"] == 101.0
    print("  LONG bias: OK")

    daily_bear = [
        _c(0, 110, 115, 108, 112),
        _c(1, 112, 114, 107, 109),   # pdh=114, pdl=107
        _c(2, 109, 113, 105, 107),   # lh (113<114) AND ll (105<107) → SHORT
    ]
    bias_bear = compute_daily_bias(daily_bear)
    assert bias_bear["bias"] == "SHORT", f"Expected SHORT got {bias_bear['bias']}"
    print("  SHORT bias: OK")

    print("\n=== Test 2: _find_sweep (LONG / bearish sweep) ===")
    # 6 candles, swing low at 98. Candle -2: wick below 98, closes above → sweep
    candles_1h = [
        _c(10, 100, 104, 98,  102),   # -6  swing low @ 98
        _c(11, 102, 106, 99,  105),   # -5
        _c(12, 105, 108, 100, 107),   # -4
        _c(13, 107, 109, 99,  108),   # -3
        _c(14, 108, 110, 95,  103),   # -2  low=95 < 98, close=103 > 98 → SWEEP
        _c(15, 103, 107, 101, 106),   # -1  post-sweep
    ]
    sweep = _find_sweep(candles_1h, "LONG", 6)
    assert sweep is not None, "Sweep not found"
    assert sweep["time"] == 14, f"Wrong sweep candle time: {sweep['time']}"
    print(f"  Sweep found at time={sweep['time']} low={sweep['low']} level={sweep['sweep_level']}: OK")

    print("\n=== Test 3: _find_fvg (bullish FVG after sweep) ===")
    # After sweep at t=14, next 3 candles form a bullish FVG
    # c0.high=107, c2.low=109 → gap [107, 109] > min_size
    candles_with_fvg = candles_1h + [
        _c(16, 106, 107, 104, 106),   # c0 high=107
        _c(17, 107, 108, 105, 108),   # c1 (middle)
        _c(18, 109, 112, 109, 111),   # c2 low=109 > c0 high=107 → FVG [107, 109]
    ]
    atr_val = _atr(candles_with_fvg, 14)
    fvg = _find_fvg(candles_with_fvg, "LONG", sweep_time=14, atr=atr_val)
    assert fvg is not None, "FVG not found"
    assert fvg["fvg_low"] == 107.0 and fvg["fvg_high"] == 109.0, f"Wrong FVG: {fvg}"
    print(f"  FVG found: [{fvg['fvg_low']}, {fvg['fvg_high']}]: OK")

    print("\n=== Test 4: check_daily_sweep full pipeline (inside FVG) ===")
    utc_in_window = datetime(2024, 1, 15, 14, 0)  # 14:00 UTC = inside 13:30-15:00
    signal = check_daily_sweep(
        symbol="BTCUSDT",
        klines_1h=candles_with_fvg,
        daily_bias={"bias": "LONG", "pdh": 115.0, "pdl": 95.0},
        current_price=108.0,   # inside FVG [107, 109]
        utc_now=utc_in_window,
    )
    assert signal is not None, "Signal should fire"
    assert signal["direction"] == "LONG"
    assert signal["fvg_low"]  == 107.0
    assert signal["fvg_high"] == 109.0
    assert signal["tp1_price"] == 115.0
    assert signal["score"] == 100
    print(f"  Signal: {signal['direction']} entry={signal['entry_price']} sl={signal['sl_price']} tp1={signal['tp1_price']}: OK")

    print("\n=== Test 5: check_daily_sweep outside NY window → None ===")
    utc_out = datetime(2024, 1, 15, 10, 0)   # 10:00 UTC, before window
    sig_out = check_daily_sweep("BTCUSDT", candles_with_fvg,
                                 {"bias": "LONG", "pdh": 115.0, "pdl": 95.0},
                                 108.0, utc_out)
    assert sig_out is None, "Should not fire outside window"
    print("  Outside window → None: OK")

    print("\n=== Test 6: price outside FVG → None ===")
    sig_out2 = check_daily_sweep("BTCUSDT", candles_with_fvg,
                                  {"bias": "LONG", "pdh": 115.0, "pdl": 95.0},
                                  current_price=112.0,  # above FVG
                                  utc_now=utc_in_window)
    assert sig_out2 is None, "Should not fire when price outside FVG"
    print("  Price above FVG → None: OK")

    print("\nAll tests passed ✓")
