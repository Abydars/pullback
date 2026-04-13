import re
with open('/Users/abid/Projects/pullback/pullback_bot/signal_engine.py', 'r') as f:
    code = f.read()

# Locate the check_breakout function block
# We use regex to find 'def check_breakout(' up to 'return signal\n\n' or similar.
pattern = re.compile(r"def check_breakout\([\s\S]+?return signal", re.MULTILINE)

replacement = """def check_breakout(
    symbol: str,
    klines_15m: list[dict],
    klines_5m: list[dict],
    klines_4h: list[dict] = [],
    oi_hist: list[dict] = [],
) -> Optional[dict]:
    \"\"\"
    SMC MTF Trend-Breakout System (4H -> 15M -> 5M)
    1. 4H checks for strict structural bias (Higher-Highs / EMA array).
    2. 15M identifies structural pivots and confirms a violent Breakout.
    3. 5M scans for the generating Order Block, waits for a Retest/Pullback tap,
       or allows a 'Violent Bypass' if momentum is statistically extreme.
    \"\"\"
    if len(klines_15m) < 50 or len(klines_5m) < 20 or len(klines_4h) < 20: 
        return None

    df4 = pd.DataFrame(klines_4h).astype(float)
    df15 = pd.DataFrame(klines_15m).astype(float)
    df5 = pd.DataFrame(klines_5m).astype(float)

    # ── Phase A: 4H Market Structure Bias ──
    ema21_4h = _ema(df4['close'], 21)
    ema50_4h = _ema(df4['close'], 50)
    last_4h_close = float(df4['close'].iloc[-1])
    
    if last_4h_close > ema21_4h.iloc[-1] and ema21_4h.iloc[-1] > ema50_4h.iloc[-1]:
        bias = "LONG"
    elif last_4h_close < ema21_4h.iloc[-1] and ema21_4h.iloc[-1] < ema50_4h.iloc[-1]:
        bias = "SHORT"
    else:
        return None 

    # ── Phase B: 15M Structural Pivot Breakout ──
    lookback_15m = df15.iloc[-50:-1]
    res_15m, sup_15m = _find_swing_levels(lookback_15m, window=3)
    
    last15 = df15.iloc[-1]
    prev15 = df15.iloc[-2]
    
    adx_val = _adx(df15, 14).iloc[-1]
    if adx_val < 20: 
        return None
        
    last_15m_vol = last15['volume']
    avg_15m_vol = df15['volume'].iloc[-21:-1].mean()
    
    breakout_confirmed = False
    broken_level = 0.0
    
    if bias == "LONG":
        if last15['close'] > res_15m and prev15['close'] <= res_15m:
            breakout_confirmed = True
            broken_level = res_15m
    else:
        if last15['close'] < sup_15m and prev15['close'] >= sup_15m:
            breakout_confirmed = True
            broken_level = sup_15m
    
    if not breakout_confirmed:
        return None
        
    # Phase C: 5M Execution & Retest
    last5 = df5.iloc[-1]
    prev5 = df5.iloc[-2]
    
    ob_high, ob_low = _detect_order_block(df5, bias, len(df5)-1, lookback=12)
    
    retest_tap = False
    violent_bypass = False
    
    vol_ratio = last_15m_vol / max(avg_15m_vol, 1)
    if vol_ratio >= 3.0 and adx_val >= 35.0:
        violent_bypass = True
        logger.info(f"[{symbol}] SMC Violent Bypass Activated! Ext. Vol: {vol_ratio:.2f}x ADX: {adx_val:.1f}")
        
    if not violent_bypass:
        if bias == "LONG":
            tap_level = broken_level * 1.002
            if last5['low'] <= tap_level or (ob_high > 0 and last5['low'] <= ob_high):
                if _is_engulfing(last5, prev5) == "BULLISH":
                    retest_tap = True
        else:
            tap_level = broken_level * 0.998
            if last5['high'] >= tap_level or (ob_low > 0 and last5['high'] >= ob_low):
                if _is_engulfing(last5, prev5) == "BEARISH":
                    retest_tap = True

        if not retest_tap: 
            return None

    score = 85 if retest_tap else 90 
    reasons = ["smc_bias", "smc_breakout"]
    if retest_tap: reasons.append("ob_retest_tap")
    if violent_bypass: reasons.append("violent_bypass")
    
    entry_price = float(last5["close"])
    _atr_series = _atr(df5, 14)
    atr5 = float(_atr_series.iloc[-1])
    
    if bias == "LONG":
        sl_price = ob_low - (atr5 * 0.5) if ob_low > 0 else (broken_level - (atr5 * 2.0))
        dist_pct = (entry_price - sl_price) / sl_price
        trail_arm = entry_price * (1 + (dist_pct * 1.5)) 
    else:
        sl_price = ob_high + (atr5 * 0.5) if ob_high > 0 else (broken_level + (atr5 * 2.0))
        dist_pct = (sl_price - entry_price) / entry_price
        trail_arm = entry_price * (1 - (dist_pct * 1.5))

    ml_passed, ml_conf, ml_reason = _run_ml_filter(symbol, df15[:-1], bias)
    if not ml_passed:
        logger.info(f"[{symbol}] SMC ML Filter rejected ({ml_conf:.2f} < threshold).")
    reasons.append(ml_reason)
    
    signal: dict = {
        "symbol":       symbol,
        "direction":    bias,
        "score":        score,
        "entry_price":  round(entry_price, 8),
        "sl_price":     round(sl_price, 8),
        "tp1_price":    round(trail_arm, 8),
        "tp2_price":    round(trail_arm, 8),
        "atr":          round(atr5, 8),
        "atr_ratio":    1.0, 
        "timeframe":    "15m",
        "timestamp":    int(last5["open_time"] / 1000),
        "reasons":      reasons,
        "signal_type":  "BREAKOUT",
        "ml_passed":    ml_passed,
        "ml_confidence": ml_conf,
    }
    logger.info(
        "V2 SMC Breakout Signal: %s %s score=%d sl=%.6f arm=%.6f reasons=%s",
        symbol, bias, score, sl_price, trail_arm, reasons,
    )
    return signal"""

new_code = pattern.sub(replacement, code)
with open('/Users/abid/Projects/pullback/pullback_bot/signal_engine.py', 'w') as f:
    f.write(new_code)
print("Updated successfully")
