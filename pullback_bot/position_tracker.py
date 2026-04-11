"""
position_tracker.py — Tracks open positions.

LIVE mode:
  - Order fill events are handled by user_data_stream.py.
  - On startup, reconciles any fills that occurred while the bot was offline.

PAPER mode:
  - Reads mark prices from scanner.mark_prices dict (updated by mark-price WS).
  - Calculates unrealized PnL on a polling interval.
  - Simulates SL / TP hits against mark price.

Both modes broadcast position_update and trade_closed via ws_broadcaster.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

import binance_client as bc
import config
import db
import ws_broadcaster as wsb

# ── Constants ─────────────────────────────────────────────────────────────────
TAKER_FEE_RATE = 0.0004          # 0.04% — Binance futures market order
MAINTENANCE_MARGIN_RATE = 0.004  # 0.4% — conservative estimate


def _net_pnl(gross: float, entry: float, close: float, qty: float) -> float:
    """Subtract round-trip taker fees from a gross PnL figure."""
    entry_fee = entry * qty * TAKER_FEE_RATE
    exit_fee  = close * qty * TAKER_FEE_RATE
    return gross - entry_fee - exit_fee


def _compute_fast_rsi(candles: list[dict], period: int = 5) -> float:
    """Computes a lightweight Wilder's RSI from a list of generic candle dicts."""
    if not candles or len(candles) <= period:
        return 50.0

    gains = []
    losses = []
    for i in range(1, len(candles)):
        change = candles[i]["close"] - candles[i-1]["close"]
        if change > 0:
            gains.append(change)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(change))
            
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    
    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period
        
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _enrich_position(trade: dict, mark: float, raw_pnl: float) -> dict:
    """
    Compute derived position fields from stored trade data.
    All monetary values in USDT.
    """
    entry   = float(trade["entry_price"])
    qty     = float(trade["qty"])
    lev     = int(trade.get("leverage") or config.MAX_LEVERAGE)
    direction = trade["direction"]
    entry_time = int(trade.get("entry_time") or 0)

    notional     = entry * qty                              # position size in USDT
    margin_usdt  = notional / lev                          # initial margin
    entry_fee    = notional * TAKER_FEE_RATE               # entry taker fee
    exit_fee     = mark * qty * TAKER_FEE_RATE             # hypothetical exit fee at mark
    fee_usdt     = entry_fee + exit_fee                    # round-trip fee estimate

    # Estimated liquidation price (simplified — ignores funding, cross-margin buffer)
    if direction == "LONG":
        liq_price = entry * (1 - 1 / lev + MAINTENANCE_MARGIN_RATE)
    else:
        liq_price = entry * (1 + 1 / lev - MAINTENANCE_MARGIN_RATE)

    net_pnl = _net_pnl(raw_pnl, entry, mark, qty)
    roe_pct = (net_pnl / margin_usdt * 100) if margin_usdt else 0.0

    duration_s = int(time.time()) - entry_time // 1000   # entry_time is ms
    duration_str = _fmt_duration(duration_s)

    pnl_pct = (net_pnl / notional * 100) if notional else 0.0

    return {
        **trade,
        "mark_price":          round(mark, 8),
        "unrealized_pnl":      round(net_pnl, 4),
        "unrealized_pnl_pct":  round(pnl_pct, 4),
        "roe_pct":             round(roe_pct, 2),
        "notional_usdt":       round(notional, 2),
        "margin_usdt":         round(margin_usdt, 2),
        "fee_usdt":            round(fee_usdt, 4),
        "liq_price":           round(liq_price, 8),
        "leverage":            lev,
        "duration":            duration_str,
    }


def _fmt_duration(seconds: int) -> str:
    if seconds < 0:
        return "—"
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    return f"{h}h {m}m"

logger = logging.getLogger(__name__)

# ── Paper-mode unrealized PnL store ──────────────────────────────────────────
# trade_id -> current unrealized PnL (USDT)
paper_unrealized: dict[int, float] = {}

# ── Trailing stop state (paper mode) ─────────────────────────────────────────
# Trailing activates when mark crosses tp1_price (the trail arm).
# Trail distance = 1×ATR = SL_distance / 1.5 (since SL = entry ± 1.5×ATR).

_trail_active: dict[int, bool] = {}    # trade_id -> trailing is armed
_trail_extreme: dict[int, float] = {}  # trade_id -> best price reached (high for LONG, low for SHORT)

# ── Portfolio trailing floor state ────────────────────────────────────────────
_portfolio_trail_armed: bool  = False   # True once total unrealized >= PORT TP target
_peak_portfolio_pnl:    float = 0.0    # highest total unrealized PnL since arming

# Simple boolean re-entrancy guard for _paper_tick.
# asyncio.Lock() required deferred init (must be created inside a running event
# loop).  A plain bool works just as well in single-threaded asyncio and needs
# no initialisation.
_tick_running: bool = False


# ── Stats broadcast helper ────────────────────────────────────────────────────

async def _broadcast_stats() -> None:
    """Push fresh today + all-time stats to all connected UI clients."""
    try:
        today = await db.get_today_stats()
        alltime = await db.get_all_stats()
        await wsb.broadcaster.broadcast("stats_update", {**today, **alltime})
    except Exception as exc:
        logger.warning("_broadcast_stats error: %s", exc)


# ── PAPER mode: PnL simulation ────────────────────────────────────────────────

async def _close_all_paper(
    trades: list[dict],
    mark_prices: dict,
    reason: str,
) -> None:
    """Close every trade in `trades` at current mark price (paper mode)."""
    total_pnl = 0.0
    for trade in trades:
        if trade.get("mode") != "paper":
            continue
        tid = trade["id"]
        symbol = trade["symbol"]
        mark = mark_prices.get(symbol) or float(trade["entry_price"])
        direction = trade["direction"]
        entry = float(trade["entry_price"])
        qty   = float(trade["qty"])

        if direction == "LONG":
            gross = (mark - entry) * qty
        else:
            gross = (entry - mark) * qty
        pnl = _net_pnl(gross, entry, mark, qty)
        pnl_pct = pnl / (entry * qty) * 100 if entry * qty else 0
        close_time = int(time.time() * 1000)

        await db.update_trade_close(
            trade_id=tid,
            close_price=mark,
            close_time=close_time,
            pnl_usdt=round(pnl, 4),
            pnl_pct=round(pnl_pct, 2),
            close_reason=reason,
        )
        paper_unrealized.pop(tid, None)
        _trail_active.pop(tid, None)
        _trail_extreme.pop(tid, None)
        total_pnl += pnl

        await wsb.broadcaster.broadcast("trade_closed", {
            **trade,
            "close_price": mark,
            "close_reason": reason,
            "pnl_usdt": round(pnl, 4),
            "pnl_pct": round(pnl_pct, 2),
            "close_time": close_time,
        })

    await wsb.broadcaster.broadcast("portfolio_stop_triggered", {
        "reason": reason,
        "total_pnl": round(total_pnl, 4),
        "count": len(trades),
    })
    await _broadcast_stats()
    logger.warning(
        "Portfolio stop (%s): closed %d trades, total_pnl=%.4f",
        reason, len(trades), total_pnl,
    )


def _should_smart_port_sl_trigger(
    open_trades: list[dict],
    paper_unrealized: dict,
    total_unrealized: float,
) -> bool:
    """
    Pure function — no side effects, no awaits, no globals modified.

    Returns True only when ALL conditions hold simultaneously:

    0. Minimum trade age gate — youngest open trade must be at least
       SMART_PORT_SL_MIN_AGE_MINUTES old (prevents firing on entry noise)

    1. Majority of open trades are negative
       (negative_count / total >= SMART_PORT_SL_NEG_RATIO)

    2. Total loss is deep enough to matter
       (total_unrealized <= -(PORTFOLIO_MIN_TP_USDT * SMART_PORT_SL_MULTIPLIER))

    3. Gradual building has stopped
       (total_unrealized < -(PORTFOLIO_MIN_TP_USDT / 2))
       — same threshold used by the scanner to halt new entries

    Requires at least INITIAL_BATCH_SIZE + 1 open trades — prevents triggering
    on the first batch alone before recovery slots have opened.
    """
    if not open_trades:
        return False

    # Condition 0 — minimum trade age gate
    now_ms = int(time.time() * 1000)
    youngest_entry = max(t["entry_time"] for t in open_trades)
    age_ms = now_ms - youngest_entry
    min_age_ms = config.SMART_PORT_SL_MIN_AGE_MINUTES * 60 * 1000
    if age_ms < min_age_ms:
        return False

    min_trades = config.INITIAL_BATCH_SIZE + 1
    if len(open_trades) < min_trades:
        return False

    # Condition 1 — majority negative
    negative_count = sum(
        1 for t in open_trades
        if paper_unrealized.get(t["id"], 0.0) < 0
    )
    if negative_count / len(open_trades) < config.SMART_PORT_SL_NEG_RATIO:
        return False

    # Condition 2 — loss deep enough
    threshold = -(config.PORTFOLIO_MIN_TP_USDT * config.SMART_PORT_SL_MULTIPLIER)
    if threshold == 0.0 or total_unrealized > threshold:
        return False

    # Condition 3 — building has stopped (scanner already halted new entries)
    half_target = config.PORTFOLIO_MIN_TP_USDT / 2
    if half_target <= 0 or total_unrealized >= -half_target:
        return False

    return True


async def _paper_tick() -> None:
    """
    Process open paper trades against the latest mark prices:
    calculate unrealized PnL, check SL/trail hits, broadcast position_update.

    Called by the scanner immediately after each mark-price batch arrives
    (~1 s cadence).  The _tick_running guard skips the tick if the previous
    one has not finished yet (avoids pile-up without blocking anything).
    """
    global _tick_running
    if _tick_running:
        return
    _tick_running = True
    try:
        from scanner import mark_prices  # avoid circular at module level
        try:
            open_trades = await db.get_open_trades()
            if not open_trades:
                import order_manager as _om
                if getattr(_om, "_active_session_id", None) is not None and len(getattr(_om, "_opening", set())) == 0:
                    try:
                        session_trades = await db.get_trades_by_session(_om._active_session_id)
                        net_pnl = sum(t["pnl_usdt"] or 0.0 for t in session_trades)
                        await db.close_session(
                            session_id  = _om._active_session_id,
                            ended_at    = int(time.time() * 1000),
                            exit_reason = "NATURAL_FLAT",
                            net_pnl     = round(net_pnl, 4),
                            trade_count = len(session_trades),
                        )
                        logger.info(
                            "Session closed naturally (0 open trades): %s trades=%d net_pnl=%.4f",
                            _om._active_session_id, len(session_trades), net_pnl
                        )
                    except Exception as sess_exc:
                        logger.error("Failed to naturally close session %s: %s", _om._active_session_id, sess_exc)
                    finally:
                        await _om.reset_session()
                return

            positions_payload: list[dict] = []

            for trade in open_trades:
                if trade.get("mode") != "paper":
                    continue

                symbol = trade["symbol"]
                mark = mark_prices.get(symbol)
                if not mark:
                    positions_payload.append(_enrich_position(trade, float(trade["entry_price"]), 0.0))
                    continue

                direction = trade["direction"]
                entry = float(trade["entry_price"])
                qty   = float(trade["qty"])
                sl    = float(trade["sl_price"])
                trail_arm  = float(trade["tp1_price"])   # activation level (1×ATR from entry)
                
                # Derive precise real-time ATR from the structurally anchored trail_arm
                atr_estimate = abs(trail_arm - entry)
                trail_dist = atr_estimate * 1.0          # Trailing stop naturally follows mathematically at exactly 1 ATR

                tid = trade["id"]

                # PnL calculation (gross price difference)
                if direction == "LONG":
                    raw_pnl = (mark - entry) * qty
                else:
                    raw_pnl = (entry - mark) * qty

                # Fee-adjusted unrealized PnL (entry fee already paid + exit fee at mark)
                paper_unrealized[tid] = _net_pnl(raw_pnl, entry, mark, qty)

                # ── Activate trailing (only when USE_TAKE_PROFIT + USE_TRAILING) ─
                trail_active = _trail_active.get(tid, False)
                
                # Smart Engine Analysis
                smart_rsi = None
                if getattr(config, "SMART_TRAILING_ENABLED", True):
                    import scanner
                    klines = scanner._kline_buffers.get(symbol, {}).get("5m", [])
                    if klines:
                        smart_rsi = _compute_fast_rsi(klines, period=5)
                        if smart_rsi is not None:
                            # Dynamically map trail_dist based on momentum
                            if direction == "LONG":
                                if smart_rsi > 70: trail_dist = atr_estimate * 0.3
                                elif smart_rsi < 55: trail_dist = atr_estimate * 0.5
                            else:
                                if smart_rsi < 30: trail_dist = atr_estimate * 0.3
                                elif smart_rsi > 45: trail_dist = atr_estimate * 0.5

                hit_price: Optional[float] = None
                close_reason: Optional[str] = None
                trail_stop: Optional[float] = None

                if not trail_active and config.USE_TAKE_PROFIT and config.USE_TRAILING:
                    armed = (direction == "LONG" and mark >= trail_arm) or \
                            (direction == "SHORT" and mark <= trail_arm)
                    
                    if armed:
                        smart_dump = False
                        if smart_rsi is not None:
                            if direction == "LONG" and smart_rsi >= 75:
                                smart_dump = True
                            elif direction == "SHORT" and smart_rsi <= 25:
                                smart_dump = True

                        if smart_dump:
                            hit_price = mark  # Close exactly at the exhausted mark price
                            close_reason = "SMART_TP"
                        else:
                            _trail_active[tid] = True
                            _trail_extreme[tid] = mark
                            trail_active = True
                            logger.info(
                                "Trail armed: #%d %s %s mark=%.6f arm=%.6f",
                                tid, symbol, direction, mark, trail_arm,
                            )

                if close_reason == "SMART_TP":
                    pass # Skip other checks, execute dump
                elif trail_active:
                    # Update extreme and derive trailing stop
                    if direction == "LONG":
                        _trail_extreme[tid] = max(_trail_extreme.get(tid, mark), mark)
                        trail_stop = _trail_extreme[tid] - trail_dist
                        if mark <= trail_stop:
                            hit_price = trail_stop
                            close_reason = "TRAIL"
                    else:
                        _trail_extreme[tid] = min(_trail_extreme.get(tid, mark), mark)
                        trail_stop = _trail_extreme[tid] + trail_dist
                        if mark >= trail_stop:
                            hit_price = trail_stop
                            close_reason = "TRAIL"
                else:
                    # SL check (skipped when USE_STOP_LOSS=false)
                    if config.USE_STOP_LOSS:
                        if direction == "LONG" and mark <= sl:
                            hit_price = sl
                            close_reason = "SL"
                        elif direction == "SHORT" and mark >= sl:
                            hit_price = sl
                            close_reason = "SL"
                    # Fixed-TP check (only when TP enabled but trailing disabled)
                    if not hit_price and config.USE_TAKE_PROFIT and not config.USE_TRAILING:
                        if direction == "LONG" and mark >= trail_arm:
                            hit_price = trail_arm
                            close_reason = "TP"
                        elif direction == "SHORT" and mark <= trail_arm:
                            hit_price = trail_arm
                            close_reason = "TP"

                if hit_price and close_reason:
                    # Close the paper trade
                    if direction == "LONG":
                        gross = (hit_price - entry) * qty
                    else:
                        gross = (entry - hit_price) * qty
                    final_pnl = _net_pnl(gross, entry, hit_price, qty)
                    final_pct = final_pnl / (entry * qty) * 100 if entry * qty else 0
                    close_time = int(time.time() * 1000)

                    # ── SMART PORTFOLIO PRUNING ───────────────────────────────
                    if close_reason == "SMART_TP" and final_pnl > 0:
                        import scanner
                        weak_candidates = []
                        weak_loss = 0.0
                        
                        for ot in open_trades:
                            oid = ot["id"]
                            if oid == tid or ot.get("mode") != trade.get("mode"): 
                                continue
                                
                            osym = ot["symbol"]
                            omark = mark_prices.get(osym)
                            if not omark:
                                continue
                                
                            oentry = float(ot["entry_price"])
                            oqty   = float(ot["qty"])
                            odir   = ot["direction"]
                            
                            oraw = (omark - oentry) * oqty if odir == "LONG" else (oentry - omark) * oqty
                            opnl = _net_pnl(oraw, oentry, omark, oqty)
                            oroepct = (opnl / (oentry * oqty / int(ot.get("leverage") or config.MAX_LEVERAGE))) * 100
                            
                            if opnl < 0 and oroepct < -5:
                                # Re-evaluate 15m Momentum for this losing trade
                                o_klines = scanner._kline_buffers.get(osym, {}).get("15m", [])
                                if len(o_klines) >= 5:
                                    orsi = _compute_fast_rsi(o_klines, period=5)
                                    is_dead = (odir == "LONG" and orsi < 45) or (odir == "SHORT" and orsi > 55)
                                    if is_dead:
                                        weak_candidates.append({
                                            "trade": ot, "mark": omark, "pnl": opnl,
                                            "entry": oentry, "qty": oqty
                                        })
                                        weak_loss += opnl
                        
                        # Execute Pruning if the winner covers the dead losses!
                        if weak_candidates and (final_pnl + weak_loss > 0):
                            logger.info("SMART PRUNING: Leader %s (Pnl: %.4f) covers %d dead trades (Loss: %.4f)", 
                                        symbol, final_pnl, len(weak_candidates), weak_loss)
                            for w in weak_candidates:
                                wt = w["trade"]
                                wtid = wt["id"]
                                wpct = w["pnl"] / (w["entry"] * w["qty"]) * 100 if w["entry"] * w["qty"] else 0
                                await db.update_trade_close(
                                    trade_id=wtid,
                                    close_price=w["mark"],
                                    close_time=close_time,
                                    pnl_usdt=round(w["pnl"], 4),
                                    pnl_pct=round(wpct, 2),
                                    close_reason="SMART_PRUNED"
                                )
                                paper_unrealized.pop(wtid, None)
                                _trail_active.pop(wtid, None)
                                _trail_extreme.pop(wtid, None)
                                await wsb.broadcaster.broadcast("trade_closed", {
                                    **wt,
                                    "close_price":  w["mark"],
                                    "close_time":   close_time,
                                    "close_reason": "SMART_PRUNED",
                                    "pnl_usdt":     round(w["pnl"], 4),
                                    "pnl_pct":      round(wpct, 2),
                                })
                    # ──────────────────────────────────────────────────────────

                    await db.update_trade_close(
                        trade_id=tid,
                        close_price=hit_price,
                        close_time=close_time,
                        pnl_usdt=round(final_pnl, 4),
                        pnl_pct=round(final_pct, 2),
                        close_reason=close_reason,
                    )
                    paper_unrealized.pop(tid, None)
                    _trail_active.pop(tid, None)
                    _trail_extreme.pop(tid, None)
                    await wsb.broadcaster.broadcast("trade_closed", {
                        **trade,
                        "close_price":  hit_price,
                        "close_time":   close_time,
                        "close_reason": close_reason,
                        "pnl_usdt":     round(final_pnl, 4),
                        "pnl_pct":      round(final_pct, 2),
                    })
                    await _broadcast_stats()
                    logger.info(
                        "Paper trade closed (%s): %s %s pnl=%.4f",
                        close_reason, symbol, direction, final_pnl,
                    )
                else:
                    enriched = _enrich_position(trade, mark, raw_pnl)
                    enriched["trail_arm"]     = round(trail_arm, 8)
                    enriched["trail_active"]  = trail_active
                    enriched["trail_stop"]    = round(trail_stop, 8) if trail_stop is not None else None
                    enriched["trail_extreme"] = round(_trail_extreme[tid], 8) if tid in _trail_extreme else None
                    enriched["use_trailing"]   = config.USE_TRAILING
                    enriched["use_stop_loss"]  = config.USE_STOP_LOSS
                    enriched["use_take_profit"] = config.USE_TAKE_PROFIT
                    positions_payload.append(enriched)

            # ── Live trades: compute unrealized PnL for display ───────────────
            for trade in open_trades:
                if trade.get("mode") != "live":
                    continue
                symbol = trade["symbol"]
                mark = mark_prices.get(symbol)
                if not mark:
                    positions_payload.append(_enrich_position(trade, float(trade["entry_price"]), 0.0))
                    continue
                entry = float(trade["entry_price"])
                qty = float(trade["qty"])
                direction = trade["direction"]
                raw_pnl = (mark - entry) * qty if direction == "LONG" else (entry - mark) * qty
                unrealized = _net_pnl(raw_pnl, entry, mark, qty)
                paper_unrealized[trade["id"]] = unrealized
                positions_payload.append(_enrich_position(trade, mark, unrealized))

            if positions_payload:
                await wsb.broadcaster.broadcast("position_update", positions_payload)

            # ── Portfolio-level stop check ────────────────────────────────────
            # Prune stale entries — trades closed outside _paper_tick (e.g. manual
            # close via API) would otherwise keep stale PnL that distorts the total.
            open_ids = {t["id"] for t in open_trades}
            for stale_tid in set(paper_unrealized) - open_ids:
                paper_unrealized.pop(stale_tid, None)
            total_unrealized = sum(paper_unrealized.values())
            stop_loss_limit = config.PORTFOLIO_STOP_LOSS_USDT
            min_tp_usdt     = config.PORTFOLIO_MIN_TP_USDT

            triggered_reason: Optional[str] = None

            # Portfolio SL — always checked regardless of TP mode
            if stop_loss_limit != 0.0 and total_unrealized <= stop_loss_limit:
                triggered_reason = "PORTFOLIO_SL"

            # Smart Portfolio SL — multi-condition holistic check
            if (triggered_reason is None
                    and config.SMART_PORT_SL_ENABLED
                    and _should_smart_port_sl_trigger(
                        open_trades, paper_unrealized, total_unrealized
                    )):
                triggered_reason = "SMART_PORT_SL"

            if triggered_reason is None and min_tp_usdt != 0.0:
                # If ALL open trades are individually in profit, let them ride their individual 
                # trailing stops to maximize gains instead of capping the whole portfolio early.
                all_positive = len(open_trades) > 0 and all(paper_unrealized.get(t["id"], 0.0) >= 0.0 for t in open_trades)
                
                global _portfolio_trail_armed, _peak_portfolio_pnl
                if all_positive:
                    if _portfolio_trail_armed:
                        _portfolio_trail_armed = False
                        _peak_portfolio_pnl    = 0.0
                        logger.info("Portfolio trail disarmed: All individual trades are profitable. Deferring to individual trailing stops.")
                        await wsb.broadcaster.broadcast("portfolio_trail_disarmed", {"reason": "all_positive"})
                else:
                    if config.PORTFOLIO_TP_MODE == "normal":
                        # Normal mode — close immediately when PnL reaches the threshold
                        if total_unrealized >= min_tp_usdt:
                            triggered_reason = "PORTFOLIO_TP"
                    else:  # "trailing"
                        # Trailing floor — 3-phase arm / trail / disarm
                        if not _portfolio_trail_armed:
                            # Phase 2a — arm: PnL just crossed the target
                            if total_unrealized >= min_tp_usdt:
                                _portfolio_trail_armed = True
                                _peak_portfolio_pnl    = total_unrealized
                                logger.info(
                                    "Portfolio trail armed: pnl=%.4f target=%.4f",
                                    total_unrealized, min_tp_usdt,
                                )
                                await wsb.broadcaster.broadcast("portfolio_trail_armed", {
                                    "total_pnl": round(total_unrealized, 4),
                                    "target":    min_tp_usdt,
                                })
                        else:
                            # Phase 2b — update peak and check floor
                            _peak_portfolio_pnl = max(_peak_portfolio_pnl, total_unrealized)
                            trail_factor = config.PORTFOLIO_TRAIL_FACTOR
                            floor = min_tp_usdt + (_peak_portfolio_pnl - min_tp_usdt) * trail_factor

                            if _peak_portfolio_pnl > min_tp_usdt and total_unrealized <= floor:
                                if total_unrealized >= min_tp_usdt:
                                    # PnL is between floor and min_tp — close with guaranteed profit
                                    triggered_reason = "PORT_TP_TRAIL"
                                else:
                                    # PnL dropped below min_tp — disarm without closing;
                                    # wait for recovery back to min_tp before re-arming.
                                    _portfolio_trail_armed = False
                                    _peak_portfolio_pnl    = 0.0
                                    logger.info(
                                        "Portfolio trail disarmed — PnL %.4f dropped below "
                                        "min target %.4f, will re-arm on recovery",
                                        total_unrealized, min_tp_usdt,
                                    )
                                    await wsb.broadcaster.broadcast("portfolio_trail_disarmed", {"reason": "dropped_below_target"})
                            elif total_unrealized < 0:
                                # Phase 3 — disarm: portfolio went negative → reset so
                                # the trail can re-arm when PnL recovers to the target.
                                _portfolio_trail_armed = False
                                _peak_portfolio_pnl    = 0.0
                                logger.info(
                                    "Portfolio trail disarmed (PnL went negative): pnl=%.4f",
                                    total_unrealized,
                                )
                                await wsb.broadcaster.broadcast("portfolio_trail_disarmed", {"reason": "went_negative"})

            if triggered_reason and open_trades:
                logger.warning(
                    "Portfolio stop triggered (%s): total_unrealized=%.4f",
                    triggered_reason, total_unrealized,
                )
                await _close_all_paper(open_trades, mark_prices, triggered_reason)

                # Close the active session
                import order_manager as _om  # deferred — avoids circular at module level
                if _om._active_session_id is not None:
                    try:
                        session_trades = await db.get_trades_by_session(_om._active_session_id)
                        logger.info(
                            "Session %s: %d trade(s) found for PnL calculation",
                            _om._active_session_id, len(session_trades),
                        )
                        net_pnl = sum(t["pnl_usdt"] or 0.0 for t in session_trades)
                        await db.close_session(
                            session_id  = _om._active_session_id,
                            ended_at    = int(time.time() * 1000),
                            exit_reason = triggered_reason,
                            net_pnl     = round(net_pnl, 4),
                            trade_count = len(session_trades),
                        )
                        logger.info(
                            "Session closed: %s reason=%s trades=%d net_pnl=%.4f",
                            _om._active_session_id, triggered_reason,
                            len(session_trades), net_pnl,
                        )
                    except Exception as sess_exc:
                        logger.error(
                            "Failed to close session %s: %s",
                            _om._active_session_id, sess_exc, exc_info=True,
                        )
                    finally:
                        # Always reset so the next cycle gets a fresh session,
                        # even if DB write failed.
                        await _om.reset_session()

                # Reset portfolio trail state so the cycle can restart cleanly
                _portfolio_trail_armed = False
                _peak_portfolio_pnl    = 0.0

        except Exception as exc:
            logger.error("paper_tick error: %s", exc)
    finally:
        _tick_running = False


async def _paper_pnl_fallback() -> None:
    """
    Safety-net: run _paper_tick every 2 s even if the scanner never notifies us
    (e.g. WS reconnecting).  Scanner-triggered ticks still take priority because
    _paper_tick returns immediately if _tick_running is True.
    """
    while True:
        await asyncio.sleep(2.0)
        await _paper_tick()


# ── Startup reconciliation ────────────────────────────────────────────────────

async def _reconcile_live() -> None:
    """
    Compare DB open trades against real Binance positions.
    Any trade in DB with status=OPEN that has no matching Binance position
    (positionAmt == 0) was closed while the server was down — mark it CLOSED.
    """
    logger.info("Reconciling live positions against Binance...")
    try:
        open_trades = await db.get_open_trades()
        if not open_trades:
            logger.info("No open trades in DB — nothing to reconcile")
            return

        binance_positions = await bc.get_positions()
        # Build map: symbol -> positionAmt (non-zero means still open)
        live_map: dict[str, float] = {}
        for pos in binance_positions:
            amt = float(pos.get("positionAmt", 0))
            if amt != 0:
                live_map[pos["symbol"]] = amt

        closed_count = 0
        for trade in open_trades:
            symbol = trade["symbol"]
            if symbol not in live_map:
                # Position no longer exists on Binance — it was closed during downtime
                # Best-effort: fetch current mark price as close price
                try:
                    close_price = await bc.get_mark_price(symbol)
                except Exception:
                    close_price = float(trade["entry_price"])

                direction = trade["direction"]
                entry = float(trade["entry_price"])
                qty = float(trade["qty"])
                if direction == "LONG":
                    gross = (close_price - entry) * qty
                else:
                    gross = (entry - close_price) * qty
                pnl = _net_pnl(gross, entry, close_price, qty)
                pnl_pct = pnl / (entry * qty) * 100 if entry * qty else 0

                await db.update_trade_close(
                    trade_id=trade["id"],
                    close_price=close_price,
                    close_time=int(time.time() * 1000),
                    pnl_usdt=round(pnl, 4),
                    pnl_pct=round(pnl_pct, 2),
                    status="CLOSED",
                    close_reason="RECONCILED",
                )
                logger.warning(
                    "Reconciled ghost trade #%d %s %s — closed at %.4f pnl=%.4f",
                    trade["id"], symbol, direction, close_price, pnl,
                )
                closed_count += 1

        logger.info("Reconciliation complete: %d ghost trade(s) closed", closed_count)

        # ── Reverse check: Binance positions not tracked in DB ────────────────
        # These can arise from SL/TP placement failures that prevented DB insert
        # (ghost trades).  We cannot safely auto-adopt without knowing the
        # original SL/TP, so alert loudly for manual intervention.
        db_symbols = {t["symbol"] for t in open_trades}
        untracked  = [(sym, amt) for sym, amt in live_map.items()
                      if sym not in db_symbols]
        if untracked:
            for sym, amt in untracked:
                logger.critical(
                    "UNTRACKED LIVE POSITION: %s positionAmt=%.4f — "
                    "not in database, manual close required",
                    sym, amt,
                )
            logger.critical(
                "Reconciliation found %d untracked Binance position(s). "
                "These are likely ghost trades from a failed order placement. "
                "Close them manually on the exchange.",
                len(untracked),
            )

    except Exception as exc:
        logger.error("Live reconciliation error: %s", exc)


async def _reconcile_paper() -> None:
    """
    For paper mode: on restart, immediately evaluate any open trades against
    the current mark price. If price has already blown through SL or TP,
    close them at the configured level (conservative — no slippage modelling).
    Runs once before the paper_pnl_loop starts.
    """
    logger.info("Reconciling paper positions after restart...")
    try:
        open_trades = await db.get_open_trades()
        if not open_trades:
            return

        # Wait a few seconds for mark_prices to populate from WebSocket
        await asyncio.sleep(5)
        from scanner import mark_prices

        for trade in open_trades:
            symbol = trade["symbol"]
            mark = mark_prices.get(symbol)
            if not mark:
                # No mark price yet — skip, pnl_loop will handle it
                continue

            direction = trade["direction"]
            sl = float(trade["sl_price"])
            tp2 = float(trade["tp2_price"])
            tp1 = float(trade["tp1_price"])
            entry = float(trade["entry_price"])
            qty = float(trade["qty"])

            hit_price: Optional[float] = None
            close_reason: Optional[str] = None

            if direction == "LONG":
                if mark <= sl:
                    hit_price, close_reason = sl, "SL"
                elif mark >= tp2:
                    hit_price, close_reason = tp2, "TP2"
                elif mark >= tp1:
                    hit_price, close_reason = tp1, "TP1"
            else:
                if mark >= sl:
                    hit_price, close_reason = sl, "SL"
                elif mark <= tp2:
                    hit_price, close_reason = tp2, "TP2"
                elif mark <= tp1:
                    hit_price, close_reason = tp1, "TP1"

            if hit_price and close_reason:
                if direction == "LONG":
                    gross = (hit_price - entry) * qty
                else:
                    gross = (entry - hit_price) * qty
                pnl = _net_pnl(gross, entry, hit_price, qty)
                pnl_pct = pnl / (entry * qty) * 100 if entry * qty else 0

                await db.update_trade_close(
                    trade_id=trade["id"],
                    close_price=hit_price,
                    close_time=int(time.time() * 1000),
                    pnl_usdt=round(pnl, 4),
                    pnl_pct=round(pnl_pct, 2),
                    close_reason=close_reason,
                )
                logger.warning(
                    "Paper restart reconcile: #%d %s %s hit %s at %.4f pnl=%.4f",
                    trade["id"], symbol, direction, close_reason, hit_price, pnl,
                )
    except Exception as exc:
        logger.error("Paper reconciliation error: %s", exc)


# ── Entry point ───────────────────────────────────────────────────────────────

async def start() -> None:
    """Start position tracking. Called from main.py startup."""
    if config.MODE == "live":
        if not config.BINANCE_API_KEY:
            logger.warning("No API key — skipping live position tracker")
            return
        try:
            await _reconcile_live()
            logger.info("Live position tracker started")
        except Exception as exc:
            logger.error("Failed to start live position tracker: %s", exc)
    else:
        # Paper: start 2s fallback loop and reconcile open trades on restart
        asyncio.create_task(_paper_pnl_fallback(), name="paper_pnl_fallback")
        asyncio.create_task(_reconcile_paper(), name="paper_reconcile")
        logger.info("Paper position tracker started")
