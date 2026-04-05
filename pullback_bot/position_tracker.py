"""
position_tracker.py — Tracks open positions.

LIVE mode:
  - Subscribes to Binance User Data Stream (listenKey) via WebSocket.
  - Handles ORDER_TRADE_UPDATE events: updates DB on fill/close.
  - Keepalive ping on listenKey every 30 minutes.

PAPER mode:
  - Reads mark prices from scanner.mark_prices dict (updated by mark-price WS).
  - Calculates unrealized PnL on a polling interval.
  - Simulates SL / TP hits against mark price.

Both modes broadcast position_update and trade_closed via ws_broadcaster.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Optional

import websockets

import binance_client as bc
import config
import db
import ws_broadcaster as wsb

# ── Constants ─────────────────────────────────────────────────────────────────
TAKER_FEE_RATE = 0.0004          # 0.04% — Binance futures market order
MAINTENANCE_MARGIN_RATE = 0.004  # 0.4% — conservative estimate


def _enrich_position(trade: dict, mark: float, raw_pnl: float) -> dict:
    """
    Compute derived position fields from stored trade data.
    All monetary values in USDT.
    """
    entry   = float(trade["entry_price"])
    qty     = float(trade["qty"])
    lev     = int(trade.get("leverage") or config.LEVERAGE)
    direction = trade["direction"]
    entry_time = int(trade.get("entry_time") or 0)

    notional     = entry * qty                              # position size in USDT
    margin_usdt  = notional / lev                          # initial margin
    fee_usdt     = notional * TAKER_FEE_RATE               # entry fee (taker)

    # Estimated liquidation price (simplified — ignores funding, cross-margin buffer)
    if direction == "LONG":
        liq_price = entry * (1 - 1 / lev + MAINTENANCE_MARGIN_RATE)
    else:
        liq_price = entry * (1 + 1 / lev - MAINTENANCE_MARGIN_RATE)

    roe_pct = (raw_pnl / margin_usdt * 100) if margin_usdt else 0.0

    duration_s = int(time.time()) - entry_time // 1000   # entry_time is ms
    duration_str = _fmt_duration(duration_s)

    pnl_pct = (raw_pnl / notional * 100) if notional else 0.0

    return {
        **trade,
        "mark_price":          round(mark, 8),
        "unrealized_pnl":      round(raw_pnl, 4),
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

# Event set by scanner whenever a new batch of mark prices arrives (~1s cadence)
_price_event: asyncio.Event = asyncio.Event()

# ── Trailing take-profit state (paper mode) ───────────────────────────────────
# Trailing activates when mark crosses tp1_price (the trail arm).
# TRAIL_STEP_RATIO is read from config each tick so UI changes apply immediately.

_trail_active: dict[int, bool] = {}    # trade_id -> trailing is armed
_trail_extreme: dict[int, float] = {}  # trade_id -> best price reached (high for LONG, low for SHORT)


# ── LIVE mode: User Data Stream ───────────────────────────────────────────────

async def _handle_order_update(event: dict) -> None:
    """Process ORDER_TRADE_UPDATE event from user data stream."""
    order = event.get("o", {})
    status = order.get("X")       # FILLED, PARTIALLY_FILLED, CANCELED, etc.
    binance_id = str(order.get("i", ""))
    symbol = order.get("s", "")
    realized_pnl = float(order.get("rp", 0))

    if status not in ("FILLED", "CANCELED", "EXPIRED"):
        return

    # Look up our trade by binance_order_id
    open_trades = await db.get_open_trades()
    for trade in open_trades:
        if trade.get("binance_order_id") == binance_id:
            if status == "FILLED":
                close_price = float(order.get("ap", trade["entry_price"]))
                close_time = int(time.time() * 1000)
                direction = trade["direction"]
                entry = trade["entry_price"]
                qty = trade["qty"]

                if realized_pnl == 0:
                    # Calculate manually
                    if direction == "LONG":
                        realized_pnl = (close_price - entry) * qty
                    else:
                        realized_pnl = (entry - close_price) * qty

                pnl_pct = realized_pnl / (entry * qty) * 100 if entry * qty else 0

                await db.update_trade_close(
                    trade_id=trade["id"],
                    close_price=close_price,
                    close_time=close_time,
                    pnl_usdt=round(realized_pnl, 4),
                    pnl_pct=round(pnl_pct, 2),
                )
                await wsb.broadcaster.broadcast("trade_closed", {
                    **trade,
                    "close_price": close_price,
                    "pnl_usdt": round(realized_pnl, 4),
                    "pnl_pct": round(pnl_pct, 2),
                })
                logger.info("Trade closed: %s pnl=%.4f", trade["symbol"], realized_pnl)
            elif status in ("CANCELED", "EXPIRED"):
                await db.update_trade_status(trade["id"], "CANCELLED")
            break


async def _run_user_data_ws(listen_key: str) -> None:
    """Connect to user data stream, handle events, reconnect on error."""
    url = f"{config.BINANCE_WS_BASE}/ws/{listen_key}"
    backoff = 1
    while True:
        logger.info("Connecting user-data WS...")
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                backoff = 1
                logger.info("User-data WS connected")
                async for raw in ws:
                    event = json.loads(raw)
                    if event.get("e") == "ORDER_TRADE_UPDATE":
                        await _handle_order_update(event)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("User-data WS error: %s — reconnect in %ds", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


async def _keepalive_listen_key(listen_key: str) -> None:
    """Ping listenKey every 30 minutes to keep it alive."""
    while True:
        await asyncio.sleep(30 * 60)
        try:
            await bc.keepalive_listen_key(listen_key)
            logger.debug("listenKey keepalive sent")
        except Exception as exc:
            logger.warning("listenKey keepalive failed: %s", exc)


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
            pnl = (mark - entry) * qty
        else:
            pnl = (entry - mark) * qty
        pnl_pct = pnl / (entry * qty) * 100 if entry * qty else 0
        close_time = int(time.time() * 1000)

        await db.update_trade_close(
            trade_id=tid,
            close_price=mark,
            close_time=close_time,
            pnl_usdt=round(pnl, 4),
            pnl_pct=round(pnl_pct, 2),
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
    logger.warning(
        "Portfolio stop (%s): closed %d trades, total_pnl=%.4f",
        reason, len(trades), total_pnl,
    )


async def _paper_pnl_loop() -> None:
    """
    Poll open paper trades, calculate unrealized PnL from mark prices,
    check for SL/TP hits, broadcast position_update.
    """
    from scanner import mark_prices  # avoid circular at module level

    while True:
        # Wake immediately when scanner fires a mark-price batch; fall through
        # after 3 s even if the event never comes (safety net for live mode).
        try:
            await asyncio.wait_for(_price_event.wait(), timeout=3.0)
        except asyncio.TimeoutError:
            pass
        _price_event.clear()
        try:
            open_trades = await db.get_open_trades()
            if not open_trades:
                continue

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
                trail_arm  = float(trade["tp1_price"])   # activation level (1:1 RR)
                risk_dist  = abs(entry - sl)
                trail_dist = risk_dist * config.TRAIL_STEP_RATIO

                tid = trade["id"]

                # PnL calculation
                if direction == "LONG":
                    raw_pnl = (mark - entry) * qty
                else:
                    raw_pnl = (entry - mark) * qty

                pnl_pct = raw_pnl / (entry * qty) * 100 if entry * qty else 0
                paper_unrealized[tid] = raw_pnl

                # ── Activate trailing when mark crosses trail arm ─────────────
                trail_active = _trail_active.get(tid, False)
                if not trail_active:
                    armed = (direction == "LONG" and mark >= trail_arm) or \
                            (direction == "SHORT" and mark <= trail_arm)
                    if armed:
                        _trail_active[tid] = True
                        _trail_extreme[tid] = mark
                        trail_active = True
                        logger.info(
                            "Trail armed: #%d %s %s mark=%.6f arm=%.6f",
                            tid, symbol, direction, mark, trail_arm,
                        )

                # ── Compute hit price ─────────────────────────────────────────
                hit_price: Optional[float] = None
                close_reason: Optional[str] = None
                trail_stop: Optional[float] = None

                if trail_active:
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
                    # Pre-trail: only original SL is active
                    if direction == "LONG" and mark <= sl:
                        hit_price = sl
                        close_reason = "SL"
                    elif direction == "SHORT" and mark >= sl:
                        hit_price = sl
                        close_reason = "SL"

                if hit_price and close_reason:
                    # Close the paper trade
                    if direction == "LONG":
                        final_pnl = (hit_price - entry) * qty
                    else:
                        final_pnl = (entry - hit_price) * qty
                    final_pct = final_pnl / (entry * qty) * 100 if entry * qty else 0
                    close_time = int(time.time() * 1000)

                    await db.update_trade_close(
                        trade_id=tid,
                        close_price=hit_price,
                        close_time=close_time,
                        pnl_usdt=round(final_pnl, 4),
                        pnl_pct=round(final_pct, 2),
                    )
                    paper_unrealized.pop(tid, None)
                    _trail_active.pop(tid, None)
                    _trail_extreme.pop(tid, None)
                    await wsb.broadcaster.broadcast("trade_closed", {
                        **trade,
                        "close_price": hit_price,
                        "close_reason": close_reason,
                        "pnl_usdt": round(final_pnl, 4),
                        "pnl_pct": round(final_pct, 2),
                    })
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
                    positions_payload.append(enriched)

            if positions_payload:
                await wsb.broadcaster.broadcast("position_update", positions_payload)

            # ── Portfolio-level stop check ────────────────────────────────────
            total_unrealized = sum(paper_unrealized.values())
            stop_loss_limit   = config.PORTFOLIO_STOP_LOSS_USDT
            take_profit_limit = config.PORTFOLIO_TAKE_PROFIT_USDT

            triggered_reason: Optional[str] = None
            if stop_loss_limit != 0.0 and total_unrealized <= stop_loss_limit:
                triggered_reason = "PORTFOLIO_SL"
            elif take_profit_limit != 0.0 and total_unrealized >= take_profit_limit:
                triggered_reason = "PORTFOLIO_TP"

            if triggered_reason and open_trades:
                logger.warning(
                    "Portfolio stop triggered (%s): total_unrealized=%.4f",
                    triggered_reason, total_unrealized,
                )
                await _close_all_paper(open_trades, mark_prices, triggered_reason)

        except Exception as exc:
            logger.error("paper_pnl_loop error: %s", exc)


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
                    pnl = (close_price - entry) * qty
                else:
                    pnl = (entry - close_price) * qty
                pnl_pct = pnl / (entry * qty) * 100 if entry * qty else 0

                await db.update_trade_close(
                    trade_id=trade["id"],
                    close_price=close_price,
                    close_time=int(time.time() * 1000),
                    pnl_usdt=round(pnl, 4),
                    pnl_pct=round(pnl_pct, 2),
                    status="CLOSED",
                )
                logger.warning(
                    "Reconciled ghost trade #%d %s %s — closed at %.4f pnl=%.4f",
                    trade["id"], symbol, direction, close_price, pnl,
                )
                closed_count += 1

        logger.info("Reconciliation complete: %d ghost trade(s) closed", closed_count)
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
                    pnl = (hit_price - entry) * qty
                else:
                    pnl = (entry - hit_price) * qty
                pnl_pct = pnl / (entry * qty) * 100 if entry * qty else 0

                await db.update_trade_close(
                    trade_id=trade["id"],
                    close_price=hit_price,
                    close_time=int(time.time() * 1000),
                    pnl_usdt=round(pnl, 4),
                    pnl_pct=round(pnl_pct, 2),
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
            # Reconcile before connecting user-data stream
            await _reconcile_live()
            listen_key = await bc.create_listen_key()
            asyncio.create_task(_run_user_data_ws(listen_key), name="user_data_ws")
            asyncio.create_task(_keepalive_listen_key(listen_key), name="listenkey_keepalive")
            logger.info("Live position tracker started (listenKey: %s...)", listen_key[:8])
        except Exception as exc:
            logger.error("Failed to start live position tracker: %s", exc)
    else:
        # Paper: start pnl loop, then reconcile in background after mark prices arrive
        asyncio.create_task(_paper_pnl_loop(), name="paper_pnl_loop")
        asyncio.create_task(_reconcile_paper(), name="paper_reconcile")
        logger.info("Paper position tracker started")
