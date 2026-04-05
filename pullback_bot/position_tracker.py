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

logger = logging.getLogger(__name__)

# ── Paper-mode unrealized PnL store ──────────────────────────────────────────
# trade_id -> current unrealized PnL (USDT)
paper_unrealized: dict[int, float] = {}


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

async def _paper_pnl_loop() -> None:
    """
    Poll open paper trades, calculate unrealized PnL from mark prices,
    check for SL/TP hits, broadcast position_update.
    """
    from scanner import mark_prices  # avoid circular at module level

    while True:
        await asyncio.sleep(2)  # update every 2 seconds
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
                    positions_payload.append({**trade, "mark_price": 0, "unrealized_pnl": 0})
                    continue

                direction = trade["direction"]
                entry = float(trade["entry_price"])
                qty = float(trade["qty"])
                sl = float(trade["sl_price"])
                tp1 = float(trade["tp1_price"])
                tp2 = float(trade["tp2_price"])

                # PnL calculation
                if direction == "LONG":
                    raw_pnl = (mark - entry) * qty
                else:
                    raw_pnl = (entry - mark) * qty

                pnl_pct = raw_pnl / (entry * qty) * 100 if entry * qty else 0
                paper_unrealized[trade["id"]] = raw_pnl

                hit_price: Optional[float] = None
                close_reason: Optional[str] = None

                # Check SL hit
                if direction == "LONG" and mark <= sl:
                    hit_price = sl
                    close_reason = "SL"
                elif direction == "SHORT" and mark >= sl:
                    hit_price = sl
                    close_reason = "SL"

                # Check TP2 hit (prioritise over TP1 if both hit same tick)
                if direction == "LONG" and mark >= tp2:
                    hit_price = tp2
                    close_reason = "TP2"
                elif direction == "SHORT" and mark <= tp2:
                    hit_price = tp2
                    close_reason = "TP2"
                elif direction == "LONG" and mark >= tp1:
                    hit_price = tp1
                    close_reason = "TP1"
                elif direction == "SHORT" and mark <= tp1:
                    hit_price = tp1
                    close_reason = "TP1"

                if hit_price and close_reason:
                    # Close the paper trade
                    if direction == "LONG":
                        final_pnl = (hit_price - entry) * qty
                    else:
                        final_pnl = (entry - hit_price) * qty
                    final_pct = final_pnl / (entry * qty) * 100 if entry * qty else 0
                    close_time = int(time.time() * 1000)

                    await db.update_trade_close(
                        trade_id=trade["id"],
                        close_price=hit_price,
                        close_time=close_time,
                        pnl_usdt=round(final_pnl, 4),
                        pnl_pct=round(final_pct, 2),
                    )
                    paper_unrealized.pop(trade["id"], None)
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
                    positions_payload.append({
                        **trade,
                        "mark_price": mark,
                        "unrealized_pnl": round(raw_pnl, 4),
                        "unrealized_pnl_pct": round(pnl_pct, 2),
                    })

            if positions_payload:
                await wsb.broadcaster.broadcast("position_update", positions_payload)

        except Exception as exc:
            logger.error("paper_pnl_loop error: %s", exc)


# ── Entry point ───────────────────────────────────────────────────────────────

async def start() -> None:
    """Start position tracking. Called from main.py startup."""
    if config.MODE == "live":
        if not config.BINANCE_API_KEY:
            logger.warning("No API key — skipping live position tracker")
            return
        try:
            listen_key = await bc.create_listen_key()
            asyncio.create_task(_run_user_data_ws(listen_key), name="user_data_ws")
            asyncio.create_task(_keepalive_listen_key(listen_key), name="listenkey_keepalive")
            logger.info("Live position tracker started (listenKey: %s...)", listen_key[:8])
        except Exception as exc:
            logger.error("Failed to start live position tracker: %s", exc)
    else:
        asyncio.create_task(_paper_pnl_loop(), name="paper_pnl_loop")
        logger.info("Paper position tracker started")
