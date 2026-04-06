"""
user_data_stream.py — Binance USDT-M User Data Stream.

Connects to the account event stream to detect live order fills.
When a SL/TP/trail order fills, closes the matching trade in the DB
and broadcasts a position update to connected UI clients.
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

# Order types that represent closing orders (SL, TP, trail)
_CLOSE_ORDER_TYPES = {"STOP_MARKET", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET"}

_CLOSE_REASON_MAP = {
    "STOP_MARKET":           "SL",
    "TAKE_PROFIT_MARKET":    "TP",
    "TRAILING_STOP_MARKET":  "TRAIL",
}


class UserDataStream:
    """Listens to Binance User Data Stream for order fill events."""

    def __init__(self) -> None:
        self._listen_key: Optional[str] = None
        self._recv_task: Optional[asyncio.Task] = None
        self._keepalive_task: Optional[asyncio.Task] = None
        self._running: bool = False

    async def start(self) -> None:
        """Create listen key, connect to stream, start recv and keepalive loops."""
        self._running = True
        self._listen_key = await bc.create_listen_key()
        logger.info("User data stream listen key obtained: %s...", self._listen_key[:8])
        self._recv_task = asyncio.ensure_future(self._recv_loop())
        self._keepalive_task = asyncio.ensure_future(self._keepalive_loop())
        logger.info("UserDataStream started")

    async def stop(self) -> None:
        """Stop recv and keepalive loops and clean up."""
        self._running = False
        for task in (self._recv_task, self._keepalive_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        logger.info("UserDataStream stopped")

    async def _keepalive_loop(self) -> None:
        """Send keepalive ping to Binance every 25 minutes."""
        while self._running:
            try:
                await asyncio.sleep(25 * 60)
                if self._listen_key:
                    await bc.keepalive_listen_key(self._listen_key)
                    logger.debug("User data stream listen key keepalive sent")
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("User data stream keepalive failed: %s", exc)

    async def _handle_event(self, event: dict) -> None:
        """Process a single stream event."""
        event_type = event.get("e")

        if event_type != "ORDER_TRADE_UPDATE":
            return

        order = event.get("o", {})
        status = order.get("X")       # execution status
        order_type = order.get("o")   # order type string

        # Only process filled closing orders
        if status != "FILLED" or order_type not in _CLOSE_ORDER_TYPES:
            return

        symbol = order.get("s", "")
        fill_price = float(order.get("L", 0) or 0)   # last fill price
        close_reason = _CLOSE_REASON_MAP.get(order_type, "FILLED")

        if not symbol or fill_price <= 0:
            logger.warning(
                "User data stream: missing symbol or fill price for %s order", order_type
            )
            return

        # Find an open live trade for this symbol
        open_trades = await db.get_open_trades()
        matching = [
            t for t in open_trades
            if t.get("symbol") == symbol and t.get("mode") == "live"
        ]
        if not matching:
            logger.debug(
                "User data stream: %s fill for %s but no open live trade found",
                order_type, symbol,
            )
            return

        trade = matching[0]
        trade_id = trade["id"]
        entry = float(trade["entry_price"])
        qty = float(trade["qty"])
        direction = trade["direction"]

        if direction == "LONG":
            gross = (fill_price - entry) * qty
        else:
            gross = (entry - fill_price) * qty

        taker_fee_rate = 0.0004
        pnl = gross - (entry * qty * taker_fee_rate) - (fill_price * qty * taker_fee_rate)
        pnl_pct = pnl / (entry * qty) * 100 if entry * qty else 0.0
        close_time = int(time.time() * 1000)

        await db.update_trade_close(
            trade_id=trade_id,
            close_price=fill_price,
            close_time=close_time,
            pnl_usdt=round(pnl, 4),
            pnl_pct=round(pnl_pct, 2),
            close_reason=close_reason,
        )

        await wsb.broadcaster.broadcast("position_closed", {
            "type": "position_closed",
            "trade_id": trade_id,
            "reason": close_reason,
        })

        await wsb.broadcaster.broadcast("trade_closed", {
            **trade,
            "close_price":  fill_price,
            "close_time":   close_time,
            "close_reason": close_reason,
            "pnl_usdt":     round(pnl, 4),
            "pnl_pct":      round(pnl_pct, 2),
        })

        logger.info(
            "Live trade closed via %s: #%d %s %s fill=%.6f pnl=%.4f (%s)",
            order_type, trade_id, symbol, direction, fill_price, pnl, close_reason,
        )

    async def _recv_loop(self) -> None:
        """
        Connect to user data stream and process events.
        Auto-reconnects with a new listen key on disconnect.
        """
        backoff = 2
        while self._running:
            try:
                if not self._listen_key:
                    self._listen_key = await bc.create_listen_key()
                    logger.info(
                        "User data stream: new listen key obtained: %s...",
                        self._listen_key[:8],
                    )

                url = f"{config.BINANCE_WS_BASE}/ws/{self._listen_key}"
                logger.info("User data stream: connecting to %s", url)

                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    backoff = 2
                    logger.info("User data stream connected")

                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            event = json.loads(raw)
                            await self._handle_event(event)
                        except Exception as exc:
                            logger.error("User data stream event error: %s", exc)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning(
                    "User data stream disconnected: %s — reconnect in %ds", exc, backoff
                )
                # Invalidate listen key so a fresh one is fetched on reconnect
                self._listen_key = None
                try:
                    await asyncio.sleep(backoff)
                except asyncio.CancelledError:
                    break
                backoff = min(backoff * 2, 30)


# Module-level singleton
user_data_stream = UserDataStream()
