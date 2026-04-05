"""
order_manager.py — Entry, SL, TP order placement.

LIVE:
  1. Set leverage for the symbol.
  2. Place MARKET entry order.
  3. Place STOP_MARKET (SL) and TAKE_PROFIT_MARKET (TP2) orders.
  4. Record trade in DB with binance_order_id.

PAPER:
  1. Calculate position size.
  2. Record virtual trade in DB (no API calls).

Both modes respect MAX_OPEN_TRADES cap.
"""
from __future__ import annotations

import logging
import math
import time
from typing import Optional

import binance_client as bc
import config
import db

logger = logging.getLogger(__name__)


def _calc_qty(entry: float, sl: float) -> float:
    """Calculate position size from risk USDT and entry/SL distance."""
    risk = abs(entry - sl)
    if risk <= 0:
        return 0.0
    return config.RISK_PER_TRADE_USDT / risk


class OrderManager:
    """Handles signal → order flow for both live and paper modes."""

    async def handle_signal(self, signal: dict) -> bool:
        """
        Act on a validated signal dict.
        Returns True if a trade was opened, False otherwise.
        """
        symbol = signal["symbol"]
        direction = signal["direction"]
        entry = signal["entry_price"]
        sl = signal["sl_price"]
        tp1 = signal["tp1_price"]
        tp2 = signal["tp2_price"]
        score = signal["score"]

        # Max open trades guard
        open_count = await db.count_open_trades()
        if open_count >= config.MAX_OPEN_TRADES:
            logger.info(
                "Signal %s %s skipped — MAX_OPEN_TRADES (%d) reached",
                symbol, direction, config.MAX_OPEN_TRADES,
            )
            return False

        # Calculate qty
        step = bc.get_step_size(symbol)
        raw_qty = _calc_qty(entry, sl)
        qty = bc.round_step(raw_qty, step)
        if qty <= 0:
            logger.warning("Calculated qty=0 for %s, skipping", symbol)
            return False

        now_ms = int(time.time() * 1000)

        if config.MODE == "paper":
            return await self._paper_open(
                symbol, direction, entry, sl, tp1, tp2, qty, now_ms, score
            )
        else:
            return await self._live_open(
                symbol, direction, entry, sl, tp1, tp2, qty, now_ms, score
            )

    # ── Paper mode ─────────────────────────────────────────────────────────────

    async def _paper_open(
        self,
        symbol: str,
        direction: str,
        entry: float,
        sl: float,
        tp1: float,
        tp2: float,
        qty: float,
        now_ms: int,
        score: int,
    ) -> bool:
        trade_id = await db.insert_trade(
            symbol=symbol,
            direction=direction,
            entry_price=entry,
            sl_price=sl,
            tp1_price=tp1,
            tp2_price=tp2,
            qty=qty,
            mode="paper",
            entry_time=now_ms,
            signal_score=score,
        )
        logger.info(
            "Paper trade opened: #%d %s %s entry=%.6f sl=%.6f tp2=%.6f qty=%g score=%d",
            trade_id, symbol, direction, entry, sl, tp2, qty, score,
        )
        return True

    # ── Live mode ──────────────────────────────────────────────────────────────

    async def _live_open(
        self,
        symbol: str,
        direction: str,
        entry: float,
        sl: float,
        tp1: float,
        tp2: float,
        qty: float,
        now_ms: int,
        score: int,
    ) -> bool:
        try:
            # 1. Set leverage
            await bc.set_leverage(symbol, config.LEVERAGE)

            # 2. Market entry
            side = "BUY" if direction == "LONG" else "SELL"
            order = await bc.place_market_order(symbol, side, qty)
            binance_order_id = str(order.get("orderId", ""))
            actual_entry = float(order.get("avgPrice") or entry)

            # 3. SL order (opposite side, reduce-only)
            sl_side = "SELL" if direction == "LONG" else "BUY"
            tick = bc.get_tick_size(symbol)
            sl_price_rounded = bc.round_step(sl, tick)
            tp2_price_rounded = bc.round_step(tp2, tick)

            await bc.place_stop_market_order(symbol, sl_side, sl_price_rounded)
            await bc.place_take_profit_market_order(symbol, sl_side, tp2_price_rounded)

            # 4. Record in DB
            trade_id = await db.insert_trade(
                symbol=symbol,
                direction=direction,
                entry_price=actual_entry,
                sl_price=sl,
                tp1_price=tp1,
                tp2_price=tp2,
                qty=qty,
                mode="live",
                entry_time=now_ms,
                signal_score=score,
                binance_order_id=binance_order_id,
            )
            logger.info(
                "Live trade opened: #%d %s %s entry=%.6f order=%s",
                trade_id, symbol, direction, actual_entry, binance_order_id,
            )
            return True

        except Exception as exc:
            logger.error("Live order failed for %s: %s", symbol, exc)
            return False


# Module-level singleton
order_manager = OrderManager()
