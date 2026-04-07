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

import asyncio
import logging
import time
from typing import Optional

import binance_client as bc
import config
import db
from ws_order_api import ws_order_api

logger = logging.getLogger(__name__)

# Serialise all handle_signal calls so the "count open → insert" sequence is
# atomic.  Without this, multiple signals firing on the same candle close all
# read count=N before any insert commits, bypassing MAX_OPEN_TRADES.
_open_lock = asyncio.Lock()


async def _calc_qty_and_leverage(
    entry: float,
    sl: float,
    atr: float,
    atr_ratio: float,
    score: int,
) -> tuple[float, int]:
    """
    Smart position sizing with four compounding adjustments:

    1. SMOOTH LEVERAGE  — continuous formula: lev = floor(4 / atr_pct)
       Low ATR → high leverage; high ATR → low leverage.  No hard buckets.

    2. SCORE SCALING    — risk_amount × (score / 100)
       A score-70 signal risks 70 % of target; score-100 risks the full amount.

    3. ATR REGIME       — risk_amount × min(1, 1 / atr_ratio)
       If current ATR is 2× its 20-bar average the size is halved, protecting
       against sudden volatility spikes.

    4. DRAWDOWN GUARD   — reduces risk when realized PnL is negative
       > -5 %  account loss → 75 %
       > -10 % → 50 %
       > -15 % → 25 %
    """
    sl_distance = abs(entry - sl)
    if sl_distance <= 0 or entry <= 0 or atr <= 0:
        return 0.0, config.MAX_LEVERAGE

    max_lev  = max(1, config.MAX_LEVERAGE)
    capital  = config.CAPITAL
    risk_pct = config.RISK_PCT

    if capital <= 0 or risk_pct <= 0:
        return 0.0, max_lev

    # ── 1. Smooth leverage ────────────────────────────────────────────────────
    # leverage = floor(4 / atr_pct) — e.g. ATR=1% → 4x, ATR=0.5% → 8x,
    # ATR=0.2% → 20x.  Constant 4 keeps leverage conservatively low at all
    # volatility levels while still scaling smoothly.
    atr_pct  = (atr / entry) * 100          # e.g. 1.0 for 1 %
    raw_lev  = int(4.0 / atr_pct) if atr_pct > 0 else max_lev
    raw_lev  = max(1, min(raw_lev, max_lev))

    # ── 2. Score scaling ──────────────────────────────────────────────────────
    # Score scales both position size AND leverage — a low-confidence signal
    # should not be allowed to use high leverage even on a calm instrument.
    # score=100 → full raw_lev; score=65 → 65% of raw_lev; floor at 50%.
    score_scale = max(0.5, score / 100.0)   # floor at 50 % to avoid micro sizes
    leverage = max(1, int(raw_lev * score_scale))

    # ── 3. ATR regime ─────────────────────────────────────────────────────────
    # atr_ratio > 1 means current ATR is elevated vs its 20-bar average.
    # Scale inversely — don't increase size when market is unusually calm.
    regime_scale = min(1.0, 1.0 / max(1.0, atr_ratio))

    # ── 4. Drawdown guard ─────────────────────────────────────────────────────
    realized_pnl  = await db.get_realized_pnl()
    drawdown_pct  = (realized_pnl / capital) * 100   # negative = loss
    if drawdown_pct <= -15:
        drawdown_scale = 0.25
    elif drawdown_pct <= -10:
        drawdown_scale = 0.50
    elif drawdown_pct <= -5:
        drawdown_scale = 0.75
    else:
        drawdown_scale = 1.0

    # ── Combine all scales into final risk amount ──────────────────────────────
    base_risk    = capital * risk_pct / 100
    risk_amount  = base_risk * score_scale * regime_scale * drawdown_scale

    sl_distance_pct = sl_distance / entry
    notional        = risk_amount / sl_distance_pct
    margin_cap      = capital / max(1, config.MAX_OPEN_TRADES)
    notional        = min(notional, margin_cap * leverage)

    qty = notional / entry

    logger.info(
        "Sizing: lev=%dx atr_pct=%.2f%% score_scale=%.2f regime_scale=%.2f "
        "drawdown_scale=%.2f → risk=$%.2f (base=$%.2f) notional=$%.2f",
        leverage, atr_pct, score_scale, regime_scale, drawdown_scale,
        risk_amount, base_risk, notional,
    )

    return qty, int(leverage)


class OrderManager:
    """Handles signal → order flow for both live and paper modes."""

    async def handle_signal(self, signal: dict) -> bool:
        """
        Act on a validated signal dict.
        Returns True if a trade was opened, False otherwise.
        """
        async with _open_lock:
            return await self._handle_signal_locked(signal)

    async def _handle_signal_locked(self, signal: dict) -> bool:
        """Called under _open_lock — check + insert are atomic."""
        symbol      = signal["symbol"]
        direction   = signal["direction"]
        entry       = signal["entry_price"]
        sl          = signal["sl_price"]
        tp1         = signal["tp1_price"]
        tp2         = signal["tp2_price"]
        score       = signal["score"]
        signal_type = signal.get("signal_type", "PULLBACK")

        # Max open trades guard — re-checked inside the lock so concurrent
        # signals can't all pass before any insert commits.
        open_count = await db.count_open_trades()
        if open_count >= config.MAX_OPEN_TRADES:
            logger.info(
                "Signal %s %s skipped — MAX_OPEN_TRADES (%d) reached",
                symbol, direction, config.MAX_OPEN_TRADES,
            )
            return False

        # Duplicate symbol guard — one open position per symbol at a time.
        open_trades = await db.get_open_trades()
        if any(t["symbol"] == symbol for t in open_trades):
            logger.info("Signal %s %s skipped — position already open", symbol, direction)
            return False

        # Per-symbol cooldown — block re-entry for SYMBOL_COOLDOWN_MINUTES after
        # any close (SL, trail, portfolio stop, manual).  close_time is written
        # by update_trade_close() on all close paths so no explicit write needed.
        cooldown_min = config.SYMBOL_COOLDOWN_MINUTES
        if cooldown_min > 0:
            last_close_ms = await db.get_last_close_time(symbol)
            if last_close_ms is not None:
                elapsed_min = (time.time() * 1000 - last_close_ms) / 60_000
                if elapsed_min < cooldown_min:
                    remaining = int(cooldown_min - elapsed_min)
                    logger.info(
                        "Signal %s %s skipped — cooldown active (%dm remaining)",
                        symbol, direction, remaining,
                    )
                    return False

        # Calculate qty and leverage
        step      = bc.get_step_size(symbol)
        atr       = signal.get("atr",       abs(entry - sl) / 1.5)
        atr_ratio = signal.get("atr_ratio", 1.0)
        raw_qty, leverage = await _calc_qty_and_leverage(
            entry, sl, atr, atr_ratio, score
        )
        qty = bc.round_step(raw_qty, step)
        if qty <= 0:
            logger.warning("Calculated qty=0 for %s, skipping", symbol)
            return False

        now_ms = int(time.time() * 1000)

        if config.MODE == "paper":
            return await self._paper_open(
                symbol, direction, entry, sl, tp1, tp2, qty, leverage, now_ms, score, signal_type
            )
        else:
            return await self._live_open(
                symbol, direction, entry, sl, tp1, tp2, qty, leverage, now_ms, score, signal_type,
                atr=atr,
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
        leverage: int,
        now_ms: int,
        score: int,
        signal_type: str = "PULLBACK",
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
            leverage=leverage,
            signal_type=signal_type,
        )
        notional = entry * qty
        logger.info(
            "Paper trade opened: #%d %s %s entry=%.6f sl=%.6f qty=%g "
            "notional=%.2f margin=%.2f leverage=%dx risk=%.2f score=%d",
            trade_id, symbol, direction, entry, sl, qty,
            notional, notional / leverage, leverage, abs(entry - sl) * qty, score,
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
        leverage: int,
        now_ms: int,
        score: int,
        signal_type: str = "PULLBACK",
        atr: float = 0.0,
    ) -> bool:
        try:
            tick = bc.get_tick_size(symbol)
            side = "BUY" if direction == "LONG" else "SELL"
            sl_side = "SELL" if direction == "LONG" else "BUY"

            # 1. Set leverage (REST only — WS API doesn't support this)
            await bc.set_leverage(symbol, leverage)

            # 2. Market entry via WS API
            order = await ws_order_api.place_order(
                symbol=symbol, side=side, type="MARKET", quantity=qty,
            )
            binance_order_id = str(order.get("orderId", ""))
            actual_entry = float(order.get("avgPrice") or entry)

            # 3. Stop Loss via WS API (skipped when USE_STOP_LOSS=false)
            if config.USE_STOP_LOSS:
                await ws_order_api.place_order(
                    symbol=symbol, side=sl_side, type="STOP_MARKET",
                    stopPrice=bc.round_step(sl, tick),
                    closePosition="true",
                )

            # 4. Trail or fixed TP via WS API (skipped when USE_TAKE_PROFIT=false)
            if config.USE_TAKE_PROFIT:
                if config.USE_TRAILING and atr > 0:
                    # Native trailing stop — activates at trail_arm, trails at 1×ATR distance
                    atr_pct = max(0.1, min(10.0, (atr / actual_entry) * 100))
                    await ws_order_api.place_order(
                        symbol=symbol, side=sl_side, type="TRAILING_STOP_MARKET",
                        activationPrice=bc.round_step(tp1, tick),
                        callbackRate=round(atr_pct, 1),
                        closePosition="true",
                    )
                else:
                    await ws_order_api.place_order(
                        symbol=symbol, side=sl_side, type="TAKE_PROFIT_MARKET",
                        stopPrice=bc.round_step(tp1, tick),
                        closePosition="true",
                    )

            # 5. Record in DB
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
                leverage=leverage,
                binance_order_id=binance_order_id,
                signal_type=signal_type,
            )
            logger.info(
                "Live trade opened: #%d %s %s entry=%.6f leverage=%dx order=%s",
                trade_id, symbol, direction, actual_entry, leverage, binance_order_id,
            )
            return True

        except Exception as exc:
            logger.error("Live order failed for %s: %s", symbol, exc)
            return False


# Module-level singleton
order_manager = OrderManager()
