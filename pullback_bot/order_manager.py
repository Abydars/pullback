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
import time
import uuid
from typing import Optional, Set, Tuple

import binance_client as bc
import config
import db

logger = logging.getLogger(__name__)

# ── Session tracking ─────────────────────────────────────────────────────────
# A session spans from the first trade open until a portfolio-level exit
# (PORT_TP_TRAIL, PORTFOLIO_TP, PORTFOLIO_SL, SMART_PORT_SL).
# reset_session() is called by position_tracker after _close_all_paper.
_active_session_id:      Optional[str] = None
_active_session_started: int           = 0


# Per-symbol in-flight guard — symbols currently being opened.
# Replaces the old global _open_lock so concurrent signals don't
# serialise all of their I/O through a single mutex.
#
# MAX_OPEN_TRADES is enforced via:  open_count + len(_opening) >= cap
# Duplicate-symbol is enforced via: symbol in _opening
#
# There is an accepted TOCTOU window between the fast DB reads and the
# _opening.add() call, but because handle_signal is now awaited
# concurrently (asyncio.gather in the flush loop) the window is only a
# few microseconds wide and the worst-case outcome is one extra trade
# above the cap per batch — far better than serialising 7-8 s of I/O.
_opening: set[str] = set()


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
    # vol coins to be governed entirely by MAX_LEVERAGE setting. 0.08% → raw_lev=50
    atr_pct  = max(0.08, (atr / entry) * 100)
    raw_lev  = int(4.0 / atr_pct)
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

    async def handle_signal(self, signal: dict) -> Tuple[bool, str]:
        """
        Act on a validated signal dict.
        Returns (acted: bool, reason: str).

        Guards (all checked before registering in-flight):
          1. Per-symbol: skip if symbol is already being opened
          2. Capacity:   skip if open + in-flight >= MAX_OPEN_TRADES
          3. Duplicate:  skip if a position is already open for this symbol
          4. Cooldown:   skip if within SYMBOL_COOLDOWN_MINUTES of last close

        No global lock is held — guards are enforced with fast DB reads and
        the module-level _opening set so concurrent signals can proceed
        through their slow I/O (set_leverage, place_order) in parallel.
        """
        symbol      = signal["symbol"]
        direction   = signal["direction"]
        entry       = signal["entry_price"]
        sl          = signal["sl_price"]
        tp1         = signal["tp1_price"]
        tp2         = signal["tp2_price"]
        score       = signal["score"]
        signal_type = signal.get("signal_type", "PULLBACK")

        # ── -1. Master Trade Switch Guard ───────────────────────────────────────
        if getattr(config, "TRADING_ENABLED", True) is False:
            logger.info("Signal %s %s skipped — Trading is currently PAUSED globally", symbol, direction)
            return False, "Trading is OFF"

        # ── 0. Portfolio Trailing Guard (Wait-and-See) ────────────────────────
        import sys
        _pt = sys.modules.get("position_tracker")
        if _pt and getattr(_pt, "_portfolio_trail_armed", False):
            logger.info("Signal %s %s skipped — Portfolio Trail is ARMED", symbol, direction)
            return False, "Portfolio Trail is currently ARMED (waiting for resolution)"

        # ── 0.5. ML Smart Filter Guard ──────────────────────────────────────────
        if not signal.get("ml_passed", True):
            conf = signal.get("ml_confidence", 0.0)
            logger.info("Signal %s %s skipped — ML Filter rejected (%.2f < threshold)", symbol, direction, conf)
            return False, f"ML Filter rejected ({conf:.2f} < threshold)"

        # ── 1. Per-symbol in-flight guard (no await, immediate) ───────────────
        if symbol in _opening:
            logger.info("Signal %s %s skipped — already opening this symbol", symbol, direction)
            return False, "Signal overlap: Position already currently opening"

        # ── 2. Capacity check ─────────────────────────────────────────────────
        # Count DB open trades + in-flight together so concurrent signals
        # cannot all pass this check before any of them inserts.
        open_count = await db.count_open_trades()
        if open_count + len(_opening) >= config.MAX_OPEN_TRADES:
            logger.info(
                "Signal %s %s skipped — MAX_OPEN_TRADES (%d) reached "
                "(open=%d in-flight=%d)",
                symbol, direction, config.MAX_OPEN_TRADES,
                open_count, len(_opening),
            )
            return False, "MAX_OPEN_TRADES capacity reached"

        # ── 3. Duplicate symbol guard ─────────────────────────────────────────
        open_trades = await db.get_open_trades()
        if any(t["symbol"] == symbol for t in open_trades):
            logger.info("Signal %s %s skipped — position already open", symbol, direction)
            return False, "Position already currently open for symbol"

        # ── 4. Per-symbol cooldown ────────────────────────────────────────────
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
                    return False, f"Cooldown block: {remaining}m remaining"

        # ── 5. Calculate qty and leverage ─────────────────────────────────────
        step      = bc.get_step_size(symbol)
        atr       = signal.get("atr",       abs(entry - sl) / 1.5)
        atr_ratio = signal.get("atr_ratio", 1.0)
        raw_qty, leverage = await _calc_qty_and_leverage(
            entry, sl, atr, atr_ratio, score
        )
        qty = bc.round_step(raw_qty, step)
        if qty <= 0:
            logger.warning("Calculated qty=0 for %s, skipping", symbol)
            return False, "Calculated Risk Quantity <= 0 (Margin/Lev cap too tight)"

        now_ms = int(time.time() * 1000)

        # ── 6. Register in-flight, then open (no lock held across I/O) ───────
        _opening.add(symbol)
        try:
            # ── Session start ────────────────────────────────────────────────
            global _active_session_id, _active_session_started
            if _active_session_id is None:
                _active_session_id      = uuid.uuid4().hex
                _active_session_started = int(time.time() * 1000)
                await db.create_session(_active_session_id, _active_session_started)
                logger.info("Session started: %s", _active_session_id)

            if config.MODE == "paper":
                return await self._paper_open(
                    symbol, direction, entry, sl, tp1, tp2,
                    qty, leverage, now_ms, score, signal_type,
                    session_id=_active_session_id,
                    ml_confidence=signal.get("ml_confidence"),
                )
            else:
                return await self._live_open(
                    symbol, direction, entry, sl, tp1, tp2,
                    qty, leverage, now_ms, score, signal_type,
                    atr=atr,
                    session_id=_active_session_id,
                    ml_confidence=signal.get("ml_confidence"),
                )
        finally:
            _opening.discard(symbol)

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
        session_id: Optional[str] = None,
        ml_confidence: Optional[float] = None,
    ) -> Tuple[bool, str]:
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
            session_id=session_id,
            ml_confidence=ml_confidence,
        )
        notional = entry * qty
        logger.info(
            "Paper trade opened: #%d %s %s entry=%.6f sl=%.6f qty=%g "
            "notional=%.2f margin=%.2f leverage=%dx risk=%.2f score=%d",
            trade_id, symbol, direction, entry, sl, qty,
            notional, notional / leverage, leverage, abs(entry - sl) * qty, score,
        )
        return True, "Trade Opened"

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
        session_id: Optional[str] = None,
        ml_confidence: Optional[float] = None,
    ) -> Tuple[bool, str]:
        tick    = bc.get_tick_size(symbol)
        side    = "BUY" if direction == "LONG" else "SELL"
        sl_side = "SELL" if direction == "LONG" else "BUY"

        # ── Steps 1 & 2: leverage + entry — abort entirely on failure ──────────
        # Only here is it safe to return False; the position doesn't exist yet.
        try:
            await bc.set_leverage(symbol, leverage)
            order = await bc.place_market_order(
                symbol=symbol, side=side, qty=qty
            )
            binance_order_id = str(order.get("orderId", ""))
            actual_entry     = float(order.get("avgPrice") or entry)
        except Exception as exc:
            logger.error("Live entry failed for %s: %s", symbol, exc)
            return False, f"Live Entry Error: {exc}"

        # ── Step 3: Catastrophic Stop Loss (Safety Net Only) ──────────────────
        # Actual SL and TP management is handled virtually by position_tracker.py
        # Here we place a 2.5x ATR physical stop just in case the server crashes.
        if config.USE_STOP_LOSS:
            try:
                # Normal SL is 1.5x ATR. Catastrophic is 2.5x ATR.
                catastrophic_sl = actual_entry - (atr * 2.5) if direction == "LONG" else actual_entry + (atr * 2.5)
                # Ensure we don't go below 0 for longs
                if catastrophic_sl <= 0 and direction == "LONG":
                    catastrophic_sl = tick  # min positive value

                await bc.place_stop_market_order(
                    symbol=symbol, side=sl_side, stop_price=bc.round_step(catastrophic_sl, tick), close_position=True
                )
                logger.info("Placed catastrophic safety SL for %s at %.4f", symbol, catastrophic_sl)
            except Exception as exc:
                logger.error(
                    "Catastrophic SL placement failed for %s (order %s). "
                    "Position is UNPROTECTED from flash crashes: %s",
                    symbol, binance_order_id, exc,
                )

        # ── Step 5: DB insert — always reached after a successful entry ───────
        try:
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
                session_id=session_id,
                ml_confidence=ml_confidence,
            )
            logger.info(
                "Live trade opened: #%d %s %s entry=%.6f leverage=%dx order=%s",
                trade_id, symbol, direction, actual_entry, leverage, binance_order_id,
            )
            return True, "Trade Opened"
        except Exception as exc:
            logger.error(
                "CRITICAL: DB insert failed for %s (order %s) — "
                "position exists on exchange but NOT in database: %s",
                symbol, binance_order_id, exc,
            )
            return False, "Failed DB insert (Warning: Ghost trade created!)"


async def restore_session() -> None:
    """
    On server restart, if open trades already carry a session_id, resume
    that session instead of creating a new one for the next signal.
    Called once from main.py startup before the scanner starts.
    """
    global _active_session_id, _active_session_started

    open_trades = await db.get_open_trades()
    session_ids = list(set(
        t["session_id"] for t in open_trades
        if t.get("session_id")
    ))

    if not session_ids:
        return

    if len(session_ids) > 1:
        logger.warning(
            "restore_session: multiple session IDs in open trades %s — using most recent",
            session_ids,
        )

    _active_session_id = session_ids[-1]
    logger.info(
        "Session restored after restart: %s (%d open trade(s))",
        _active_session_id, len(open_trades),
    )


async def reset_session() -> None:
    """Clear active session state after a portfolio exit. Idempotent."""
    global _active_session_id, _active_session_started
    _active_session_id      = None
    _active_session_started = 0


# Module-level singleton
order_manager = OrderManager()
