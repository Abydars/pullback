"""
scanner.py — Background scanner loop.

Responsibilities:
  1. Activity filter: builds the active watchlist from Binance exchange info + 24h tickers.
     Runs once at startup then refreshes every WATCHLIST_REFRESH_MINUTES.
  2. Kline WebSocket subscription: combined multi-stream for all watchlist symbols.
     Streams: <symbol>@kline_1m, <symbol>@kline_5m, <symbol>@kline_15m
  3. Scanner loop: for each symbol keeps a rolling kline buffer; calls
     signal_engine.check_pullback on every confirmed 15m candle close.
  4. On valid signal: logs to DB, broadcasts via ws_broadcaster, calls
     order_manager.handle_signal.
"""
from __future__ import annotations

import asyncio
import datetime
import json
import logging
import time
from collections import defaultdict
from typing import Optional

import numpy as np

import websockets

import binance_client as bc
import config
import db
import signal_engine
import ws_broadcaster as wsb

logger = logging.getLogger(__name__)

# ── State ──────────────────────────────────────────────────────────────────────
# active_watchlist: list of symbol strings currently being scanned
active_watchlist: list[str] = []

# Task handle for the kline WS — kept so refresh_watchlist_loop can cancel
# and restart it when the symbol set changes.
_kline_ws_task: Optional[asyncio.Task] = None

# Binance hard limit: 1024 streams per WS connection; we use 3 per symbol
# (1m/5m/15m), so we must never exceed 341 symbols on a single connection.
_MAX_WS_SYMBOLS = 1024 // 3  # 341

# kline buffers: symbol -> interval -> list[candle_dict]
# Each candle dict: {open, high, low, close, volume}
_kline_buffers: dict[str, dict[str, list[dict]]] = defaultdict(
    lambda: {"1m": [], "5m": [], "15m": []}
)

# Cooldown: minimum seconds between signals for the same symbol
_last_signal_ts: dict[str, float] = {}
_SIGNAL_COOLDOWN_S = 300  # 5 min — one signal per 15m candle at most

# Signal batch queue — signals from concurrent 15m closes are held here
# for a short window, then flushed in score order so the best signal gets
# the first trade slot rather than whichever symbol's WS message arrived first.
_pending_signals: list[dict] = []
_flush_task: Optional[asyncio.Task] = None

# reference to order_manager (set by main.py to avoid circular import)
_order_manager = None

# BTC regime — tracks last broadcast value to avoid spamming
_last_btc_regime: str = "NEUTRAL"


def set_order_manager(om) -> None:
    global _order_manager
    _order_manager = om


# ── Watchlist builder ─────────────────────────────────────────────────────────

async def build_watchlist() -> list[str]:
    """
    Fetch active USDT-M perpetuals, apply volume + movement filter.
    Returns list of symbol strings.
    """
    logger.info("Building watchlist...")
    try:
        if config.SIGNAL_MODE == "funding_predator":
            rates = await bc.get_all_premium_indices()
            candidates = []
            for r in rates:
                fr = float(r.get("lastFundingRate", 0))
                if fr >= config.FUNDING_PREDATOR_THRESHOLD:
                    candidates.append((r["symbol"], fr))
            candidates.sort(key=lambda x: x[1], reverse=True)
            watchlist = [c[0] for c in candidates]
            # Fallback for UI if no assets mathematically breach the custom extreme threshold right now
            if not watchlist:
                 all_rates = sorted(rates, key=lambda x: float(x.get("lastFundingRate", 0)), reverse=True)
                 watchlist = [r["symbol"] for r in all_rates[:15]]
            logger.info("Funding Predator Watchlist built: %d symbols", len(watchlist))
            return watchlist
            
        perpetuals = await bc.get_active_perpetual_symbols()
        perp_set = {s["symbol"] for s in perpetuals}

        tickers = await bc.get_24h_tickers()
        watchlist: list[str] = []
        for t in tickers:
            sym = t["symbol"]
            if sym not in perp_set:
                continue
            vol = float(t.get("quoteVolume", 0))
            chg = abs(float(t.get("priceChangePercent", 0)))
            if vol >= config.MIN_VOLUME_24H and chg >= config.MIN_PRICE_CHANGE_PCT:
                watchlist.append(sym)

        watchlist.sort()
        logger.info("Watchlist built: %d symbols", len(watchlist))
        return watchlist
    except Exception as exc:
        logger.error("build_watchlist error: %s", exc)
        return active_watchlist  # keep old list on error


async def _auto_train_missing_models(symbols: list[str]) -> None:
    """Check if newly added symbols have trained models; if not, spawn a background thread to train them."""
    if not getattr(config, "ML_FILTER_ENABLED", False):
        return
        
    import os
    from train_ml_model import train_for_symbol
    
    missing = []
    models_dir = os.path.join(os.path.dirname(__file__), "models")
    for s in symbols:
        model_path = os.path.join(models_dir, f"{s}_model.pkl")
        if not os.path.exists(model_path):
            missing.append(s)
            
    if not missing:
        return
        
    logger.info("Found %d untrained symbols. Spawning background ML training...", len(missing))
    
    def _train_batch():
        for sym in missing:
            try:
                train_for_symbol(sym)
            except Exception as e:
                logger.error("Auto-train failed for %s: %s", sym, e)
                
    asyncio.create_task(asyncio.to_thread(_train_batch))


async def refresh_watchlist_loop() -> None:
    """Refresh the active_watchlist every WATCHLIST_REFRESH_MINUTES."""
    global active_watchlist, _kline_ws_task
    while True:
        new_watchlist = await build_watchlist()

        old_set = set(active_watchlist)
        new_set = set(new_watchlist)

        # Clean up buffers for symbols that dropped off the watchlist.
        # BTCUSDT is always excluded — its kline buffer is required for
        # BTC regime detection regardless of watchlist state.
        dropped = (old_set - new_set) - {"BTCUSDT"}
        added   = new_set - old_set
        for sym in dropped:
            _kline_buffers.pop(sym, None)
            _last_signal_ts.pop(sym, None)
            mark_prices.pop(sym, None)
        if dropped:
            logger.debug(
                "Watchlist cleanup: removed %d dropped symbol(s) from buffers",
                len(dropped),
            )

        # Update module-level list before seeding so _run_kline_ws reads the
        # fresh set on its next iteration.
        active_watchlist = new_watchlist
        await wsb.broadcaster.broadcast(
            "scanner_watchlist",
            {"symbols": active_watchlist, "count": len(active_watchlist)},
        )

        # Seed historical klines for genuinely new symbols so signal_engine
        # has enough candle history immediately.
        if added:
            added_list = sorted(added)
            logger.info(
                "New watchlist symbols (%d): seeding klines — %s%s",
                len(added_list),
                ", ".join(added_list[:10]),
                f" … +{len(added_list) - 10}" if len(added_list) > 10 else "",
            )
            for i in range(0, len(added_list), 10):
                batch = added_list[i : i + 10]
                await asyncio.gather(*[_seed_klines(sym) for sym in batch])
                await asyncio.sleep(0.5)

        # If the symbol set changed, restart the kline WS so it subscribes
        # to the updated stream URL.  The new task reads active_watchlist
        # directly and will not re-seed (added symbols were just seeded above).
        if added or dropped:
            if _kline_ws_task and not _kline_ws_task.done():
                _kline_ws_task.cancel()
                try:
                    await _kline_ws_task
                except (asyncio.CancelledError, Exception):
                    pass
            _kline_ws_task = asyncio.create_task(_run_kline_ws(), name="kline_ws")
            logger.info(
                "Kline WS restarted for updated watchlist "
                "(%d added, %d dropped, %d total)",
                len(added), len(dropped), len(active_watchlist),
            )

        # Automatically train ML models for any newly added symbols that lack one.
        if added:
            asyncio.create_task(_auto_train_missing_models(list(added)))

        await asyncio.sleep(config.WATCHLIST_REFRESH_MINUTES * 60)


# ── Kline REST seed (initial fill before WS) ───────────────────────────────────

async def _seed_klines(symbol: str) -> None:
    """Pre-fill kline buffers from REST API for a symbol."""
    for interval, limit in [("15m", 500), ("5m", 60), ("1m", 60)]:
        try:
            raw = await bc.get_klines(symbol, interval, limit)
            candles = [
                {
                    "time":   int(c[0]) // 1000,   # ms → seconds, matches WS candle format
                    "open":   float(c[1]),
                    "high":   float(c[2]),
                    "low":    float(c[3]),
                    "close":  float(c[4]),
                    "volume": float(c[5]),
                }
                for c in raw
            ]
            _kline_buffers[symbol][interval] = candles
        except Exception as exc:
            logger.warning("Seed klines %s/%s: %s", symbol, interval, exc)


# ── WebSocket kline stream ─────────────────────────────────────────────────────

def _make_stream_url(symbols: list[str]) -> str:
    """Build combined stream URL for 1m/5m/15m klines."""
    streams = []
    for sym in symbols:
        s = sym.lower()
        streams += [f"{s}@kline_1m", f"{s}@kline_5m", f"{s}@kline_15m"]
    combined = "/".join(streams)
    return f"{config.BINANCE_WS_BASE}/stream?streams={combined}"


def _parse_kline_msg(msg: dict) -> Optional[tuple[str, str, dict, bool]]:
    """
    Parse a combined stream kline message.
    Returns (symbol, interval, candle_dict, is_closed) or None.
    candle_dict includes 'time' in UNIX seconds (for Lightweight Charts).
    """
    data = msg.get("data", {})
    if data.get("e") != "kline":
        return None
    k = data["k"]
    candle = {
        "time": int(k["t"]) // 1000,   # ms -> seconds
        "open": float(k["o"]),
        "high": float(k["h"]),
        "low": float(k["l"]),
        "close": float(k["c"]),
        "volume": float(k["v"]),
    }
    return data["s"], k["i"], candle, k["x"]  # x=True when candle closed


def _compute_ema_tail(buf: list[dict], period: int) -> float:
    """
    Lightweight pure-Python EMA of closes in buf.
    Never creates a pandas Series — safe to call on the event loop.
    """
    if len(buf) < period:
        return 0.0
    k = 2.0 / (period + 1)
    ema = buf[0]["close"]
    for candle in buf[1:]:
        ema = candle["close"] * k + ema * (1 - k)
    return ema


def _update_buffer(symbol: str, interval: str, candle: dict, is_closed: bool) -> None:
    """Update the in-memory kline buffer for a symbol/interval.

    Match by candle open-time so that:
    - In-progress ticks update buf[-1] in place (no duplicate entry).
    - A closing tick updates buf[-1] in place (no double-append of the same candle).
    - The first tick of a genuinely new candle appends correctly.
    """
    buf = _kline_buffers[symbol][interval]
    if buf and buf[-1]["time"] == candle["time"]:
        buf[-1] = candle
    else:
        buf.append(candle)
        # Keep only what we need (500 for 15m — EMA200 needs ~2.5× period for
        # stable burn-in; 60 for 5m; 10 for 1m which is only used for mark-price
        # stable burn-in; 60 for 5m; 60 for 1m (for micro-scalp history)
        limit = {"15m": 500, "5m": 60, "1m": 60}.get(interval, 60)
        if len(buf) > limit:
            _kline_buffers[symbol][interval] = buf[-limit:]


async def _run_kline_ws_shard(symbols: list[str], shard_id: int) -> None:
    """Sub-task for handling a partitioned chunk of symbols in a WebSocket."""
    backoff = 1
    needs_reseed = False   # True only after an unexpected disconnect
    while True:
        try:
            # After an unexpected disconnect, re-seed to fill any candle
            # gaps that accumulated while the WS was down.
            if needs_reseed and symbols:
                logger.info(
                    "Kline WS reconnect: re-seeding %d symbols to patch gap...",
                    len(symbols),
                )
                for i in range(0, len(symbols), 10):
                    batch = symbols[i : i + 10]
                    await asyncio.gather(*[_seed_klines(sym) for sym in batch])
                    await asyncio.sleep(0.5)
                needs_reseed = False

            url = _make_stream_url(symbols)
            logger.info("Connecting kline WS (%d symbols)...", len(symbols))

            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                backoff = 1
                logger.info("Kline WS connected")
                async for raw in ws:
                    msg = json.loads(raw)
                    parsed = _parse_kline_msg(msg)
                    if parsed:
                        sym, interval, candle, is_closed = parsed
                        _update_buffer(sym, interval, candle, is_closed)
                        
                        # ZERO-LATENCY DISPATCH: Route the signal engine hook dynamically based on active mode BEFORE broadcasting
                        if is_closed:
                            if config.SIGNAL_MODE == "micro_scalp" and interval == "1m":
                                asyncio.create_task(_evaluate_symbol(sym), name=f"eval_{sym}")
                            elif config.SIGNAL_MODE != "micro_scalp" and interval == "5m":
                                asyncio.create_task(_evaluate_symbol(sym), name=f"eval_{sym}")
                                
                        # Broadcast to any UI client subscribed to this symbol/interval
                        buf = _kline_buffers[sym][interval]
                        ema50 = _compute_ema_tail(buf, 50) if len(buf) >= 50 else 0.0
                        ema200 = _compute_ema_tail(buf, 200) if len(buf) >= 200 else 0.0
                        await wsb.broadcaster.broadcast_kline(
                            sym, interval,
                            {
                                "symbol": sym,
                                "interval": interval,
                                "candle": candle,
                                "ema50": round(ema50, 8),
                                "ema200": round(ema200, 8),
                                "is_closed": is_closed,
                            },
                        )
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("Kline WS Shard %d error: %s — reconnect in %ds", shard_id, exc, backoff)
            needs_reseed = True
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

async def _run_kline_ws() -> None:
    """Manager loop that spawns sharded connections for the active watchlist."""
    try:
        symbols = list(active_watchlist)
        chunk_size = 300  # Well below 341 max streams limit per connection
        chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]
        
        logger.info("Spawning %d WS shards to cover %d symbols", len(chunks), len(symbols))
        
        tasks = [
            asyncio.create_task(_run_kline_ws_shard(chunk, i))
            for i, chunk in enumerate(chunks)
        ]
        
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for t in tasks:
            t.cancel()
        raise


# ── Per-symbol signal evaluation (triggered by 15m candle close) ─────────────

async def _evaluate_symbol(symbol: str) -> None:
    """
    Run the signal engine for one symbol immediately after its 15m candle
    closes. Called as an asyncio task from the kline WS handler — no polling.
    """
    try:
        k15 = _kline_buffers[symbol]["15m"]
        k5  = _kline_buffers[symbol]["5m"]
        k1m = _kline_buffers[symbol]["1m"]
        if config.SIGNAL_MODE != "micro_scalp" and (len(k15) < 210 or len(k5) < 50):
            return
        if config.SIGNAL_MODE == "micro_scalp" and len(k1m) < 20:
            return

        # Cooldown — avoid spamming signals for the same symbol
        now = time.time()
        if now - _last_signal_ts.get(symbol, 0) < _SIGNAL_COOLDOWN_S:
            return

        # ── Funding Rate Execution Guard ─────────────
        funding_guard_active = False
        if config.FUNDING_GUARD_ENABLED and config.SIGNAL_MODE != "funding_predator":
            import datetime
            now_t = datetime.datetime.utcnow()
            now_mins = now_t.hour * 60 + now_t.minute
            
            # Funding occurs every 8 hours (480 minutes): 00:00, 08:00, 16:00 UTC
            nearest_tick = round(now_mins / 480.0) * 480
            if abs(now_mins - nearest_tick) <= config.FUNDING_GUARD_MINUTES:
                funding_guard_active = True
                
        # ── Global Session Time Guard ─────────────
        custom_str = getattr(config, "TRADE_CUSTOM_SESSIONS", "").strip()
        if config.SESSION_GUARD_ENABLED or custom_str:
            import datetime
            now_time = datetime.datetime.utcnow().time()
            
            allowed_ranges = []
            if custom_str:
                for r in custom_str.split(","):
                    try:
                        st, en = r.strip().split("-")
                        sh, sm = map(int, st.split(":"))
                        eh, em = map(int, en.split(":"))
                        allowed_ranges.append(((sh, sm), (eh, em)))
                    except Exception:
                        pass
                
            if not allowed_ranges:
                return # Block immediately if Guard is on but all toggles/custom blocks are empty.
                
            allowed = False
            for (st_h, st_m), (en_h, en_m) in allowed_ranges:
                start_t = datetime.time(st_h, st_m)
                end_t = datetime.time(en_h, en_m)
                
                if start_t <= end_t:
                    if start_t <= now_time <= end_t:
                        allowed = True
                        break
                else: # Midnight overlap (e.g. Asia 23:00 -> 08:00)
                    if now_time >= start_t or now_time <= end_t:
                        allowed = True
                        break
                        
            if not allowed:
                return

        # This function is now triggered precisely on a 5m candle close.
        # Both k15[-1] (which may still be forming) and k5[-1] (just closed) 
        # are valid snapshots for evaluation.
        mode = config.SIGNAL_MODE

        # ── BTC Regime Filter — computed once per evaluation call ─────────────
        global _last_btc_regime
        btc_k15 = _kline_buffers.get("BTCUSDT", {}).get("15m", [])
        regime = signal_engine.get_btc_regime(btc_k15)
        if regime != _last_btc_regime:
            _last_btc_regime = regime
            await wsb.broadcaster.broadcast("btc_regime", {"regime": regime})

        def _regime_blocks(sig: dict) -> bool:
            """Return True if this signal should be suppressed by the regime filter."""
            if symbol == "BTCUSDT":
                return False   # never block BTC itself
            if regime in ("BULL_BREAKOUT", "BEAR_BREAKDOWN"):
                # Skip blocking if the symbol is not sufficiently correlated with BTC
                threshold = config.BTC_CORR_THRESHOLD
                if threshold > 0.0 and len(btc_k15) >= 20 and len(k15) >= 20:
                    btc_closes = [float(c["close"]) for c in btc_k15[-20:]]
                    sym_closes = [float(c["close"]) for c in k15[-20:]]
                    try:
                        corr = float(np.corrcoef(btc_closes, sym_closes)[0][1])
                    except Exception:
                        corr = 1.0  # assume correlated on error — safe default
                    if abs(corr) < threshold:
                        logger.debug(
                            "BTC %s — %s correlation=%.2f < threshold=%.2f, NOT blocking",
                            regime, symbol, corr, threshold,
                        )
                        return False  # not correlated enough — allow signal
            if regime == "BULL_BREAKOUT" and sig["direction"] == "SHORT":
                logger.debug("BTC BULL_BREAKOUT — blocking SHORT %s", symbol)
                return True
            if regime == "BEAR_BREAKDOWN" and sig["direction"] == "LONG":
                logger.debug("BTC BEAR_BREAKDOWN — blocking LONG %s", symbol)
                return True
            return False

        def _funding_blocks(sig: dict) -> bool:
            if not funding_guard_active:
                return False
            allow_shorts = getattr(config, "FUNDING_GUARD_ALLOW_SHORTS", False)
            if allow_shorts:
                if sig["direction"] == "LONG":
                    logger.debug("Funding Guard active (LONGs paused) — blocking LONG %s", symbol)
                    return True
                return False
            else:
                logger.debug("Funding Guard active — blocking %s", symbol)
                return True

        candidates: list[dict] = []

        if mode in ("pullback", "both"):
            s = await asyncio.to_thread(
                signal_engine.check_pullback, symbol, k15[:], k5[:]
            )
            if s and not _regime_blocks(s) and not _funding_blocks(s):
                candidates.append(s)
                
        if mode == "micro_scalp":
            s = await asyncio.to_thread(
                signal_engine.check_micro_scalp, symbol, k1m[:]
            )
            if s and not _regime_blocks(s) and not _funding_blocks(s):
                candidates.append(s)

        if mode in ("breakout", "both"):
            s = await asyncio.to_thread(
                signal_engine.check_breakout, symbol, k15[:], k5[:]
            )
            if s and not _regime_blocks(s) and not _funding_blocks(s):
                candidates.append(s)

        if not candidates:
            return

        # If both fired, take the higher-scoring signal for this symbol
        sig = max(candidates, key=lambda x: x["score"])

        # ── 3. Anti-Correlation System Guard ──────────────────────────────────────
        # Reject candidates that perfectly hedge (inverse) or duplicate (positive) open trades.
        import db
        import numpy as np
        open_trades = await db.get_open_trades()
        if open_trades:
            sym_closes = [float(c["close"]) for c in k15[-20:]]
            if len(sym_closes) >= 20:
                for t in open_trades:
                    open_sym = t["symbol"]
                    if open_sym == symbol:
                        continue
                    if t["direction"] != sig["direction"]:
                        continue
                    
                    open_k15 = _kline_buffers.get(open_sym, {}).get("15m", [])
                    if len(open_k15) >= 20:
                        open_closes = [float(c["close"]) for c in open_k15[-20:]]
                        try:
                            corr = float(np.corrcoef(sym_closes, open_closes)[0][1])
                        except Exception:
                            continue
                        
                        if corr > 0.85:
                            logger.info("Guard: Blocked %s %s due to POSITIVE correlation (%.2f) with open trade %s", symbol, sig["direction"], corr, open_sym)
                            return
                        if corr < -0.65:
                            logger.info("Guard: Blocked %s %s due to INVERSE hedges (%.2f) with open trade %s", symbol, sig["direction"], corr, open_sym)
                            return

        _last_signal_ts[symbol] = now

        # Queue for batch ranking — flush fires after _BATCH_WINDOW_S so
        # signals from all symbols that closed on the same candle are sorted
        # by score before any trade slot is allocated.
        _pending_signals.append(sig)
        await wsb.broadcaster.broadcast("scanner_alert", sig)
        _arm_flush()

    except Exception as exc:
        logger.error("_evaluate_symbol %s error: %s", symbol, exc)


def _arm_flush() -> None:
    """Start the flush timer if not already running."""
    global _flush_task
    if _flush_task is None or _flush_task.done():
        _flush_task = asyncio.create_task(
            _flush_pending_signals(), name="signal_flush"
        )


async def _flush_pending_signals() -> None:
    """
    Wait for the batch window, then execute pending signals concurrently.

    The batch is sorted by score so higher-confidence signals are submitted
    first (asyncio.gather starts tasks in creation order, so the highest-
    scored signal's handle_signal runs first at each yield point).

    A direction-cap filter (MAX_SAME_DIRECTION) is applied before dispatch:
    signals are processed in score order; once a direction has hit the cap,
    additional signals in that direction are logged with acted_on=False and
    skipped.  This prevents N correlated bets on a single broad market move.

    Each admitted signal runs independently — the _opening set in
    order_manager prevents exceeding MAX_OPEN_TRADES without a global lock.
    """
    await asyncio.sleep(config.SIGNAL_BATCH_WINDOW_S)

    if not _pending_signals:
        return

    # Drain the queue atomically
    batch = sorted(_pending_signals, key=lambda s: s["score"], reverse=True)
    _pending_signals.clear()

    # ── Direction-cap filter ───────────────────────────────────────────────────
    # Seed counts from already-open trades so the cap accounts for existing
    # directional exposure, not just signals in this batch.
    cap = config.MAX_SAME_DIRECTION
    open_trades = await db.get_open_trades()
    long_count  = sum(1 for t in open_trades if t["direction"] == "LONG")
    short_count = sum(1 for t in open_trades if t["direction"] == "SHORT")
    admitted: list[dict] = []
    capped:   list[dict] = []

    for sig in batch:
        if sig["direction"] == "LONG":
            if long_count < cap:
                long_count += 1
                admitted.append(sig)
            else:
                capped.append(sig)
        else:  # SHORT
            if short_count < cap:
                short_count += 1
                admitted.append(sig)
            else:
                capped.append(sig)

    logger.info(
        "Signal batch: %d signal(s) total, %d admitted, %d capped by MAX_SAME_DIRECTION=%d "
        "— admitted: %s",
        len(batch), len(admitted), len(capped), cap,
        ", ".join(f"{s['symbol']}({s['direction'][0]},{s['score']})" for s in admitted),
    )
    if capped:
        logger.info(
            "Direction-capped (logged, not traded): %s",
            ", ".join(f"{s['symbol']}({s['direction'][0]},{s['score']})" for s in capped),
        )

    # Log capped signals immediately with acted_on=False so they appear in history
    for sig in capped:
        await db.insert_scanner_log(
            symbol=sig["symbol"],
            score=sig["score"],
            direction=sig["direction"],
            timestamp=sig["timestamp"],
            acted_on=False,
            ml_confidence=sig.get("ml_confidence"),
            reason="Direction Capped (MAX_SAME_DIRECTION limit reached)",
        )

    # ── Gradual build cap (PnL-aware) ────────────────────────────────────────
    # Limit how many new trades open in a single scan.  admitted is already
    # sorted highest-score first so the best signals are always taken.
    # Deferred signals are not logged — they re-appear next scan if the
    # setup is still valid on the new candle.
    #
    # When existing positions are losing, adding more trades increases
    # correlated exposure.  Scale back the per-scan limit based on how
    # negative total unrealized PnL is relative to PORTFOLIO_MIN_TP_USDT/2:
    #   >= 0          → full INITIAL_BATCH_SIZE (market cooperating)
    #   > -half_target → 1 (cautious)
    #   <= -half_target → 0 (deeply negative, stop building)
    # When current_open == 0 always use INITIAL_BATCH_SIZE (fresh cycle).
    import position_tracker as _pt
    import order_manager as _om
    total_unrealized = sum(_pt.paper_unrealized.values()) if _pt.paper_unrealized else 0.0

    current_open    = len(open_trades) + len(_om._opening)  # include in-flight trades
    available_slots = config.MAX_OPEN_TRADES - current_open

    if current_open == 0:
        pnl_limit = config.INITIAL_BATCH_SIZE
    else:
        half_target = config.PORTFOLIO_MIN_TP_USDT / 2
        if total_unrealized >= 0:
            pnl_limit = config.INITIAL_BATCH_SIZE
        elif half_target > 0 and total_unrealized > -half_target:
            pnl_limit = 1
        else:
            pnl_limit = 0

    scan_limit = min(pnl_limit, available_slots)
    this_scan  = admitted[:scan_limit]
    deferred   = admitted[scan_limit:]

    if deferred:
        logger.info(
            "Gradual build: %d opening this scan, %d deferred — "
            "open=%d, unrealized=%.2f, limit=%d",
            len(this_scan), len(deferred), current_open, total_unrealized, scan_limit,
        )
        # Log deferred signals so they appear in scanner history / UI.
        # acted_on=False because no trade was opened this scan; the signal
        # may re-fire on the next candle if the setup remains valid.
        for sig in deferred:
            await db.insert_scanner_log(
                symbol=sig["symbol"],
                score=sig["score"],
                direction=sig["direction"],
                timestamp=sig["timestamp"],
                acted_on=False,
                ml_confidence=sig.get("ml_confidence"),
                reason="Deferred (Gradual Build Limit / Drawdown Guard)",
            )

    async def _act(sig: dict) -> None:
        result = await _order_manager.handle_signal(sig) if _order_manager else (False, "Order Manager Offline")
        acted, reason = result if isinstance(result, tuple) else (False, "Unknown")
        await db.insert_scanner_log(
            symbol=sig["symbol"],
            score=sig["score"],
            direction=sig["direction"],
            timestamp=sig["timestamp"],
            acted_on=acted,
            ml_confidence=sig.get("ml_confidence"),
            reason=reason,
        )

    if this_scan:
        await asyncio.gather(*[asyncio.create_task(_act(sig)) for sig in this_scan])


# ── Mark-price WebSocket (for paper PnL) ─────────────────────────────────────
# Shared mark price registry used by position_tracker
mark_prices: dict[str, float] = {}


async def _run_mark_price_ws() -> None:
    """Subscribe to !markPrice@arr to get mark prices for all symbols.
    Filters against the live active_watchlist so additions and removals
    are reflected without restarting this task.
    """
    url = f"{config.BINANCE_WS_BASE}/ws/!markPrice@arr@1s"
    backoff = 1
    while True:
        logger.info("Connecting mark-price WS...")
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                backoff = 1
                async for raw in ws:
                    data = json.loads(raw)
                    if isinstance(data, list):
                        current_set = set(active_watchlist)
                        for item in data:
                            s = item.get("s", "")
                            if s in current_set:
                                mark_prices[s] = float(item.get("p", 0))
                        # Trigger position tracker immediately — no event indirection,
                        # so trail/SL checks run within the same event-loop cycle.
                        import sys as _sys
                        _pt = _sys.modules.get("position_tracker")
                        if _pt and hasattr(_pt, "_paper_tick"):
                            asyncio.ensure_future(_pt._paper_tick())
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("Mark-price WS error: %s — reconnect in %ds", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


# ── Entry point ───────────────────────────────────────────────────────────────

async def start(order_manager=None) -> None:
    """
    Main entry: build watchlist, seed klines, start all async tasks.
    Called from main.py startup event.
    """
    global active_watchlist
    if order_manager:
        set_order_manager(order_manager)

    # Boot temporal clock strategy
    asyncio.create_task(_run_funding_predator_clock(), name="funding_predator")

    # Initial watchlist
    active_watchlist = await build_watchlist()
    
    funding_rates_map = {}
    if config.SIGNAL_MODE == "funding_predator":
        try:
            import binance_client as bc
            api_rates = await bc.get_all_premium_indices()
            for r in api_rates:
                if r["symbol"] in active_watchlist:
                    funding_rates_map[r["symbol"]] = float(r.get("lastFundingRate", 0))
        except Exception: pass

    await wsb.broadcaster.broadcast(
        "scanner_watchlist",
        {"symbols": active_watchlist, "count": len(active_watchlist), "funding_targets": funding_rates_map},
    )

    # Seed klines concurrently in batches of 10 (Binance rate-limit safe)
    logger.info("Seeding klines for %d symbols...", len(active_watchlist))
    batch_size = 10
    for i in range(0, len(active_watchlist), batch_size):
        batch = active_watchlist[i : i + batch_size]
        await asyncio.gather(*[_seed_klines(sym) for sym in batch])
        await asyncio.sleep(0.5)  # brief pause between batches

    # Start background tasks (fire and forget)
    # scanner_loop removed — evaluation is now triggered directly by the
    # kline WS whenever a 15m candle closes (zero-latency, no polling)
    global _kline_ws_task
    _kline_ws_task = asyncio.create_task(_run_kline_ws(), name="kline_ws")
    asyncio.create_task(_run_mark_price_ws(), name="mark_price_ws")
    asyncio.create_task(refresh_watchlist_loop(), name="watchlist_refresh")
    
    # Ensure any missing ML models from the initial startup watchlist are trained in the background
    asyncio.create_task(_auto_train_missing_models(list(active_watchlist)))
    
    logger.info("Scanner started — evaluation driven by 15m candle close events.")

async def _run_funding_predator_clock() -> None:
    """
    Dedicated clock loop for the Funding Predator.
    Executes purely on UTC temporal checks bypassing the websocket queue entirely.
    """
    import binance_client as bc
    import signal_engine
    import order_manager as om
    
    target_tick_hours = {0, 8, 16}
    target_hour_pre = {7, 15, 23}
    
    last_ambush_hour = -1
    
    while True:
        await asyncio.sleep(1)
        if config.SIGNAL_MODE != "funding_predator":
            continue
            
        now = datetime.datetime.utcnow()
        if now.hour in target_hour_pre and now.minute == 55 and now.second == 0 and last_ambush_hour != now.hour:
            last_ambush_hour = now.hour
            logger.info("Funding Predator: 5 minutes to tick. Scanning /premiumIndex...")
            try:
                rates = await bc.get_all_premium_indices()
                candidates = []
                for r in rates:
                    fr = float(r.get("lastFundingRate", 0))
                    if fr >= config.FUNDING_PREDATOR_THRESHOLD:
                        candidates.append((r["symbol"], fr, float(r.get("markPrice", 0))))
                
                if not candidates:
                    logger.info("Funding Predator: No severe rates found. Aborting ambush.")
                    continue
                    
                candidates.sort(key=lambda x: x[1], reverse=True)
                best_target_sym, best_rate, best_mark = candidates[0]
                
                logger.info(f"Funding Predator: Target locked on {best_target_sym} at {best_rate*100}%. Waiting for tick...")
                
                # Calculate precise sleep required to hit execution window exactly, CPU stays at 0%
                now_inner = datetime.datetime.utcnow()
                next_hour = None
                for t_hour in sorted(target_tick_hours):
                    if now_inner.hour < t_hour:
                        next_hour = t_hour
                        break
                if next_hour is None:
                    next_hour = min(target_tick_hours)
                
                target_dt = datetime.datetime(now_inner.year, now_inner.month, now_inner.day, next_hour, 0, 1)
                if next_hour < now_inner.hour:
                    target_dt += datetime.timedelta(days=1)
                    
                sleep_seconds = (target_dt - now_inner).total_seconds()
                
                if sleep_seconds > 0:
                     await asyncio.sleep(sleep_seconds)
                
                logger.warning(f"Funding Predator: ZERO HOUR TICK EXECUTING FIRE ON {best_target_sym}")
                signal = signal_engine.check_funding_predator(best_target_sym, best_rate, best_mark)
                if signal:
                    # Direct payload bypass injection to order manager (circumventing score batches)
                    await om.order_manager.handle_signal(signal)
                # Cooldown so we don't rapid-trigger
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Funding Predator Error: {e}")
