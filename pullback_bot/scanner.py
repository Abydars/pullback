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

_oi_cache: dict[str, list[dict]] = {}  # {symbol: [oihist_dict, ...]}

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
    for interval, limit in [("15m", 500), ("5m", 60), ("1m", 60), ("4h", 200)]:
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
        streams += [f"{s}@kline_1m", f"{s}@kline_5m", f"{s}@kline_15m", f"{s}@kline_4h"]
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
        limit = {"15m": 500, "5m": 60, "1m": 60, "4h": 200}.get(interval, 60)
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
                            if interval == "5m":
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
    """Manager loop that spawns sharded connections for the required symbols."""
    try:
        import db
        import ws_broadcaster as wsb
        
        required = set(active_watchlist)
        
        # Add all open trade symbols to keep their momentum logic active!
        open_trades = await db.get_open_trades()
        for t in open_trades:
            required.add(t["symbol"])
            
        # Add all UI-subscribed chart symbols
        for sym, _ in wsb.broadcaster._chart_subs.values():
            required.add(sym.upper())
            
        symbols = list(required)
        chunk_size = 100  # 100 symbols = 400 streams per connection (Binance hard limit is 1024 streams per connection)
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
        k4h = _kline_buffers[symbol]["4h"]
        if len(k15) < 210 or len(k5) < 50 or (config.FILTER_MTF_ENABLED and len(k4h) < 150):
            return

        # Cooldown — avoid spamming signals for the same symbol
        now = time.time()
        if now - _last_signal_ts.get(symbol, 0) < _SIGNAL_COOLDOWN_S:
            return

        # ── Funding Rate Execution Guard ─────────────
        funding_guard_active = False
        if config.FUNDING_GUARD_ENABLED:
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
                signal_engine.check_pullback, symbol, k15[:], k5[:], k4h[:], _oi_cache.get(symbol, [])
            )
            if s and not _regime_blocks(s) and not _funding_blocks(s):
                candidates.append(s)
                
        if mode in ("breakout", "both"):
            s = await asyncio.to_thread(
                signal_engine.check_breakout, symbol, k15[:], k5[:], k4h[:], _oi_cache.get(symbol, [])
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
                            await db.insert_scanner_log(
                                symbol=symbol,
                                score=sig["score"],
                                direction=sig["direction"],
                                timestamp=now,
                                acted_on=False,
                                ml_confidence=sig.get("ml_confidence"),
                                reason=f"Blocked: POSITIVE correlation ({corr:.2f}) with open trade {open_sym}",
                                metadata=json.dumps({"entry": sig.get("entry_price"), "sl": sig.get("sl_price"), "tp": sig.get("tp1_price"), "atr": sig.get("atr"), "type": sig.get("signal_type"), "reasons": sig.get("reasons", [])})
                            )
                            return
                        if corr < -0.65:
                            logger.info("Guard: Blocked %s %s due to INVERSE hedges (%.2f) with open trade %s", symbol, sig["direction"], corr, open_sym)
                            await db.insert_scanner_log(
                                symbol=symbol,
                                score=sig["score"],
                                direction=sig["direction"],
                                timestamp=now,
                                acted_on=False,
                                ml_confidence=sig.get("ml_confidence"),
                                reason=f"Blocked: INVERSE hedge ({corr:.2f}) with open trade {open_sym}",
                                metadata=json.dumps({"entry": sig.get("entry_price"), "sl": sig.get("sl_price"), "tp": sig.get("tp1_price"), "atr": sig.get("atr"), "type": sig.get("signal_type"), "reasons": sig.get("reasons", [])})
                            )
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

    # ── ML Filter pre-filter ───────────────────────────────────────────────────
    # Reject and log ML-failed signals immediately so they don't consume batch slots
    valid_batch: list[dict] = []
    for sig in batch:
        if not sig.get("ml_passed", True):
            conf = sig.get("ml_confidence", 0.0)
            logger.info("Signal %s %s skipped — ML Filter rejected (%.2f < threshold)", sig["symbol"], sig["direction"], conf)
            await db.insert_scanner_log(
                symbol=sig["symbol"],
                score=sig["score"],
                direction=sig["direction"],
                timestamp=sig["timestamp"],
                acted_on=False,
                ml_confidence=conf,
                reason=f"ML Filter rejected ({conf:.2f} < threshold)",
                metadata=json.dumps({"entry": sig.get("entry_price"), "sl": sig.get("sl_price"), "tp": sig.get("tp1_price"), "atr": sig.get("atr"), "type": sig.get("signal_type"), "reasons": sig.get("reasons", [])})
            )
        else:
            valid_batch.append(sig)
            
    batch = valid_batch

    # ── 1. Determine Global & PnL-aware Admission Limits ────────────────────────
    import position_tracker as _pt
    import order_manager as _om
    
    open_trades = await db.get_open_trades()
    current_open = len(open_trades) + len(_om._opening)
    available_slots = config.MAX_OPEN_TRADES - current_open
    total_unrealized = sum(_pt.paper_unrealized.values()) if _pt.paper_unrealized else 0.0

    if current_open == 0:
        pnl_limit = config.INITIAL_BATCH_SIZE
        g_reason = "Fresh Cycle (Unrestricted limits)"
    else:
        half_target = config.PORTFOLIO_MIN_TP_USDT / 2
        if total_unrealized >= 0:
            pnl_limit = config.INITIAL_BATCH_SIZE
            g_reason = f"Profitable Portfolio (Unrealized: ${total_unrealized:.2f})"
        elif half_target > 0 and total_unrealized > -half_target:
            pnl_limit = 1
            g_reason = f"Cautious Throttle (Unrealized: ${total_unrealized:.2f})"
        else:
            pnl_limit = 0
            g_reason = f"Drawdown Guard Active (Unrealized: ${total_unrealized:.2f} <= -${half_target:.2f})"

    scan_limit = min(pnl_limit, available_slots)
    
    if available_slots <= 0:
        primary_reason = f"Deferred: MAX_OPEN_TRADES limit reached ({config.MAX_OPEN_TRADES} max slots full)"
    elif pnl_limit == 0:
        primary_reason = f"Deferred: {g_reason}"
    else:
        primary_reason = f"Deferred: Gradual Build Limits ({scan_limit} allowed per scan)"

    # ── 2. Direction-cap filter & Trade Allocation ──────────────────────────────
    cap = config.MAX_SAME_DIRECTION
    long_count  = sum(1 for t in open_trades if t["direction"] == "LONG")
    short_count = sum(1 for t in open_trades if t["direction"] == "SHORT")
    
    admitted: list[dict] = []
    direction_capped: list[dict] = []
    deferred: list[dict] = []

    for sig in batch:
        dir_cap_hit = False
        if sig["direction"] == "LONG" and long_count >= cap:
            dir_cap_hit = True
        elif sig["direction"] == "SHORT" and short_count >= cap:
            dir_cap_hit = True

        # Ensure we only apply "Direction Capped" if it genuinely survived the Scan Limit.
        # If we have 0 slots remaining (e.g. Drawdown Guard), ALL remaining batch items are Deferred.
        if len(admitted) >= scan_limit:
            deferred.append(sig)
        elif dir_cap_hit:
            direction_capped.append(sig)
        else:
            # We have slots available AND it passes direction cap. Welcome to the portfolio!
            if sig["direction"] == "LONG":
                long_count += 1
            else:
                short_count += 1
            admitted.append(sig)

    logger.info(
        "Signal batch: %d total, %d admitted, %d direction-capped, %d deferred "
        "[Limit=%d, Open=%d, Unrealized=%.2f]",
        len(batch), len(admitted), len(direction_capped), len(deferred),
        scan_limit, current_open, total_unrealized
    )
    if direction_capped:
        logger.info(
            "Direction-capped (logged): %s",
            ", ".join(f"{s['symbol']}({s['direction'][0]},{s['score']})" for s in direction_capped),
        )
    
    for sig in direction_capped:
        await db.insert_scanner_log(
            symbol=sig["symbol"], score=sig["score"], direction=sig["direction"], timestamp=sig["timestamp"],
            acted_on=False, ml_confidence=sig.get("ml_confidence"),
            reason=f"Direction Capped (MAX_SAME_DIRECTION={cap} limit reached)",
            metadata=json.dumps({"entry": sig.get("entry_price"), "sl": sig.get("sl_price"), "tp": sig.get("tp1_price"), "atr": sig.get("atr"), "type": sig.get("signal_type"), "reasons": sig.get("reasons", [])})
        )
    if deferred:
        for sig in deferred:
            await db.insert_scanner_log(
                symbol=sig["symbol"],
                score=sig["score"],
                direction=sig["direction"],
                timestamp=sig["timestamp"],
                acted_on=False,
                ml_confidence=sig.get("ml_confidence"),
                reason=primary_reason,
                metadata=json.dumps({"entry": sig.get("entry_price"), "sl": sig.get("sl_price"), "tp": sig.get("tp1_price"), "atr": sig.get("atr"), "type": sig.get("signal_type"), "reasons": sig.get("reasons", [])})
            )

    async def _act(sig: dict) -> None:
        try:
            result = await _order_manager.handle_signal(sig) if _order_manager else (False, "Order Manager Offline")
            acted, reason = result if isinstance(result, tuple) else (False, "Unknown")
        except Exception as exc:
            logger.error("Error in signal handling for %s: %s", sig["symbol"], exc)
            acted, reason = False, f"Exception: {exc}"

        await db.insert_scanner_log(
            symbol=sig["symbol"],
            score=sig["score"],
            direction=sig["direction"],
            timestamp=sig["timestamp"],
            acted_on=acted,
            ml_confidence=sig.get("ml_confidence"),
            reason=reason,
            metadata=json.dumps({"entry": sig.get("entry_price"), "sl": sig.get("sl_price"), "tp": sig.get("tp1_price"), "atr": sig.get("atr"), "type": sig.get("signal_type"), "reasons": sig.get("reasons", [])})
        )

    if admitted:
        await asyncio.gather(*[asyncio.create_task(_act(sig)) for sig in admitted])


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
                        for item in data:
                            s = item.get("s", "")
                            if s:
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



    # Initial watchlist
    active_watchlist = await build_watchlist()
    
    funding_rates_map = {}


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
    asyncio.create_task(_run_oi_worker(), name="oi_worker")
    
    # Ensure any missing ML models from the initial startup watchlist are trained in the background
    asyncio.create_task(_auto_train_missing_models(list(active_watchlist)))
    
    logger.info("Scanner started — evaluation driven by 15m candle close events.")

def request_kline_ws_restart() -> None:
    """Invoked explicitly by main.py when UI subscribes to a new missing symbol."""
    global _kline_ws_task
    if _kline_ws_task and not _kline_ws_task.done():
        _kline_ws_task.cancel()
    _kline_ws_task = asyncio.create_task(_run_kline_ws(), name="kline_ws")
    logger.info("Kline WS dynamically restarted for UI/Trade requirements.")

async def _run_oi_worker() -> None:
    """Periodically fetches open interest history for all active symbols."""
    while True:
        if config.FILTER_OI_ENABLED and active_watchlist:
            symbols = list(active_watchlist)
            for i in range(0, len(symbols), 10):
                batch = symbols[i : i + 10]
                tasks = [bc.get_open_interest_hist(sym, period="5m", limit=30) for sym in batch]
                try:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for sym, res in zip(batch, results):
                        if not isinstance(res, Exception) and res:
                            _oi_cache[sym] = res
                except Exception as e:
                    logger.warning("OI worker batch fail: %s", e)
                await asyncio.sleep(2)  # delay between batches
        await asyncio.sleep(300)  # poll every 5 minutes

