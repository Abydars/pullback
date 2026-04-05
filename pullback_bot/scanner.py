"""
scanner.py — Background scanner loop.

Responsibilities:
  1. Activity filter: builds the active watchlist from Binance exchange info + 24h tickers.
     Runs once at startup then refreshes every WATCHLIST_REFRESH_MINUTES.
  2. Kline WebSocket subscription: combined multi-stream for all watchlist symbols.
     Streams: <symbol>@kline_1m, <symbol>@kline_5m, <symbol>@kline_15m
  3. Scanner loop: for each symbol keeps a rolling kline buffer; calls
     signal_engine.check_pullback every SCANNER_INTERVAL_SECONDS.
  4. On valid signal: logs to DB, broadcasts via ws_broadcaster, calls
     order_manager.handle_signal.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict
from typing import Optional

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

# kline buffers: symbol -> interval -> list[candle_dict]
# Each candle dict: {open, high, low, close, volume}
_kline_buffers: dict[str, dict[str, list[dict]]] = defaultdict(
    lambda: {"1m": [], "5m": [], "15m": []}
)

# last signal time per symbol (to avoid flooding)
_last_signal_ts: dict[str, float] = {}
_SIGNAL_COOLDOWN_S = 300  # 5 min between signals per symbol

# reference to order_manager (set by main.py to avoid circular import)
_order_manager = None


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


async def refresh_watchlist_loop() -> None:
    """Refresh the active_watchlist every WATCHLIST_REFRESH_MINUTES."""
    global active_watchlist
    while True:
        active_watchlist = await build_watchlist()
        await wsb.broadcaster.broadcast(
            "scanner_watchlist",
            {"symbols": active_watchlist, "count": len(active_watchlist)},
        )
        await asyncio.sleep(config.WATCHLIST_REFRESH_MINUTES * 60)


# ── Kline REST seed (initial fill before WS) ───────────────────────────────────

async def _seed_klines(symbol: str) -> None:
    """Pre-fill kline buffers from REST API for a symbol."""
    for interval, limit in [("15m", 220), ("5m", 60), ("1m", 10)]:
        try:
            raw = await bc.get_klines(symbol, interval, limit)
            candles = [
                {
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
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
    """Update the in-memory kline buffer for a symbol/interval."""
    buf = _kline_buffers[symbol][interval]
    if buf and not is_closed:
        # Update the last (in-progress) candle
        buf[-1] = candle
    elif is_closed:
        buf.append(candle)
        # Keep only what we need (220 for 15m, 60 for 5m, 10 for 1m)
        limit = {"15m": 220, "5m": 60, "1m": 10}.get(interval, 60)
        if len(buf) > limit:
            _kline_buffers[symbol][interval] = buf[-limit:]


async def _run_kline_ws(symbols: list[str]) -> None:
    """
    Subscribe to kline WebSocket for given symbols.
    Auto-reconnects with exponential backoff.
    Broadcasts kline_update (with EMA values) to subscribed UI clients.
    """
    backoff = 1
    while True:
        url = _make_stream_url(symbols)
        logger.info("Connecting kline WS (%d symbols)...", len(symbols))
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                backoff = 1
                logger.info("Kline WS connected")
                async for raw in ws:
                    msg = json.loads(raw)
                    parsed = _parse_kline_msg(msg)
                    if parsed:
                        sym, interval, candle, is_closed = parsed
                        _update_buffer(sym, interval, candle, is_closed)
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
            logger.warning("Kline WS error: %s — reconnect in %ds", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


# ── Scanner evaluation loop ───────────────────────────────────────────────────

async def _evaluate_symbols() -> None:
    """Run signal check on all watchlist symbols."""
    for symbol in list(active_watchlist):
        try:
            k15 = _kline_buffers[symbol]["15m"]
            k5 = _kline_buffers[symbol]["5m"]
            if len(k15) < 210 or len(k5) < 50:
                continue

            # Cooldown check
            now = time.time()
            if now - _last_signal_ts.get(symbol, 0) < _SIGNAL_COOLDOWN_S:
                continue

            # Run CPU-heavy pandas computation in a thread so the event
            # loop stays free for HTTP/WebSocket serving
            sig = await asyncio.to_thread(
                signal_engine.check_pullback, symbol, k15[:], k5[:]
            )
            if sig is None:
                continue

            # Signal found
            _last_signal_ts[symbol] = now
            acted = False

            # Try to place order
            if _order_manager:
                acted = await _order_manager.handle_signal(sig)

            # Log to DB
            await db.insert_scanner_log(
                symbol=symbol,
                score=sig["score"],
                direction=sig["direction"],
                timestamp=sig["timestamp"],
                acted_on=acted,
            )

            # Broadcast to UI
            await wsb.broadcaster.broadcast("scanner_alert", sig)

        except Exception as exc:
            logger.error("Evaluate %s error: %s", symbol, exc)


async def scanner_loop() -> None:
    """Evaluate watchlist on SCANNER_INTERVAL_SECONDS cadence."""
    while True:
        await asyncio.sleep(config.SCANNER_INTERVAL_SECONDS)
        await _evaluate_symbols()


# ── Mark-price WebSocket (for paper PnL) ─────────────────────────────────────
# Shared mark price registry used by position_tracker
mark_prices: dict[str, float] = {}


async def _run_mark_price_ws(symbols: list[str]) -> None:
    """Subscribe to !markPrice@arr to get mark prices for all symbols."""
    url = f"{config.BINANCE_WS_BASE}/ws/!markPrice@arr"
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
                            if s in symbols:
                                mark_prices[s] = float(item.get("p", 0))
                        # Signal position_tracker to wake up and re-evaluate
                        import sys as _sys
                        _pt = _sys.modules.get("position_tracker")
                        if _pt and hasattr(_pt, "_price_event"):
                            _pt._price_event.set()
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
    await wsb.broadcaster.broadcast(
        "scanner_watchlist",
        {"symbols": active_watchlist, "count": len(active_watchlist)},
    )

    # Seed klines concurrently in batches of 10 (Binance rate-limit safe)
    logger.info("Seeding klines for %d symbols...", len(active_watchlist))
    batch_size = 10
    for i in range(0, len(active_watchlist), batch_size):
        batch = active_watchlist[i : i + batch_size]
        await asyncio.gather(*[_seed_klines(sym) for sym in batch])
        await asyncio.sleep(0.5)  # brief pause between batches

    # Start background tasks (fire and forget)
    asyncio.create_task(_run_kline_ws(active_watchlist), name="kline_ws")
    asyncio.create_task(_run_mark_price_ws(active_watchlist), name="mark_price_ws")
    asyncio.create_task(scanner_loop(), name="scanner_loop")
    asyncio.create_task(refresh_watchlist_loop(), name="watchlist_refresh")
    logger.info("Scanner started.")
