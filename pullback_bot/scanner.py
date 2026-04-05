"""
scanner.py — Background scanner loop (Daily Sweep / ICT-SMC strategy).

Responsibilities:
  1. Activity filter: builds the active watchlist from Binance exchange info + 24h tickers.
     Runs once at startup then refreshes every WATCHLIST_REFRESH_MINUTES.
  2. Daily bias refresh: fetches daily klines at 00:05 UTC and computes bias for each symbol.
  3. Kline WebSocket subscription: combined multi-stream for all watchlist symbols.
     Streams: <symbol>@kline_1m, <symbol>@kline_1h
  4. Scanner loop: for each symbol calls signal_engine.check_daily_sweep every
     SCANNER_INTERVAL_SECONDS during the NY open window.
  5. On valid signal: logs to DB, broadcasts via ws_broadcaster, calls
     order_manager.handle_signal.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
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
# Intervals: "1m" (last 10), "1h" (last 50)
_kline_buffers: dict[str, dict[str, list[dict]]] = defaultdict(
    lambda: {"1m": [], "1h": []}
)

# Daily bias cache: symbol -> {"bias": str, "pdh": float, "pdl": float}
daily_bias_cache: dict[str, dict] = {}

# last signal time per symbol (4-hour cooldown for Daily Sweep)
_last_signal_ts: dict[str, float] = {}
_SIGNAL_COOLDOWN_S = 5400   # 90 min — one signal per symbol per NY session

# mark price registry (used by position_tracker for paper PnL)
mark_prices: dict[str, float] = {}

# reference to order_manager (set by start() to avoid circular import)
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


# ── Daily bias ────────────────────────────────────────────────────────────────

async def _seed_daily_klines(symbol: str) -> None:
    """Fetch daily klines and compute daily bias for one symbol."""
    try:
        raw = await bc.get_klines(symbol, "1d", 5)
        candles = [
            {
                "time":   int(c[0]) // 1000,
                "open":   float(c[1]),
                "high":   float(c[2]),
                "low":    float(c[3]),
                "close":  float(c[4]),
                "volume": float(c[5]),
            }
            for c in raw
        ]
        daily_bias_cache[symbol] = signal_engine.compute_daily_bias(candles)
    except Exception as exc:
        logger.warning("Daily klines %s: %s", symbol, exc)


async def _refresh_daily_bias() -> None:
    """Refresh daily bias for all watchlist symbols concurrently."""
    logger.info("Refreshing daily bias for %d symbols...", len(active_watchlist))
    batch_size = 10
    for i in range(0, len(active_watchlist), batch_size):
        batch = active_watchlist[i: i + batch_size]
        await asyncio.gather(*[_seed_daily_klines(sym) for sym in batch])
        await asyncio.sleep(0.3)
    logger.info("Daily bias refresh complete")
    await wsb.broadcaster.broadcast("daily_bias_update", {
        s: daily_bias_cache[s] for s in active_watchlist if s in daily_bias_cache
    })


async def _daily_bias_refresh_loop() -> None:
    """
    Wait until 00:05 UTC then refresh daily bias once per day.
    Runs continuously so restarts across midnight are handled.
    """
    h, m = map(int, config.DAILY_BIAS_REFRESH_UTC.split(":"))
    while True:
        now = datetime.now(timezone.utc)
        # Seconds until next HH:MM UTC
        target_s = h * 3600 + m * 60
        now_s = now.hour * 3600 + now.minute * 60 + now.second
        wait = (target_s - now_s) % 86400
        if wait == 0:
            wait = 86400  # avoid spinning if we're exactly on time
        logger.debug("Daily bias refresh in %.0fs", wait)
        await asyncio.sleep(wait)
        await _refresh_daily_bias()


# ── Kline REST seed ───────────────────────────────────────────────────────────

async def _seed_klines(symbol: str) -> None:
    """Pre-fill 1h and 1m kline buffers from REST API."""
    for interval, limit in [("1h", 50), ("1m", 10)]:
        try:
            raw = await bc.get_klines(symbol, interval, limit)
            candles = [
                {
                    "time":   int(c[0]) // 1000,
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
    """Build combined stream URL for 1m + 1h klines."""
    streams = []
    for sym in symbols:
        s = sym.lower()
        streams += [f"{s}@kline_1m", f"{s}@kline_1h"]
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
        "time":   int(k["t"]) // 1000,
        "open":   float(k["o"]),
        "high":   float(k["h"]),
        "low":    float(k["l"]),
        "close":  float(k["c"]),
        "volume": float(k["v"]),
    }
    return data["s"], k["i"], candle, k["x"]  # x=True when candle closed


def _update_buffer(symbol: str, interval: str, candle: dict, is_closed: bool) -> None:
    """Update the in-memory kline buffer for a symbol/interval."""
    buf = _kline_buffers[symbol][interval]
    if buf and buf[-1]["time"] == candle["time"] and not is_closed:
        buf[-1] = candle
    elif is_closed or (not buf) or buf[-1]["time"] != candle["time"]:
        # Closed candle or genuinely new timestamp — append
        if buf and buf[-1]["time"] == candle["time"]:
            buf[-1] = candle  # replace in-progress with final
        else:
            buf.append(candle)
        limit = {"1h": 50, "1m": 10}.get(interval, 50)
        if len(buf) > limit:
            _kline_buffers[symbol][interval] = buf[-limit:]


async def _run_kline_ws(symbols: list[str]) -> None:
    """
    Subscribe to kline WebSocket for given symbols.
    Auto-reconnects with exponential backoff.
    Broadcasts kline_update to subscribed UI clients.
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
                        await wsb.broadcaster.broadcast_kline(
                            sym, interval,
                            {
                                "symbol":    sym,
                                "interval":  interval,
                                "candle":    candle,
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
    """Run Daily Sweep signal check on all watchlist symbols."""
    utc_now = datetime.now(timezone.utc)

    for symbol in list(active_watchlist):
        try:
            k1h = _kline_buffers[symbol]["1h"]
            bias = daily_bias_cache.get(symbol)

            # Need enough 1h candles and a bias reading
            if len(k1h) < config.SWEEP_LOOKBACK + 3 or bias is None:
                continue

            # Cooldown check
            now = time.time()
            if now - _last_signal_ts.get(symbol, 0) < _SIGNAL_COOLDOWN_S:
                continue

            mark = mark_prices.get(symbol)
            if not mark:
                # Fall back to last 1h close
                mark = k1h[-1]["close"] if k1h else 0.0
            if not mark:
                continue

            # Run signal check in a thread (pure-Python but still keeps event loop free)
            sig = await asyncio.to_thread(
                signal_engine.check_daily_sweep,
                symbol, k1h[:], bias, mark, utc_now,
            )
            if sig is None:
                continue

            # Signal found
            _last_signal_ts[symbol] = now
            acted = False

            if _order_manager:
                acted = await _order_manager.handle_signal(sig)

            await db.insert_scanner_log(
                symbol=symbol,
                score=sig["score"],
                direction=sig["direction"],
                timestamp=sig["timestamp"],
                acted_on=acted,
            )

            await wsb.broadcaster.broadcast("scanner_alert", sig)
            logger.info(
                "Daily Sweep signal: %s %s entry=%.6f",
                symbol, sig["direction"], sig["entry_price"],
            )

        except Exception as exc:
            logger.error("Evaluate %s error: %s", symbol, exc)


async def scanner_loop() -> None:
    """Evaluate watchlist on SCANNER_INTERVAL_SECONDS cadence."""
    while True:
        await asyncio.sleep(config.SCANNER_INTERVAL_SECONDS)
        await _evaluate_symbols()


# ── Mark-price WebSocket (for paper PnL) ─────────────────────────────────────

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
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("Mark-price WS error: %s — reconnect in %ds", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


# ── Entry point ───────────────────────────────────────────────────────────────

async def start(order_manager=None) -> None:
    """
    Main entry: build watchlist, seed klines + daily bias, start all async tasks.
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

    # Seed klines + daily bias concurrently in batches
    logger.info("Seeding klines + daily bias for %d symbols...", len(active_watchlist))
    batch_size = 10
    for i in range(0, len(active_watchlist), batch_size):
        batch = active_watchlist[i: i + batch_size]
        await asyncio.gather(
            *[_seed_klines(sym) for sym in batch],
            *[_seed_daily_klines(sym) for sym in batch],
        )
        await asyncio.sleep(0.5)  # brief pause between batches

    # Start background tasks
    asyncio.create_task(_run_kline_ws(active_watchlist), name="kline_ws")
    asyncio.create_task(_run_mark_price_ws(active_watchlist), name="mark_price_ws")
    asyncio.create_task(scanner_loop(), name="scanner_loop")
    asyncio.create_task(refresh_watchlist_loop(), name="watchlist_refresh")
    asyncio.create_task(_daily_bias_refresh_loop(), name="daily_bias_refresh")
    logger.info("Scanner started.")
