"""
scanner.py — Background scanner loop (Daily Sweep / ICT-SMC strategy).

Architecture
------------
Event-driven — no polling loop. Three triggers:

  TRIGGER 1  _run_kline_ws()  on every 1h candle CLOSE
    a. Skip if outside NY window or bias is NEUTRAL
    b. Run sweep detection → if found, cache in sweep_cache[symbol]
       and broadcast "sweep_detected"
    c. If sweep cached, run FVG detection → if found, cache in
       fvg_cache[symbol] and broadcast "fvg_formed"

  TRIGGER 2  _run_mark_price_ws()  on every mark-price tick
    For each symbol in fvg_cache:
    a. Skip if outside NY window or bias is NEUTRAL
    b. Call check_daily_sweep() with the current mark price
    c. If signal returned: clear caches, fire order, log, broadcast

  DAILY     _daily_bias_refresh_loop()  at 00:05 UTC
    Recomputes daily bias for every watchlist symbol from REST.

Public state (read by main.py)
-------------------------------
  active_watchlist  list[str]
  daily_bias_cache  dict[str, dict]
  mark_prices       dict[str, float]
  sweep_cache       dict[str, dict]   # symbol -> sweep info
  fvg_cache         dict[str, dict]   # symbol -> fvg zone info
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

# ── Public state ───────────────────────────────────────────────────────────────
active_watchlist: list[str] = []

# kline buffers: symbol -> interval -> list[candle_dict]
_kline_buffers: dict[str, dict[str, list[dict]]] = defaultdict(
    lambda: {"1m": [], "1h": []}
)

# Phase caches — cleared when a signal fires; repopulate on next sweep+FVG
sweep_cache: dict[str, dict] = {}   # symbol -> {candle, sweep_level, direction, time}
fvg_cache:   dict[str, dict] = {}   # symbol -> {fvg_high, fvg_low, fvg_mid, direction}

# Daily bias cache: symbol -> {"bias": str, "pdh": float, "pdl": float}
daily_bias_cache: dict[str, dict] = {}

# Mark price registry (used by position_tracker for paper PnL)
mark_prices: dict[str, float] = {}

# Reference to order_manager (set by start() to avoid circular import)
_order_manager = None


def set_order_manager(om) -> None:
    global _order_manager
    _order_manager = om


# ── Watchlist builder ──────────────────────────────────────────────────────────

async def build_watchlist() -> list[str]:
    """Fetch active USDT-M perpetuals, apply volume + movement filter."""
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
        return active_watchlist


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


# ── Daily bias ─────────────────────────────────────────────────────────────────

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
    """Wait until 00:05 UTC then refresh daily bias once per day."""
    h, m = map(int, config.DAILY_BIAS_REFRESH_UTC.split(":"))
    while True:
        now = datetime.now(timezone.utc)
        target_s = h * 3600 + m * 60
        now_s = now.hour * 3600 + now.minute * 60 + now.second
        wait = (target_s - now_s) % 86400 or 86400
        logger.debug("Daily bias refresh in %.0fs", wait)
        await asyncio.sleep(wait)
        await _refresh_daily_bias()


# ── Kline REST seed ────────────────────────────────────────────────────────────

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


# ── Buffer helpers ─────────────────────────────────────────────────────────────

def _make_stream_url(symbols: list[str]) -> str:
    streams = []
    for sym in symbols:
        s = sym.lower()
        streams += [f"{s}@kline_1m", f"{s}@kline_1h"]
    return f"{config.BINANCE_WS_BASE}/stream?streams={'/'.join(streams)}"


def _parse_kline_msg(msg: dict) -> Optional[tuple[str, str, dict, bool]]:
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
    return data["s"], k["i"], candle, k["x"]


def _update_buffer(symbol: str, interval: str, candle: dict, is_closed: bool) -> None:
    buf = _kline_buffers[symbol][interval]
    if buf and buf[-1]["time"] == candle["time"]:
        buf[-1] = candle          # update in-progress or replace with final
    else:
        buf.append(candle)
    if is_closed:
        limit = {"1h": 50, "1m": 10}.get(interval, 50)
        if len(buf) > limit:
            _kline_buffers[symbol][interval] = buf[-limit:]


# ── Trigger 1: 1h candle close → sweep + FVG detection ───────────────────────

async def _on_1h_close(symbol: str, utc_now: datetime) -> None:
    """
    Called each time a 1h candle closes for a watchlist symbol.
    Runs sweep detection, then FVG detection if a sweep is cached.
    All work is pure-Python — safe to call directly on the event loop
    for a single symbol (no pandas, no blocking I/O).
    """
    bias = daily_bias_cache.get(symbol)
    if not bias or bias.get("bias") == "NEUTRAL":
        return

    if not signal_engine._in_ny_window(utc_now):
        return

    direction = bias["bias"]
    k1h = _kline_buffers[symbol]["1h"]

    if len(k1h) < config.SWEEP_LOOKBACK + 3:
        return

    # ── Phase 2: Sweep ──────────────────────────────────────────────────────
    sweep = signal_engine._find_sweep(k1h, direction, config.SWEEP_LOOKBACK)

    if sweep is not None:
        existing = sweep_cache.get(symbol)
        if not existing or existing.get("time") != sweep.get("time"):
            # New (or updated) sweep found — cache it
            sweep_cache[symbol] = {
                "candle":      sweep,
                "sweep_level": sweep["sweep_level"],
                "direction":   direction,
                "time":        sweep.get("time", 0),
            }
            logger.info(
                "Sweep detected: %s %s at level=%.6f sweep_candle_time=%s",
                symbol, direction, sweep["sweep_level"], sweep.get("time"),
            )
            await wsb.broadcaster.broadcast("sweep_detected", {
                "symbol":      symbol,
                "direction":   direction,
                "sweep_level": sweep["sweep_level"],
                "sweep_time":  sweep.get("time", 0),
            })
    elif symbol in sweep_cache and sweep_cache[symbol].get("direction") != direction:
        # Bias flipped — invalidate stale sweep/FVG
        sweep_cache.pop(symbol, None)
        fvg_cache.pop(symbol, None)
        return

    # ── Phase 3: FVG (only if we have a cached sweep) ───────────────────────
    cached_sweep = sweep_cache.get(symbol)
    if not cached_sweep:
        return

    atr = signal_engine._atr(k1h, period=14)
    if atr <= 0:
        return

    fvg = signal_engine._find_fvg(
        k1h, direction, cached_sweep["time"], atr
    )

    if fvg is not None:
        existing_fvg = fvg_cache.get(symbol)
        if not existing_fvg or (
            existing_fvg.get("fvg_low")  != fvg["fvg_low"] or
            existing_fvg.get("fvg_high") != fvg["fvg_high"]
        ):
            fvg_cache[symbol] = {**fvg, "direction": direction}
            logger.info(
                "FVG formed: %s %s [%.6f - %.6f]",
                symbol, direction, fvg["fvg_low"], fvg["fvg_high"],
            )
            await wsb.broadcaster.broadcast("fvg_formed", {
                "symbol":    symbol,
                "direction": direction,
                "fvg_high":  fvg["fvg_high"],
                "fvg_low":   fvg["fvg_low"],
            })


# ── Trigger 2: mark price tick → entry check ──────────────────────────────────

async def _on_mark_price(symbol: str, mark: float, utc_now: datetime) -> None:
    """
    Called on every mark-price update for symbols that have an FVG cached.
    Runs the full check_daily_sweep() with current mark price.
    """
    bias = daily_bias_cache.get(symbol)
    if not bias or bias.get("bias") == "NEUTRAL":
        return

    if not signal_engine._in_ny_window(utc_now):
        return

    k1h = _kline_buffers[symbol]["1h"]

    sig = signal_engine.check_daily_sweep(
        symbol, k1h, bias, mark, utc_now,
    )
    if sig is None:
        return

    # ── Signal fired ─────────────────────────────────────────────────────────
    # Clear caches — repopulate only when next sweep+FVG forms
    sweep_cache.pop(symbol, None)
    fvg_cache.pop(symbol, None)

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
        "Signal fired: %s %s entry=%.6f sl=%.6f tp1=%.6f",
        symbol, sig["direction"], sig["entry_price"],
        sig["sl_price"], sig["tp1_price"],
    )


# ── Kline WebSocket ────────────────────────────────────────────────────────────

async def _run_kline_ws(symbols: list[str]) -> None:
    """
    Subscribe to combined 1m + 1h kline stream.
    On every 1h close → call _on_1h_close() (Trigger 1).
    Broadcasts kline_update to subscribed UI clients.
    Auto-reconnects with exponential backoff.
    """
    backoff = 1
    watchlist_set = set(symbols)
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
                    if not parsed:
                        continue

                    sym, interval, candle, is_closed = parsed
                    _update_buffer(sym, interval, candle, is_closed)

                    # Broadcast to subscribed UI clients
                    await wsb.broadcaster.broadcast_kline(
                        sym, interval,
                        {
                            "symbol":    sym,
                            "interval":  interval,
                            "candle":    candle,
                            "is_closed": is_closed,
                        },
                    )

                    # Trigger 1 — 1h candle close
                    if is_closed and interval == "1h" and sym in watchlist_set:
                        utc_now = datetime.now(timezone.utc)
                        await _on_1h_close(sym, utc_now)

        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("Kline WS error: %s — reconnect in %ds", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


# ── Mark-price WebSocket ───────────────────────────────────────────────────────

async def _run_mark_price_ws(symbols: list[str]) -> None:
    """
    Subscribe to !markPrice@arr.
    Updates mark_prices dict (used by position_tracker for paper PnL).
    Trigger 2 — runs _on_mark_price() for every symbol in fvg_cache.
    Auto-reconnects with exponential backoff.
    """
    url = f"{config.BINANCE_WS_BASE}/ws/!markPrice@arr"
    backoff = 1
    symbol_set = set(symbols)
    while True:
        logger.info("Connecting mark-price WS...")
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                backoff = 1
                async for raw in ws:
                    data = json.loads(raw)
                    if not isinstance(data, list):
                        continue

                    utc_now = datetime.now(timezone.utc)
                    for item in data:
                        sym = item.get("s", "")
                        if sym not in symbol_set:
                            continue
                        price = float(item.get("p", 0))
                        if price <= 0:
                            continue
                        mark_prices[sym] = price

                        # Trigger 2 — only for symbols with an FVG cached
                        if sym in fvg_cache:
                            await _on_mark_price(sym, price, utc_now)

        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("Mark-price WS error: %s — reconnect in %ds", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


# ── Entry point ────────────────────────────────────────────────────────────────

async def start(order_manager=None) -> None:
    """
    Build watchlist, seed klines + daily bias, start all async tasks.
    Called from main.py startup event.
    """
    global active_watchlist
    if order_manager:
        set_order_manager(order_manager)

    active_watchlist = await build_watchlist()
    await wsb.broadcaster.broadcast(
        "scanner_watchlist",
        {"symbols": active_watchlist, "count": len(active_watchlist)},
    )

    logger.info("Seeding klines + daily bias for %d symbols...", len(active_watchlist))
    batch_size = 10
    for i in range(0, len(active_watchlist), batch_size):
        batch = active_watchlist[i: i + batch_size]
        await asyncio.gather(
            *[_seed_klines(sym) for sym in batch],
            *[_seed_daily_klines(sym) for sym in batch],
        )
        await asyncio.sleep(0.5)

    asyncio.create_task(_run_kline_ws(active_watchlist),      name="kline_ws")
    asyncio.create_task(_run_mark_price_ws(active_watchlist), name="mark_price_ws")
    asyncio.create_task(refresh_watchlist_loop(),             name="watchlist_refresh")
    asyncio.create_task(_daily_bias_refresh_loop(),           name="daily_bias_refresh")
    logger.info("Scanner started (event-driven mode).")
