"""
main.py — FastAPI application entry point.

Startup sequence:
  1. Init DB (create tables)
  2. Load exchange info (symbol filters cache)
  3. Start scanner (watchlist + kline WS + evaluation loop)
  4. Start position tracker (live user-data WS or paper PnL loop)

Endpoints:
  GET  /                          Serve frontend/index.html
  WS   /ws                        WebSocket for UI clients
  GET  /api/trades                Last 50 closed trades
  GET  /api/positions             Current open trades (with unrealized PnL)
  GET  /api/stats                 Today's stats
  GET  /api/klines                Historical klines for chart
  GET  /api/watchlist             Current active watchlist
  GET  /api/status                Bot health / config summary
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

import binance_client as bc
import config
import db
import order_manager as om
import position_tracker
import scanner
import ws_broadcaster as wsb

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(title="Pullback Bot", version="1.0.0")

_FRONTEND = Path(__file__).parent / "frontend" / "index.html"


# ── Startup / Shutdown ────────────────────────────────────────────────────────

@app.on_event("startup")
async def on_startup() -> None:
    logger.info("=== Pullback Bot starting (MODE=%s) ===", config.MODE)

    # 1. Database
    await db.init_db()

    # 2. Exchange info (populates symbol filter cache for order sizing)
    try:
        await bc.get_exchange_info()
    except Exception as exc:
        logger.warning("Exchange info fetch failed: %s (non-fatal)", exc)

    # 3. Scanner
    await scanner.start(order_manager=om.order_manager)

    # 4. Position tracker
    await position_tracker.start()

    # 5. Periodic system_status broadcast
    asyncio.create_task(_status_broadcast_loop(), name="status_broadcast")

    logger.info("=== Bot ready on port %d ===", config.PORT)


async def _status_broadcast_loop() -> None:
    """Broadcast system_status every 5 seconds."""
    while True:
        await asyncio.sleep(5)
        try:
            open_count = await db.count_open_trades()
            await wsb.broadcaster.broadcast("system_status", {
                "mode": config.MODE,
                "timestamp": int(time.time()),
                "open_positions": open_count,
                "watchlist_count": len(scanner.active_watchlist),
                "ws_clients": wsb.broadcaster.client_count,
            })
        except Exception as exc:
            logger.debug("status_broadcast error: %s", exc)


# ── WebSocket endpoint ─────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await wsb.broadcaster.connect(ws)
    try:
        # Send initial state snapshot
        raw_open = await db.get_open_trades()
        open_trades = [
            position_tracker._enrich_position(
                t,
                scanner.mark_prices.get(t["symbol"], float(t["entry_price"])),
                0.0,   # unrealized PnL will be updated by position_update within 2s
            )
            for t in raw_open
        ]
        recent_trades = await db.get_recent_trades(50)
        stats = await db.get_today_stats()
        await wsb.broadcaster.send_to(ws, "init", {
            "mode": config.MODE,
            "open_trades": open_trades,
            "recent_trades": recent_trades,
            "stats": stats,
            "watchlist": scanner.active_watchlist,
        })

        # Listen for client messages (e.g. subscribe_chart)
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
                msg_type = msg.get("type")
                if msg_type == "subscribe_chart":
                    symbol = msg.get("symbol", "BTCUSDT").upper()
                    interval = msg.get("interval", "15m")
                    await wsb.broadcaster.subscribe_chart(ws, symbol, interval)
                    # Send buffered kline history for that symbol/interval
                    buf = scanner._kline_buffers.get(symbol, {}).get(interval, [])
                    await wsb.broadcaster.send_to(ws, "kline_history", {
                        "symbol": symbol,
                        "interval": interval,
                        "candles": buf[-200:],
                    })
            except (json.JSONDecodeError, KeyError):
                pass

    except WebSocketDisconnect:
        pass
    finally:
        await wsb.broadcaster.disconnect(ws)


# ── REST endpoints ─────────────────────────────────────────────────────────────

@app.get("/")
async def serve_ui() -> FileResponse:
    return FileResponse(_FRONTEND)


@app.get("/api/trades")
async def api_trades() -> JSONResponse:
    trades = await db.get_recent_trades(50)
    return JSONResponse(trades)


@app.get("/api/positions")
async def api_positions() -> JSONResponse:
    open_trades = await db.get_open_trades()
    result = []
    for t in open_trades:
        mark = scanner.mark_prices.get(t["symbol"], 0.0) or float(t["entry_price"])
        entry = float(t["entry_price"])
        qty = float(t["qty"])
        if t["direction"] == "LONG":
            upnl = (mark - entry) * qty
        else:
            upnl = (entry - mark) * qty
        result.append(position_tracker._enrich_position(t, mark, upnl))
    return JSONResponse(result)


@app.get("/api/stats")
async def api_stats() -> JSONResponse:
    stats = await db.get_today_stats()
    stats["open_positions"] = await db.count_open_trades()
    stats["watchlist_count"] = len(scanner.active_watchlist)
    stats["mode"] = config.MODE
    return JSONResponse(stats)


@app.get("/api/klines")
async def api_klines(symbol: str = "BTCUSDT", interval: str = "15m", limit: int = 200) -> JSONResponse:
    """
    Fetch historical klines from Binance REST.
    Returns array of {time, open, high, low, close, volume}.
    time is in UNIX seconds (Lightweight Charts requirement).
    """
    limit = min(limit, 1000)
    try:
        raw = await bc.get_klines(symbol.upper(), interval, limit)
        candles = [
            {
                "time": int(c[0]) // 1000,   # ms -> seconds
                "open": float(c[1]),
                "high": float(c[2]),
                "low": float(c[3]),
                "close": float(c[4]),
                "volume": float(c[5]),
            }
            for c in raw
        ]
        return JSONResponse(candles)
    except Exception as exc:
        logger.error("api_klines error: %s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/api/watchlist")
async def api_watchlist() -> JSONResponse:
    return JSONResponse({"symbols": scanner.active_watchlist, "count": len(scanner.active_watchlist)})


@app.get("/api/status")
async def api_status() -> JSONResponse:
    return JSONResponse({
        "mode": config.MODE,
        "testnet": config.BINANCE_TESTNET,
        "watchlist_count": len(scanner.active_watchlist),
        "open_positions": await db.count_open_trades(),
        "ws_clients": wsb.broadcaster.client_count,
        "signal_threshold": config.SIGNAL_SCORE_THRESHOLD,
        "risk_per_trade": config.RISK_PER_TRADE_USDT,
        "max_open_trades": config.MAX_OPEN_TRADES,
        "leverage": config.LEVERAGE,
        "timestamp": int(time.time()),
    })


# ── Dev entrypoint ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=config.PORT, reload=False)
