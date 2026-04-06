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
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
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
        closed_trades = await db.get_closed_trades(500)
        today_stats   = await db.get_today_stats()
        all_stats     = await db.get_all_stats()
        await wsb.broadcaster.send_to(ws, "init", {
            "mode": config.MODE,
            "open_trades": open_trades,
            "recent_trades": closed_trades,
            "stats": {**today_stats, **all_stats},
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


@app.delete("/api/trades/{trade_id}")
async def delete_trade(trade_id: int) -> JSONResponse:
    """Hard-delete a single closed trade record."""
    deleted = await db.delete_trade(trade_id)
    if not deleted:
        return JSONResponse({"error": "Trade not found or still open"}, status_code=404)
    await wsb.broadcaster.broadcast("trade_deleted", {"id": trade_id})
    stats = {**(await db.get_today_stats()), **(await db.get_all_stats())}
    await wsb.broadcaster.broadcast("stats_update", stats)
    return JSONResponse({"ok": True})


@app.delete("/api/trades")
async def delete_all_trades() -> JSONResponse:
    """Hard-delete all closed trade records."""
    count = await db.delete_all_closed_trades()
    await wsb.broadcaster.broadcast("trades_cleared", {"count": count})
    stats = {**(await db.get_today_stats()), **(await db.get_all_stats())}
    await wsb.broadcaster.broadcast("stats_update", stats)
    return JSONResponse({"ok": True, "deleted": count})


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


@app.post("/api/positions/{trade_id}/close")
async def close_position(trade_id: int) -> JSONResponse:
    """Close a specific open position at current mark price."""
    open_trades = await db.get_open_trades()
    trade = next((t for t in open_trades if t["id"] == trade_id), None)
    if not trade:
        return JSONResponse({"error": "Trade not found or already closed"}, status_code=404)

    symbol = trade["symbol"]
    mark = scanner.mark_prices.get(symbol) or float(trade["entry_price"])

    if config.MODE == "paper":
        direction = trade["direction"]
        entry = float(trade["entry_price"])
        qty = float(trade["qty"])
        gross = (mark - entry) * qty if direction == "LONG" else (entry - mark) * qty
        pnl = position_tracker._net_pnl(gross, entry, mark, qty)
        pnl_pct = pnl / (entry * qty) * 100 if entry * qty else 0.0
        close_time = int(time.time() * 1000)
        await db.update_trade_close(
            trade_id=trade_id,
            close_price=mark,
            close_time=close_time,
            pnl_usdt=round(pnl, 4),
            pnl_pct=round(pnl_pct, 2),
            close_reason="MANUAL",
        )
        await wsb.broadcaster.broadcast("trade_closed", {
            **trade,
            "close_price": mark,
            "close_reason": "MANUAL",
            "pnl_usdt": round(pnl, 4),
            "pnl_pct": round(pnl_pct, 2),
            "close_time": close_time,
        })
        return JSONResponse({"ok": True, "pnl_usdt": round(pnl, 4)})
    else:
        try:
            side = "SELL" if trade["direction"] == "LONG" else "BUY"
            await bc.cancel_all_orders(symbol)
            await bc.place_market_order(symbol, side, float(trade["qty"]), reduce_only=True)
            return JSONResponse({"ok": True})
        except Exception as exc:
            logger.error("close_position live error: %s", exc)
            return JSONResponse({"error": str(exc)}, status_code=500)


@app.post("/api/positions/close-all")
async def close_all_positions() -> JSONResponse:
    """Close all open positions at current mark price."""
    open_trades = await db.get_open_trades()
    if not open_trades:
        return JSONResponse({"ok": True, "closed": 0})

    results = []
    for trade in open_trades:
        symbol = trade["symbol"]
        mark = scanner.mark_prices.get(symbol) or float(trade["entry_price"])

        if config.MODE == "paper":
            direction = trade["direction"]
            entry = float(trade["entry_price"])
            qty = float(trade["qty"])
            gross = (mark - entry) * qty if direction == "LONG" else (entry - mark) * qty
            pnl = position_tracker._net_pnl(gross, entry, mark, qty)
            pnl_pct = pnl / (entry * qty) * 100 if entry * qty else 0.0
            close_time = int(time.time() * 1000)
            await db.update_trade_close(
                trade_id=trade["id"],
                close_price=mark,
                close_time=close_time,
                pnl_usdt=round(pnl, 4),
                pnl_pct=round(pnl_pct, 2),
                close_reason="MANUAL",
            )
            await wsb.broadcaster.broadcast("trade_closed", {
                **trade,
                "close_price": mark,
                "close_reason": "MANUAL",
                "pnl_usdt": round(pnl, 4),
                "pnl_pct": round(pnl_pct, 2),
                "close_time": close_time,
            })
            results.append({"id": trade["id"], "ok": True})
        else:
            try:
                side = "SELL" if trade["direction"] == "LONG" else "BUY"
                await bc.cancel_all_orders(symbol)
                await bc.place_market_order(symbol, side, float(trade["qty"]), reduce_only=True)
                results.append({"id": trade["id"], "ok": True})
            except Exception as exc:
                logger.error("close_all_positions live error for %s: %s", symbol, exc)
                results.append({"id": trade["id"], "error": str(exc)})

    return JSONResponse({"ok": True, "closed": len(results), "results": results})


@app.post("/api/positions/manual")
async def manual_trade(request: Request) -> JSONResponse:
    """Open a manual paper/live trade for a symbol at current mark price."""
    body = await request.json()
    symbol    = body.get("symbol", "").upper()
    direction = body.get("direction", "").upper()
    sl_price  = body.get("sl_price")

    if not symbol or direction not in ("LONG", "SHORT") or sl_price is None:
        return JSONResponse({"error": "symbol, direction (LONG|SHORT), sl_price required"}, status_code=400)

    mark = scanner.mark_prices.get(symbol)
    if not mark:
        return JSONResponse({"error": f"No mark price for {symbol} — is it on the watchlist?"}, status_code=400)

    sl = float(sl_price)
    entry = mark
    risk_dist = abs(entry - sl)

    if risk_dist == 0:
        return JSONResponse({"error": "SL price equals entry price"}, status_code=400)
    if direction == "LONG" and sl >= entry:
        return JSONResponse({"error": "SL must be below entry for LONG"}, status_code=400)
    if direction == "SHORT" and sl <= entry:
        return JSONResponse({"error": "SL must be above entry for SHORT"}, status_code=400)

    # TP at 1:1 (trail arm) and 2:1 RR
    if direction == "LONG":
        tp1 = entry + risk_dist
        tp2 = entry + risk_dist * 2
    else:
        tp1 = entry - risk_dist
        tp2 = entry - risk_dist * 2

    signal = {
        "symbol":      symbol,
        "direction":   direction,
        "entry_price": entry,
        "sl_price":    sl,
        "tp1_price":   tp1,
        "tp2_price":   tp2,
        "score":       100,
    }

    ok = await om.order_manager.handle_signal(signal)
    if ok:
        return JSONResponse({"ok": True, "entry": entry})
    return JSONResponse({"error": "Trade rejected — check MAX_OPEN_TRADES or position size"}, status_code=400)


@app.get("/api/watchlist")
async def api_watchlist() -> JSONResponse:
    return JSONResponse({"symbols": scanner.active_watchlist, "count": len(scanner.active_watchlist)})


@app.get("/api/config")
async def api_config_get() -> JSONResponse:
    """Return all editable config values."""
    data = config.get_all()
    data["_restart_required_keys"] = list(config.RESTART_REQUIRED_KEYS)
    return JSONResponse(data)


@app.post("/api/config")
async def api_config_post(request: Request) -> JSONResponse:
    """Apply one or more config changes. Body: {key: value, ...}"""
    body = await request.json()
    errors: dict[str, str] = {}
    applied: list[str] = []
    needs_restart: list[str] = []

    for key, raw in body.items():
        try:
            config.update(key, str(raw))
            applied.append(key)
            if key in config.RESTART_REQUIRED_KEYS:
                needs_restart.append(key)
        except ValueError as exc:
            errors[key] = str(exc)

    return JSONResponse({
        "applied": applied,
        "needs_restart": needs_restart,
        "errors": errors,
        "ok": len(errors) == 0,
    })


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
