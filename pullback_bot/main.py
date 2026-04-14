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
import base64
from pathlib import Path

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, Response
import binance_client as bc
import config
import db
import analytics
import order_manager as om
import position_tracker
import scanner
import ws_broadcaster as wsb
from user_data_stream import user_data_stream

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(title="Pullback Bot", version="1.0.0")

@app.middleware("http")
async def cookie_auth(request: Request, call_next):
    if not getattr(config, "WEB_PASSWORD", ""):
        return await call_next(request)
        
    allowed_paths = {"/", "/api/auth", "/favicon.ico"}
    if request.url.path in allowed_paths:
        return await call_next(request)
        
    auth_cookie = request.cookies.get("pullback_auth")
    if not auth_cookie or auth_cookie != config.WEB_PASSWORD:
        return Response("Unauthorized", status_code=401)
        
    return await call_next(request)

_FRONTEND = Path(__file__).parent / "frontend" / "index.html"

@app.post("/api/auth")
async def api_auth(request: Request):
    if not getattr(config, "WEB_PASSWORD", ""):
        return JSONResponse({"ok": True})
        
    try:
        body = await request.json()
        pwd = body.get("password")
    except Exception:
        pwd = None
        
    if pwd == config.WEB_PASSWORD:
        resp = JSONResponse({"ok": True})
        resp.set_cookie("pullback_auth", config.WEB_PASSWORD, httponly=True, max_age=86400*30)
        return resp
        
    return JSONResponse({"error": "Invalid password"}, status_code=401)


# ── Startup / Shutdown ────────────────────────────────────────────────────────

@app.on_event("startup")
async def on_startup() -> None:
    logger.info("=== Pullback Bot starting (MODE=%s) ===", config.MODE)

    # 1. Database
    await db.init_db()

    # 1b. Overlay DB-persisted config on top of .env defaults
    await config.load_from_db()

    # 2. Exchange info (populates symbol filter cache for order sizing)
    try:
        await bc.get_exchange_info()
    except Exception as exc:
        logger.warning("Exchange info fetch failed: %s (non-fatal)", exc)

    # 3. Start Core Binance WS API for Execution
    asyncio.create_task(bc.start_ws_api_client(), name="binance_ws_api")

    # 3a. User data stream (live mode only)
    if config.MODE == "live":
        await user_data_stream.start()
        logger.info("Live mode: User data stream started")

    # 3b. Restore active session if open trades already have a session_id
    await om.restore_session()

    # 4. Scanner
    await scanner.start(order_manager=om.order_manager)

    # 5. Position tracker
    await position_tracker.start()

    # 6. Periodic system_status broadcast + WS heartbeat
    asyncio.create_task(_status_broadcast_loop(), name="status_broadcast")
    asyncio.create_task(wsb.broadcaster.start_heartbeat(), name="ws_heartbeat")

    logger.info("=== Bot ready on port %d ===", config.PORT)


@app.on_event("shutdown")
async def on_shutdown() -> None:
    logger.info("=== Pullback Bot shutting down ===")
    if config.MODE == "live":
        await user_data_stream.stop()


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
                "btc_regime": scanner._last_btc_regime,
            })
        except Exception as exc:
            logger.debug("status_broadcast error: %s", exc)


# ── WebSocket endpoint ─────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    if getattr(config, "WEB_PASSWORD", ""):
        auth_cookie = ws.cookies.get("pullback_auth")
        if not auth_cookie or auth_cookie != config.WEB_PASSWORD:
            await ws.close(code=1008, reason="Unauthorized")
            return
            
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
            "btc_regime": scanner._last_btc_regime,
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
                    
                    if symbol not in scanner._kline_buffers or not scanner._kline_buffers[symbol].get(interval, []):
                        # The symbol is completely dead (not in active_watchlist or open trades)
                        # We must seed it instantly and restart the WS to pick it up live
                        await scanner._seed_klines(symbol)
                        scanner.request_kline_ws_restart()

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


@app.get("/api/scanner_events")
async def api_scanner_events(limit: int = 100, offset: int = 0) -> JSONResponse:
    logs = await db.get_recent_scanner_log(limit, offset)
    return JSONResponse(logs)


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


@app.get("/api/sl-suggest")
async def sl_suggest(symbol: str, direction: str) -> JSONResponse:
    """
    Suggest SL/TP for a manual trade — consistent with the signal engine:
      SL  = entry ± 1.5×ATR14   (beyond normal 15m noise)
      TP1 = entry ± 1.0×ATR14   (trail arm activation)
    """
    import pandas as pd

    direction = direction.upper()
    if direction not in ("LONG", "SHORT"):
        return JSONResponse({"error": "direction must be LONG or SHORT"}, status_code=400)

    mark = scanner.mark_prices.get(symbol)
    if not mark:
        return JSONResponse({"error": f"No mark price for {symbol}"}, status_code=400)

    buf = scanner._kline_buffers.get(symbol, {}).get("15m", [])
    if len(buf) < 20:
        return JSONResponse({"error": "Insufficient candle history"}, status_code=400)

    df = pd.DataFrame(buf[-50:])
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [df["high"] - df["low"],
         (df["high"] - prev_close).abs(),
         (df["low"]  - prev_close).abs()], axis=1
    ).max(axis=1)
    atr = float(tr.ewm(span=14, adjust=False).mean().iloc[-1])

    entry = mark
    if direction == "LONG":
        sl  = round(entry - atr * 1.5, 8)
        tp1 = round(entry + atr * 1.0, 8)
    else:
        sl  = round(entry + atr * 1.5, 8)
        tp1 = round(entry - atr * 1.0, 8)

    risk = abs(entry - sl)
    if risk == 0:
        return JSONResponse({"error": "Computed SL equals entry"}, status_code=400)

    return JSONResponse({
        "entry":    round(entry, 8),
        "sl_price": sl,
        "tp1":      tp1,
        "tp2":      tp1,
        "atr":      round(atr, 8),
        "sl_pct":   round(risk / entry * 100, 3),
    })


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

    # Trail arm and ATR: entry ± 1×ATR14 (consistent with signal engine).
    import pandas as pd
    buf15 = scanner._kline_buffers.get(symbol, {}).get("15m", [])
    if len(buf15) >= 14:
        _df = pd.DataFrame(buf15[-50:])
        _prev_close = _df["close"].shift(1)
        _tr = pd.concat([_df["high"] - _df["low"],
                         (_df["high"] - _prev_close).abs(),
                         (_df["low"]  - _prev_close).abs()], axis=1).max(axis=1)
        _atr = float(_tr.ewm(span=14, adjust=False).mean().iloc[-1])
    else:
        _atr = risk_dist / 1.5   # fallback: infer ATR from user-provided SL distance

    if direction == "LONG":
        tp1 = round(entry + _atr * 1.0, 8)
    else:
        tp1 = round(entry - _atr * 1.0, 8)
    tp2 = tp1  # DB compat

    signal = {
        "symbol":      symbol,
        "direction":   direction,
        "entry_price": entry,
        "sl_price":    sl,
        "tp1_price":   tp1,
        "tp2_price":   tp2,
        "atr":         _atr,
        "score":       100,
        "signal_type": "MANUAL",
    }

    ok = await om.order_manager.handle_signal(signal)
    if ok:
        return JSONResponse({"ok": True, "entry": entry})
    return JSONResponse({"error": "Trade rejected — check MAX_OPEN_TRADES or position size"}, status_code=400)


@app.get("/api/watchlist")
async def api_watchlist() -> JSONResponse:
    return JSONResponse({"symbols": scanner.active_watchlist, "count": len(scanner.active_watchlist)})


@app.get("/api/sessions")
async def api_sessions() -> JSONResponse:
    """Return completed sessions (ended_at IS NOT NULL), newest first, with their trades."""
    sessions = await db.get_sessions(50)
    return JSONResponse(sessions)


@app.get("/api/analytics")
async def api_analytics(start: int = None, end: int = None) -> JSONResponse:
    """Return comprehensive computed analytics payload."""
    trades = await db.get_analytics_trades(start_ms=start, end_ms=end)
    stats = analytics.compute_analytics(trades)
    return JSONResponse(stats)


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
            await config.update(key, str(raw))
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
        "risk_pct": config.RISK_PCT,
        "max_open_trades": config.MAX_OPEN_TRADES,
        "max_leverage": config.MAX_LEVERAGE,
        "timestamp": int(time.time()),
    })


# ── Dev entrypoint ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=config.PORT, reload=False)
