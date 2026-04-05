"""
ws_broadcaster.py — Manages connected FastAPI WebSocket clients.
Push JSON messages to all connected UI clients by type.
Supports per-client chart subscriptions (symbol + interval).
"""
import asyncio
import json
import logging
from typing import Any, Optional

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class WSBroadcaster:
    """
    Thread-safe registry of active WebSocket connections.
    Call broadcast(type, data) to push a message to all clients.
    Per-client chart subscriptions tracked in _chart_subs.
    """

    def __init__(self) -> None:
        self._clients: set[WebSocket] = set()
        # ws -> (symbol, interval)
        self._chart_subs: dict[WebSocket, tuple[str, str]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        async with self._lock:
            self._clients.add(ws)
        logger.info("WS client connected. Total: %d", len(self._clients))

    async def disconnect(self, ws: WebSocket) -> None:
        async with self._lock:
            self._clients.discard(ws)
            self._chart_subs.pop(ws, None)
        logger.info("WS client disconnected. Total: %d", len(self._clients))

    # ── Chart subscription ─────────────────────────────────────────────────────

    async def subscribe_chart(self, ws: WebSocket, symbol: str, interval: str) -> None:
        """Register which symbol+interval a client wants chart kline_update for."""
        async with self._lock:
            self._chart_subs[ws] = (symbol.upper(), interval)
        logger.debug("Chart sub: %s %s/%s", id(ws), symbol, interval)

    async def broadcast_kline(self, symbol: str, interval: str, data: Any) -> None:
        """Send kline_update only to clients subscribed to that symbol+interval."""
        message = json.dumps({"type": "kline_update", "data": data})
        async with self._lock:
            targets = [
                ws
                for ws, sub in self._chart_subs.items()
                if sub == (symbol.upper(), interval)
            ]

        dead: list[WebSocket] = []
        for ws in targets:
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)

        if dead:
            async with self._lock:
                for ws in dead:
                    self._clients.discard(ws)
                    self._chart_subs.pop(ws, None)

    # ── General broadcast ──────────────────────────────────────────────────────

    async def broadcast(self, msg_type: str, data: Any) -> None:
        """Send {type, data} JSON to all connected clients. Dead clients removed."""
        message = json.dumps({"type": msg_type, "data": data})
        async with self._lock:
            clients = list(self._clients)

        dead: list[WebSocket] = []
        for ws in clients:
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)

        if dead:
            async with self._lock:
                for ws in dead:
                    self._clients.discard(ws)
                    self._chart_subs.pop(ws, None)
            logger.debug("Removed %d dead WS client(s)", len(dead))

    async def send_to(self, ws: WebSocket, msg_type: str, data: Any) -> None:
        """Send a message to a single client."""
        try:
            await ws.send_text(json.dumps({"type": msg_type, "data": data}))
        except Exception as exc:
            logger.warning("send_to failed: %s", exc)

    @property
    def client_count(self) -> int:
        return len(self._clients)

    def get_chart_sub(self, ws: WebSocket) -> Optional[tuple[str, str]]:
        return self._chart_subs.get(ws)


# Global singleton — imported everywhere
broadcaster = WSBroadcaster()
