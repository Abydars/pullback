"""
ws_order_api.py — Binance USDT-M Futures WebSocket API client.

Provides low-latency order operations over a persistent WS connection.
Falls back gracefully (raises) so callers can retry via REST if needed.

Supported: order.place, order.cancel, order.cancelAll
Not via WS: set_leverage, exchange_info (use REST for those)
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
import uuid
from typing import Any, Optional

import websockets

import config

logger = logging.getLogger(__name__)

# ── Endpoints ─────────────────────────────────────────────────────────────────

_WS_URL_MAINNET = "wss://ws-fapi.binance.com/ws-fapi/v1"
_WS_URL_TESTNET = "wss://testnet.binancefuture.com/ws-fapi/v1"


def _ws_url() -> str:
    return _WS_URL_TESTNET if config.BINANCE_TESTNET else _WS_URL_MAINNET


# ── Signing ───────────────────────────────────────────────────────────────────

def _sign_params(params: dict) -> dict:
    """
    Sign params for WS API: HMAC-SHA256 of alphabetically sorted key=value pairs
    (including apiKey and timestamp, excluding signature) joined by '&'.
    Returns a new dict with signature added.
    """
    p = dict(params)
    p["apiKey"] = config.BINANCE_API_KEY
    p["timestamp"] = int(time.time() * 1000)

    # Sort alphabetically by key, build query string
    sorted_pairs = "&".join(f"{k}={v}" for k, v in sorted(p.items()))

    sig = hmac.new(
        config.BINANCE_API_SECRET.encode("utf-8"),
        sorted_pairs.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    p["signature"] = sig
    return p


class BinanceWsOrderApi:
    """Persistent authenticated WebSocket connection to Binance USDT-M Futures WS API."""

    def __init__(self) -> None:
        self._ws: Optional[Any] = None
        self._connected: bool = False
        self._recv_task: Optional[asyncio.Task] = None
        # Pending futures: request id -> asyncio.Future
        self._pending: dict[str, asyncio.Future] = {}

    async def start(self) -> None:
        """Connect to WS API and start the receive loop as a background task."""
        await self._connect()
        self._recv_task = asyncio.ensure_future(self._recv_loop())
        logger.info("BinanceWsOrderApi started (testnet=%s)", config.BINANCE_TESTNET)

    async def stop(self) -> None:
        """Clean shutdown — cancel recv loop and close connection."""
        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
        await self._close()
        logger.info("BinanceWsOrderApi stopped")

    async def _connect(self) -> None:
        """Open the WebSocket connection."""
        url = _ws_url()
        self._ws = await websockets.connect(url, ping_interval=20, ping_timeout=10)
        self._connected = True
        logger.info("WS order API connected: %s", url)

    async def _close(self) -> None:
        """Close the WebSocket connection if open."""
        self._connected = False
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

    def _fail_all_pending(self, exc: Exception) -> None:
        """Fail all outstanding request futures with the given exception."""
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(exc)
        self._pending.clear()

    async def _recv_loop(self) -> None:
        """
        Receive loop — reads messages from WS and resolves pending futures.
        Auto-reconnects with 2s backoff on disconnect.
        """
        backoff = 2
        while True:
            try:
                if not self._connected or self._ws is None:
                    await self._connect()
                    backoff = 2

                async for raw in self._ws:
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        logger.warning("WS order API: invalid JSON: %s", raw[:200])
                        continue

                    req_id = str(msg.get("id", ""))
                    fut = self._pending.pop(req_id, None)
                    if fut and not fut.done():
                        status = msg.get("status", 0)
                        if status == 200:
                            fut.set_result(msg.get("result", {}))
                        else:
                            error = msg.get("error", {})
                            fut.set_exception(
                                RuntimeError(
                                    f"Binance WS API error {status}: "
                                    f"{error.get('code')} {error.get('msg')}"
                                )
                            )

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("WS order API disconnected: %s — reconnect in %ds", exc, backoff)
                self._connected = False
                disc_exc = ConnectionError(f"WS order API disconnected: {exc}")
                self._fail_all_pending(disc_exc)
                try:
                    await asyncio.sleep(backoff)
                except asyncio.CancelledError:
                    break
                backoff = min(backoff * 2, 30)

    async def request(
        self,
        method: str,
        params: dict,
        timeout: float = 10.0,
    ) -> dict:
        """
        Send a signed WS API request and await the response.

        Raises:
            ConnectionError: if not connected
            TimeoutError: if no response within timeout seconds
            RuntimeError: on Binance API error
        """
        if not self._connected or self._ws is None:
            raise ConnectionError("WS order API is not connected")

        req_id = str(uuid.uuid4())
        signed_params = _sign_params(params)

        payload = json.dumps({
            "id": req_id,
            "method": method,
            "params": signed_params,
        })

        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending[req_id] = fut

        try:
            await self._ws.send(payload)
            result = await asyncio.wait_for(asyncio.shield(fut), timeout=timeout)
            return result
        except asyncio.TimeoutError:
            self._pending.pop(req_id, None)
            if not fut.done():
                fut.cancel()
            raise TimeoutError(f"WS order API request '{method}' timed out after {timeout}s")
        except Exception:
            self._pending.pop(req_id, None)
            raise

    async def place_order(self, **kwargs) -> dict:
        """Place an order via WS API (order.place)."""
        return await self.request("order.place", dict(kwargs))

    async def cancel_order(self, symbol: str, order_id: int) -> dict:
        """Cancel an order via WS API (order.cancel)."""
        return await self.request("order.cancel", {"symbol": symbol, "orderId": order_id})

    async def cancel_all_orders(self, symbol: str) -> dict:
        """Cancel all open orders for a symbol via WS API (order.cancelAll)."""
        return await self.request("order.cancelAll", {"symbol": symbol})


# Module-level singleton
ws_order_api = BinanceWsOrderApi()
