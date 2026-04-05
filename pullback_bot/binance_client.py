"""
binance_client.py — Binance USDT-M Futures REST client.
Handles HMAC-SHA256 signed requests, testnet support, exchange info caching.
"""
import hashlib
import hmac
import logging
import time
from typing import Any, Optional
from urllib.parse import urlencode

import httpx

import config

logger = logging.getLogger(__name__)

_BASE = config.BINANCE_REST_BASE
_API_KEY = config.BINANCE_API_KEY
_SECRET = config.BINANCE_API_SECRET

# Exchange info cache: {symbol -> symbol_info dict}
_exchange_info_cache: dict[str, dict] = {}


# ── Signing ────────────────────────────────────────────────────────────────────

def _sign(params: dict) -> dict:
    """Append timestamp and signature to params dict (mutates and returns it)."""
    params["timestamp"] = int(time.time() * 1000)
    query = urlencode(params)
    sig = hmac.new(
        _SECRET.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    params["signature"] = sig
    return params


def _headers() -> dict:
    return {"X-MBX-APIKEY": _API_KEY}


# ── Generic request helpers ────────────────────────────────────────────────────

async def _get(path: str, params: Optional[dict] = None, signed: bool = False) -> Any:
    url = f"{_BASE}{path}"
    p = params.copy() if params else {}
    if signed:
        p = _sign(p)
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url, params=p, headers=_headers() if signed else {})
    if resp.status_code != 200:
        logger.error("GET %s -> %d: %s", path, resp.status_code, resp.text[:300])
        resp.raise_for_status()
    return resp.json()


async def _post(path: str, params: dict, signed: bool = True) -> Any:
    url = f"{_BASE}{path}"
    p = _sign(params.copy()) if signed else params.copy()
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(url, params=p, headers=_headers())
    if resp.status_code != 200:
        logger.error("POST %s -> %d: %s", path, resp.status_code, resp.text[:300])
        resp.raise_for_status()
    return resp.json()


async def _delete(path: str, params: dict, signed: bool = True) -> Any:
    url = f"{_BASE}{path}"
    p = _sign(params.copy()) if signed else params.copy()
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.delete(url, params=p, headers=_headers())
    if resp.status_code != 200:
        logger.error("DELETE %s -> %d: %s", path, resp.status_code, resp.text[:300])
        resp.raise_for_status()
    return resp.json()


# ── Public market data ─────────────────────────────────────────────────────────

async def get_exchange_info() -> dict:
    """Fetch full exchange info and cache per-symbol filters."""
    global _exchange_info_cache
    data = await _get("/fapi/v1/exchangeInfo")
    for sym in data.get("symbols", []):
        _exchange_info_cache[sym["symbol"]] = sym
    logger.info("Exchange info loaded: %d symbols", len(_exchange_info_cache))
    return data


async def get_active_perpetual_symbols() -> list[dict]:
    """Return list of USDT-M perpetual symbols that are currently TRADING."""
    data = await get_exchange_info()
    result = []
    for sym in data.get("symbols", []):
        if (
            sym.get("quoteAsset") == "USDT"
            and sym.get("contractType") == "PERPETUAL"
            and sym.get("status") == "TRADING"
        ):
            result.append(sym)
    logger.info("Active perpetual USDT symbols: %d", len(result))
    return result


async def get_24h_tickers() -> list[dict]:
    """Fetch 24h ticker stats for all futures symbols."""
    return await _get("/fapi/v1/ticker/24hr")


async def get_klines(symbol: str, interval: str, limit: int = 200) -> list[list]:
    """
    Fetch historical klines.
    Returns list of [open_time, open, high, low, close, volume, ...].
    """
    data = await _get(
        "/fapi/v1/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
    )
    return data


async def get_mark_price(symbol: str) -> float:
    """Return current mark price for a symbol."""
    data = await _get("/fapi/v1/premiumIndex", params={"symbol": symbol})
    return float(data["markPrice"])


async def get_positions() -> list[dict]:
    """Get all open positions (signed)."""
    return await _get("/fapi/v2/positionRisk", params={}, signed=True)


# ── Account / Order methods (signed) ──────────────────────────────────────────

async def set_margin_type(symbol: str, margin_type: str) -> dict:
    """Set margin type to ISOLATED or CROSSED. Binance returns error 400 if already set — ignore it."""
    try:
        return await _post(
            "/fapi/v1/marginType",
            params={"symbol": symbol, "marginType": margin_type},
        )
    except Exception as exc:
        # Code -4046: "No need to change margin type" — not a real error
        if "-4046" in str(exc) or "No need to change" in str(exc):
            return {}
        raise


async def set_leverage(symbol: str, leverage: int) -> dict:
    return await _post(
        "/fapi/v1/leverage",
        params={"symbol": symbol, "leverage": leverage},
    )


async def place_market_order(
    symbol: str,
    side: str,         # BUY | SELL
    qty: float,
    reduce_only: bool = False,
) -> dict:
    params: dict[str, Any] = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": qty,
    }
    if reduce_only:
        params["reduceOnly"] = "true"
    return await _post("/fapi/v1/order", params=params)


async def place_stop_market_order(
    symbol: str,
    side: str,
    stop_price: float,
    close_position: bool = True,
) -> dict:
    params: dict[str, Any] = {
        "symbol": symbol,
        "side": side,
        "type": "STOP_MARKET",
        "stopPrice": round(stop_price, 8),
        "closePosition": "true" if close_position else "false",
    }
    return await _post("/fapi/v1/order", params=params)


async def place_take_profit_market_order(
    symbol: str,
    side: str,
    stop_price: float,
    close_position: bool = True,
) -> dict:
    params: dict[str, Any] = {
        "symbol": symbol,
        "side": side,
        "type": "TAKE_PROFIT_MARKET",
        "stopPrice": round(stop_price, 8),
        "closePosition": "true" if close_position else "false",
    }
    return await _post("/fapi/v1/order", params=params)


async def cancel_order(symbol: str, order_id: int) -> dict:
    return await _delete(
        "/fapi/v1/order",
        params={"symbol": symbol, "orderId": order_id},
    )


async def cancel_all_orders(symbol: str) -> dict:
    return await _delete(
        "/fapi/v1/allOpenOrders",
        params={"symbol": symbol},
    )


# ── User Data Stream ───────────────────────────────────────────────────────────

async def create_listen_key() -> str:
    data = await _post("/fapi/v1/listenKey", params={}, signed=False)
    return data["listenKey"]


async def keepalive_listen_key(listen_key: str) -> None:
    async with httpx.AsyncClient(timeout=10) as client:
        await client.put(
            f"{_BASE}/fapi/v1/listenKey",
            params={"listenKey": listen_key},
            headers=_headers(),
        )


# ── Symbol filter helpers ─────────────────────────────────────────────────────

def get_step_size(symbol: str) -> float:
    """Return the LOT_SIZE stepSize for a symbol from the cached exchange info."""
    info = _exchange_info_cache.get(symbol, {})
    for f in info.get("filters", []):
        if f["filterType"] == "LOT_SIZE":
            return float(f["stepSize"])
    return 0.001  # safe fallback


def get_tick_size(symbol: str) -> float:
    """Return the PRICE_FILTER tickSize for a symbol."""
    info = _exchange_info_cache.get(symbol, {})
    for f in info.get("filters", []):
        if f["filterType"] == "PRICE_FILTER":
            return float(f["tickSize"])
    return 0.01


def round_step(value: float, step: float) -> float:
    """Round value to the nearest step (for qty/price rounding)."""
    if step <= 0:
        return value
    import math
    precision = max(0, -int(math.floor(math.log10(step))))
    return round(round(value / step) * step, precision)
