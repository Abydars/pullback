"""
binance_client.py — Binance USDT-M Futures REST client.
Handles HMAC-SHA256 signed requests, testnet support, exchange info caching.
"""
import hashlib
import hmac
import logging
import time
import asyncio
import base64
import json
import uuid
import websockets
from typing import Any, Optional
from urllib.parse import urlencode
from cryptography.hazmat.primitives import serialization

import httpx
import config

logger = logging.getLogger(__name__)

# Exchange info cache: {symbol -> symbol_info dict}
_exchange_info_cache: dict[str, dict] = {}

_WS_API_URL = "wss://testnet.binancefuture.com/ws-fapi/v1" if config.BINANCE_TESTNET else "wss://ws-fapi.binance.com/ws-fapi/v1"
_private_key_obj = None

def _get_private_key():
    global _private_key_obj
    if _private_key_obj:
        return _private_key_obj
        
    k = config.BINANCE_PRIVATE_KEY
    if not k:
        return None
        
    pem_data = k.replace("\\n", "\n").encode("utf-8")
    if b"BEGIN PRIVATE KEY" not in pem_data:
        pem_data = b"-----BEGIN PRIVATE KEY-----\n" + pem_data + b"\n-----END PRIVATE KEY-----\n"

    try:
        _private_key_obj = serialization.load_pem_private_key(pem_data, password=None)
        return _private_key_obj
    except Exception as e:
        logger.error("Failed to load Ed25519 private key: %s", e)
        return None

def _sign_ed25519(params: dict, sort: bool = False) -> str:
    # REST queries require exact insertion order. WS JSON payloads require alphabetical sorting.
    p = dict(sorted(params.items())) if sort else params
    query_string = urlencode(p)
    
    pk = _get_private_key()
    if not pk:
        raise ValueError("Ed25519 Private Key is not loaded or invalid.")
        
    signature_bytes = pk.sign(query_string.encode("utf-8"))
    return base64.b64encode(signature_bytes).decode("utf-8")


# ── WS API Client ─────────────────────────────────────────────────────────────

_ws_connection = None
_pending_requests = {}

async def start_ws_api_client():
    global _ws_connection
    backoff = 1
    while True:
        try:
            ws_url = "wss://testnet.binancefuture.com/ws-fapi/v1" if config.BINANCE_TESTNET else "wss://ws-fapi.binance.com/ws-fapi/v1"
            logger.info("Connecting to Binance WS API: %s", ws_url)
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
                _ws_connection = ws
                backoff = 1
                logger.info("Binance WS API Connected!")
                async for message in ws:
                    data = json.loads(message)
                    req_id = data.get("id")
                    if req_id and req_id in _pending_requests:
                        fut = _pending_requests.pop(req_id)
                        if not fut.done():
                            fut.set_result(data)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning("Binance WS API connection lost: %s. Reconnecting in %ds", e, backoff)
            _ws_connection = None
            for req_id, fut in list(_pending_requests.items()):
                if not fut.done():
                    fut.set_exception(Exception(f"WS disconnected: {e}"))
            _pending_requests.clear()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

async def _ws_request(method: str, params: dict, signed: bool = True) -> dict:
    if not _ws_connection:
        raise Exception("WebSocket API not connected. Cannot place order.")
        
    req_id = str(uuid.uuid4())
    fut = asyncio.get_event_loop().create_future()
    _pending_requests[req_id] = fut
    
    payload_params = params.copy()
    payload_params["apiKey"] = config.BINANCE_API_KEY
    if signed:
        payload_params["timestamp"] = int(time.time() * 1000)
        payload_params["signature"] = _sign_ed25519(payload_params, sort=True)
        
    payload = {
        "id": req_id,
        "method": method,
        "params": payload_params
    }
    
    await _ws_connection.send(json.dumps(payload))
    
    try:
        response = await asyncio.wait_for(fut, timeout=10.0)
    except asyncio.TimeoutError:
        _pending_requests.pop(req_id, None)
        raise Exception(f"WS request timeout for {method}")
        
    if "error" in response:
        raise Exception(f"Binance WS API Error: {response['error']}")
        
    return response.get("result", {})



# ── Signing ────────────────────────────────────────────────────────────────────

def _sign(params: dict) -> dict:
    """Append timestamp and signature to params. Uses Ed25519 if PK exists, else HMAC."""
    params["timestamp"] = int(time.time() * 1000)
    
    if config.BINANCE_PRIVATE_KEY:
        params["signature"] = _sign_ed25519(params)
        return params

    query = urlencode(params)
    sig = hmac.new(
        config.BINANCE_API_SECRET.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    params["signature"] = sig
    return params


def _headers() -> dict:
    return {"X-MBX-APIKEY": config.BINANCE_API_KEY}


# ── Generic request helpers ────────────────────────────────────────────────────

async def _get(path: str, params: Optional[dict] = None, signed: bool = False) -> Any:
    url = f"{config.BINANCE_REST_BASE}{path}"
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
    url = f"{config.BINANCE_REST_BASE}{path}"
    p = _sign(params.copy()) if signed else params.copy()
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(url, params=p, headers=_headers())
    if resp.status_code != 200:
        logger.error("POST %s -> %d: %s", path, resp.status_code, resp.text[:300])
        resp.raise_for_status()
    return resp.json()


async def _delete(path: str, params: dict, signed: bool = True) -> Any:
    url = f"{config.BINANCE_REST_BASE}{path}"
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

async def get_all_premium_indices() -> list[dict]:
    """Return the premium index (including lastFundingRate) for all symbols."""
    return await _get("/fapi/v1/premiumIndex")


async def get_open_interest_hist(symbol: str, period: str = "5m", limit: int = 30) -> list[dict]:
    """Fetch Open Interest History for a specific symbol."""
    return await _get(
        "/futures/data/openInterestHist",
        params={"symbol": symbol, "period": period, "limit": limit}
    )


async def get_positions() -> list[dict]:
    """Get all open positions (signed)."""
    return await _get("/fapi/v2/positionRisk", params={}, signed=True)

async def get_balance() -> float:
    """Fetch exact account wallet balance."""
    try:
        res = await _get("/fapi/v2/account", params={}, signed=True)
        return float(res.get("availableBalance", 0.0))
    except Exception as e:
        logger.error(f"Failed to fetch wallet balance: {e}")
        return 0.0

# ── Account / Order methods (signed) ──────────────────────────────────────────

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
    position_side: str = None,
) -> dict:
    params: dict[str, Any] = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": qty,
    }
    if reduce_only:
        params["reduceOnly"] = "true"
    if position_side:
        params["positionSide"] = position_side
    return await _ws_request("order.place", params=params)


async def place_stop_market_order(
    symbol: str,
    side: str,
    stop_price: float,
    close_position: bool = True,
    position_side: str = None,
) -> dict:
    params: dict[str, Any] = {
        "symbol": symbol,
        "side": side,
        "type": "STOP_MARKET",
        "stopPrice": round(stop_price, 6),
        "closePosition": "true" if close_position else "false",
    }
    if position_side:
        params["positionSide"] = position_side
    return await _post("/fapi/v1/order", params=params)


async def place_take_profit_market_order(
    symbol: str,
    side: str,
    stop_price: float,
    close_position: bool = True,
    position_side: str = None,
) -> dict:
    params: dict[str, Any] = {
        "symbol": symbol,
        "side": side,
        "type": "TAKE_PROFIT_MARKET",
        "stopPrice": round(stop_price, 6),
        "closePosition": "true" if close_position else "false",
    }
    if position_side:
        params["positionSide"] = position_side
    return await _post("/fapi/v1/order", params=params)


async def cancel_order(symbol: str, order_id: int) -> dict:
    return await _ws_request("order.cancel", params={"symbol": symbol, "orderId": order_id})

async def get_order(symbol: str, order_id: str) -> dict:
    return await _get("/fapi/v1/order", params={"symbol": symbol, "orderId": order_id}, signed=True)

async def get_order_commission(symbol: str, order_id: str) -> float:
    """
    Fetch exact trading commissions incurred by an order ID.
    Converts any non-USDT commission fee (e.g. BNB) into USDT using the prevailing mark price.
    Returns the total USDT-normalized commission.
    """
    try:
        trades = await _get("/fapi/v1/userTrades", params={"symbol": symbol, "orderId": order_id}, signed=True)
        total_usdt_commission = 0.0
        
        # Cache for asset mark prices to avoid redundant queries during normalization
        mark_prices = {}
        
        for t in trades:
            comm = float(t.get("commission", 0))
            asset = str(t.get("commissionAsset", ""))
            
            if asset.upper() == "USDT" or comm == 0:
                total_usdt_commission += comm
                continue
                
            # If fee was charged in BNB or another token, normalize to USDT
            pair = f"{asset.upper()}USDT"
            if pair not in mark_prices:
                try:
                    price_res = await _get("/fapi/v1/premiumIndex", params={"symbol": pair}, signed=False)
                    mark_prices[pair] = float(price_res["markPrice"])
                except Exception:
                    mark_prices[pair] = 1.0 # fallback if pair isn't valid, very rare
                    
            converted_comm = comm * mark_prices[pair]
            total_usdt_commission += converted_comm

        return total_usdt_commission
    except Exception as e:
        logger.error(f"Failed to fetch commission for order {order_id} ({symbol}): {e}")
        return 0.0


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
