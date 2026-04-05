"""
config.py — Load all bot settings from .env with typed defaults.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from the directory containing this file
_env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=_env_path, override=False)


def _get(key: str, default: str) -> str:
    return os.environ.get(key, default).strip()


def _bool(key: str, default: bool) -> bool:
    val = os.environ.get(key, str(default)).strip().lower()
    return val in ("1", "true", "yes")


def _int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, str(default)).strip())
    except ValueError:
        return default


def _float(key: str, default: float) -> float:
    try:
        return float(os.environ.get(key, str(default)).strip())
    except ValueError:
        return default


# ── Trading Mode ──────────────────────────────────────────────────────────────
MODE: str = _get("MODE", "paper")               # "live" | "paper"
assert MODE in ("live", "paper"), f"MODE must be 'live' or 'paper', got '{MODE}'"

# ── Binance Credentials ───────────────────────────────────────────────────────
BINANCE_API_KEY: str = _get("BINANCE_API_KEY", "")
BINANCE_API_SECRET: str = _get("BINANCE_API_SECRET", "")
BINANCE_TESTNET: bool = _bool("BINANCE_TESTNET", False)

# ── Binance Endpoints ─────────────────────────────────────────────────────────
if BINANCE_TESTNET:
    BINANCE_REST_BASE = "https://testnet.binancefuture.com"
    BINANCE_WS_BASE = "wss://stream.binancefuture.com"
else:
    BINANCE_REST_BASE = "https://fapi.binance.com"
    BINANCE_WS_BASE = "wss://fstream.binance.com"

# ── Scanner Settings ──────────────────────────────────────────────────────────
MIN_VOLUME_24H: float = _float("MIN_VOLUME_24H", 20_000_000)   # USDT
MIN_PRICE_CHANGE_PCT: float = _float("MIN_PRICE_CHANGE_PCT", 0.5)
SCANNER_INTERVAL_SECONDS: int = _int("SCANNER_INTERVAL_SECONDS", 30)
WATCHLIST_REFRESH_MINUTES: int = _int("WATCHLIST_REFRESH_MINUTES", 15)

# ── Signal Settings ───────────────────────────────────────────────────────────
SIGNAL_SCORE_THRESHOLD: int = _int("SIGNAL_SCORE_THRESHOLD", 70)

# ── Risk / Order Settings ─────────────────────────────────────────────────────
RISK_PER_TRADE_USDT: float = _float("RISK_PER_TRADE_USDT", 10.0)
MAX_OPEN_TRADES: int = _int("MAX_OPEN_TRADES", 5)
LEVERAGE: int = _int("LEVERAGE", 10)

# ── Server ────────────────────────────────────────────────────────────────────
PORT: int = _int("PORT", 8080)

# ── Database ──────────────────────────────────────────────────────────────────
DB_PATH: str = _get("DB_PATH", str(Path(__file__).parent / "trades.db"))

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL: str = _get("LOG_LEVEL", "INFO")
