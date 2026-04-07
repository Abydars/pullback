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
WATCHLIST_REFRESH_MINUTES: int = _int("WATCHLIST_REFRESH_MINUTES", 15)

# ── Signal Settings ───────────────────────────────────────────────────────────
# SIGNAL_MODE controls which strategy the scanner runs on each 15m candle close:
#   pullback  — trend-following reversion to EMA50/swing zone (default)
#   breakout  — close outside 20-candle consolidation range with volume
#   both      — run both; trade whichever fires first (cooldown applies per symbol)
SIGNAL_MODE: str = _get("SIGNAL_MODE", "pullback")
SIGNAL_SCORE_THRESHOLD: int = _int("SIGNAL_SCORE_THRESHOLD", 70)

# ── Risk / Order Settings ─────────────────────────────────────────────────────
# CAPITAL: total account equity in USDT.
# Per-trade margin = CAPITAL / MAX_OPEN_TRADES.
# Leverage is determined by ATR volatility (not config-based).
CAPITAL: float = _float("CAPITAL", 0.0)
# RISK_PCT: percentage of CAPITAL to risk per trade (default 2 %).
# Dollar risk = CAPITAL × RISK_PCT / 100.
RISK_PCT: float = _float("RISK_PCT", 2.0)
MAX_OPEN_TRADES: int = _int("MAX_OPEN_TRADES", 5)
# MAX_LEVERAGE: absolute ceiling on leverage regardless of ATR tier.
MAX_LEVERAGE: int = _int("MAX_LEVERAGE", 20)
# USE_TRAILING: True = trail arm activates a trailing stop.
# False = arm price is a fixed TP; trade closes immediately when hit.
USE_TRAILING: bool = _bool("USE_TRAILING", True)
# USE_STOP_LOSS: True = place a hard SL order when entering a trade.
# False = no SL order is placed (use only if you manage risk another way).
USE_STOP_LOSS: bool = _bool("USE_STOP_LOSS", True)
# USE_TAKE_PROFIT: True = place a TP/trailing-stop order when entering a trade.
# False = no TP or trail order is placed; exit manually or via portfolio stop.
USE_TAKE_PROFIT: bool = _bool("USE_TAKE_PROFIT", True)

# ── Portfolio-level stops ─────────────────────────────────────────────────────
# If total unrealized PnL across all open positions reaches either threshold,
# close every position immediately.
# 0.0 = disabled (default).
PORTFOLIO_STOP_LOSS_USDT: float  = _float("PORTFOLIO_STOP_LOSS_USDT",  0.0)   # e.g. -50
PORTFOLIO_TAKE_PROFIT_USDT: float = _float("PORTFOLIO_TAKE_PROFIT_USDT", 0.0)  # e.g. 100

# ── Server ────────────────────────────────────────────────────────────────────
PORT: int = _int("PORT", 8080)

# ── Database ──────────────────────────────────────────────────────────────────
DB_PATH: str = _get("DB_PATH", str(Path(__file__).parent / "trades.db"))

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL: str = _get("LOG_LEVEL", "INFO")


# ── Runtime update helpers ────────────────────────────────────────────────────

# Keys that can be changed at runtime via /api/config
EDITABLE_KEYS: dict[str, type] = {
    "MIN_VOLUME_24H":            float,
    "MIN_PRICE_CHANGE_PCT":      float,
    "WATCHLIST_REFRESH_MINUTES": int,
    "SIGNAL_MODE":               str,
    "SIGNAL_SCORE_THRESHOLD":    int,
    "CAPITAL":                   float,
    "RISK_PCT":                  float,
    "MAX_OPEN_TRADES":           int,
    "MAX_LEVERAGE":              int,
    "USE_TRAILING":              bool,
    "USE_STOP_LOSS":             bool,
    "USE_TAKE_PROFIT":           bool,
    "PORTFOLIO_STOP_LOSS_USDT":  float,
    "PORTFOLIO_TAKE_PROFIT_USDT":float,
    "LOG_LEVEL":                 str,
    "MODE":                      str,
}

# Keys that require a bot restart to take full effect
RESTART_REQUIRED_KEYS = {"MODE", "PORT"}


def get_all() -> dict:
    """Return all editable config values as a plain dict."""
    return {k: globals()[k] for k in EDITABLE_KEYS}


def update(key: str, raw_value: str) -> None:
    """
    Validate, cast, and apply a config change in-memory + persist to .env.
    Raises ValueError on bad key or type.
    """
    if key not in EDITABLE_KEYS:
        raise ValueError(f"Unknown or non-editable config key: {key!r}")

    cast = EDITABLE_KEYS[key]
    try:
        if cast is bool:
            value = raw_value.strip().lower() in ("1", "true", "yes")
        else:
            value = cast(raw_value)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Invalid value for {key}: {raw_value!r} ({exc})")

    # Extra validation
    if key == "MODE" and value not in ("live", "paper"):
        raise ValueError("MODE must be 'live' or 'paper'")
    if key == "SIGNAL_MODE" and value not in ("pullback", "breakout", "both"):
        raise ValueError("SIGNAL_MODE must be 'pullback', 'breakout', or 'both'")
    if key == "SIGNAL_SCORE_THRESHOLD" and not (0 <= value <= 100):
        raise ValueError("SIGNAL_SCORE_THRESHOLD must be 0–100")
    if key == "MAX_LEVERAGE" and not (1 <= value <= 125):
        raise ValueError("MAX_LEVERAGE must be 1–125")
    if key == "RISK_PCT" and not (0.1 <= value <= 100.0):
        raise ValueError("RISK_PCT must be 0.1–100")

    # Apply in-memory
    globals()[key] = value

    # Persist to .env
    _write_env(key, str(value))


def _write_env(key: str, value: str) -> None:
    """Upsert KEY=value in the .env file."""
    lines: list[str] = []
    found = False
    if _env_path.exists():
        for line in _env_path.read_text().splitlines(keepends=True):
            if line.strip().startswith(f"{key}="):
                lines.append(f"{key}={value}\n")
                found = True
            else:
                lines.append(line)
    if not found:
        # Append a trailing newline if needed before adding
        if lines and not lines[-1].endswith("\n"):
            lines.append("\n")
        lines.append(f"{key}={value}\n")
    _env_path.write_text("".join(lines))

