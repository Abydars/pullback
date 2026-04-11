"""
config.py — Load all bot settings from .env with typed defaults.

Boot flow
---------
1. Module import: values are read from .env (or OS env) into module globals.
   This provides defaults for every key — including keys never changed via UI.
2. Startup (main.py): after DB is ready, config.load_from_db() is called.
   It overlays any keys stored in the `config` table, so UI changes made
   before the last restart are restored exactly.

Runtime changes (via /api/config → config.update())
----------------------------------------------------
  • Applied in-memory immediately (globals()[key] = value).
  • Persisted to the DB config table via db.upsert_config().
  • .env is NOT written at runtime — it is read-only after startup.

This means .env is the source of defaults / secrets; the DB is the
single source of truth for any key that has ever been changed via UI.
"""
import logging
import os
from pathlib import Path
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

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

# ── Institutional Confirmations (Filters) ─────────────────────────────────────
FILTER_MTF_ENABLED: bool = _bool("FILTER_MTF_ENABLED", True)
FILTER_OI_ENABLED: bool = _bool("FILTER_OI_ENABLED", True)
FILTER_VWAP_ENABLED: bool = _bool("FILTER_VWAP_ENABLED", True)
FILTER_RSI_ENABLED: bool = _bool("FILTER_RSI_ENABLED", True)

# ── ML Smart Filter Settings ──────────────────────────────────────────────────
ML_FILTER_ENABLED: bool = _bool("ML_FILTER_ENABLED", False)
ML_CONFIDENCE_THRESHOLD: float = _float("ML_CONFIDENCE_THRESHOLD", 0.70)

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
# Feature: Trading Session Time Guard
SESSION_GUARD_ENABLED: bool = _bool("SESSION_GUARD_ENABLED", False)
TRADE_CUSTOM_SESSIONS: str = _get("TRADE_CUSTOM_SESSIONS", "")

# Feature: Funding Rate Time Guard
FUNDING_GUARD_ENABLED: bool = _bool("FUNDING_GUARD_ENABLED", False)
FUNDING_GUARD_MINUTES: int = int(_get("FUNDING_GUARD_MINUTES", "5"))
FUNDING_GUARD_ALLOW_SHORTS: bool = _bool("FUNDING_GUARD_ALLOW_SHORTS", False)

#   pullback         — trend-following reversion to EMA50/swing zone (default)
#   breakout         — close outside 20-candle consolidation range with volume
#   both             — run pullback & breakout concurrently
SIGNAL_MODE: str = _get("SIGNAL_MODE", "pullback")
SIGNAL_SCORE_THRESHOLD: int = _int("SIGNAL_SCORE_THRESHOLD", 70)
# SIGNAL_BATCH_WINDOW_S: seconds to wait after the first signal fires before
# ranking the batch by score.  0.0 = immediate entry on first signal (fastest).
# 3.0 = collect all signals from the same candle close, then open the highest
# scored ones first.  Has no meaningful impact on the 15m timeframe.
SIGNAL_BATCH_WINDOW_S: float = _float("SIGNAL_BATCH_WINDOW_S", 3.0)

# ── BTC Regime Filter ─────────────────────────────────────────────────────────
# When enabled, the scanner blocks alt-coin signals that trade against the
# current BTC 15m momentum: SHORT signals are suppressed during a BTC bull
# breakout; LONG signals are suppressed during a BTC bear breakdown.
# BTC_BREAKOUT_ROC: 3-candle (45 min) rate-of-change threshold.
#   15m single-candle noise ≈ 0.3–0.5%; a 3-candle sustained move of 0.8%
#   is a genuine regime shift, not random noise.
BTC_REGIME_FILTER: bool  = _bool("BTC_REGIME_FILTER", True)
BTC_BREAKOUT_ROC:  float = _float("BTC_BREAKOUT_ROC", 0.008)
# BTC_CORR_THRESHOLD: minimum Pearson correlation for the regime filter to
# apply to a symbol.  0.0 = never block (filter off for all symbols),
# 0.5 = only block symbols that move 50%+ with BTC, 1.0 = block all (old behavior).
BTC_CORR_THRESHOLD: float = _float("BTC_CORR_THRESHOLD", 0.5)

# ── Risk / Order Settings ─────────────────────────────────────────────────────
# CAPITAL: total account equity in USDT.
# Per-trade margin = CAPITAL / MAX_OPEN_TRADES.
# Leverage is determined by ATR volatility (not config-based).
CAPITAL: float = _float("CAPITAL", 0.0)
# RISK_PCT: percentage of CAPITAL to risk per trade (default 2 %).
# Dollar risk = CAPITAL × RISK_PCT / 100.
RISK_PCT: float = _float("RISK_PCT", 2.0)
MAX_OPEN_TRADES: int = _int("MAX_OPEN_TRADES", 5)
# MAX_SAME_DIRECTION: maximum number of trades allowed in the same direction
# (LONG or SHORT) within a single signal batch (one 15m candle close).
# Prevents N correlated bets when a broad market move fires N same-direction
# signals simultaneously.  Default 3; must be ≤ MAX_OPEN_TRADES.
MAX_SAME_DIRECTION: int = _int("MAX_SAME_DIRECTION", 3)
# INITIAL_BATCH_SIZE: maximum number of new trades to open in a single
# 15m candle scan. Signals beyond this limit are deferred — they are not
# logged or dropped, just not acted on this scan.  Builds positions
# gradually across multiple candles rather than all at once.  Default 2.
INITIAL_BATCH_SIZE: int = _int("INITIAL_BATCH_SIZE", 2)
# MAX_LEVERAGE: absolute ceiling on leverage regardless of ATR tier.
MAX_LEVERAGE: int = _int("MAX_LEVERAGE", 20)
# USE_TRAILING: True = trail arm activates a trailing stop.
# False = arm price is a fixed TP; trade closes immediately when hit.
USE_TRAILING: bool = _bool("USE_TRAILING", True)
# SMART_TRAILING_ENABLED: Override default trailing with dynamic momentum evaluation at TP.
SMART_TRAILING_ENABLED: bool = _bool("SMART_TRAILING_ENABLED", True)
# USE_STOP_LOSS: True = place a hard SL order when entering a trade.
# False = no SL order is placed (use only if you manage risk another way).
USE_STOP_LOSS: bool = _bool("USE_STOP_LOSS", True)
# USE_TAKE_PROFIT: True = place a TP/trailing-stop order when entering a trade.
# False = no TP or trail order is placed; exit manually or via portfolio stop.
USE_TAKE_PROFIT: bool = _bool("USE_TAKE_PROFIT", True)
# SYMBOL_COOLDOWN_MINUTES: minimum minutes between trades on the same symbol.
# After any close (SL, trail, portfolio stop, manual), that symbol is blocked
# from re-entry until this many minutes have elapsed.  0 = disabled.
SYMBOL_COOLDOWN_MINUTES: int = _int("SYMBOL_COOLDOWN_MINUTES", 60)

# ── Portfolio-level stops ─────────────────────────────────────────────────────
# If total unrealized PnL across all open positions reaches either threshold,
# close every position immediately.
# 0.0 = disabled (default).
PORTFOLIO_STOP_LOSS_USDT: float = _float("PORTFOLIO_STOP_LOSS_USDT", 0.0)   # e.g. -50
# PORTFOLIO_MIN_TP_USDT: minimum PnL to protect.
# In "trailing" mode this is the floor activation level; in "normal" mode the
# bot closes all positions immediately when unrealized PnL reaches this value.
# 0.0 = disabled (default).
PORTFOLIO_MIN_TP_USDT: float = _float("PORTFOLIO_MIN_TP_USDT", 0.0)         # e.g. 100
# PORTFOLIO_TP_MODE: controls how the portfolio take-profit fires.
# "trailing" = arm a trailing floor when PnL hits PORTFOLIO_MIN_TP_USDT.
# "normal"   = close all immediately when PnL hits PORTFOLIO_MIN_TP_USDT.
PORTFOLIO_TP_MODE: str = _get("PORTFOLIO_TP_MODE", "trailing")
# PORTFOLIO_TRAIL_FACTOR: (trailing mode only) floor formula:
# floor = PORTFOLIO_MIN_TP_USDT + (peak - PORTFOLIO_MIN_TP_USDT) * factor.
# 0.5 = trail 50 % of gains above target; 0.0 = lock floor at target;
# 1.0 = never trail below peak.
PORTFOLIO_TRAIL_FACTOR: float = _float("PORTFOLIO_TRAIL_FACTOR", 0.5)
# SMART_PORT_SL: closes all positions when a majority are losing AND the total
# loss is deep enough AND gradual building has already stopped — all three
# conditions must hold simultaneously.  Works alongside the fixed PORT SL;
# whichever fires first wins.
SMART_PORT_SL_ENABLED:    bool  = _bool("SMART_PORT_SL_ENABLED",   True)
SMART_PORT_SL_NEG_RATIO:  float = _float("SMART_PORT_SL_NEG_RATIO",  0.65)
SMART_PORT_SL_MULTIPLIER: float = _float("SMART_PORT_SL_MULTIPLIER", 0.8)
SMART_PORT_SL_MIN_AGE_MINUTES: int = _int("SMART_PORT_SL_MIN_AGE_MINUTES", 5)

# ── Server ────────────────────────────────────────────────────────────────────
PORT: int = _int("PORT", 8080)
WEB_PASSWORD: str = _get("WEB_PASSWORD", "")

# ── Database ──────────────────────────────────────────────────────────────────
DB_PATH: str = _get("DB_PATH", str(Path(__file__).parent / "trades.db"))

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL: str = _get("LOG_LEVEL", "INFO")


# ── Runtime update helpers ────────────────────────────────────────────────────

# Keys that can be changed at runtime via /api/config
EDITABLE_KEYS: dict[str, type] = {
    "SESSION_GUARD_ENABLED":     bool,
    "TRADE_CUSTOM_SESSIONS":     str,
    "FUNDING_GUARD_ENABLED":     bool,
    "FUNDING_GUARD_MINUTES":     int,
    "FUNDING_GUARD_ALLOW_SHORTS":bool,
    "MIN_VOLUME_24H":            float,
    "MIN_PRICE_CHANGE_PCT":      float,
    "WATCHLIST_REFRESH_MINUTES": int,
    "SIGNAL_MODE":               str,
    "SIGNAL_SCORE_THRESHOLD":    int,
    "SIGNAL_BATCH_WINDOW_S":     float,
    "BTC_REGIME_FILTER":         bool,
    "BTC_BREAKOUT_ROC":          float,
    "BTC_CORR_THRESHOLD":        float,
    "CAPITAL":                   float,
    "RISK_PCT":                  float,
    "MAX_OPEN_TRADES":           int,
    "MAX_SAME_DIRECTION":        int,
    "INITIAL_BATCH_SIZE":        int,
    "MAX_LEVERAGE":              int,
    "USE_TRAILING":              bool,
    "SMART_TRAILING_ENABLED":    bool,
    "USE_STOP_LOSS":             bool,
    "USE_TAKE_PROFIT":           bool,
    "SYMBOL_COOLDOWN_MINUTES":   int,
    "PORTFOLIO_STOP_LOSS_USDT":  float,
    "PORTFOLIO_MIN_TP_USDT":     float,
    "PORTFOLIO_TP_MODE":         str,
    "PORTFOLIO_TRAIL_FACTOR":    float,
    "SMART_PORT_SL_ENABLED":          bool,
    "SMART_PORT_SL_NEG_RATIO":        float,
    "SMART_PORT_SL_MULTIPLIER":       float,
    "SMART_PORT_SL_MIN_AGE_MINUTES":  int,
    "LOG_LEVEL":                 str,
    "MODE":                      str,
    "WEB_PASSWORD":              str,
    "ML_FILTER_ENABLED":         bool,
    "ML_CONFIDENCE_THRESHOLD":   float,
    "FILTER_MTF_ENABLED":        bool,
    "FILTER_OI_ENABLED":         bool,
    "FILTER_VWAP_ENABLED":       bool,
    "FILTER_RSI_ENABLED":        bool,
}

# Keys that require a bot restart to take full effect
RESTART_REQUIRED_KEYS = {"MODE", "PORT"}

# Keys that should not be sent or logged plainly if possible
SECRET_KEYS = {"WEB_PASSWORD"}


def get_all() -> dict:
    """Return all editable config values as a plain dict."""
    d = {k: globals()[k] for k in EDITABLE_KEYS}
    if "WEB_PASSWORD" in d:
        d["WEB_PASSWORD"] = "***" if d["WEB_PASSWORD"] else ""
    return d


async def load_from_db() -> None:
    """
    Overlay DB-persisted config values on top of .env defaults.
    Called once at startup, after db.init_db() and before scanner start.
    Importing db inside the function body avoids the circular import
    (db.py imports DB_PATH from this module at module level).
    """
    import db as _db
    rows = await _db.get_all_config()
    for key, raw_value in rows.items():
        if key not in EDITABLE_KEYS:
            continue
        cast = EDITABLE_KEYS[key]
        try:
            if cast is bool:
                value = raw_value.strip().lower() in ("1", "true", "yes")
            else:
                value = cast(raw_value)
            globals()[key] = value
            logger.debug("Config loaded from DB: %s=%s", key, value)
        except (ValueError, TypeError) as exc:
            logger.warning("Config load_from_db: failed to cast %s=%r: %s", key, raw_value, exc)


async def update(key: str, raw_value: str) -> None:
    """
    Validate, cast, and apply a config change in-memory + persist to DB.
    Raises ValueError on bad key or type.
    """
    if key not in EDITABLE_KEYS:
        raise ValueError(f"Unknown or non-editable config key: {key!r}")
        
    if key == "WEB_PASSWORD" and raw_value == "***":
        # Ignore masked password updates submitted from UI
        return

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
    if key == "SIGNAL_BATCH_WINDOW_S" and not (0.0 <= value <= 10.0):
        raise ValueError("SIGNAL_BATCH_WINDOW_S must be 0.0–10.0")
    if key == "MAX_LEVERAGE" and not (1 <= value <= 125):
        raise ValueError("MAX_LEVERAGE must be 1–125")
    if key == "RISK_PCT" and not (0.1 <= value <= 100.0):
        raise ValueError("RISK_PCT must be 0.1–100")
    if key == "MAX_SAME_DIRECTION" and not (1 <= value <= max(10, globals().get("MAX_OPEN_TRADES", 10))):
        raise ValueError("MAX_SAME_DIRECTION must be between 1 and MAX_OPEN_TRADES")
    if key == "INITIAL_BATCH_SIZE" and not (1 <= value <= max(10, globals().get("MAX_OPEN_TRADES", 10))):
        raise ValueError("INITIAL_BATCH_SIZE must be between 1 and MAX_OPEN_TRADES")
    if key == "PORTFOLIO_TP_MODE" and value not in ("trailing", "normal"):
        raise ValueError("PORTFOLIO_TP_MODE must be 'trailing' or 'normal'")
    if key == "PORTFOLIO_TRAIL_FACTOR" and not (0.0 <= value <= 1.0):
        raise ValueError("PORTFOLIO_TRAIL_FACTOR must be 0.0–1.0")
    if key == "SMART_PORT_SL_NEG_RATIO" and not (0.1 <= value <= 1.0):
        raise ValueError("SMART_PORT_SL_NEG_RATIO must be 0.1–1.0")
    if key == "SMART_PORT_SL_MULTIPLIER" and not (0.1 <= value <= 5.0):
        raise ValueError("SMART_PORT_SL_MULTIPLIER must be 0.1–5.0")
    if key == "SMART_PORT_SL_MIN_AGE_MINUTES" and not (0 <= value <= 60):
        raise ValueError("SMART_PORT_SL_MIN_AGE_MINUTES must be 0–60")
    if key == "BTC_BREAKOUT_ROC" and not (0.001 <= value <= 0.05):
        raise ValueError("BTC_BREAKOUT_ROC must be between 0.001 and 0.05")
    if key == "BTC_CORR_THRESHOLD" and not (0.0 <= value <= 1.0):
        raise ValueError("BTC_CORR_THRESHOLD must be 0.0–1.0")

    # Apply in-memory
    globals()[key] = value

    # Persist to DB (survives restart; .env is read-only at runtime)
    import db as _db
    await _db.upsert_config(key, str(value))

