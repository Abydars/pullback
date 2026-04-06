"""
db.py — SQLite schema creation and async query helpers.
"""
import aiosqlite
import logging
from typing import Optional
from config import DB_PATH

logger = logging.getLogger(__name__)

# ── Schema ─────────────────────────────────────────────────────────────────────

CREATE_TRADES_TABLE = """
CREATE TABLE IF NOT EXISTS trades (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol      TEXT    NOT NULL,
    direction   TEXT    NOT NULL,          -- LONG | SHORT
    entry_price REAL    NOT NULL,
    sl_price    REAL    NOT NULL,
    tp1_price   REAL    NOT NULL,
    tp2_price   REAL    NOT NULL,
    qty         REAL    NOT NULL,
    status      TEXT    NOT NULL DEFAULT 'OPEN',  -- OPEN | CLOSED | CANCELLED
    mode        TEXT    NOT NULL,          -- live | paper
    leverage    INTEGER,                   -- leverage at time of entry
    entry_time  INTEGER NOT NULL,          -- unix ms
    close_time  INTEGER,
    close_price REAL,
    close_reason TEXT,                     -- SL | TRAIL | MANUAL | PORTFOLIO_SL | PORTFOLIO_TP | …
    pnl_usdt    REAL,
    pnl_pct     REAL,
    signal_score INTEGER,
    binance_order_id TEXT                  -- NULL in paper mode
);
"""

CREATE_SCANNER_LOG_TABLE = """
CREATE TABLE IF NOT EXISTS scanner_log (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol    TEXT    NOT NULL,
    score     INTEGER NOT NULL,
    direction TEXT    NOT NULL,
    timestamp INTEGER NOT NULL,
    acted_on  INTEGER NOT NULL DEFAULT 0   -- 0=false, 1=true
);
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);",
    "CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);",
    "CREATE INDEX IF NOT EXISTS idx_scanner_log_ts ON scanner_log(timestamp);",
]


async def init_db() -> None:
    """Create tables, indexes, and run lightweight column migrations."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_TRADES_TABLE)
        await db.execute(CREATE_SCANNER_LOG_TABLE)
        for idx in CREATE_INDEXES:
            await db.execute(idx)
        # Migrations: add columns that didn't exist in earlier schema versions
        for col_sql in [
            "ALTER TABLE trades ADD COLUMN close_reason TEXT",
            "ALTER TABLE trades ADD COLUMN leverage INTEGER",
        ]:
            try:
                await db.execute(col_sql)
                logger.info("Migration applied: %s", col_sql)
            except Exception:
                pass  # column already exists
        await db.commit()
    logger.info("Database initialised at %s", DB_PATH)


# ── Trade helpers ──────────────────────────────────────────────────────────────

async def insert_trade(
    symbol: str,
    direction: str,
    entry_price: float,
    sl_price: float,
    tp1_price: float,
    tp2_price: float,
    qty: float,
    mode: str,
    entry_time: int,
    signal_score: int,
    leverage: Optional[int] = None,
    binance_order_id: Optional[str] = None,
) -> int:
    """Insert a new trade and return its id."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            INSERT INTO trades
                (symbol, direction, entry_price, sl_price, tp1_price, tp2_price,
                 qty, mode, entry_time, signal_score, leverage, binance_order_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (symbol, direction, entry_price, sl_price, tp1_price, tp2_price,
             qty, mode, entry_time, signal_score, leverage, binance_order_id),
        )
        await db.commit()
        return cursor.lastrowid


async def update_trade_close(
    trade_id: int,
    close_price: float,
    close_time: int,
    pnl_usdt: float,
    pnl_pct: float,
    status: str = "CLOSED",
    close_reason: Optional[str] = None,
) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE trades
            SET close_price=?, close_time=?, pnl_usdt=?, pnl_pct=?, status=?, close_reason=?
            WHERE id=?
            """,
            (close_price, close_time, pnl_usdt, pnl_pct, status, close_reason, trade_id),
        )
        await db.commit()


async def update_trade_status(trade_id: int, status: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE trades SET status=? WHERE id=?", (status, trade_id)
        )
        await db.commit()


async def get_open_trades() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM trades WHERE status='OPEN' ORDER BY entry_time DESC"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_recent_trades(limit: int = 50) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM trades ORDER BY entry_time DESC LIMIT ?", (limit,)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_closed_trades(limit: int = 500) -> list[dict]:
    """Return closed trades newest-first, for trade history display."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM trades WHERE status='CLOSED' ORDER BY close_time DESC LIMIT ?",
            (limit,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def delete_trade(trade_id: int) -> bool:
    """Hard-delete a single closed trade. Returns True if a row was deleted."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "DELETE FROM trades WHERE id=? AND status='CLOSED'", (trade_id,)
        )
        await db.commit()
        return cursor.rowcount > 0


async def delete_all_closed_trades() -> int:
    """Hard-delete all closed trades. Returns count deleted."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("DELETE FROM trades WHERE status='CLOSED'")
        await db.commit()
        return cursor.rowcount


async def get_all_stats() -> dict:
    """Return all-time closed-trade stats."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT
                COUNT(*)                                             AS total,
                SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END)      AS wins,
                SUM(COALESCE(pnl_usdt, 0))                          AS total_pnl
            FROM trades
            WHERE status = 'CLOSED'
            """
        )
        row = await cursor.fetchone()
        total    = row[0] or 0
        wins     = row[1] or 0
        total_pnl = row[2] or 0.0
        return {
            "total_all":     total,
            "win_rate_all":  round(wins / total * 100, 1) if total > 0 else 0.0,
            "total_pnl_all": round(total_pnl, 2),
        }


async def count_open_trades() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT COUNT(*) FROM trades WHERE status='OPEN'"
        )
        row = await cursor.fetchone()
        return row[0] if row else 0


async def get_today_stats() -> dict:
    """Return trade count, win count, total PnL for today (UTC)."""
    import time
    day_start_ms = int(time.time() // 86400 * 86400 * 1000)
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END) as wins,
                SUM(COALESCE(pnl_usdt, 0)) as total_pnl
            FROM trades
            WHERE entry_time >= ? AND status='CLOSED'
            """,
            (day_start_ms,),
        )
        row = await cursor.fetchone()
        total = row[0] or 0
        wins = row[1] or 0
        total_pnl = row[2] or 0.0
        return {
            "total_today": total,
            "win_rate": round(wins / total * 100, 1) if total > 0 else 0.0,
            "total_pnl_today": round(total_pnl, 2),
        }


# ── Scanner log helpers ────────────────────────────────────────────────────────

async def insert_scanner_log(
    symbol: str, score: int, direction: str, timestamp: int, acted_on: bool = False
) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO scanner_log (symbol, score, direction, timestamp, acted_on) VALUES (?,?,?,?,?)",
            (symbol, score, direction, timestamp, int(acted_on)),
        )
        await db.commit()


async def get_recent_scanner_log(limit: int = 100) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM scanner_log ORDER BY timestamp DESC LIMIT ?", (limit,)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
