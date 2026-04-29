from __future__ import annotations

import sqlite3
from pathlib import Path

from src.config import DATA_DIR, ensure_directories


DB_PATH = DATA_DIR / "market_data.sqlite"


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS tickers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS price_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_id INTEGER NOT NULL,
    price_date TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    adjusted_close REAL NOT NULL,
    volume INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ticker_id) REFERENCES tickers(id),
    UNIQUE (ticker_id, price_date)
);

CREATE INDEX IF NOT EXISTS idx_price_history_ticker_date
ON price_history (ticker_id, price_date);
"""


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    ensure_directories()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def initialize_database() -> None:
    with get_connection() as conn:
        conn.executescript(SCHEMA_SQL)


def upsert_ticker(conn: sqlite3.Connection, symbol: str) -> int:
    conn.execute("INSERT OR IGNORE INTO tickers (symbol) VALUES (?)", (symbol,))
    row = conn.execute("SELECT id FROM tickers WHERE symbol = ?", (symbol,)).fetchone()
    if row is None:
        raise RuntimeError(f"Could not create ticker: {symbol}")
    return int(row["id"])
