from __future__ import annotations

import sqlite3

import pandas as pd


LATEST_METRICS_SQL = """
WITH price_features AS (
    SELECT
        t.symbol AS ticker,
        ph.price_date,
        ph.close AS close_price,
        ph.volume,
        LAG(ph.close) OVER (
            PARTITION BY t.symbol ORDER BY ph.price_date
        ) AS previous_close,
        AVG(ph.close) OVER (
            PARTITION BY t.symbol ORDER BY ph.price_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS ma_20,
        AVG(ph.close) OVER (
            PARTITION BY t.symbol ORDER BY ph.price_date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS ma_50,
        AVG(ph.volume) OVER (
            PARTITION BY t.symbol ORDER BY ph.price_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS avg_volume_20
    FROM price_history ph
    JOIN tickers t ON t.id = ph.ticker_id
),
returns AS (
    SELECT
        *,
        CASE
            WHEN previous_close IS NULL OR previous_close = 0 THEN NULL
            ELSE ((close_price - previous_close) / previous_close) * 100
        END AS daily_return_pct
    FROM price_features
),
volatility AS (
    SELECT
        *,
        AVG(daily_return_pct * daily_return_pct) OVER (
            PARTITION BY ticker ORDER BY price_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS avg_squared_return_20
    FROM returns
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY price_date DESC) AS recency_rank
    FROM volatility
)
SELECT
    ticker,
    price_date,
    ROUND(close_price, 2) AS close_price,
    ROUND(daily_return_pct, 2) AS daily_return_pct,
    ROUND(ma_20, 2) AS ma_20,
    ROUND(ma_50, 2) AS ma_50,
    ROUND(SQRT(avg_squared_return_20), 2) AS volatility_20d_pct,
    CASE
        WHEN ma_20 > ma_50 AND close_price > ma_20 THEN 'Bullish'
        WHEN ma_20 < ma_50 AND close_price < ma_20 THEN 'Bearish'
        ELSE 'Neutral'
    END AS trend,
    volume,
    ROUND(avg_volume_20, 0) AS avg_volume_20
FROM ranked
WHERE recency_rank = 1
ORDER BY ticker;
"""


PRICE_HISTORY_SQL = """
WITH enriched AS (
    SELECT
        t.symbol AS ticker,
        ph.price_date,
        ph.open,
        ph.high,
        ph.low,
        ph.close,
        ph.volume,
        AVG(ph.close) OVER (
            PARTITION BY t.symbol ORDER BY ph.price_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS ma_20,
        AVG(ph.close) OVER (
            PARTITION BY t.symbol ORDER BY ph.price_date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS ma_50
    FROM price_history ph
    JOIN tickers t ON t.id = ph.ticker_id
)
SELECT *
FROM enriched
WHERE ticker = ?
ORDER BY price_date;
"""


def get_latest_metrics(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query(LATEST_METRICS_SQL, conn)


def get_price_history(conn: sqlite3.Connection, ticker: str) -> pd.DataFrame:
    return pd.read_sql_query(PRICE_HISTORY_SQL, conn, params=(ticker,))
