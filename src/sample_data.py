from __future__ import annotations

from datetime import date, timedelta
from math import sin

from src.database import get_connection, initialize_database, upsert_ticker


SAMPLE_TICKERS = {
    "AAPL": 185.0,
    "MSFT": 405.0,
    "NVDA": 920.0,
    "JPM": 198.0,
    "SPY": 510.0,
}


def seed_sample_data(days: int = 90) -> int:
    initialize_database()
    start_date = date.today() - timedelta(days=days)
    rows_saved = 0

    with get_connection() as conn:
        for symbol, base_price in SAMPLE_TICKERS.items():
            ticker_id = upsert_ticker(conn, symbol)
            for offset in range(days):
                current_date = start_date + timedelta(days=offset)
                if current_date.weekday() >= 5:
                    continue

                drift = offset * 0.18
                wave = sin(offset / 5) * (base_price * 0.015)
                close = base_price + drift + wave
                open_price = close * (1 + sin(offset / 3) * 0.004)
                high = max(open_price, close) * 1.012
                low = min(open_price, close) * 0.988
                volume = int(1_000_000 + (offset % 12) * 85_000 + base_price * 1_000)

                conn.execute(
                    """
                    INSERT INTO price_history (
                        ticker_id, price_date, open, high, low, close, adjusted_close, volume
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(ticker_id, price_date) DO UPDATE SET
                        open = excluded.open,
                        high = excluded.high,
                        low = excluded.low,
                        close = excluded.close,
                        adjusted_close = excluded.adjusted_close,
                        volume = excluded.volume
                    """,
                    (
                        ticker_id,
                        current_date.isoformat(),
                        round(open_price, 2),
                        round(high, 2),
                        round(low, 2),
                        round(close, 2),
                        round(close, 2),
                        volume,
                    ),
                )
                rows_saved += 1
        conn.commit()

    return rows_saved
