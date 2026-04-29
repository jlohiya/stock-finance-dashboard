from __future__ import annotations

import argparse
from collections.abc import Iterable

import pandas as pd
import yfinance as yf

from src.config import DEFAULT_PERIOD, DEFAULT_TICKERS
from src.database import get_connection, initialize_database, upsert_ticker


class MarketDataRefreshError(RuntimeError):
    """Raised when no requested ticker data could be refreshed."""


def normalize_history(history: pd.DataFrame) -> pd.DataFrame:
    if history.empty:
        return history

    normalized = history.reset_index()
    normalized.columns = [str(column).lower().replace(" ", "_") for column in normalized.columns]

    if "date" not in normalized.columns:
        raise ValueError("Expected yfinance history to include a date column")

    normalized["price_date"] = pd.to_datetime(normalized["date"]).dt.strftime("%Y-%m-%d")
    if "adj_close" not in normalized.columns:
        normalized["adj_close"] = normalized["close"]

    return normalized[
        ["price_date", "open", "high", "low", "close", "adj_close", "volume"]
    ].dropna()


def refresh_market_data(tickers: Iterable[str], period: str = DEFAULT_PERIOD) -> dict[str, str]:
    initialize_database()
    clean_tickers = sorted({ticker.strip().upper() for ticker in tickers if ticker.strip()})
    results: dict[str, str] = {}

    with get_connection() as conn:
        for symbol in clean_tickers:
            try:
                ticker_id = upsert_ticker(conn, symbol)
                history = yf.Ticker(symbol).history(period=period, auto_adjust=False)
                prices = normalize_history(history)
            except Exception as exc:
                results[symbol] = f"failed: {exc}"
                continue

            if prices.empty:
                results[symbol] = "no rows returned"
                continue

            for row in prices.itertuples(index=False):
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
                        row.price_date,
                        float(row.open),
                        float(row.high),
                        float(row.low),
                        float(row.close),
                        float(row.adj_close),
                        int(row.volume),
                    ),
                )
            results[symbol] = f"saved {len(prices)} rows"
        conn.commit()

    if clean_tickers and not any(status.startswith("saved") for status in results.values()):
        details = "; ".join(f"{symbol}: {status}" for symbol, status in results.items())
        raise MarketDataRefreshError(f"No market data was saved. {details}")

    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh market data from yfinance.")
    parser.add_argument("--tickers", nargs="+", default=DEFAULT_TICKERS)
    parser.add_argument("--period", default=DEFAULT_PERIOD)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    refresh_results = refresh_market_data(args.tickers, args.period)
    for ticker, status in refresh_results.items():
        print(f"{ticker}: {status}")
