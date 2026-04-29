from __future__ import annotations

import sqlite3

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.config import DEFAULT_TICKERS, PROJECT_ROOT
from src.database import DB_PATH, get_connection, initialize_database
from src.fetch_data import MarketDataRefreshError, refresh_market_data
from src.metrics import get_latest_metrics, get_price_history
from src.sample_data import seed_sample_data


st.set_page_config(
    page_title="Stock Finance Analytics",
    page_icon=":chart_with_upwards_trend:",
    layout="wide",
)


def ensure_data() -> None:
    initialize_database()


@st.cache_data(ttl=900)
def load_metrics() -> pd.DataFrame:
    with get_connection() as conn:
        return get_latest_metrics(conn)


@st.cache_data(ttl=900)
def load_history(ticker: str) -> pd.DataFrame:
    with get_connection() as conn:
        return get_price_history(conn, ticker)


def render_candlestick(history: pd.DataFrame, ticker: str) -> None:
    fig = go.Figure(
        data=[
            go.Candlestick(
                x=history["price_date"],
                open=history["open"],
                high=history["high"],
                low=history["low"],
                close=history["close"],
                name=ticker,
            ),
            go.Scatter(
                x=history["price_date"],
                y=history["ma_20"],
                mode="lines",
                line=dict(color="#2f80ed", width=1.5),
                name="20-day MA",
            ),
            go.Scatter(
                x=history["price_date"],
                y=history["ma_50"],
                mode="lines",
                line=dict(color="#f2994a", width=1.5),
                name="50-day MA",
            ),
        ]
    )
    fig.update_layout(
        height=520,
        margin=dict(l=8, r=8, t=16, b=8),
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    st.plotly_chart(fig, use_container_width=True)


def render_sidebar() -> tuple[list[str], str]:
    st.sidebar.header("Data Refresh")
    ticker_text = st.sidebar.text_input("Tickers", " ".join(DEFAULT_TICKERS))
    period = st.sidebar.selectbox("Period", ["3mo", "6mo", "1y", "2y", "5y"], index=2)
    tickers = [symbol.strip().upper() for symbol in ticker_text.replace(",", " ").split() if symbol.strip()]

    if st.sidebar.button("Refresh now", type="primary"):
        if tickers:
            with st.spinner("Fetching market data..."):
                try:
                    results = refresh_market_data(tickers, period)
                except MarketDataRefreshError as exc:
                    st.sidebar.error(str(exc))
                else:
                    st.cache_data.clear()
                    saved = [ticker for ticker, status in results.items() if status.startswith("saved")]
                    failed = [ticker for ticker, status in results.items() if not status.startswith("saved")]
                    if saved:
                        st.sidebar.success(f"Updated: {', '.join(saved)}")
                    if failed:
                        st.sidebar.warning(f"Skipped: {', '.join(failed)}")
        else:
            st.sidebar.error("Enter at least one ticker")

    if st.sidebar.button("Load sample data"):
        rows = seed_sample_data()
        st.cache_data.clear()
        st.sidebar.success(f"Loaded {rows} sample rows")

    st.sidebar.caption(f"Database: {DB_PATH.relative_to(PROJECT_ROOT)}")
    return tickers, period


def main() -> None:
    ensure_data()
    tickers, _ = render_sidebar()

    st.title("Stock Finance Analytics Dashboard")
    st.caption("Live yfinance ingestion, SQLite storage, SQL metrics, and scheduled reporting.")

    try:
        metrics = load_metrics()
    except sqlite3.Error as exc:
        st.error(f"Could not load database metrics: {exc}")
        return

    if metrics.empty:
        st.warning("No price data available yet. Refresh live data or load sample data from the sidebar.")
        return

    visible_metrics = metrics[metrics["ticker"].isin(tickers)] if tickers else metrics
    if visible_metrics.empty:
        visible_metrics = metrics

    cols = st.columns(4)
    latest_close = visible_metrics["close_price"].mean()
    avg_return = visible_metrics["daily_return_pct"].mean()
    avg_volatility = visible_metrics["volatility_20d_pct"].mean()
    bullish_count = int((visible_metrics["trend"] == "Bullish").sum())

    cols[0].metric("Average Close", f"${latest_close:,.2f}")
    cols[1].metric("Avg Daily Return", f"{avg_return:,.2f}%")
    cols[2].metric("Avg 20D Volatility", f"{avg_volatility:,.2f}%")
    cols[3].metric("Bullish Tickers", bullish_count)

    st.subheader("Latest SQL Metrics")
    st.dataframe(
        visible_metrics[
            [
                "ticker",
                "price_date",
                "close_price",
                "daily_return_pct",
                "ma_20",
                "ma_50",
                "volatility_20d_pct",
                "trend",
                "volume",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

    selected = st.selectbox("Chart ticker", visible_metrics["ticker"].sort_values().unique())
    history = load_history(selected)
    render_candlestick(history, selected)

    st.subheader("Project Notes")
    st.markdown(
        """
- Data ingestion is separated from dashboard rendering, so it can be scheduled independently.
- SQLite stores raw historical prices; SQL window functions calculate analytics.
- The dashboard reads from the database instead of calling the API on every page load.
- Cron can refresh the database before market open or after market close.
"""
    )


if __name__ == "__main__":
    main()
