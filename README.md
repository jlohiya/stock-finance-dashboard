# Stock Finance Analytics Dashboard

A Python + SQL project that pulls market data with `yfinance`, stores it in SQLite, computes SQL-driven metrics, and serves a local Streamlit dashboard.

## What It Does

- Pulls recent OHLCV stock data from Yahoo Finance via `yfinance`
- Stores normalized ticker and price history records in SQLite
- Uses SQL queries for moving averages, daily returns, volatility, trend direction, and volume changes
- Generates an automated Markdown report
- Runs as a Streamlit dashboard on localhost
- Includes a cron example for scheduled refreshes

## Project Structure

```text
stock-finance-dashboard/
├── app.py                    # Streamlit dashboard
├── requirements.txt          # Python dependencies
├── README.md                 # Setup and usage guide
├── sql/
│   └── trend_queries.sql     # SQL analytics examples
├── scripts/
│   ├── update_data.py        # Scheduled ingestion entry point
│   └── generate_report.py    # Markdown report generator
├── src/
│   ├── config.py             # App settings
│   ├── database.py           # SQLite schema and query helpers
│   ├── fetch_data.py         # yfinance ingestion
│   └── metrics.py            # SQL metrics used by dashboard/report
├── reports/
│   └── .gitkeep
└── cron/
    └── example.crontab       # Example scheduled refresh
```

## Local Setup

Run these commands from the folder that contains this project:

```bash
cd stock-finance-dashboard
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python scripts/update_data.py
streamlit run app.py
```

Streamlit will print a local URL, usually:

```text
http://localhost:8501
```

## Refresh Data Manually

```bash
source .venv/bin/activate
python scripts/update_data.py --tickers AAPL MSFT NVDA JPM SPY --period 1y
```

If Yahoo Finance rate-limits your machine, seed local sample data so the SQL dashboard and report still run:

```bash
python scripts/seed_sample_data.py
```

## Generate A Report

```bash
source .venv/bin/activate
python scripts/generate_report.py
```

The report will be written to:

```text
reports/latest_report.md
```

## Schedule With Cron

Edit `cron/example.crontab` and replace `/ABSOLUTE/PATH/TO/stock-finance-dashboard` with your actual project path.

Then install it:

```bash
crontab cron/example.crontab
```

Example schedule:

```cron
0 9 * * 1-5 cd /ABSOLUTE/PATH/TO/stock-finance-dashboard && . .venv/bin/activate && python scripts/update_data.py >> logs/cron.log 2>&1
```

## SQL Highlights

See `sql/trend_queries.sql` for examples covering:

- 20-day and 50-day moving averages
- Daily returns
- Rolling volatility approximation
- Latest trend classification
- Volume spikes

## GitHub Push Commands

```bash
git init
git add .
git commit -m "Build stock finance analytics dashboard"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/stock-finance-dashboard.git
git push -u origin main
```

## Notes

- SQLite keeps the project easy to run locally.
- The schema is intentionally simple and easy to extend.
- For production, PostgreSQL plus a hosted scheduler would be the natural next step.
