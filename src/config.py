from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reports"
LOGS_DIR = PROJECT_ROOT / "logs"

DEFAULT_TICKERS = ["AAPL", "MSFT", "NVDA", "JPM", "SPY"]
DEFAULT_PERIOD = "1y"


def ensure_directories() -> None:
    for path in (DATA_DIR, REPORTS_DIR, LOGS_DIR):
        path.mkdir(parents=True, exist_ok=True)
