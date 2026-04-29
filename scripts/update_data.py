from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.fetch_data import parse_args, refresh_market_data


def main() -> None:
    args = parse_args()
    results = refresh_market_data(args.tickers, args.period)
    for ticker, status in results.items():
        print(f"{ticker}: {status}")


if __name__ == "__main__":
    main()
