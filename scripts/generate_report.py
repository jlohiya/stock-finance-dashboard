from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import REPORTS_DIR, ensure_directories
from src.database import get_connection, initialize_database
from src.metrics import get_latest_metrics


def main() -> None:
    ensure_directories()
    initialize_database()

    with get_connection() as conn:
        metrics = get_latest_metrics(conn)

    report_path = REPORTS_DIR / "latest_report.md"
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = [
        "# Stock Finance Analytics Report",
        "",
        f"Generated at: {generated_at}",
        "",
    ]

    if metrics.empty:
        lines.append("No market data available. Run `python scripts/update_data.py` first.")
    else:
        lines.extend(
            [
                "## Latest Metrics",
                "",
                metrics.to_markdown(index=False),
                "",
                "## Summary",
                "",
                f"- Tickers analyzed: {len(metrics)}",
                f"- Bullish tickers: {(metrics['trend'] == 'Bullish').sum()}",
                f"- Bearish tickers: {(metrics['trend'] == 'Bearish').sum()}",
                f"- Average 20-day volatility: {metrics['volatility_20d_pct'].mean():.2f}%",
            ]
        )

    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {report_path}")


if __name__ == "__main__":
    main()
