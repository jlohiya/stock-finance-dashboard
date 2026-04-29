from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.sample_data import seed_sample_data


if __name__ == "__main__":
    rows = seed_sample_data()
    print(f"Seeded {rows} sample price rows")
