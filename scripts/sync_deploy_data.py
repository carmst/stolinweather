#!/usr/bin/env python3
"""Copy compact runtime artifacts into the Vercel deploy bundle."""

from __future__ import annotations

import shutil
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE_FILES = (
    ("kalshi/latest_markets.json", "output/kalshi/latest_markets.json"),
    ("models/latest_scored_markets.json", "output/models/latest_scored_markets.json"),
    ("history/latest_noaa_history.json", "output/history/latest_noaa_history.json"),
    ("preliminary/latest_preliminary_daily_highs.json", "output/preliminary/latest_preliminary_daily_highs.json"),
    ("weather/latest_forecasts.json", "output/weather/latest_forecasts.json"),
    ("weather/latest_forecasts_noaa.json", "output/weather/latest_forecasts_noaa.json"),
    ("weather/latest_forecasts_visual_crossing.json", "output/weather/latest_forecasts_visual_crossing.json"),
)


def main() -> int:
    copied = 0
    for relative_target, relative_source in SOURCE_FILES:
        source = ROOT / relative_source
        if not source.exists():
            print(f"Skipping missing artifact: {relative_source}")
            continue
        target = ROOT / "deploy_data" / relative_target
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        copied += 1

    print(f"Synced {copied} deploy data artifacts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
