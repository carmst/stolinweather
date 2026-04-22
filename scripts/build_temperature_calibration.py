#!/usr/bin/env python3
"""Build simple city/week calibration artifacts from NOAA historical highs."""

from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
HISTORY_PATH = ROOT / "output" / "history" / "latest_noaa_history.json"
OUTPUT_DIR = ROOT / "output" / "models"
LATEST_PATH = OUTPUT_DIR / "temperature_calibration.json"
MIN_WEEKLY_COUNT = 20


def mean(values: list[float]) -> float:
    return sum(values) / len(values)


def stdev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    avg = mean(values)
    variance = sum((value - avg) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(variance)


def percentile(sorted_values: list[float], fraction: float) -> float:
    if not sorted_values:
        return 0.0
    index = min(len(sorted_values) - 1, max(0, int(len(sorted_values) * fraction)))
    return sorted_values[index]


def stats_for(values: list[float]) -> dict[str, float | int]:
    sorted_values = sorted(values)
    return {
        "count": len(values),
        "mean_high_f": round(mean(values), 2),
        "sigma_f": round(max(1.5, stdev(values)), 2),
        "p10_high_f": round(percentile(sorted_values, 0.10), 2),
        "p50_high_f": round(percentile(sorted_values, 0.50), 2),
        "p90_high_f": round(percentile(sorted_values, 0.90), 2),
    }


def main() -> int:
    payload = json.loads(HISTORY_PATH.read_text())
    calibration = {
        "pulled_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source": "noaa-history-calibration-v2",
        "primary_bucket": "iso_week",
        "fallback_bucket": "month",
        "min_weekly_count": MIN_WEEKLY_COUNT,
        "locations": {},
    }

    for entry in payload.get("locations", []):
        location = entry["market"]["location"]
        monthly_grouped: dict[int, list[float]] = defaultdict(list)
        weekly_grouped: dict[int, list[float]] = defaultdict(list)
        for obs in entry.get("observations", []):
            observation_date = datetime.strptime(obs["date"], "%Y-%m-%d").date()
            monthly_grouped[int(obs["month"])].append(float(obs["tmax_f"]))
            weekly_grouped[int(observation_date.isocalendar().week)].append(float(obs["tmax_f"]))

        monthly = {}
        for month, values in monthly_grouped.items():
            if not values:
                continue
            monthly[str(month)] = stats_for(values)

        weekly = {}
        for week, values in weekly_grouped.items():
            if len(values) < MIN_WEEKLY_COUNT:
                continue
            weekly[str(week)] = stats_for(values)

        calibration["locations"][location] = {
            "station": entry["station"],
            "monthly": monthly,
            "weekly": weekly,
        }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_PATH.write_text(json.dumps(calibration, indent=2) + "\n", encoding="utf-8")

    print(f"Wrote calibration for {len(calibration['locations'])} locations")
    print(f"Calibration file: {LATEST_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
