#!/usr/bin/env python3
"""Build a lead-time forecast error model from NOAA training rows."""

from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TRAINING_PATH = ROOT / "output" / "models" / "forecast_training_rows.json"
OUTPUT_PATH = ROOT / "output" / "models" / "forecast_error_model.json"


def mean(values: list[float]) -> float:
    return sum(values) / len(values)


def stdev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    avg = mean(values)
    variance = sum((value - avg) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(variance)


def main() -> int:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not TRAINING_PATH.exists():
        model = {
            "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "source": "forecast-error-model-v1",
            "row_count": 0,
            "locations": {},
        }
        OUTPUT_PATH.write_text(json.dumps(model, indent=2) + "\n", encoding="utf-8")
        print("No training file found; wrote empty forecast error model")
        print(f"Model file: {OUTPUT_PATH}")
        return 0

    payload = json.loads(TRAINING_PATH.read_text())
    grouped: dict[tuple[str, int, str], list[float]] = defaultdict(list)

    for row in payload.get("rows", []):
        grouped[(row["location"], int(row["month"]), row["lead_bucket"])].append(float(row["error_f"]))

    locations: dict[str, dict[str, dict[str, dict[str, float]]]] = defaultdict(lambda: defaultdict(dict))
    for (location, month, bucket), errors in grouped.items():
        locations[location][str(month)][bucket] = {
            "count": len(errors),
            "mean_error_f": round(mean(errors), 2),
            "sigma_error_f": round(max(1.25, stdev(errors)), 2),
        }

    model = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source": "forecast-error-model-v1",
        "row_count": len(payload.get("rows", [])),
        "locations": {location: dict(months) for location, months in locations.items()},
    }
    OUTPUT_PATH.write_text(json.dumps(model, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote forecast error model for {len(model['locations'])} locations")
    print(f"Training rows: {model['row_count']}")
    print(f"Model file: {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
