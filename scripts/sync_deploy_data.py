#!/usr/bin/env python3
"""Copy compact runtime artifacts into the Vercel deploy bundle."""

from __future__ import annotations

import shutil
import json
from collections import defaultdict
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
MODEL_SNAPSHOT_DIR = ROOT / "output" / "models" / "snapshots"
DRIFT_TARGET = ROOT / "deploy_data" / "models" / "latest_forecast_drift.json"
DRIFT_POINT_LIMIT = 144
OBSERVATION_SOURCE_DIR = ROOT / "output" / "preliminary" / "observations"
OBSERVATION_TARGET_DIR = ROOT / "deploy_data" / "preliminary" / "observations"
OBSERVATION_FILE_LIMIT = 7


def build_forecast_drift_artifact() -> int:
    if not MODEL_SNAPSHOT_DIR.exists():
        return 0

    grouped: dict[str, list[dict]] = defaultdict(list)
    for snapshot_path in sorted(MODEL_SNAPSHOT_DIR.glob("*.jsonl")):
        with snapshot_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                pulled_at = payload.get("pulled_at")
                if not pulled_at:
                    continue
                for market in payload.get("markets", []):
                    ticker = market.get("ticker")
                    forecast_date = market.get("forecast_date")
                    if not ticker or not forecast_date:
                        continue
                    grouped[f"{ticker}|{forecast_date}"].append(
                        {
                            "pulledAt": pulled_at,
                            "forecastDate": forecast_date,
                            "modelHighF": market.get("adjusted_forecast_max_f"),
                            "forecastMaxF": market.get("forecast_max_f"),
                            "noaaHighF": market.get("noaa_forecast_max_f"),
                            "openMeteoHighF": market.get("open_meteo_forecast_max_f"),
                            "visualCrossingHighF": market.get("visual_crossing_forecast_max_f"),
                        }
                    )

    compact = {}
    for key, points in grouped.items():
        points.sort(key=lambda point: point["pulledAt"])
        compact[key] = points[-DRIFT_POINT_LIMIT:]

    DRIFT_TARGET.parent.mkdir(parents=True, exist_ok=True)
    with DRIFT_TARGET.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "pointLimit": DRIFT_POINT_LIMIT,
                "marketCount": len(compact),
                "series": compact,
            },
            handle,
            separators=(",", ":"),
        )
        handle.write("\n")
    return len(compact)


def sync_recent_observation_logs() -> int:
    if not OBSERVATION_SOURCE_DIR.exists():
        return 0

    OBSERVATION_TARGET_DIR.mkdir(parents=True, exist_ok=True)
    for stale in OBSERVATION_TARGET_DIR.glob("*.jsonl"):
        stale.unlink()

    copied = 0
    for source in sorted(OBSERVATION_SOURCE_DIR.glob("*.jsonl"))[-OBSERVATION_FILE_LIMIT:]:
        shutil.copy2(source, OBSERVATION_TARGET_DIR / source.name)
        copied += 1
    return copied


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

    drift_count = build_forecast_drift_artifact()
    observation_count = sync_recent_observation_logs()
    print(f"Synced {copied} deploy data artifacts")
    if drift_count:
        print(f"Synced compact forecast drift for {drift_count} market-date series")
    if observation_count:
        print(f"Synced {observation_count} recent intraday observation logs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
