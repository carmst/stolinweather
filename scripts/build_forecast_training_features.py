#!/usr/bin/env python3
"""Build rich model-ready high-temperature training rows from forecast snapshots."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from weather_feature_utils import build_combined_features, floor_checkpoint, provider_prefix


ROOT = Path(__file__).resolve().parents[1]
WEATHER_SNAPSHOT_DIR = ROOT / "output" / "weather" / "snapshots"
HISTORY_PATH = ROOT / "output" / "history" / "latest_noaa_history.json"
OUTPUT_PATH = ROOT / "output" / "models" / "forecast_training_features.json"


def load_history() -> dict[str, dict[str, dict]]:
    payload = json.loads(HISTORY_PATH.read_text())
    history: dict[str, dict[str, dict]] = {}
    for entry in payload.get("locations", []):
        market_id = entry.get("market", {}).get("market_id")
        if not market_id:
            continue
        history[market_id] = {
            obs["date"]: obs
            for obs in entry.get("observations", [])
            if obs.get("date") and obs.get("tmax_f") is not None
        }
    return history


def iter_snapshots() -> list[dict]:
    snapshots = []
    for path in sorted(WEATHER_SNAPSHOT_DIR.glob("*.jsonl")):
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    snapshots.append(json.loads(line))
    return snapshots


def main() -> int:
    history = load_history()
    grouped = defaultdict(dict)

    for snapshot in iter_snapshots():
        market = snapshot.get("market", {})
        market_id = market.get("market_id")
        pulled_at = snapshot.get("pulled_at")
        provider = provider_prefix(snapshot.get("provider"))
        if not market_id or not pulled_at:
            continue
        if market_id not in history:
            continue

        checkpoint_at = floor_checkpoint(pulled_at)
        for daily in snapshot.get("daily", []):
            forecast_date = daily.get("date")
            if not forecast_date or forecast_date not in history[market_id]:
                continue
            key = (market_id, forecast_date, checkpoint_at)
            grouped[key][provider] = snapshot

    rows = []
    for (market_id, forecast_date, checkpoint_at), provider_snapshots in sorted(grouped.items()):
        reference = next(iter(provider_snapshots.values()))
        market = reference.get("market", {})
        row = build_combined_features(
            market_id=market_id,
            location=market.get("location"),
            forecast_date=forecast_date,
            checkpoint_at=checkpoint_at,
            provider_snapshots=provider_snapshots,
        )
        if not row:
            continue
        if row["lead_hours"] < -12:
            continue
        observed = history[market_id][forecast_date]
        actual_high = float(observed["tmax_f"])
        row["observed_max_f"] = actual_high
        row["actual_source_type"] = observed.get("source_type")
        row["residual_observed_minus_blended_f"] = round(actual_high - float(row["blended_forecast_max_f"]), 2)
        rows.append(row)

    payload = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source": "high-temp-training-features-v1",
        "row_count": len(rows),
        "rows": rows,
    }
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {len(rows)} rich high-temp training rows")
    print(f"Training file: {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
