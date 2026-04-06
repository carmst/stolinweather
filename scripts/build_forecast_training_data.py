#!/usr/bin/env python3
"""Join archived NOAA forecast snapshots with observed NOAA daily highs."""

from __future__ import annotations

import json
from datetime import datetime, time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
WEATHER_SNAPSHOT_DIR = ROOT / "output" / "weather" / "snapshots"
HISTORY_PATH = ROOT / "output" / "history" / "latest_noaa_history.json"
OUTPUT_PATH = ROOT / "output" / "models" / "forecast_training_rows.json"


def load_history() -> dict[str, dict[str, float]]:
    payload = json.loads(HISTORY_PATH.read_text())
    history = {}
    for entry in payload.get("locations", []):
        market_id = entry.get("market", {}).get("market_id")
        if not market_id:
            continue
        history[market_id] = {
            obs["date"]: float(obs["tmax_f"])
            for obs in entry.get("observations", [])
            if obs.get("date") and obs.get("tmax_f") is not None
        }
    return history


def iter_snapshots() -> list[dict]:
    snapshots = []
    for path in sorted(WEATHER_SNAPSHOT_DIR.glob("*-noaa.jsonl")):
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    snapshots.append(json.loads(line))
    return snapshots


def target_timestamp(forecast_date: str, tz_name: str) -> datetime:
    local = datetime.combine(
        datetime.strptime(forecast_date, "%Y-%m-%d").date(),
        time(15, 0),
        tzinfo=ZoneInfo(tz_name),
    )
    return local.astimezone(timezone.utc)


def lead_bucket(lead_hours: float) -> str:
    if lead_hours < 18:
        return "same_day"
    if lead_hours < 42:
        return "next_day"
    if lead_hours < 66:
        return "day_2"
    return "day_3_plus"


def main() -> int:
    history = load_history()
    rows = []

    for snapshot in iter_snapshots():
        market = snapshot.get("market", {})
        market_id = market.get("market_id")
        if not market_id or market_id not in history:
            continue

        observed = history[market_id]
        pulled_at = datetime.fromisoformat(snapshot["pulled_at"].replace("Z", "+00:00"))
        tz_name = market.get("timezone", "UTC")

        for daily_row in snapshot.get("daily", []):
            forecast_date = daily_row.get("date")
            forecast_max = daily_row.get("temperature_2m_max")
            observed_max = observed.get(forecast_date)

            if not forecast_date or forecast_max is None or observed_max is None:
                continue

            lead_hours = (target_timestamp(forecast_date, tz_name) - pulled_at).total_seconds() / 3600.0
            if lead_hours < -12:
                continue

            rows.append(
                {
                    "market_id": market_id,
                    "location": market.get("location"),
                    "pulled_at": snapshot["pulled_at"],
                    "forecast_date": forecast_date,
                    "month": int(forecast_date[5:7]),
                    "lead_hours": round(lead_hours, 2),
                    "lead_bucket": lead_bucket(lead_hours),
                    "forecast_max_f": float(forecast_max),
                    "observed_max_f": float(observed_max),
                    "error_f": round(float(forecast_max) - float(observed_max), 2),
                }
            )

    payload = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "row_count": len(rows),
        "rows": rows,
    }
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {payload['row_count']} forecast training rows")
    print(f"Training file: {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
