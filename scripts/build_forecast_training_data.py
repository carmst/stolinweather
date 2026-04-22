#!/usr/bin/env python3
"""Build training rows from blended provider forecasts against official highs."""

from __future__ import annotations

import json
from datetime import datetime, time, timezone
from pathlib import Path
from statistics import median
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


def iter_provider_snapshots(provider_suffix: str | None) -> list[dict]:
    snapshots = []
    pattern = "*.jsonl" if provider_suffix is None else f"*-{provider_suffix}.jsonl"
    for path in sorted(WEATHER_SNAPSHOT_DIR.glob(pattern)):
        if provider_suffix is None and path.name.endswith("-noaa.jsonl"):
            continue
        if provider_suffix is None and path.name.endswith("-visual-crossing.jsonl"):
            continue
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


def daily_forecast_index(snapshots: list[dict], provider: str) -> dict[tuple[str, str, str], dict]:
    index = {}
    for snapshot in snapshots:
        market = snapshot.get("market", {})
        market_id = market.get("market_id")
        pulled_at = snapshot.get("pulled_at")
        if not market_id or not pulled_at:
            continue
        for daily_row in snapshot.get("daily", []):
            forecast_date = daily_row.get("date")
            forecast_max = daily_row.get("temperature_2m_max")
            if not forecast_date or forecast_max is None:
                continue
            key = (market_id, pulled_at, forecast_date)
            index[key] = {
                "provider": provider,
                "snapshot": snapshot,
                "forecast_max_f": float(forecast_max),
                "forecast_min_f": (
                    float(daily_row["temperature_2m_min"])
                    if daily_row.get("temperature_2m_min") is not None
                    else None
                ),
            }
    return index


def main() -> int:
    history = load_history()
    noaa_index = daily_forecast_index(iter_provider_snapshots("noaa"), "noaa")
    open_meteo_index = daily_forecast_index(iter_provider_snapshots(None), "open-meteo")
    visual_crossing_index = daily_forecast_index(iter_provider_snapshots("visual-crossing"), "visual-crossing")

    all_keys = sorted(set(noaa_index) | set(open_meteo_index) | set(visual_crossing_index))
    rows = []

    for market_id, pulled_at_text, forecast_date in all_keys:
        if market_id not in history:
            continue

        observed_max = history[market_id].get(forecast_date)
        if observed_max is None:
            continue

        chosen = noaa_index.get((market_id, pulled_at_text, forecast_date))
        if chosen is None:
            chosen = open_meteo_index.get((market_id, pulled_at_text, forecast_date))
        if chosen is None:
            chosen = visual_crossing_index.get((market_id, pulled_at_text, forecast_date))
        if chosen is None:
            continue

        snapshot = chosen["snapshot"]
        market = snapshot.get("market", {})
        tz_name = market.get("timezone", "UTC")
        pulled_at = datetime.fromisoformat(pulled_at_text.replace("Z", "+00:00"))
        lead_hours = (target_timestamp(forecast_date, tz_name) - pulled_at).total_seconds() / 3600.0
        if lead_hours < -12:
            continue

        noaa_forecast = noaa_index.get((market_id, pulled_at_text, forecast_date), {}).get("forecast_max_f")
        open_meteo_forecast = open_meteo_index.get((market_id, pulled_at_text, forecast_date), {}).get("forecast_max_f")
        visual_crossing_forecast = visual_crossing_index.get((market_id, pulled_at_text, forecast_date), {}).get("forecast_max_f")
        source_values = [
            value
            for value in (noaa_forecast, open_meteo_forecast, visual_crossing_forecast)
            if value is not None
        ]
        if not source_values:
            continue

        blended_forecast = round(float(median(source_values)), 2)
        provider_spread = round(max(source_values) - min(source_values), 2) if len(source_values) >= 2 else 0.0

        rows.append(
            {
                "market_id": market_id,
                "location": market.get("location"),
                "pulled_at": pulled_at_text,
                "forecast_date": forecast_date,
                "month": int(forecast_date[5:7]),
                "lead_hours": round(lead_hours, 2),
                "lead_bucket": lead_bucket(lead_hours),
                "noaa_forecast_max_f": noaa_forecast,
                "open_meteo_forecast_max_f": open_meteo_forecast,
                "visual_crossing_forecast_max_f": visual_crossing_forecast,
                "source_count": len(source_values),
                "provider_spread_f": provider_spread,
                "blended_forecast_max_f": blended_forecast,
                "observed_max_f": float(observed_max),
                "blended_error_f": round(blended_forecast - float(observed_max), 2),
            }
        )

    payload = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source": "blended-forecast-training-v1",
        "row_count": len(rows),
        "rows": rows,
    }
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {payload['row_count']} blended forecast training rows")
    print(f"Training file: {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
