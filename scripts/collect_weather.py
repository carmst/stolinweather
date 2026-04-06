#!/usr/bin/env python3
"""Collect normalized weather forecast snapshots for tracked markets."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "tracked_markets.json"
OUTPUT_DIR = ROOT / "output" / "weather"
SNAPSHOT_DIR = OUTPUT_DIR / "snapshots"
LATEST_PATH = OUTPUT_DIR / "latest_forecasts.json"
PROVIDER = "open-meteo"

HOURLY_FIELDS = [
    "temperature_2m",
    "dew_point_2m",
    "relative_humidity_2m",
    "pressure_msl",
    "precipitation",
    "cloud_cover",
    "wind_speed_10m",
    "wind_gusts_10m",
]

DAILY_FIELDS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
]

CURRENT_FIELDS = [
    "temperature_2m",
    "dew_point_2m",
    "relative_humidity_2m",
    "pressure_msl",
    "precipitation",
    "cloud_cover",
    "wind_speed_10m",
    "wind_gusts_10m",
]


def load_markets(config_path: Path) -> list[dict[str, Any]]:
    with config_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def fetch_forecast(market: dict[str, Any], forecast_days: int) -> dict[str, Any]:
    params = {
        "latitude": market["latitude"],
        "longitude": market["longitude"],
        "timezone": "UTC",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
        "forecast_days": forecast_days,
        "models": "best_match",
        "current": ",".join(CURRENT_FIELDS),
        "hourly": ",".join(HOURLY_FIELDS),
        "daily": ",".join(DAILY_FIELDS),
    }
    url = f"https://api.open-meteo.com/v1/forecast?{urlencode(params)}"

    with urlopen(url, timeout=30) as response:
        return json.load(response)


def build_hourly_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    rows = []

    for index, timestamp in enumerate(times):
        row = {"time": timestamp}
        for field in HOURLY_FIELDS:
            values = hourly.get(field, [])
            row[field] = values[index] if index < len(values) else None
        rows.append(row)

    return rows


def build_daily_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    daily = payload.get("daily", {})
    times = daily.get("time", [])
    rows = []

    for index, timestamp in enumerate(times):
        row = {"date": timestamp}
        for field in DAILY_FIELDS:
            values = daily.get(field, [])
            row[field] = values[index] if index < len(values) else None
        rows.append(row)

    return rows


def normalize_snapshot(
    market: dict[str, Any], payload: dict[str, Any], pulled_at: str
) -> dict[str, Any]:
    current = payload.get("current", {})

    return {
        "pulled_at": pulled_at,
        "provider": PROVIDER,
        "market": {
            "market_id": market["market_id"],
            "contract": market["contract"],
            "location": market["location"],
            "event_type": market["event_type"],
            "threshold": market["threshold"],
            "latitude": market["latitude"],
            "longitude": market["longitude"],
            "timezone": market["timezone"],
            "kalshi_series": market.get("kalshi_series", []),
            "location_aliases": market.get("location_aliases", []),
        },
        "units": {
            "temperature": payload.get("hourly_units", {}).get("temperature_2m"),
            "dew_point": payload.get("hourly_units", {}).get("dew_point_2m"),
            "humidity": payload.get("hourly_units", {}).get("relative_humidity_2m"),
            "pressure": payload.get("hourly_units", {}).get("pressure_msl"),
            "precipitation": payload.get("hourly_units", {}).get("precipitation"),
            "wind_speed": payload.get("hourly_units", {}).get("wind_speed_10m"),
            "wind_gusts": payload.get("hourly_units", {}).get("wind_gusts_10m"),
        },
        "current": {
            "time": current.get("time"),
            "temperature_2m": current.get("temperature_2m"),
            "dew_point_2m": current.get("dew_point_2m"),
            "relative_humidity_2m": current.get("relative_humidity_2m"),
            "pressure_msl": current.get("pressure_msl"),
            "precipitation": current.get("precipitation"),
            "cloud_cover": current.get("cloud_cover"),
            "wind_speed_10m": current.get("wind_speed_10m"),
            "wind_gusts_10m": current.get("wind_gusts_10m"),
        },
        "hourly": build_hourly_rows(payload),
        "daily": build_daily_rows(payload),
    }


def ensure_output_dirs() -> None:
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)


def write_latest(snapshots: list[dict[str, Any]]) -> None:
    with LATEST_PATH.open("w", encoding="utf-8") as handle:
        json.dump(snapshots, handle, indent=2)
        handle.write("\n")


def append_snapshot_file(snapshots: list[dict[str, Any]], pulled_at: str) -> Path:
    day = pulled_at[:10]
    target = SNAPSHOT_DIR / f"{day}.jsonl"
    with target.open("a", encoding="utf-8") as handle:
        for snapshot in snapshots:
            handle.write(json.dumps(snapshot))
            handle.write("\n")
    return target


def run_collection(config_path: Path, forecast_days: int) -> tuple[list[dict[str, Any]], Path]:
    markets = load_markets(config_path)
    pulled_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    snapshots = []

    for market in markets:
        payload = fetch_forecast(market, forecast_days)
        snapshots.append(normalize_snapshot(market, payload, pulled_at))

    ensure_output_dirs()
    write_latest(snapshots)
    snapshot_path = append_snapshot_file(snapshots, pulled_at)
    return snapshots, snapshot_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pull forecast snapshots for tracked weather markets."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=CONFIG_PATH,
        help="Path to the tracked market config JSON.",
    )
    parser.add_argument(
        "--forecast-days",
        type=int,
        default=3,
        help="How many days of forecast data to request per location.",
    )
    parser.add_argument(
        "--print-latest",
        action="store_true",
        help="Print the normalized latest snapshot JSON to stdout after writing files.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        snapshots, snapshot_path = run_collection(args.config, args.forecast_days)
    except FileNotFoundError as error:
        print(f"Config error: {error}", file=sys.stderr)
        return 1
    except HTTPError as error:
        print(f"Provider HTTP error: {error.code} {error.reason}", file=sys.stderr)
        return 1
    except URLError as error:
        print(f"Provider connection error: {error.reason}", file=sys.stderr)
        return 1

    print(f"Collected {len(snapshots)} market snapshots")
    print(f"Latest file: {LATEST_PATH}")
    print(f"History file: {snapshot_path}")

    if args.print_latest:
        json.dump(snapshots, sys.stdout, indent=2)
        sys.stdout.write("\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
