#!/usr/bin/env python3
"""Collect normalized Visual Crossing forecast snapshots for tracked markets."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen
from zoneinfo import ZoneInfo

from load_json_to_postgres import (
    DB_SCHEMA_PATH,
    resolve_connection,
    run_sql_file,
    sync_reference_tables,
    sync_weather_payload,
)


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "tracked_markets.json"
OUTPUT_DIR = ROOT / "output" / "weather"
SNAPSHOT_DIR = OUTPUT_DIR / "snapshots"
LATEST_PATH = OUTPUT_DIR / "latest_forecasts_visual_crossing.json"
PROVIDER = "visual-crossing"
BASE_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"

HOURLY_ELEMENTS = [
    "datetimeEpoch",
    "temp",
    "dew",
    "humidity",
    "pressure",
    "precip",
    "cloudcover",
    "windspeed",
    "windgust",
]

DAILY_ELEMENTS = [
    "datetime",
    "tempmax",
    "tempmin",
    "precip",
    "windspeed",
    "windgust",
]


def load_markets(config_path: Path) -> list[dict[str, Any]]:
    with config_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_api_key(explicit_key: str | None) -> str:
    api_key = explicit_key or os.getenv("VISUAL_CROSSING_API_KEY")
    if not api_key:
        raise RuntimeError("VISUAL_CROSSING_API_KEY is not set.")
    return api_key


def iso_timestamp_from_epoch(epoch: int | float | None) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def local_iso_timestamp(date_text: str, time_text: str, timezone_name: str) -> str:
    local_dt = datetime.fromisoformat(f"{date_text}T{time_text}:00").replace(tzinfo=ZoneInfo(timezone_name))
    return local_dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def fetch_forecast(market: dict[str, Any], api_key: str, forecast_days: int) -> dict[str, Any]:
    location = f"{market['latitude']},{market['longitude']}"
    params = {
        "unitGroup": "us",
        "include": "current,days,hours",
        "elements": ",".join(sorted(set(HOURLY_ELEMENTS + DAILY_ELEMENTS))),
        "key": api_key,
        "contentType": "json",
    }
    url = f"{BASE_URL}/{location}/{datetime.now(ZoneInfo(market['timezone'])).date()}/{(datetime.now(ZoneInfo(market['timezone'])).date()).isoformat()}?{urlencode(params)}"
    # Replace single-day range with requested day span.
    local_today = datetime.now(ZoneInfo(market["timezone"])).date()
    end_date = local_today.fromordinal(local_today.toordinal() + forecast_days - 1)
    url = f"{BASE_URL}/{location}/{local_today.isoformat()}/{end_date.isoformat()}?{urlencode(params)}"

    with urlopen(url, timeout=30) as response:
        return json.load(response)


def build_hourly_rows(payload: dict[str, Any], market_timezone: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for day in payload.get("days", []):
        date_text = day.get("datetime")
        for hour in day.get("hours", []):
            epoch = hour.get("datetimeEpoch")
            if epoch is not None:
                timestamp = iso_timestamp_from_epoch(epoch)
            elif date_text and hour.get("datetime"):
                timestamp = local_iso_timestamp(date_text, hour["datetime"], market_timezone)
            else:
                timestamp = None

            rows.append(
                {
                    "time": timestamp,
                    "temperature_2m": hour.get("temp"),
                    "dew_point_2m": hour.get("dew"),
                    "relative_humidity_2m": hour.get("humidity"),
                    "pressure_msl": hour.get("pressure"),
                    "precipitation": hour.get("precip"),
                    "cloud_cover": hour.get("cloudcover"),
                    "wind_speed_10m": hour.get("windspeed"),
                    "wind_gusts_10m": hour.get("windgust"),
                }
            )
    return rows


def build_daily_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "date": day.get("datetime"),
            "temperature_2m_max": day.get("tempmax"),
            "temperature_2m_min": day.get("tempmin"),
            "precipitation_sum": day.get("precip"),
            "wind_speed_10m_max": day.get("windspeed"),
            "wind_gusts_10m_max": day.get("windgust"),
        }
        for day in payload.get("days", [])
    ]


def build_current_row(payload: dict[str, Any]) -> dict[str, Any]:
    current = payload.get("currentConditions", {})
    return {
        "time": iso_timestamp_from_epoch(current.get("datetimeEpoch")),
        "temperature_2m": current.get("temp"),
        "dew_point_2m": current.get("dew"),
        "relative_humidity_2m": current.get("humidity"),
        "pressure_msl": current.get("pressure"),
        "precipitation": current.get("precip"),
        "cloud_cover": current.get("cloudcover"),
        "wind_speed_10m": current.get("windspeed"),
        "wind_gusts_10m": current.get("windgust"),
        "conditions": current.get("conditions"),
    }


def normalize_snapshot(market: dict[str, Any], payload: dict[str, Any], pulled_at: str) -> dict[str, Any]:
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
            "temperature": "F",
            "dew_point": "F",
            "humidity": "%",
            "pressure": "mb",
            "precipitation": "inch",
            "wind_speed": "mph",
            "wind_gusts": "mph",
        },
        "current": build_current_row(payload),
        "hourly": build_hourly_rows(payload, market["timezone"]),
        "daily": build_daily_rows(payload),
        "provider_meta": {
            "resolved_address": payload.get("resolvedAddress"),
            "address": payload.get("address"),
            "timezone": payload.get("timezone"),
            "tzoffset": payload.get("tzoffset"),
            "description": payload.get("description"),
        },
    }


def ensure_output_dirs() -> None:
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)


def write_latest(snapshots: list[dict[str, Any]]) -> None:
    with LATEST_PATH.open("w", encoding="utf-8") as handle:
        json.dump(snapshots, handle, indent=2)
        handle.write("\n")


def append_snapshot_file(snapshots: list[dict[str, Any]], pulled_at: str) -> Path:
    day = pulled_at[:10]
    target = SNAPSHOT_DIR / f"{day}-visual-crossing.jsonl"
    with target.open("a", encoding="utf-8") as handle:
        for snapshot in snapshots:
            handle.write(json.dumps(snapshot))
            handle.write("\n")
    return target


def latest_pulled_at(path: Path) -> datetime | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if not payload:
        return None
    pulled_at = payload[0].get("pulled_at")
    if not pulled_at:
        return None
    return datetime.fromisoformat(pulled_at.replace("Z", "+00:00"))


def should_skip(min_interval_minutes: int) -> bool:
    if min_interval_minutes <= 0:
        return False
    last_pull = latest_pulled_at(LATEST_PATH)
    if not last_pull:
        return False
    age_minutes = (datetime.now(timezone.utc) - last_pull).total_seconds() / 60.0
    return age_minutes < min_interval_minutes


def run_collection(
    config_path: Path,
    forecast_days: int,
    api_key: str,
) -> tuple[list[dict[str, Any]], Path]:
    markets = load_markets(config_path)
    pulled_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    snapshots = []

    for market in markets:
        payload = fetch_forecast(market, api_key, forecast_days)
        snapshots.append(normalize_snapshot(market, payload, pulled_at))

    ensure_output_dirs()
    write_latest(snapshots)
    snapshot_path = append_snapshot_file(snapshots, pulled_at)
    return snapshots, snapshot_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pull Visual Crossing forecast snapshots for tracked markets.")
    parser.add_argument("--config", type=Path, default=CONFIG_PATH, help="Path to the tracked market config JSON.")
    parser.add_argument("--forecast-days", type=int, default=3, help="How many days of forecast data to request.")
    parser.add_argument("--api-key", help="Visual Crossing API key. Falls back to VISUAL_CROSSING_API_KEY.")
    parser.add_argument(
        "--min-interval-minutes",
        type=int,
        default=0,
        help="Skip the fetch if the latest Visual Crossing pull is newer than this many minutes.",
    )
    parser.add_argument("--print-latest", action="store_true", help="Print the normalized latest snapshot JSON to stdout.")
    parser.add_argument("--sync-db", action="store_true", help="Also upsert the collected snapshots into Postgres.")
    parser.add_argument("--database-url", help="Postgres connection string. Falls back to DATABASE_URL.")
    parser.add_argument("--init-db-schema", action="store_true", help="Apply db/schema.sql before syncing to Postgres.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        api_key = load_api_key(args.api_key)
    except RuntimeError as error:
        print(error, file=sys.stderr)
        return 1

    if should_skip(args.min_interval_minutes):
        print(f"Skipped Visual Crossing pull because latest snapshot is newer than {args.min_interval_minutes} minutes")
        return 0

    try:
        snapshots, snapshot_path = run_collection(args.config, args.forecast_days, api_key)
    except FileNotFoundError as error:
        print(f"Config error: {error}", file=sys.stderr)
        return 1
    except HTTPError as error:
        print(f"Provider HTTP error: {error.code} {error.reason}", file=sys.stderr)
        return 1
    except URLError as error:
        print(f"Provider connection error: {error.reason}", file=sys.stderr)
        return 1

    print(f"Collected {len(snapshots)} Visual Crossing market snapshots")
    print(f"Latest file: {LATEST_PATH}")
    print(f"History file: {snapshot_path}")

    if args.sync_db:
        try:
            psql, database_url = resolve_connection(args.database_url)
            if args.init_db_schema:
                run_sql_file(psql, database_url, DB_SCHEMA_PATH)
            sync_reference_tables(psql, database_url)
            sync_weather_payload(psql, database_url, snapshots)
        except (RuntimeError, subprocess.CalledProcessError) as error:
            print(f"Postgres sync error: {error}", file=sys.stderr)
            return 1
        print("Synced Visual Crossing snapshots to Postgres")

    if args.print_latest:
        print(json.dumps(snapshots, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
