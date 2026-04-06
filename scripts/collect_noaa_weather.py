#!/usr/bin/env python3
"""Collect NOAA/NWS forecast and observation snapshots for tracked markets."""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "tracked_markets.json"
OUTPUT_DIR = ROOT / "output" / "weather"
SNAPSHOT_DIR = OUTPUT_DIR / "snapshots"
LATEST_PATH = OUTPUT_DIR / "latest_forecasts_noaa.json"
PROVIDER = "noaa-nws"
USER_AGENT = "weather-app/1.0 (local dev)"
MAX_RETRIES = 4


def fetch_json(url: str) -> dict[str, Any]:
    delay = 1.0

    for attempt in range(MAX_RETRIES):
        request = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/geo+json"})
        try:
            with urlopen(request, timeout=30) as response:
                return json.load(response)
        except HTTPError as error:
            if error.code != 429 or attempt == MAX_RETRIES - 1:
                raise
            time.sleep(delay)
            delay *= 2

    raise RuntimeError("Exceeded NOAA retry attempts")


def load_markets(config_path: Path) -> list[dict[str, Any]]:
    with config_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def celsius_to_fahrenheit(value: float | None) -> float | None:
    if value is None:
        return None
    return (value * 9.0 / 5.0) + 32.0


def meters_per_second_to_mph(value: float | None) -> float | None:
    if value is None:
        return None
    return value * 2.23693629


def millimeters_to_inches(value: float | None) -> float | None:
    if value is None:
        return None
    return value / 25.4


def parse_qv(value: dict[str, Any] | None, converter=None) -> float | None:
    if not value:
        return None

    raw = value.get("value")
    if raw is None:
        return None

    parsed = float(raw)
    return converter(parsed) if converter else parsed


def derive_daily_rows(hourly_periods: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "date": None,
            "temperature_2m_max": None,
            "temperature_2m_min": None,
            "precipitation_sum": 0.0,
            "wind_speed_10m_max": None,
            "wind_gusts_10m_max": None,
        }
    )

    for period in hourly_periods:
        start = period.get("time")
        if not start:
            continue

        date_key = start[:10]
        row = grouped[date_key]
        row["date"] = date_key

        temp = period.get("temperature_2m")
        precip = period.get("probability_of_precipitation")
        wind = period.get("wind_speed_10m")
        gust = period.get("wind_gusts_10m")

        row["temperature_2m_max"] = temp if row["temperature_2m_max"] is None else max(row["temperature_2m_max"], temp)
        row["temperature_2m_min"] = temp if row["temperature_2m_min"] is None else min(row["temperature_2m_min"], temp)
        if precip is not None:
            row["precipitation_sum"] += precip / 100.0
        row["wind_speed_10m_max"] = wind if row["wind_speed_10m_max"] is None else max(row["wind_speed_10m_max"], wind)
        if gust is not None:
            row["wind_gusts_10m_max"] = gust if row["wind_gusts_10m_max"] is None else max(row["wind_gusts_10m_max"], gust)

    return [grouped[key] for key in sorted(grouped.keys())]


def normalize_hourly_periods(hourly_payload: dict[str, Any]) -> list[dict[str, Any]]:
    periods = hourly_payload.get("properties", {}).get("periods", [])
    rows = []

    for period in periods:
        precip = period.get("probabilityOfPrecipitation", {})
        rows.append(
            {
                "time": period.get("startTime"),
                "temperature_2m": period.get("temperature"),
                "dew_point_2m": None,
                "relative_humidity_2m": None,
                "pressure_msl": None,
                "precipitation": None,
                "cloud_cover": None,
                "wind_speed_10m": parse_wind_speed(period.get("windSpeed")),
                "wind_gusts_10m": None,
                "probability_of_precipitation": precip.get("value"),
                "short_forecast": period.get("shortForecast"),
            }
        )

    return rows


def parse_wind_speed(value: str | None) -> float | None:
    if not value:
        return None

    first_part = value.split(" ")[0]
    if "to" in first_part:
        first_part = first_part.split("to")[0]
    if "-" in first_part:
        first_part = first_part.split("-")[0]
    try:
        return float(first_part)
    except ValueError:
        return None


def normalize_observation(observation_payload: dict[str, Any]) -> dict[str, Any]:
    props = observation_payload.get("properties", {})
    return {
        "time": props.get("timestamp"),
        "temperature_2m": parse_qv(props.get("temperature"), celsius_to_fahrenheit),
        "dew_point_2m": parse_qv(props.get("dewpoint"), celsius_to_fahrenheit),
        "relative_humidity_2m": parse_qv(props.get("relativeHumidity")),
        "pressure_msl": parse_qv(props.get("barometricPressure"), lambda value: value / 100.0),
        "precipitation": parse_qv(props.get("precipitationLastHour"), millimeters_to_inches),
        "cloud_cover": None,
        "wind_speed_10m": parse_qv(props.get("windSpeed"), meters_per_second_to_mph),
        "wind_gusts_10m": parse_qv(props.get("windGust"), meters_per_second_to_mph),
        "text_description": props.get("textDescription"),
    }


def fetch_latest_observation(stations_url: str) -> tuple[dict[str, Any] | None, list[str]]:
    stations_payload = fetch_json(stations_url)
    station_ids = [feature.get("properties", {}).get("stationIdentifier") for feature in stations_payload.get("features", [])]
    station_ids = [station_id for station_id in station_ids if station_id]

    for station_id in station_ids[:5]:
        try:
            latest = fetch_json(f"https://api.weather.gov/stations/{station_id}/observations/latest")
            return normalize_observation(latest), station_ids[:5]
        except HTTPError:
            continue
        except URLError:
            continue

    return None, station_ids[:5]


def fetch_city_snapshot(market: dict[str, Any]) -> dict[str, Any]:
    points_payload = fetch_json(f"https://api.weather.gov/points/{market['latitude']},{market['longitude']}")
    props = points_payload.get("properties", {})
    forecast_payload = fetch_json(props["forecast"])
    hourly_payload = fetch_json(props["forecastHourly"])
    observation, station_ids = fetch_latest_observation(props["observationStations"])
    hourly_rows = normalize_hourly_periods(hourly_payload)
    daily_rows = derive_daily_rows(hourly_rows)

    return {
        "grid_id": props.get("gridId"),
        "grid_x": props.get("gridX"),
        "grid_y": props.get("gridY"),
        "forecast_url": props.get("forecast"),
        "forecast_hourly_url": props.get("forecastHourly"),
        "stations_url": props.get("observationStations"),
        "stations": station_ids,
        "forecast_periods": forecast_payload.get("properties", {}).get("periods", []),
        "current": observation or (hourly_rows[0] if hourly_rows else {}),
        "hourly": hourly_rows,
        "daily": daily_rows,
    }


def normalize_snapshot(market: dict[str, Any], noaa_data: dict[str, Any], pulled_at: str) -> dict[str, Any]:
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
            "pressure": "hPa",
            "precipitation": "inch",
            "wind_speed": "mph",
            "wind_gusts": "mph",
        },
        "current": noaa_data["current"],
        "hourly": noaa_data["hourly"],
        "daily": noaa_data["daily"],
        "noaa": {
            "grid_id": noaa_data["grid_id"],
            "grid_x": noaa_data["grid_x"],
            "grid_y": noaa_data["grid_y"],
            "forecast_url": noaa_data["forecast_url"],
            "forecast_hourly_url": noaa_data["forecast_hourly_url"],
            "stations_url": noaa_data["stations_url"],
            "stations": noaa_data["stations"],
            "forecast_periods": noaa_data["forecast_periods"][:14],
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
    target = SNAPSHOT_DIR / f"{day}-noaa.jsonl"
    with target.open("a", encoding="utf-8") as handle:
        for snapshot in snapshots:
            handle.write(json.dumps(snapshot))
            handle.write("\n")
    return target


def run_collection(config_path: Path) -> tuple[list[dict[str, Any]], Path]:
    markets = load_markets(config_path)
    pulled_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    snapshots = []

    for market in markets:
        noaa_data = fetch_city_snapshot(market)
        snapshots.append(normalize_snapshot(market, noaa_data, pulled_at))
        time.sleep(0.1)

    ensure_output_dirs()
    write_latest(snapshots)
    snapshot_path = append_snapshot_file(snapshots, pulled_at)
    return snapshots, snapshot_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pull NOAA/NWS forecast snapshots for tracked markets.")
    parser.add_argument("--config", type=Path, default=CONFIG_PATH, help="Path to the tracked market config JSON.")
    parser.add_argument("--print-latest", action="store_true", help="Print the normalized latest snapshot JSON to stdout.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        snapshots, snapshot_path = run_collection(args.config)
    except FileNotFoundError as error:
        print(f"Config error: {error}", file=sys.stderr)
        return 1
    except HTTPError as error:
        print(f"NOAA HTTP error: {error.code} {error.reason}", file=sys.stderr)
        return 1
    except URLError as error:
        print(f"NOAA connection error: {error.reason}", file=sys.stderr)
        return 1

    print(f"Collected {len(snapshots)} NOAA market snapshots")
    print(f"Latest file: {LATEST_PATH}")
    print(f"History file: {snapshot_path}")

    if args.print_latest:
        json.dump(snapshots, sys.stdout, indent=2)
        sys.stdout.write("\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
