#!/usr/bin/env python3
"""Collect NOAA NCEI historical daily high temperature observations."""

from __future__ import annotations

import argparse
import json
import math
import os
import socket
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "tracked_markets.json"
OUTPUT_DIR = ROOT / "output" / "history"
LATEST_PATH = OUTPUT_DIR / "latest_noaa_history.json"
SNAPSHOT_DIR = OUTPUT_DIR / "snapshots"
BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2"
TOKEN_ENV_VARS = ("NOAA_CDO_TOKEN", "NCEI_CDO_TOKEN")
MAX_RETRIES = 4
REQUEST_TIMEOUT_SECONDS = 60


def require_token() -> str:
    for name in TOKEN_ENV_VARS:
        token = os.environ.get(name)
        if token:
            return token
    raise RuntimeError("Missing NOAA CDO token. Set NOAA_CDO_TOKEN or NCEI_CDO_TOKEN.")


def load_markets(config_path: Path) -> list[dict[str, Any]]:
    with config_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def fetch_json(token: str, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
    url = f"{BASE_URL}/{endpoint}?{urlencode(params)}"
    delay = 1.0

    for attempt in range(MAX_RETRIES):
        request = Request(
            url,
            headers={
                "token": token,
                "User-Agent": "weather-app/1.0 (local dev)",
                "Accept": "application/json",
            },
        )
        try:
            with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                return json.load(response)
        except HTTPError as error:
            if error.code not in (429, 503) or attempt == MAX_RETRIES - 1:
                raise
            time.sleep(delay)
            delay *= 2
        except socket.timeout:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(delay)
            delay *= 2

    raise RuntimeError("Exceeded NOAA history retry attempts")


def load_existing_entries() -> dict[str, dict[str, Any]]:
    if not LATEST_PATH.exists():
        return {}

    payload = json.loads(LATEST_PATH.read_text())
    return {
        entry["market"]["market_id"]: entry
        for entry in payload.get("locations", [])
        if entry.get("market", {}).get("market_id")
    }


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 3958.8
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return 2 * radius * math.asin(math.sqrt(a))


def find_station(token: str, market: dict[str, Any]) -> dict[str, Any]:
    lat = float(market["latitude"])
    lon = float(market["longitude"])
    extent = f"{lat - 1.0},{lon - 1.0},{lat + 1.0},{lon + 1.0}"
    payload = fetch_json(
        token,
        "stations",
        {
            "datasetid": "GHCND",
            "extent": extent,
            "limit": 1000,
            "sortfield": "datacoverage",
            "sortorder": "desc",
        },
    )
    stations = payload.get("results", [])
    if not stations:
        raise RuntimeError(f"No historical station found near {market['location']}")

    ranked = []
    for station in stations:
        station_lat = station.get("latitude")
        station_lon = station.get("longitude")
        if station_lat is None or station_lon is None:
            continue
        distance = haversine_miles(lat, lon, float(station_lat), float(station_lon))
        ranked.append((distance, -float(station.get("datacoverage", 0)), station))

    ranked.sort(key=lambda item: (item[0], item[1]))
    return ranked[0][2]


def daterange_chunks(start_date: date, end_date: date) -> list[tuple[date, date]]:
    chunks = []
    current = start_date
    while current <= end_date:
        chunk_end = min(date(current.year, 12, 31), end_date)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks


def collect_station_history(
    token: str, station_id: str, start_date: date, end_date: date
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for chunk_start, chunk_end in daterange_chunks(start_date, end_date):
        payload = fetch_json(
            token,
            "data",
            {
                "datasetid": "GHCND",
                "stationid": station_id,
                "datatypeid": "TMAX",
                "startdate": chunk_start.isoformat(),
                "enddate": chunk_end.isoformat(),
                "units": "standard",
                "limit": 1000,
                "sortfield": "date",
                "sortorder": "asc",
            },
        )
        rows.extend(payload.get("results", []))
        time.sleep(0.15)

    return rows


def normalize_history_entry(
    market: dict[str, Any], station: dict[str, Any], rows: list[dict[str, Any]], pulled_at: str
) -> dict[str, Any]:
    normalized_rows = []
    for row in rows:
        observed_date = row["date"][:10]
        value = row.get("value")
        if value is None:
            continue
        normalized_rows.append(
            {
                "date": observed_date,
                "tmax_f": float(value),
                "month": int(observed_date[5:7]),
                "day_of_year": datetime.strptime(observed_date, "%Y-%m-%d").timetuple().tm_yday,
            }
        )

    return {
        "pulled_at": pulled_at,
        "market": {
            "market_id": market["market_id"],
            "location": market["location"],
            "latitude": market["latitude"],
            "longitude": market["longitude"],
            "kalshi_series": market.get("kalshi_series", []),
        },
        "station": {
            "id": station.get("id"),
            "name": station.get("name"),
            "latitude": station.get("latitude"),
            "longitude": station.get("longitude"),
            "datacoverage": station.get("datacoverage"),
        },
        "observations": normalized_rows,
    }


def ensure_output_dirs() -> None:
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)


def write_outputs(payload: dict[str, Any]) -> Path:
    ensure_output_dirs()
    with LATEST_PATH.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")

    target = SNAPSHOT_DIR / f"{payload['pulled_at'][:10]}-history.json"
    with target.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")
    return target


def run_collection(config_path: Path, lookback_years: int, resume: bool) -> tuple[dict[str, Any], Path]:
    token = require_token()
    markets = load_markets(config_path)
    pulled_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    end_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    start_date = end_date.replace(year=end_date.year - lookback_years)

    existing_entries = load_existing_entries() if resume else {}
    entries = []
    for market in markets:
        if market["market_id"] in existing_entries:
            entries.append(existing_entries[market["market_id"]])
            continue

        station = find_station(token, market)
        rows = collect_station_history(token, station["id"], start_date, end_date)
        entries.append(normalize_history_entry(market, station, rows, pulled_at))
        write_outputs(
            {
                "pulled_at": pulled_at,
                "source": "noaa-cdo-ghcnd",
                "lookback_years": lookback_years,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "locations": entries,
            }
        )
        time.sleep(0.15)

    payload = {
        "pulled_at": pulled_at,
        "source": "noaa-cdo-ghcnd",
        "lookback_years": lookback_years,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "locations": entries,
    }
    snapshot_path = write_outputs(payload)
    return payload, snapshot_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pull NOAA historical daily highs for tracked cities.")
    parser.add_argument("--config", type=Path, default=CONFIG_PATH)
    parser.add_argument("--lookback-years", type=int, default=5)
    parser.add_argument("--resume", action="store_true", help="Resume from the latest saved history file when present.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        payload, snapshot_path = run_collection(args.config, args.lookback_years, args.resume)
    except RuntimeError as error:
        print(str(error), file=sys.stderr)
        return 1
    except HTTPError as error:
        print(f"NOAA history HTTP error: {error.code} {error.reason}", file=sys.stderr)
        return 1
    except URLError as error:
        print(f"NOAA history connection error: {error.reason}", file=sys.stderr)
        return 1

    print(f"Collected history for {len(payload['locations'])} locations")
    print(f"Latest file: {LATEST_PATH}")
    print(f"Snapshot file: {snapshot_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
