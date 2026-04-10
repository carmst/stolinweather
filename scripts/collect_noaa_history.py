#!/usr/bin/env python3
"""Collect NOAA NCEI historical daily high temperature observations."""

from __future__ import annotations

import argparse
import html
import json
import math
import os
import re
import socket
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from load_json_to_postgres import (
    DB_SCHEMA_PATH,
    resolve_connection,
    run_sql_file,
    sync_history_payload,
    sync_reference_tables,
)


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "tracked_markets.json"
SETTLEMENT_PATH = ROOT / "config" / "kalshi_settlement_sources.json"
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
        markets = json.load(handle)
    settlement_rows = json.loads(SETTLEMENT_PATH.read_text(encoding="utf-8"))
    settlement_by_market = {row["market_id"]: row for row in settlement_rows if row.get("market_id")}
    for market in markets:
        settlement = settlement_by_market.get(market["market_id"])
        if settlement:
            market["settlement"] = settlement
    return markets


def fetch_text(url: str) -> str:
    delay = 1.0

    for attempt in range(MAX_RETRIES):
        request = Request(
            url,
            headers={
                "User-Agent": "weather-app/1.0 (local dev)",
                "Accept": "text/html, text/plain;q=0.9, */*;q=0.1",
            },
        )
        try:
            with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                return response.read().decode("utf-8", errors="replace")
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

    raise RuntimeError("Exceeded CLI fetch retry attempts")


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


def normalize_station_text(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def station_priority(station_id: str | None) -> int:
    if not station_id:
        return 9
    raw = station_id.split(":")[-1]
    if raw.startswith("USW"):
        return 0
    if raw.startswith("USC"):
        return 1
    if raw.startswith("USR"):
        return 2
    if raw.startswith("US1"):
        return 5
    return 3


def station_name_bonus(station: dict[str, Any], settlement: dict[str, Any] | None) -> int:
    if not settlement:
        return 0
    station_name = normalize_station_text(station.get("name"))
    target_name = normalize_station_text(settlement.get("settlement_station_name"))
    if not station_name or not target_name:
        return 0
    bonus = 0
    for token in target_name.split():
        if len(token) >= 4 and token in station_name:
            bonus -= 3
    airport_code_match = re.search(r"\(([A-Z0-9]+)\)", settlement.get("settlement_station_name") or "")
    if airport_code_match:
        airport_code = airport_code_match.group(1).lower()
        if airport_code in station_name:
            bonus -= 8
    return bonus


def cli_report_observation(text: str) -> tuple[str, float] | None:
    text_only = html.unescape(re.sub(r"<[^>]+>", " ", text))
    text_only = re.sub(r"\s+", " ", text_only)
    date_match = re.search(r"FOR ([A-Z]+ \d{1,2} \d{4})", text_only)
    if not date_match:
        return None
    observation_date = datetime.strptime(date_match.group(1), "%B %d %Y").date().isoformat()
    max_match = re.search(r"(?:YESTERDAY|TODAY)\s+MAXIMUM\s+(\d+(?:\.\d+)?)", text_only)
    if not max_match:
        return None
    return observation_date, float(max_match.group(1))


def collect_cli_history(market: dict[str, Any], pulled_at: str) -> tuple[dict[str, Any], list[dict[str, Any]]] | None:
    settlement = market.get("settlement") or {}
    source_url = settlement.get("kalshi_source_url")
    if not source_url:
        return None
    raw_text = fetch_text(source_url)
    parsed = cli_report_observation(raw_text)
    if not parsed:
        return None
    observation_date, tmax_f = parsed
    station = {
        "id": settlement.get("settlement_station_id"),
        "name": settlement.get("settlement_station_name"),
        "latitude": settlement.get("settlement_station_latitude"),
        "longitude": settlement.get("settlement_station_longitude"),
        "datacoverage": None,
    }
    rows = [
        {
            "date": observation_date,
            "value": tmax_f,
            "source_type": "nws_cli",
            "source_url": source_url,
            "raw_text": raw_text,
        }
    ]
    return station, rows


def merge_history_rows(
    station_rows: list[dict[str, Any]],
    cli_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}

    for row in station_rows:
        observed_date = row["date"][:10]
        merged[observed_date] = {
            **row,
            "date": observed_date,
            "source_type": row.get("source_type", "ncei_ghcnd"),
        }

    for row in cli_rows:
        observed_date = row["date"][:10]
        merged[observed_date] = {
            **row,
            "date": observed_date,
            "source_type": row.get("source_type", "nws_cli"),
        }

    return [merged[key] for key in sorted(merged)]


def station_covers_range(station: dict[str, Any], start_date: date, end_date: date) -> bool:
    min_date = station.get("mindate")
    max_date = station.get("maxdate")
    if not min_date or not max_date:
        return False
    return min_date[:10] <= start_date.isoformat() and max_date[:10] >= end_date.isoformat()


def find_candidate_stations(token: str, market: dict[str, Any], start_date: date, end_date: date) -> list[dict[str, Any]]:
    settlement = market.get("settlement") or {}
    lat = float(settlement.get("settlement_station_latitude") or market["latitude"])
    lon = float(settlement.get("settlement_station_longitude") or market["longitude"])
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
        range_penalty = 0 if station_covers_range(station, start_date, end_date) else 1
        ranked.append(
            (
                range_penalty,
                station_priority(station.get("id")),
                distance,
                station_name_bonus(station, settlement),
                -float(station.get("datacoverage", 0)),
                station,
            )
        )

    ranked.sort(key=lambda item: (item[0], item[1], item[2], item[3], item[4]))
    return [item[5] for item in ranked]


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
                "source_type": row.get("source_type", "ncei_ghcnd"),
                "source_url": row.get("source_url"),
                "raw_text": row.get("raw_text"),
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


def supplement_existing_entry_with_cli(
    market: dict[str, Any],
    existing_entry: dict[str, Any],
    cli_station: dict[str, Any] | None,
    cli_rows: list[dict[str, Any]],
    pulled_at: str,
) -> dict[str, Any]:
    observations_by_date: dict[str, dict[str, Any]] = {}

    for row in existing_entry.get("observations", []):
        observed_date = row.get("date")
        if not observed_date:
            continue
        observations_by_date[observed_date] = {
            **row,
            "source_type": row.get("source_type", existing_entry.get("source_type", "ncei_ghcnd")),
            "source_url": row.get("source_url", existing_entry.get("source_url")),
            "raw_text": row.get("raw_text", existing_entry.get("raw_text")),
        }

    for row in cli_rows:
        observed_date = row["date"][:10]
        observations_by_date[observed_date] = {
            "date": observed_date,
            "tmax_f": float(row["value"]),
            "month": int(observed_date[5:7]),
            "day_of_year": datetime.strptime(observed_date, "%Y-%m-%d").timetuple().tm_yday,
            "source_type": row.get("source_type", "nws_cli"),
            "source_url": row.get("source_url"),
            "raw_text": row.get("raw_text"),
        }

    station = cli_station or existing_entry.get("station") or {
        "id": None,
        "name": None,
        "latitude": market["latitude"],
        "longitude": market["longitude"],
        "datacoverage": None,
    }

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
        "observations": [observations_by_date[key] for key in sorted(observations_by_date)],
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
        existing_entry = existing_entries.get(market["market_id"])

        cli_station = None
        cli_rows: list[dict[str, Any]] = []
        try:
            cli_result = collect_cli_history(market, pulled_at)
        except (HTTPError, URLError, RuntimeError, socket.timeout):
            cli_result = None
        if cli_result:
            cli_station, cli_rows = cli_result

        if existing_entry is not None:
            entries.append(
                supplement_existing_entry_with_cli(market, existing_entry, cli_station, cli_rows, pulled_at)
            )
            continue

        candidate_stations = find_candidate_stations(token, market, start_date, end_date)
        selected_station = None
        selected_rows: list[dict[str, Any]] = []

        for station in candidate_stations[:10]:
            rows = collect_station_history(token, station["id"], start_date, end_date)
            if rows:
                selected_station = station
                selected_rows = rows
                break

        if selected_station is None and cli_station is not None:
            selected_station = cli_station
        elif selected_station is None:
            selected_station = candidate_stations[0]

        merged_rows = merge_history_rows(selected_rows, cli_rows)
        entries.append(normalize_history_entry(market, selected_station, merged_rows, pulled_at))
        write_outputs(
            {
                "pulled_at": pulled_at,
                "source": "noaa-cdo-ghcnd+nws-cli",
                "lookback_years": lookback_years,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "locations": entries,
            }
        )
        time.sleep(0.15)

    payload = {
        "pulled_at": pulled_at,
        "source": "noaa-cdo-ghcnd+nws-cli",
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
    parser.add_argument("--sync-db", action="store_true", help="Also upsert collected observations into Postgres.")
    parser.add_argument("--database-url", help="Postgres connection string. Falls back to DATABASE_URL.")
    parser.add_argument(
        "--init-db-schema",
        action="store_true",
        help="Apply db/schema.sql before syncing to Postgres.",
    )
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
    if args.sync_db:
        try:
            psql, database_url = resolve_connection(args.database_url)
            if args.init_db_schema:
                run_sql_file(psql, database_url, DB_SCHEMA_PATH)
            sync_reference_tables(psql, database_url)
            sync_history_payload(psql, database_url, payload)
        except (RuntimeError, subprocess.CalledProcessError) as error:
            print(f"Postgres sync error: {error}", file=sys.stderr)
            return 1
        print("Synced NOAA history to Postgres")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
