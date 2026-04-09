#!/usr/bin/env python3
"""Build a provisional daily-high log from intraday NOAA observed temperatures."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
NOAA_FORECAST_PATH = ROOT / "output" / "weather" / "latest_forecasts_noaa.json"
NOAA_SNAPSHOT_DIR = ROOT / "output" / "weather" / "snapshots"
OUTPUT_DIR = ROOT / "output" / "preliminary"
OBSERVATION_DIR = OUTPUT_DIR / "observations"
LATEST_PATH = OUTPUT_DIR / "latest_preliminary_daily_highs.json"
SNAPSHOT_DIR = OUTPUT_DIR / "snapshots"


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def local_date_key(timestamp: str, timezone_name: str) -> str | None:
    if not timestamp:
      return None
    parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    return parsed.astimezone(ZoneInfo(timezone_name)).date().isoformat()


def observation_entry(snapshot: dict[str, Any]) -> dict[str, Any] | None:
    market = snapshot.get("market", {})
    current = snapshot.get("current", {})
    market_id = market.get("market_id")
    location = market.get("location")
    timezone_name = market.get("timezone") or "UTC"
    observed_at = current.get("time")
    observed_temp = current.get("temperature_2m")

    if not market_id or not location or observed_at is None or observed_temp is None:
        return None

    observation_date = local_date_key(observed_at, timezone_name)
    if not observation_date:
        return None

    return {
        "market_id": market_id,
        "location": location,
        "observation_date": observation_date,
        "observed_at": observed_at,
        "observed_temp_f": float(observed_temp),
        "pulled_at": snapshot.get("pulled_at"),
        "provider": "noaa-nws-current-observation",
        "stations": snapshot.get("noaa", {}).get("stations", []),
        "timezone": timezone_name,
    }


def iter_historical_snapshots(snapshot_dir: Path) -> list[dict[str, Any]]:
    snapshots: list[dict[str, Any]] = []
    for path in sorted(snapshot_dir.glob("*-noaa.jsonl")):
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    snapshots.append(json.loads(line))
    return snapshots


def write_observation_logs(entries: list[dict[str, Any]]) -> None:
    OBSERVATION_DIR.mkdir(parents=True, exist_ok=True)
    grouped: dict[str, dict[tuple[str, str], dict[str, Any]]] = {}

    for entry in entries:
        observation_date = entry["observation_date"]
        target = OBSERVATION_DIR / f"{observation_date}.jsonl"
        existing: dict[tuple[str, str], dict[str, Any]] = {}
        if observation_date not in grouped:
            if target.exists():
                with target.open("r", encoding="utf-8") as handle:
                    for line in handle:
                        line = line.strip()
                        if not line:
                            continue
                        row = json.loads(line)
                        existing[(row["market_id"], row["observed_at"])] = row
            grouped[observation_date] = existing
        grouped[observation_date][(entry["market_id"], entry["observed_at"])] = entry

    for observation_date, rows in grouped.items():
        target = OBSERVATION_DIR / f"{observation_date}.jsonl"
        with target.open("w", encoding="utf-8") as handle:
            for row in sorted(rows.values(), key=lambda item: (item["market_id"], item["observed_at"])):
                handle.write(json.dumps(row))
                handle.write("\n")


def build_daily_high_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}

    for path in sorted(OBSERVATION_DIR.glob("*.jsonl")):
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                key = (row["market_id"], row["observation_date"])
                grouped.setdefault(key, []).append(row)

    for (market_id, observation_date), entries in grouped.items():
        max_entry = max(entries, key=lambda item: float(item["observed_temp_f"]))
        rows.append(
            {
                "market_id": market_id,
                "location": max_entry["location"],
                "forecast_date": observation_date,
                "preliminary_high_f": round(float(max_entry["observed_temp_f"]), 2),
                "pulled_at": max_entry.get("pulled_at"),
                "provider": "noaa-nws-intraday-max",
                "stations": max_entry.get("stations", []),
                "observation_count": len(entries),
                "last_observed_at": max(
                    (entry["observed_at"] for entry in entries),
                    default=max_entry["observed_at"],
                ),
                "max_observed_at": max_entry["observed_at"],
            }
        )

    return sorted(rows, key=lambda row: (row["forecast_date"], row["location"]))


def write_payload(payload: dict[str, Any]) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    snapshot_path = SNAPSHOT_DIR / f"{payload['generated_at'][:10]}.json"
    snapshot_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return snapshot_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        type=Path,
        default=NOAA_FORECAST_PATH,
        help="Path to latest NOAA forecast snapshots.",
    )
    parser.add_argument(
        "--snapshot-dir",
        type=Path,
        default=NOAA_SNAPSHOT_DIR,
        help="Directory of archived NOAA jsonl snapshot files to backfill intraday observations.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    snapshots = iter_historical_snapshots(args.snapshot_dir)
    if args.source.exists():
        snapshots.extend(load_json(args.source))
    entries = [entry for snapshot in snapshots if (entry := observation_entry(snapshot))]
    write_observation_logs(entries)
    rows = build_daily_high_rows()
    payload = {
        "generated_at": utc_now(),
        "source": "noaa-preliminary-daily-high-log-v2",
        "rows": rows,
    }
    snapshot_path = write_payload(payload)
    print(f"Logged {len(entries)} intraday observations")
    print(f"Built {len(rows)} preliminary daily highs")
    print(f"Latest file: {LATEST_PATH}")
    print(f"Snapshot file: {snapshot_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
