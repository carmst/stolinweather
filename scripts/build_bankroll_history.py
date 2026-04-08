#!/usr/bin/env python3
"""Build a theoretical bankroll history from resolved daily city bet ledgers."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from load_json_to_postgres import DB_SCHEMA_PATH, resolve_connection, run_sql_file, sync_bankroll_payload
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
RESOLVED_DIR = ROOT / "output" / "bets" / "resolved"
OUTPUT_DIR = ROOT / "output" / "bets" / "bankroll"
LATEST_PATH = OUTPUT_DIR / "latest_bankroll_summary.json"
HISTORY_PATH = OUTPUT_DIR / "bankroll_history.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def resolved_files() -> list[Path]:
    return sorted(RESOLVED_DIR.glob("*.json"))


def build_day_summary(day_payload: dict[str, Any], bankroll_start: float) -> dict[str, Any]:
    risk_pct = float(day_payload.get("risk_pct_per_city") or 0.01)
    bets = day_payload.get("bets", [])
    stake_per_city = round(bankroll_start * risk_pct, 2)
    total_staked = round(sum(float(bet.get("stake_dollars") or 0.0) for bet in bets), 2)
    resolved_bets = [bet for bet in bets if bet.get("bet_won") is not None]
    pending_bets = [bet for bet in bets if bet.get("bet_won") is None]
    realized_pnl = round(sum(float(bet.get("pnl_dollars") or 0.0) for bet in resolved_bets), 4)
    bankroll_end = round(bankroll_start + realized_pnl, 4)

    return {
        "date": day_payload.get("target_date"),
        "bankroll_start": round(bankroll_start, 4),
        "bankroll_end": bankroll_end,
        "risk_pct_per_city": risk_pct,
        "stake_per_city_dollars": stake_per_city,
        "bet_count": len(bets),
        "resolved_bet_count": len(resolved_bets),
        "pending_bet_count": len(pending_bets),
        "wins": sum(1 for bet in resolved_bets if bet.get("bet_won") is True),
        "losses": sum(1 for bet in resolved_bets if bet.get("bet_won") is False),
        "total_staked_dollars": total_staked,
        "realized_pnl_dollars": realized_pnl,
        "roi_on_staked": round(realized_pnl / total_staked, 4) if total_staked > 0 else None,
    }


def build_history(starting_bankroll: float) -> dict[str, Any]:
    days = []
    bankroll = round(starting_bankroll, 4)

    for path in resolved_files():
        payload = load_json(path)
        day_summary = build_day_summary(payload, bankroll)
        days.append(day_summary)
        bankroll = day_summary["bankroll_end"]

    return {
        "generated_at": utc_now(),
        "starting_bankroll": round(starting_bankroll, 4),
        "current_bankroll": round(bankroll, 4),
        "day_count": len(days),
        "days": days,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--starting-bankroll",
        type=float,
        default=100.0,
        help="Initial bankroll before the first resolved betting day. Defaults to 100.",
    )
    parser.add_argument("--sync-db", action="store_true", help="Also upsert bankroll history into Postgres.")
    parser.add_argument("--database-url", help="Postgres connection string. Falls back to DATABASE_URL.")
    parser.add_argument(
        "--init-db-schema",
        action="store_true",
        help="Apply db/schema.sql before syncing to Postgres.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    history = build_history(args.starting_bankroll)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_PATH.write_text(json.dumps(history, indent=2) + "\n", encoding="utf-8")

    latest = {
        "generated_at": history["generated_at"],
        "starting_bankroll": history["starting_bankroll"],
        "current_bankroll": history["current_bankroll"],
        "day_count": history["day_count"],
        "latest_day": history["days"][-1] if history["days"] else None,
    }
    LATEST_PATH.write_text(json.dumps(latest, indent=2) + "\n", encoding="utf-8")

    print(f"Current bankroll: {history['current_bankroll']:.2f}")
    print(f"Resolved days: {history['day_count']}")
    print(f"History file: {HISTORY_PATH}")
    print(f"Latest summary: {LATEST_PATH}")
    if args.sync_db:
        try:
            psql, database_url = resolve_connection(args.database_url)
            if args.init_db_schema:
                run_sql_file(psql, database_url, DB_SCHEMA_PATH)
            sync_bankroll_payload(psql, database_url, history)
        except (RuntimeError, subprocess.CalledProcessError) as error:
            print(f"Postgres sync error: {error}", file=sys.stderr)
            return 1
        print("Synced bankroll history to Postgres")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
