#!/usr/bin/env python3
"""Resolve logged city bets against NOAA observed daily highs."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from load_json_to_postgres import DB_SCHEMA_PATH, resolve_connection, run_sql_file, sync_bets_payload


ROOT = Path(__file__).resolve().parents[1]
HISTORY_PATH = ROOT / "output" / "history" / "latest_noaa_history.json"
PRELIMINARY_PATH = ROOT / "output" / "preliminary" / "latest_preliminary_daily_highs.json"
BET_DIR = ROOT / "output" / "bets"
LATEST_BETS_PATH = BET_DIR / "latest_daily_bets.json"
RESOLVED_DIR = BET_DIR / "resolved"


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_history_index(payload: dict[str, Any]) -> dict[tuple[str, str], float]:
    index: dict[tuple[str, str], float] = {}
    for location in payload.get("locations", []):
        market_id = location.get("market", {}).get("market_id")
        if not market_id:
            continue
        for observation in location.get("observations", []):
            date_key = observation.get("date")
            tmax_f = observation.get("tmax_f")
            if date_key and isinstance(tmax_f, (int, float)):
                index[(market_id, date_key)] = float(tmax_f)
    return index


def build_preliminary_index(payload: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    index: dict[tuple[str, str], dict[str, Any]] = {}
    for row in payload.get("rows", []):
        market_id = row.get("market_id")
        forecast_date = row.get("forecast_date")
        preliminary_high = row.get("preliminary_high_f")
        if market_id and forecast_date and isinstance(preliminary_high, (int, float)):
            index[(market_id, forecast_date)] = {
                "preliminary_high_f": float(preliminary_high),
                "pulled_at": row.get("pulled_at"),
                "provider": row.get("provider"),
                "stations": row.get("stations", []),
            }
    return index


def contract_yes_outcome(bet: dict[str, Any], observed_high: float) -> bool | None:
    strike_type = bet.get("strike_type")
    floor = bet.get("floor_strike")
    cap = bet.get("cap_strike")

    if strike_type == "greater" and isinstance(floor, (int, float)):
        return observed_high > float(floor)
    if strike_type == "less" and isinstance(cap, (int, float)):
        return observed_high < float(cap)
    if strike_type == "between" and isinstance(floor, (int, float)) and isinstance(cap, (int, float)):
        return float(floor) <= observed_high <= float(cap)
    return None


def resolve_bet(bet: dict[str, Any], observed_high: float) -> dict[str, Any]:
    yes_outcome = contract_yes_outcome(bet, observed_high)
    if yes_outcome is None:
        return {
            **bet,
            "status": "unresolved",
            "resolved_at": utc_now(),
            "observed_high_f": round(observed_high, 2),
            "error": "Could not evaluate strike type",
        }

    recommended_side = bet.get("recommended_side")
    bet_won = (recommended_side == "yes" and yes_outcome) or (recommended_side == "no" and not yes_outcome)
    contract_cost = bet.get("contract_cost")
    pnl = None
    if isinstance(contract_cost, (int, float)):
        pnl = round((1.0 - float(contract_cost)) if bet_won else -float(contract_cost), 4)
    contract_count = bet.get("contract_count")
    pnl_dollars = (
        round(float(contract_count) * float(pnl), 4)
        if isinstance(contract_count, (int, float)) and pnl is not None
        else None
    )

    return {
        **bet,
        "status": "resolved",
        "resolved_at": utc_now(),
        "observed_high_f": round(observed_high, 2),
        "contract_yes_outcome": yes_outcome,
        "bet_won": bet_won,
        "pnl_per_contract": pnl,
        "pnl_dollars": pnl_dollars,
    }


def soft_resolve_bet(bet: dict[str, Any], preliminary: dict[str, Any]) -> dict[str, Any]:
    preliminary_high = preliminary["preliminary_high_f"]
    yes_outcome = contract_yes_outcome(bet, preliminary_high)
    recommended_side = bet.get("recommended_side")
    provisional_bet_won = None
    if yes_outcome is not None:
        provisional_bet_won = (recommended_side == "yes" and yes_outcome) or (
            recommended_side == "no" and not yes_outcome
        )

    return {
        **bet,
        "status": "soft_resolved",
        "resolved_at": None,
        "observed_high_f": None,
        "bet_won": None,
        "pnl_per_contract": None,
        "pnl_dollars": None,
        "preliminary_observed_high_f": round(preliminary_high, 2),
        "preliminary_contract_yes_outcome": yes_outcome,
        "preliminary_bet_won": provisional_bet_won,
        "preliminary_source": preliminary.get("provider"),
        "preliminary_pulled_at": preliminary.get("pulled_at"),
        "preliminary_stations": preliminary.get("stations", []),
    }


def resolve_payload(
    bet_payload: dict[str, Any],
    history_index: dict[tuple[str, str], float],
    preliminary_index: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    resolved_bets = []
    for bet in bet_payload.get("bets", []):
        key = (bet.get("weather_market_id"), bet.get("forecast_date"))
        observed_high = history_index.get(key)
        if observed_high is None:
            preliminary = preliminary_index.get(key)
            if preliminary is None:
                resolved_bets.append(
                    {
                        **bet,
                        "status": "pending",
                        "resolved_at": None,
                        "observed_high_f": None,
                        "bet_won": None,
                        "pnl_per_contract": None,
                        "pnl_dollars": None,
                    }
                )
            else:
                resolved_bets.append(soft_resolve_bet(bet, preliminary))
            continue
        resolved_bets.append(resolve_bet(bet, observed_high))

    wins = sum(1 for bet in resolved_bets if bet.get("bet_won") is True)
    losses = sum(1 for bet in resolved_bets if bet.get("bet_won") is False)
    pending = sum(1 for bet in resolved_bets if bet.get("status") == "pending")
    soft_resolved = sum(1 for bet in resolved_bets if bet.get("status") == "soft_resolved")
    total_pnl = round(sum(float(bet.get("pnl_per_contract") or 0.0) for bet in resolved_bets), 4)
    total_pnl_dollars = round(sum(float(bet.get("pnl_dollars") or 0.0) for bet in resolved_bets), 4)

    return {
        **bet_payload,
        "resolved_at": utc_now(),
        "wins": wins,
        "losses": losses,
        "pending": pending,
        "soft_resolved": soft_resolved,
        "total_pnl_per_contract": total_pnl,
        "total_pnl_dollars": total_pnl_dollars,
        "bets": resolved_bets,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        help="Resolve a specific ledger file in output/bets/snapshots/YYYY-MM-DD.json. Defaults to latest_daily_bets.json.",
    )
    parser.add_argument("--sync-db", action="store_true", help="Also upsert the resolved ledger into Postgres.")
    parser.add_argument("--database-url", help="Postgres connection string. Falls back to DATABASE_URL.")
    parser.add_argument(
        "--init-db-schema",
        action="store_true",
        help="Apply db/schema.sql before syncing to Postgres.",
    )
    return parser.parse_args()


def ledger_path_for_args(args: argparse.Namespace) -> Path:
    if args.date:
        return BET_DIR / "snapshots" / f"{args.date}.json"
    return LATEST_BETS_PATH


def main() -> int:
    args = parse_args()
    ledger_path = ledger_path_for_args(args)
    if not ledger_path.exists():
        raise SystemExit(f"Missing bet ledger: {ledger_path}")

    bet_payload = load_json(ledger_path)
    history_index = build_history_index(load_json(HISTORY_PATH))
    preliminary_index = (
        build_preliminary_index(load_json(PRELIMINARY_PATH))
        if PRELIMINARY_PATH.exists()
        else {}
    )
    resolved_payload = resolve_payload(bet_payload, history_index, preliminary_index)

    RESOLVED_DIR.mkdir(parents=True, exist_ok=True)
    output_path = RESOLVED_DIR / f"{resolved_payload['target_date']}.json"
    output_path.write_text(json.dumps(resolved_payload, indent=2) + "\n", encoding="utf-8")

    print(
        f"Resolved {len(resolved_payload['bets']) - resolved_payload['pending']} bets "
        f"for {resolved_payload['target_date']}"
    )
    print(
        f"Wins: {resolved_payload['wins']} | Losses: {resolved_payload['losses']} | "
        f"Soft: {resolved_payload.get('soft_resolved', 0)} | Pending: {resolved_payload['pending']}"
    )
    print(f"Total PnL/contract: {resolved_payload['total_pnl_per_contract']:+.2f}")
    print(f"Total PnL dollars: {resolved_payload['total_pnl_dollars']:+.2f}")
    print(f"Resolved file: {output_path}")
    if args.sync_db:
        try:
            psql, database_url = resolve_connection(args.database_url)
            if args.init_db_schema:
                run_sql_file(psql, database_url, DB_SCHEMA_PATH)
            sync_bets_payload(psql, database_url, resolved_payload)
        except (RuntimeError, subprocess.CalledProcessError) as error:
            print(f"Postgres sync error: {error}", file=sys.stderr)
            return 1
        print("Synced resolved bets to Postgres")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
