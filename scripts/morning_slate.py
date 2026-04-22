#!/usr/bin/env python3
"""Print the Morning Slate from the saved daily bet ledger."""

from __future__ import annotations

import argparse
import json
import re
from datetime import date
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
BETS_DIR = ROOT / "output" / "bets"


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def ledger_path(target_date: str) -> Path:
    resolved = BETS_DIR / "resolved" / f"{target_date}.json"
    if resolved.exists():
        return resolved
    return BETS_DIR / "snapshots" / f"{target_date}.json"


def clean_text(value: Any) -> str:
    text = re.sub(r"\*\*", "", str(value or ""))
    return re.sub(r"\s+", " ", text).strip()


def fmt_money(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "--"
    return f"{value:+.2f}"


def fmt_prob(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "--"
    return f"{value * 100:.1f}%"


def fmt_temp(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "--"
    return f"{value:.1f}F"


def outcome_label(bet: dict[str, Any]) -> str:
    if isinstance(bet.get("bet_won"), bool):
        return "WIN" if bet["bet_won"] else "LOSS"
    if isinstance(bet.get("preliminary_bet_won"), bool):
        return "SOFT WIN" if bet["preliminary_bet_won"] else "SOFT LOSS"
    if isinstance(bet.get("contract_yes_outcome"), bool):
        return "YES WON" if bet["contract_yes_outcome"] else "YES LOST"
    if isinstance(bet.get("preliminary_contract_yes_outcome"), bool):
        return "YES SOFT WON" if bet["preliminary_contract_yes_outcome"] else "YES SOFT LOST"
    return clean_text(bet.get("status") or "pending").upper()


def row_for_bet(bet: dict[str, Any]) -> dict[str, str]:
    return {
        "City": clean_text(bet.get("city")),
        "Side": clean_text(bet.get("recommended_side")).upper(),
        "EV": fmt_money(bet.get("expected_value")),
        "Cost": fmt_money(bet.get("contract_cost")).replace("+", ""),
        "Win%": fmt_prob(bet.get("model_win_probability")),
        "Kalshi%": fmt_prob(bet.get("kalshi_yes_probability")),
        "Model High": fmt_temp(bet.get("adjusted_forecast_max_f")),
        "Outcome": outcome_label(bet),
        "Contract": clean_text(bet.get("title")),
    }


def print_table(rows: list[dict[str, str]]) -> None:
    if not rows:
        print("No Morning Slate contracts matched.")
        return

    columns = ["City", "Side", "EV", "Cost", "Win%", "Kalshi%", "Model High", "Outcome", "Contract"]
    widths = {
        column: min(
            52 if column == "Contract" else 18,
            max(len(column), *(len(row[column]) for row in rows)),
        )
        for column in columns
    }

    print(" | ".join(column.ljust(widths[column]) for column in columns))
    print("-+-".join("-" * widths[column] for column in columns))
    for row in rows:
        rendered = []
        for column in columns:
            value = row[column]
            if column == "Contract" and len(value) > widths[column]:
                value = value[: widths[column] - 1] + "…"
            rendered.append(value.ljust(widths[column]))
        print(" | ".join(rendered))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=date.today().isoformat(), help="Slate date in YYYY-MM-DD format.")
    parser.add_argument("--side", choices=("yes", "no", "all"), default="yes", help="Side to print. Defaults to yes.")
    parser.add_argument("--min-ev", type=float, default=None, help="Only include contracts with EV >= this value.")
    parser.add_argument("--top", type=int, default=20, help="Maximum rows to print. Defaults to 20.")
    parser.add_argument("--sort", choices=("ev", "city"), default="ev", help="Sort order. Defaults to EV descending.")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a table.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    path = ledger_path(args.date)
    if not path.exists():
        raise SystemExit(f"Missing Morning Slate ledger: {path}")

    payload = load_json(path)
    bets = payload.get("bets", [])
    if args.side != "all":
        bets = [bet for bet in bets if bet.get("recommended_side") == args.side]
    if args.min_ev is not None:
        bets = [
            bet
            for bet in bets
            if isinstance(bet.get("expected_value"), (int, float)) and bet["expected_value"] >= args.min_ev
        ]

    if args.sort == "ev":
        bets.sort(key=lambda bet: (bet.get("expected_value") if isinstance(bet.get("expected_value"), (int, float)) else -999), reverse=True)
    else:
        bets.sort(key=lambda bet: clean_text(bet.get("city")))

    if args.top > 0:
        bets = bets[: args.top]

    print(f"Morning Slate | {args.date} | source: {path.relative_to(ROOT)} | rows: {len(bets)}")
    if args.json:
        print(json.dumps(bets, indent=2))
    else:
        print_table([row_for_bet(bet) for bet in bets])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
