#!/usr/bin/env python3
"""Select one daily temperature contract per city and write a bet ledger snapshot."""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCORED_PATH = ROOT / "output" / "models" / "latest_scored_markets.json"
OUTPUT_DIR = ROOT / "output" / "bets"
LATEST_PATH = OUTPUT_DIR / "latest_daily_bets.json"
SNAPSHOT_DIR = OUTPUT_DIR / "snapshots"
BANKROLL_SUMMARY_PATH = OUTPUT_DIR / "bankroll" / "latest_bankroll_summary.json"
DEFAULT_STARTING_BANKROLL = 100.0
DEFAULT_RISK_PCT = 0.01


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_bankroll(default_value: float) -> float:
    if BANKROLL_SUMMARY_PATH.exists():
        payload = load_json(BANKROLL_SUMMARY_PATH)
        ending_bankroll = payload.get("current_bankroll")
        if isinstance(ending_bankroll, (int, float)):
            return round(float(ending_bankroll), 2)
    return round(default_value, 2)


def pick_side_price(market: dict[str, Any], side: str) -> float | None:
    if side == "yes":
        candidates = [
            market.get("yes_ask_dollars"),
            market.get("last_price_dollars"),
            market.get("yes_bid_dollars"),
            market.get("implied_probability"),
        ]
    else:
        implied_no = (
            1.0 - float(market["implied_probability"])
            if isinstance(market.get("implied_probability"), (int, float))
            else None
        )
        last_price = (
            1.0 - float(market["last_price_dollars"])
            if isinstance(market.get("last_price_dollars"), (int, float))
            else None
        )
        candidates = [
            market.get("no_ask_dollars"),
            last_price,
            market.get("no_bid_dollars"),
            implied_no,
        ]

    for value in candidates:
        if isinstance(value, (int, float)):
            return round(float(value), 4)
    return None


def side_metrics(market: dict[str, Any]) -> dict[str, Any]:
    model_prob = float(market.get("model_probability") or 0.0)
    recommended_side = "yes" if model_prob >= 0.5 else "no"
    win_prob = model_prob if recommended_side == "yes" else 1.0 - model_prob
    contract_cost = pick_side_price(market, recommended_side)
    expected_value = None if contract_cost is None else round(win_prob - contract_cost, 4)
    expected_return = (
        None if contract_cost in (None, 0) else round(expected_value / contract_cost, 4)
    )
    return {
        "recommended_side": recommended_side,
        "win_probability": round(win_prob, 4),
        "contract_cost": contract_cost,
        "expected_value": expected_value,
        "expected_return": expected_return,
    }


def location_key(market: dict[str, Any]) -> str:
    return (
        market.get("weather_market_id")
        or market.get("matched_location")
        or market.get("series_ticker")
        or market.get("event_ticker")
        or market.get("ticker")
    )


def score_for_selection(market: dict[str, Any], metrics: dict[str, Any]) -> tuple[float, float, float]:
    expected_value = metrics.get("expected_value")
    expected_return = metrics.get("expected_return")
    win_probability = metrics.get("win_probability") or 0.0
    return (
        float(expected_value if expected_value is not None else -999.0),
        float(expected_return if expected_return is not None else -999.0),
        float(win_probability),
    )


def build_bet_entry(
    market: dict[str, Any],
    metrics: dict[str, Any],
    logged_at: str,
    bankroll: float,
    risk_pct: float,
) -> dict[str, Any]:
    risk_dollars = round(bankroll * risk_pct, 2)
    contract_cost = metrics["contract_cost"]
    contract_count = (
        round(risk_dollars / contract_cost, 4)
        if isinstance(contract_cost, (int, float)) and contract_cost > 0
        else None
    )
    max_profit_dollars = (
        round(contract_count * (1.0 - contract_cost), 4)
        if contract_count is not None and isinstance(contract_cost, (int, float))
        else None
    )
    expected_value_dollars = (
        round(contract_count * metrics["expected_value"], 4)
        if contract_count is not None and metrics["expected_value"] is not None
        else None
    )

    return {
        "logged_at": logged_at,
        "status": "pending",
        "forecast_date": market.get("forecast_date"),
        "city": market.get("matched_location"),
        "weather_market_id": market.get("weather_market_id"),
        "series_ticker": market.get("series_ticker"),
        "event_ticker": market.get("event_ticker"),
        "ticker": market.get("ticker"),
        "title": market.get("title"),
        "signal": market.get("signal_short") or market.get("model_signal"),
        "lead_bucket": market.get("lead_bucket"),
        "strike_type": market.get("strike_type"),
        "floor_strike": market.get("floor_strike"),
        "cap_strike": market.get("cap_strike"),
        "forecast_max_f": market.get("forecast_max_f"),
        "adjusted_forecast_max_f": market.get("adjusted_forecast_max_f"),
        "forecast_sigma_f": market.get("forecast_sigma_f"),
        "recommended_side": metrics["recommended_side"],
        "model_win_probability": metrics["win_probability"],
        "yes_probability": round(float(market.get("model_probability") or 0.0), 4),
        "kalshi_yes_probability": round(float(market.get("implied_probability") or 0.0), 4),
        "contract_cost": contract_cost,
        "expected_value": metrics["expected_value"],
        "expected_return": metrics["expected_return"],
        "bankroll_at_bet": bankroll,
        "risk_pct_of_bankroll": risk_pct,
        "stake_dollars": risk_dollars,
        "contract_count": contract_count,
        "max_profit_dollars": max_profit_dollars,
        "expected_value_dollars": expected_value_dollars,
        "yes_ask_dollars": market.get("yes_ask_dollars"),
        "no_ask_dollars": market.get("no_ask_dollars"),
        "volume": market.get("volume"),
        "close_time": market.get("close_time"),
    }


def select_daily_bets(
    markets: list[dict[str, Any]], target_date: str, bankroll: float, risk_pct: float
) -> list[dict[str, Any]]:
    by_city: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}

    for market in markets:
        if market.get("forecast_date") != target_date:
            continue

        metrics = side_metrics(market)
        key = location_key(market)
        current = by_city.get(key)
        if not current or score_for_selection(market, metrics) > score_for_selection(current[0], current[1]):
            by_city[key] = (market, metrics)

    logged_at = utc_now()
    selected = [
        build_bet_entry(market, metrics, logged_at, bankroll, risk_pct)
        for market, metrics in by_city.values()
    ]
    selected.sort(key=lambda item: (item.get("city") or "", -(item.get("expected_value") or -999.0)))
    return selected


def write_payload(payload: dict[str, Any]) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    snapshot_path = SNAPSHOT_DIR / f"{payload['target_date']}.json"
    snapshot_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return snapshot_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Target forecast date to log in YYYY-MM-DD format. Defaults to local today.",
    )
    parser.add_argument(
        "--bankroll",
        type=float,
        default=DEFAULT_STARTING_BANKROLL,
        help="Starting bankroll to use if no bankroll summary exists. Defaults to 100.",
    )
    parser.add_argument(
        "--risk-pct",
        type=float,
        default=DEFAULT_RISK_PCT,
        help="Fraction of bankroll to stake per city. Defaults to 0.01 (1%%).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = load_json(SCORED_PATH)
    markets = payload.get("markets", [])
    bankroll = resolve_bankroll(args.bankroll)
    bets = select_daily_bets(markets, args.date, bankroll, args.risk_pct)
    result = {
        "generated_at": utc_now(),
        "target_date": args.date,
        "starting_bankroll": bankroll,
        "risk_pct_per_city": args.risk_pct,
        "stake_per_city_dollars": round(bankroll * args.risk_pct, 2),
        "bet_count": len(bets),
        "bets": bets,
    }
    snapshot_path = write_payload(result)

    print(f"Logged {result['bet_count']} daily city bets for {args.date}")
    print(f"Latest file: {LATEST_PATH}")
    print(f"Snapshot file: {snapshot_path}")
    if bets:
        first = bets[0]
        print(
            f"Example: {first['city']} | {first['title']} | "
            f"{first['recommended_side'].upper()} @ {first['contract_cost']} | "
            f"stake ${first['stake_dollars']:.2f}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
