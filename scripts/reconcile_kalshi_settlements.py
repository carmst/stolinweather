#!/usr/bin/env python3
"""Compare our resolved contract outcomes against Kalshi's reported result."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from load_json_to_postgres import (
    DB_SCHEMA_PATH,
    resolve_connection,
    run_json_ingest,
    run_sql_file,
)


ROOT = Path(__file__).resolve().parents[1]
RESOLVED_DIR = ROOT / "output" / "bets" / "resolved"
OUTPUT_DIR = ROOT / "output" / "settlements"
LATEST_PATH = OUTPUT_DIR / "latest_kalshi_reconciliation.json"
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2/markets"


RECONCILIATION_SQL = r"""
with payload as (
  select doc from _codex_ingest_payload
)
insert into app.kalshi_settlement_reconciliations (
  target_date, reconciled_at, weather_market_id, city, ticker, event_ticker, recommended_side,
  title, observed_high_f, computed_contract_yes_outcome, kalshi_contract_yes_outcome,
  kalshi_status, kalshi_result, kalshi_market_payload, alignment_status, notes
)
select
  nullif(doc->>'target_date', '')::date,
  nullif(doc->>'reconciled_at', '')::timestamptz,
  row->>'weather_market_id',
  row->>'city',
  row->>'ticker',
  row->>'event_ticker',
  row->>'recommended_side',
  row->>'title',
  nullif(row->>'observed_high_f', '')::double precision,
  case when row ? 'computed_contract_yes_outcome' then (row->>'computed_contract_yes_outcome')::boolean else null end,
  case when row ? 'kalshi_contract_yes_outcome' then (row->>'kalshi_contract_yes_outcome')::boolean else null end,
  row->>'kalshi_status',
  row->>'kalshi_result',
  coalesce(row->'kalshi_market_payload', '{}'::jsonb),
  row->>'alignment_status',
  row->>'notes'
from payload, jsonb_array_elements(doc->'rows') as row
on conflict (target_date, ticker, recommended_side) do update set
  reconciled_at = excluded.reconciled_at,
  weather_market_id = excluded.weather_market_id,
  city = excluded.city,
  event_ticker = excluded.event_ticker,
  title = excluded.title,
  observed_high_f = excluded.observed_high_f,
  computed_contract_yes_outcome = excluded.computed_contract_yes_outcome,
  kalshi_contract_yes_outcome = excluded.kalshi_contract_yes_outcome,
  kalshi_status = excluded.kalshi_status,
  kalshi_result = excluded.kalshi_result,
  kalshi_market_payload = excluded.kalshi_market_payload,
  alignment_status = excluded.alignment_status,
  notes = excluded.notes,
  updated_at = now();
"""


def utc_now() -> str:
  return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> Any:
  with path.open("r", encoding="utf-8") as handle:
    return json.load(handle)


def fetch_kalshi_market(ticker: str) -> dict[str, Any]:
  with urlopen(f"{BASE_URL}/{ticker}", timeout=30) as response:
    payload = json.load(response)
  return payload.get("market", payload)


def bool_from_kalshi_result(market: dict[str, Any]) -> bool | None:
  result = str(market.get("result") or "").strip().lower()
  if result in {"yes", "y", "true", "1"}:
    return True
  if result in {"no", "n", "false", "0"}:
    return False

  for key in ("yes_settlement_value_dollars", "yes_settlement_value", "settlement_value_dollars"):
    value = market.get(key)
    if value in (None, ""):
      continue
    try:
      return float(value) >= 0.5
    except (TypeError, ValueError):
      continue

  return None


def alignment_status(computed: bool | None, kalshi: bool | None, status: str | None) -> tuple[str, str | None]:
  if computed is None:
    return "computed_unresolved", "Our resolver did not produce a contract YES outcome."
  if kalshi is None:
    return "kalshi_pending", f"Kalshi has not published a usable result yet. Current status: {status or 'unknown'}."
  if computed == kalshi:
    return "aligned", None
  return "mismatch", "Our computed contract outcome disagrees with Kalshi's reported result."


def build_reconciliation_payload(resolved_payload: dict[str, Any]) -> dict[str, Any]:
  reconciled_at = utc_now()
  rows = []
  for bet in resolved_payload.get("bets", []):
    ticker = bet.get("ticker")
    if not ticker:
      continue

    try:
      kalshi_market = fetch_kalshi_market(ticker)
      fetch_error = None
    except (HTTPError, URLError, TimeoutError, ValueError) as error:
      kalshi_market = {}
      fetch_error = str(error)

    computed_yes = bet.get("contract_yes_outcome")
    kalshi_yes = bool_from_kalshi_result(kalshi_market) if kalshi_market else None
    status, notes = alignment_status(
      computed_yes if isinstance(computed_yes, bool) else None,
      kalshi_yes,
      kalshi_market.get("status") if kalshi_market else None,
    )
    if fetch_error:
      status = "kalshi_fetch_error"
      notes = fetch_error

    rows.append(
      {
        "target_date": resolved_payload.get("target_date"),
        "reconciled_at": reconciled_at,
        "weather_market_id": bet.get("weather_market_id"),
        "city": bet.get("city"),
        "ticker": ticker,
        "event_ticker": bet.get("event_ticker"),
        "recommended_side": bet.get("recommended_side"),
        "title": bet.get("title"),
        "observed_high_f": bet.get("observed_high_f"),
        "computed_contract_yes_outcome": computed_yes if isinstance(computed_yes, bool) else None,
        "kalshi_contract_yes_outcome": kalshi_yes,
        "kalshi_status": kalshi_market.get("status"),
        "kalshi_result": kalshi_market.get("result"),
        "kalshi_market_payload": kalshi_market,
        "alignment_status": status,
        "notes": notes,
      }
    )

  counts: dict[str, int] = {}
  for row in rows:
    counts[row["alignment_status"]] = counts.get(row["alignment_status"], 0) + 1

  return {
    "target_date": resolved_payload.get("target_date"),
    "reconciled_at": reconciled_at,
    "source": "kalshi-result-reconciliation-v1",
    "counts": counts,
    "rows": rows,
  }


def parse_args() -> argparse.Namespace:
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument("--date", required=True, help="Resolved ledger date, YYYY-MM-DD.")
  parser.add_argument("--sync-db", action="store_true", help="Also upsert reconciliation rows into Postgres.")
  parser.add_argument("--database-url", help="Postgres connection string. Falls back to DATABASE_URL.")
  parser.add_argument("--init-db-schema", action="store_true", help="Apply db/schema.sql before syncing.")
  return parser.parse_args()


def main() -> int:
  args = parse_args()
  resolved_path = RESOLVED_DIR / f"{args.date}.json"
  if not resolved_path.exists():
    raise SystemExit(f"Missing resolved ledger: {resolved_path}")

  payload = build_reconciliation_payload(load_json(resolved_path))
  OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
  output_path = OUTPUT_DIR / f"{payload['target_date']}.json"
  output_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
  LATEST_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

  print(f"Reconciled {len(payload['rows'])} bets for {payload['target_date']}")
  for key, value in sorted(payload["counts"].items()):
    print(f"{key}: {value}")
  print(f"Reconciliation file: {output_path}")

  if args.sync_db:
    try:
      psql, database_url = resolve_connection(args.database_url)
      if args.init_db_schema:
        run_sql_file(psql, database_url, DB_SCHEMA_PATH)
      run_json_ingest(psql, database_url, RECONCILIATION_SQL, payload)
    except (RuntimeError, subprocess.CalledProcessError) as error:
      print(f"Postgres sync error: {error}")
      return 1
    print("Synced Kalshi reconciliation to Postgres")

  return 0


if __name__ == "__main__":
  raise SystemExit(main())
