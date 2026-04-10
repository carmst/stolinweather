#!/usr/bin/env python3
"""Load current JSON artifacts into the Postgres schema using psql.

This module is also imported by the collectors/scorer so a live run can push
fresh artifacts directly into Postgres after writing the local JSON outputs.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DB_SCHEMA_PATH = ROOT / "db" / "schema.sql"
CONFIG_PATH = ROOT / "config" / "tracked_markets.json"
SETTLEMENT_PATH = ROOT / "config" / "kalshi_settlement_sources.json"
KALSHI_PATH = ROOT / "output" / "kalshi" / "latest_markets.json"
WEATHER_NOAA_PATH = ROOT / "output" / "weather" / "latest_forecasts_noaa.json"
WEATHER_OPEN_METEO_PATH = ROOT / "output" / "weather" / "latest_forecasts.json"
SCORED_PATH = ROOT / "output" / "models" / "latest_scored_markets.json"
BETS_PATH = ROOT / "output" / "bets" / "latest_daily_bets.json"
BANKROLL_PATH = ROOT / "output" / "bets" / "bankroll" / "bankroll_history.json"
HISTORY_PATH = ROOT / "output" / "history" / "latest_noaa_history.json"
PRELIMINARY_PATH = ROOT / "output" / "preliminary" / "latest_preliminary_daily_highs.json"


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def require_psql() -> str:
    psql = shutil.which("psql")
    if not psql:
        raise RuntimeError("psql is required but was not found in PATH.")
    return psql


def require_database_url(cli_value: str | None) -> str:
    url = cli_value or os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("Set DATABASE_URL or pass --database-url.")
    return url


def json_literal(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=True)


def sql_text(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def sql_text_array(values: list[str] | None) -> str:
    if not values:
        return "ARRAY[]::text[]"
    rendered = ", ".join(sql_text(value) for value in values)
    return f"ARRAY[{rendered}]"


def sql_numeric(value: Any) -> str:
    if value is None:
        return "NULL"
    return str(value)


def run_sql_file(psql: str, database_url: str, path: Path) -> None:
    subprocess.run(
        [psql, database_url, "-v", "ON_ERROR_STOP=1", "-f", str(path)],
        check=True,
    )


def run_sql(psql: str, database_url: str, sql: str) -> None:
    subprocess.run(
        [psql, database_url, "-v", "ON_ERROR_STOP=1", "-c", sql],
        check=True,
    )


def run_sql_text_file(psql: str, database_url: str, sql: str) -> None:
    handle = tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False, encoding="utf-8")
    try:
        handle.write(sql)
        handle.write("\n")
        handle.close()
        run_sql_file(psql, database_url, Path(handle.name))
    finally:
        Path(handle.name).unlink(missing_ok=True)


def upsert_reference_tables(psql: str, database_url: str) -> None:
    tracked = load_json(CONFIG_PATH)
    settlement = load_json(SETTLEMENT_PATH)

    statements = ["begin;"]
    for row in tracked:
        statements.append(
            f"""
insert into app.market_locations (
  market_id, city, contract, event_type, latitude, longitude, timezone, kalshi_series, location_aliases
)
values (
  {sql_text(row['market_id'])},
  {sql_text(row['location'])},
  {sql_text(row['contract'])},
  {sql_text(row['event_type'])},
  {sql_numeric(row['latitude'])},
  {sql_numeric(row['longitude'])},
  {sql_text(row['timezone'])},
  {sql_text_array(row.get('kalshi_series', []))},
  {sql_text_array(row.get('location_aliases', []))}
)
on conflict (market_id) do update set
  city = excluded.city,
  contract = excluded.contract,
  event_type = excluded.event_type,
  latitude = excluded.latitude,
  longitude = excluded.longitude,
  timezone = excluded.timezone,
  kalshi_series = excluded.kalshi_series,
  location_aliases = excluded.location_aliases,
  updated_at = now();
""".strip()
        )

    for row in settlement:
        statements.append(
            f"""
insert into app.settlement_sources (
  series_ticker, market_id, city, kalshi_market_title, status, kalshi_source_label, kalshi_source_url,
  settlement_station_name, settlement_station_id, settlement_station_latitude, settlement_station_longitude,
  settlement_dataset, settlement_product, notes
)
values (
  {sql_text(row['series_ticker'])},
  {sql_text(row['market_id'])},
  {sql_text(row['city'])},
  {sql_text(row['kalshi_market_title'])},
  {sql_text(row.get('status'))},
  {sql_text(row.get('kalshi_source_label'))},
  {sql_text(row.get('kalshi_source_url'))},
  {sql_text(row.get('settlement_station_name'))},
  {sql_text(row.get('settlement_station_id'))},
  {sql_numeric(row.get('settlement_station_latitude'))},
  {sql_numeric(row.get('settlement_station_longitude'))},
  {sql_text(row.get('settlement_dataset'))},
  {sql_text(row.get('settlement_product'))},
  {sql_text(row.get('notes'))}
)
on conflict (series_ticker) do update set
  market_id = excluded.market_id,
  city = excluded.city,
  kalshi_market_title = excluded.kalshi_market_title,
  status = excluded.status,
  kalshi_source_label = excluded.kalshi_source_label,
  kalshi_source_url = excluded.kalshi_source_url,
  settlement_station_name = excluded.settlement_station_name,
  settlement_station_id = excluded.settlement_station_id,
  settlement_station_latitude = excluded.settlement_station_latitude,
  settlement_station_longitude = excluded.settlement_station_longitude,
  settlement_dataset = excluded.settlement_dataset,
  settlement_product = excluded.settlement_product,
  notes = excluded.notes,
  updated_at = now();
""".strip()
        )

    statements.append("commit;")
    run_sql(psql, database_url, "\n".join(statements))


def run_json_ingest(psql: str, database_url: str, sql: str, payload: Any) -> None:
    payload_literal = sql_text(json_literal(payload))
    rendered_sql = f"""
begin;
create temp table if not exists _codex_ingest_payload (doc jsonb) on commit drop;
truncate _codex_ingest_payload;
insert into _codex_ingest_payload (doc) values ({payload_literal}::jsonb);
{sql}
commit;
"""
    run_sql_text_file(psql, database_url, rendered_sql)


KALSHI_SQL = r"""
with payload as (
  select doc from _codex_ingest_payload
),
events as (
  select distinct
    market->>'event_ticker' as event_ticker,
    market->>'series_ticker' as series_ticker,
    market->>'series_title' as series_title,
    market->>'series_slug' as series_slug,
    nullif(market->>'close_time', '')::timestamptz as close_time,
    nullif(market->>'open_time', '')::timestamptz as open_time,
    market->>'status' as status,
    nullif(substring(market->>'event_ticker' from '([0-9]{2}[A-Z]{3}[0-9]{2})$'), '') as event_code
  from payload, jsonb_array_elements(doc->'markets') as market
),
upsert_events as (
  insert into app.kalshi_events (
    event_ticker, series_ticker, series_title, series_slug, event_date, close_time, open_time, status
  )
  select
    event_ticker,
    series_ticker,
    series_title,
    series_slug,
    case
      when event_code is null then null
      else to_date(event_code, 'YYMONDD')
    end,
    close_time,
    open_time,
    status
  from events
  on conflict (event_ticker) do update set
    series_ticker = excluded.series_ticker,
    series_title = excluded.series_title,
    series_slug = excluded.series_slug,
    event_date = excluded.event_date,
    close_time = excluded.close_time,
    open_time = excluded.open_time,
    status = excluded.status,
    updated_at = now()
  returning event_ticker
)
insert into app.kalshi_markets (
  ticker, event_ticker, series_ticker, title, subtitle, market_type, strike_type,
  floor_strike, cap_strike, functional_strike, custom_strike, status, result
)
select
  market->>'ticker',
  market->>'event_ticker',
  market->>'series_ticker',
  market->>'title',
  market->>'subtitle',
  market->>'market_type',
  market->>'strike_type',
  nullif(market->>'floor_strike', '')::double precision,
  nullif(market->>'cap_strike', '')::double precision,
  nullif(market->>'functional_strike', '')::double precision,
  market->>'custom_strike',
  market->>'status',
  market->>'result'
from payload, jsonb_array_elements(doc->'markets') as market
on conflict (ticker) do update set
  event_ticker = excluded.event_ticker,
  series_ticker = excluded.series_ticker,
  title = excluded.title,
  subtitle = excluded.subtitle,
  market_type = excluded.market_type,
  strike_type = excluded.strike_type,
  floor_strike = excluded.floor_strike,
  cap_strike = excluded.cap_strike,
  functional_strike = excluded.functional_strike,
  custom_strike = excluded.custom_strike,
  status = excluded.status,
  result = excluded.result,
  updated_at = now();

with payload as (
  select doc from _codex_ingest_payload
)
insert into app.kalshi_market_snapshots (
  pulled_at, ticker, event_ticker, yes_bid_dollars, yes_ask_dollars, no_bid_dollars, no_ask_dollars,
  last_price_dollars, implied_probability, volume, volume_24h, state_hash
)
select
  nullif(market->>'pulled_at', '')::timestamptz,
  market->>'ticker',
  market->>'event_ticker',
  nullif(market->>'yes_bid_dollars', '')::numeric,
  nullif(market->>'yes_ask_dollars', '')::numeric,
  nullif(market->>'no_bid_dollars', '')::numeric,
  nullif(market->>'no_ask_dollars', '')::numeric,
  nullif(market->>'last_price_dollars', '')::numeric,
  nullif(market->>'implied_probability', '')::numeric,
  nullif(market->>'volume', '')::numeric,
  nullif(market->>'volume_24h', '')::numeric,
  md5(market::text)
from payload, jsonb_array_elements(doc->'markets') as market
on conflict (ticker, pulled_at) do update set
  yes_bid_dollars = excluded.yes_bid_dollars,
  yes_ask_dollars = excluded.yes_ask_dollars,
  no_bid_dollars = excluded.no_bid_dollars,
  no_ask_dollars = excluded.no_ask_dollars,
  last_price_dollars = excluded.last_price_dollars,
  implied_probability = excluded.implied_probability,
  volume = excluded.volume,
  volume_24h = excluded.volume_24h,
  state_hash = excluded.state_hash;
"""


WEATHER_SQL = r"""
with payload as (
  select doc from _codex_ingest_payload
),
rows as (
  select value as snapshot
  from payload, jsonb_array_elements(doc) as value
),
inserted as (
  insert into app.weather_snapshots (
    provider, pulled_at, market_id, location, latitude, longitude, timezone,
    current_json, units_json, provider_meta_json, state_hash
  )
  select
    snapshot->>'provider',
    nullif(snapshot->>'pulled_at', '')::timestamptz,
    snapshot->'market'->>'market_id',
    snapshot->'market'->>'location',
    nullif(snapshot->'market'->>'latitude', '')::double precision,
    nullif(snapshot->'market'->>'longitude', '')::double precision,
    snapshot->'market'->>'timezone',
    snapshot->'current',
    snapshot->'units',
    case
      when snapshot ? 'provider_meta' then snapshot->'provider_meta'
      when snapshot ? 'noaa' then snapshot->'noaa'
      else null
    end,
    md5(snapshot::text)
  from rows
  on conflict (provider, market_id, pulled_at) do update set
    current_json = excluded.current_json,
    units_json = excluded.units_json,
    provider_meta_json = excluded.provider_meta_json,
    state_hash = excluded.state_hash
  returning id, provider, market_id, pulled_at
)
insert into app.weather_hourly_forecasts (
  snapshot_id, forecast_time, temperature_2m, dew_point_2m, relative_humidity_2m, pressure_msl,
  precipitation, cloud_cover, wind_speed_10m, wind_gusts_10m, probability_of_precipitation, short_forecast
)
select
  ws.id,
  nullif(hourly->>'time', '')::timestamptz,
  nullif(hourly->>'temperature_2m', '')::double precision,
  nullif(hourly->>'dew_point_2m', '')::double precision,
  nullif(hourly->>'relative_humidity_2m', '')::double precision,
  nullif(hourly->>'pressure_msl', '')::double precision,
  nullif(hourly->>'precipitation', '')::double precision,
  nullif(hourly->>'cloud_cover', '')::double precision,
  nullif(hourly->>'wind_speed_10m', '')::double precision,
  nullif(hourly->>'wind_gusts_10m', '')::double precision,
  nullif(hourly->>'probability_of_precipitation', '')::double precision,
  hourly->>'short_forecast'
from rows r
join app.weather_snapshots ws
  on ws.provider = r.snapshot->>'provider'
 and ws.market_id = r.snapshot->'market'->>'market_id'
 and ws.pulled_at = nullif(r.snapshot->>'pulled_at', '')::timestamptz
cross join jsonb_array_elements(coalesce(r.snapshot->'hourly', '[]'::jsonb)) as hourly
on conflict (snapshot_id, forecast_time) do update set
  temperature_2m = excluded.temperature_2m,
  dew_point_2m = excluded.dew_point_2m,
  relative_humidity_2m = excluded.relative_humidity_2m,
  pressure_msl = excluded.pressure_msl,
  precipitation = excluded.precipitation,
  cloud_cover = excluded.cloud_cover,
  wind_speed_10m = excluded.wind_speed_10m,
  wind_gusts_10m = excluded.wind_gusts_10m,
  probability_of_precipitation = excluded.probability_of_precipitation,
  short_forecast = excluded.short_forecast;

with payload as (
  select doc from _codex_ingest_payload
),
rows as (
  select value as snapshot
  from payload, jsonb_array_elements(doc) as value
)
insert into app.weather_daily_forecasts (
  snapshot_id, forecast_date, temperature_2m_max, temperature_2m_min, precipitation_sum,
  wind_speed_10m_max, wind_gusts_10m_max
)
select
  ws.id,
  nullif(daily->>'date', '')::date,
  nullif(daily->>'temperature_2m_max', '')::double precision,
  nullif(daily->>'temperature_2m_min', '')::double precision,
  nullif(daily->>'precipitation_sum', '')::double precision,
  nullif(daily->>'wind_speed_10m_max', '')::double precision,
  nullif(daily->>'wind_gusts_10m_max', '')::double precision
from rows r
join app.weather_snapshots ws
  on ws.provider = r.snapshot->>'provider'
 and ws.market_id = r.snapshot->'market'->>'market_id'
 and ws.pulled_at = nullif(r.snapshot->>'pulled_at', '')::timestamptz
cross join jsonb_array_elements(coalesce(r.snapshot->'daily', '[]'::jsonb)) as daily
on conflict (snapshot_id, forecast_date) do update set
  temperature_2m_max = excluded.temperature_2m_max,
  temperature_2m_min = excluded.temperature_2m_min,
  precipitation_sum = excluded.precipitation_sum,
  wind_speed_10m_max = excluded.wind_speed_10m_max,
  wind_gusts_10m_max = excluded.wind_gusts_10m_max;
"""


SCORED_SQL = r"""
with payload as (
  select doc from _codex_ingest_payload
)
insert into app.scored_market_snapshots (
  pulled_at, ticker, event_ticker, market_id, forecast_date, matched_location, matched_latitude, matched_longitude,
  forecast_max_f, forecast_min_f, adjusted_forecast_max_f, forecast_sigma_f, noaa_forecast_max_f,
  open_meteo_forecast_max_f, forecast_source_spread_f, lead_bucket, model_probability, edge,
  signal_short, market_context, model_signal, weather_market_id, scoring_version, state_hash
)
select
  nullif(market->>'pulled_at', '')::timestamptz,
  market->>'ticker',
  market->>'event_ticker',
  market->>'weather_market_id',
  nullif(market->>'forecast_date', '')::date,
  market->>'matched_location',
  nullif(market->>'matched_latitude', '')::double precision,
  nullif(market->>'matched_longitude', '')::double precision,
  nullif(market->>'forecast_max_f', '')::double precision,
  nullif(market->>'forecast_min_f', '')::double precision,
  nullif(market->>'adjusted_forecast_max_f', '')::double precision,
  nullif(market->>'forecast_sigma_f', '')::double precision,
  nullif(market->>'noaa_forecast_max_f', '')::double precision,
  nullif(market->>'open_meteo_forecast_max_f', '')::double precision,
  nullif(market->>'forecast_source_spread_f', '')::double precision,
  market->>'lead_bucket',
  nullif(market->>'model_probability', '')::numeric,
  nullif(market->>'edge', '')::numeric,
  market->>'signal_short',
  market->>'market_context',
  market->>'model_signal',
  market->>'weather_market_id',
  coalesce(doc->>'source', 'temperature-scorer-v1'),
  md5(market::text)
from payload, jsonb_array_elements(doc->'markets') as market
on conflict (ticker, pulled_at) do update set
  market_id = excluded.market_id,
  forecast_date = excluded.forecast_date,
  matched_location = excluded.matched_location,
  matched_latitude = excluded.matched_latitude,
  matched_longitude = excluded.matched_longitude,
  forecast_max_f = excluded.forecast_max_f,
  forecast_min_f = excluded.forecast_min_f,
  adjusted_forecast_max_f = excluded.adjusted_forecast_max_f,
  forecast_sigma_f = excluded.forecast_sigma_f,
  noaa_forecast_max_f = excluded.noaa_forecast_max_f,
  open_meteo_forecast_max_f = excluded.open_meteo_forecast_max_f,
  forecast_source_spread_f = excluded.forecast_source_spread_f,
  lead_bucket = excluded.lead_bucket,
  model_probability = excluded.model_probability,
  edge = excluded.edge,
  signal_short = excluded.signal_short,
  market_context = excluded.market_context,
  model_signal = excluded.model_signal,
  weather_market_id = excluded.weather_market_id,
  scoring_version = excluded.scoring_version,
  state_hash = excluded.state_hash;
"""


BETS_SQL = r"""
with payload as (
  select doc from _codex_ingest_payload
)
insert into app.daily_bet_runs (
  target_date, generated_at, starting_bankroll, risk_pct_per_city, stake_per_city_dollars, bet_count
)
select
  nullif(doc->>'target_date', '')::date,
  nullif(doc->>'generated_at', '')::timestamptz,
  nullif(doc->>'starting_bankroll', '')::numeric,
  nullif(doc->>'risk_pct_per_city', '')::numeric,
  nullif(doc->>'stake_per_city_dollars', '')::numeric,
  nullif(doc->>'bet_count', '')::integer
from payload
on conflict (target_date) do update set
  generated_at = excluded.generated_at,
  starting_bankroll = excluded.starting_bankroll,
  risk_pct_per_city = excluded.risk_pct_per_city,
  stake_per_city_dollars = excluded.stake_per_city_dollars,
  bet_count = excluded.bet_count;

with payload as (
  select doc from _codex_ingest_payload
)
insert into app.daily_bets (
  target_date, logged_at, weather_market_id, city, series_ticker, event_ticker, ticker, title,
  strike_type, floor_strike, cap_strike, forecast_date, lead_bucket, forecast_max_f, adjusted_forecast_max_f,
  forecast_sigma_f, recommended_side, model_win_probability, yes_probability, kalshi_yes_probability,
  contract_cost, expected_value, expected_return, bankroll_at_bet, risk_pct_of_bankroll, stake_dollars,
  contract_count, max_profit_dollars, expected_value_dollars, yes_ask_dollars, no_ask_dollars, volume,
  close_time, signal, status, resolved_at, observed_high_f, contract_yes_outcome, bet_won, pnl_per_contract, pnl_dollars
)
select
  nullif(doc->>'target_date', '')::date,
  nullif(bet->>'logged_at', '')::timestamptz,
  bet->>'weather_market_id',
  bet->>'city',
  bet->>'series_ticker',
  bet->>'event_ticker',
  bet->>'ticker',
  bet->>'title',
  bet->>'strike_type',
  nullif(bet->>'floor_strike', '')::double precision,
  nullif(bet->>'cap_strike', '')::double precision,
  nullif(bet->>'forecast_date', '')::date,
  bet->>'lead_bucket',
  nullif(bet->>'forecast_max_f', '')::double precision,
  nullif(bet->>'adjusted_forecast_max_f', '')::double precision,
  nullif(bet->>'forecast_sigma_f', '')::double precision,
  bet->>'recommended_side',
  nullif(bet->>'model_win_probability', '')::numeric,
  nullif(bet->>'yes_probability', '')::numeric,
  nullif(bet->>'kalshi_yes_probability', '')::numeric,
  nullif(bet->>'contract_cost', '')::numeric,
  nullif(bet->>'expected_value', '')::numeric,
  nullif(bet->>'expected_return', '')::numeric,
  nullif(bet->>'bankroll_at_bet', '')::numeric,
  nullif(bet->>'risk_pct_of_bankroll', '')::numeric,
  nullif(bet->>'stake_dollars', '')::numeric,
  nullif(bet->>'contract_count', '')::numeric,
  nullif(bet->>'max_profit_dollars', '')::numeric,
  nullif(bet->>'expected_value_dollars', '')::numeric,
  nullif(bet->>'yes_ask_dollars', '')::numeric,
  nullif(bet->>'no_ask_dollars', '')::numeric,
  nullif(bet->>'volume', '')::numeric,
  nullif(bet->>'close_time', '')::timestamptz,
  bet->>'signal',
  coalesce(bet->>'status', 'pending'),
  nullif(bet->>'resolved_at', '')::timestamptz,
  nullif(bet->>'observed_high_f', '')::double precision,
  case when bet ? 'contract_yes_outcome' then (bet->>'contract_yes_outcome')::boolean else null end,
  case when bet ? 'bet_won' then (bet->>'bet_won')::boolean else null end,
  nullif(bet->>'pnl_per_contract', '')::numeric,
  nullif(bet->>'pnl_dollars', '')::numeric
from payload, jsonb_array_elements(doc->'bets') as bet
on conflict (target_date, city, recommended_side) do update set
  logged_at = excluded.logged_at,
  weather_market_id = excluded.weather_market_id,
  series_ticker = excluded.series_ticker,
  event_ticker = excluded.event_ticker,
  ticker = excluded.ticker,
  title = excluded.title,
  strike_type = excluded.strike_type,
  floor_strike = excluded.floor_strike,
  cap_strike = excluded.cap_strike,
  forecast_date = excluded.forecast_date,
  lead_bucket = excluded.lead_bucket,
  forecast_max_f = excluded.forecast_max_f,
  adjusted_forecast_max_f = excluded.adjusted_forecast_max_f,
  forecast_sigma_f = excluded.forecast_sigma_f,
  recommended_side = excluded.recommended_side,
  model_win_probability = excluded.model_win_probability,
  yes_probability = excluded.yes_probability,
  kalshi_yes_probability = excluded.kalshi_yes_probability,
  contract_cost = excluded.contract_cost,
  expected_value = excluded.expected_value,
  expected_return = excluded.expected_return,
  bankroll_at_bet = excluded.bankroll_at_bet,
  risk_pct_of_bankroll = excluded.risk_pct_of_bankroll,
  stake_dollars = excluded.stake_dollars,
  contract_count = excluded.contract_count,
  max_profit_dollars = excluded.max_profit_dollars,
  expected_value_dollars = excluded.expected_value_dollars,
  yes_ask_dollars = excluded.yes_ask_dollars,
  no_ask_dollars = excluded.no_ask_dollars,
  volume = excluded.volume,
  close_time = excluded.close_time,
  signal = excluded.signal,
  status = excluded.status,
  resolved_at = excluded.resolved_at,
  observed_high_f = excluded.observed_high_f,
  contract_yes_outcome = excluded.contract_yes_outcome,
  bet_won = excluded.bet_won,
  pnl_per_contract = excluded.pnl_per_contract,
  pnl_dollars = excluded.pnl_dollars;
"""


BANKROLL_SQL = r"""
with payload as (
  select doc from _codex_ingest_payload
)
insert into app.bankroll_history (
  target_date, bankroll_start, bankroll_end, risk_pct_per_city, stake_per_city_dollars, bet_count,
  resolved_bet_count, pending_bet_count, wins, losses, total_staked_dollars, realized_pnl_dollars, roi_on_staked, generated_at
)
select
  nullif(day->>'date', '')::date,
  nullif(day->>'bankroll_start', '')::numeric,
  nullif(day->>'bankroll_end', '')::numeric,
  nullif(day->>'risk_pct_per_city', '')::numeric,
  nullif(day->>'stake_per_city_dollars', '')::numeric,
  nullif(day->>'bet_count', '')::integer,
  nullif(day->>'resolved_bet_count', '')::integer,
  nullif(day->>'pending_bet_count', '')::integer,
  nullif(day->>'wins', '')::integer,
  nullif(day->>'losses', '')::integer,
  nullif(day->>'total_staked_dollars', '')::numeric,
  nullif(day->>'realized_pnl_dollars', '')::numeric,
  nullif(day->>'roi_on_staked', '')::numeric,
  nullif(doc->>'generated_at', '')::timestamptz
from payload, jsonb_array_elements(doc->'days') as day
on conflict (target_date) do update set
  bankroll_start = excluded.bankroll_start,
  bankroll_end = excluded.bankroll_end,
  risk_pct_per_city = excluded.risk_pct_per_city,
  stake_per_city_dollars = excluded.stake_per_city_dollars,
  bet_count = excluded.bet_count,
  resolved_bet_count = excluded.resolved_bet_count,
  pending_bet_count = excluded.pending_bet_count,
  wins = excluded.wins,
  losses = excluded.losses,
  total_staked_dollars = excluded.total_staked_dollars,
  realized_pnl_dollars = excluded.realized_pnl_dollars,
  roi_on_staked = excluded.roi_on_staked,
  generated_at = excluded.generated_at;
"""


HISTORY_SQL = r"""
with payload as (
  select doc from _codex_ingest_payload
)
insert into app.daily_observations (
  market_id, observation_date, source_type, station_id, station_name, tmax_f, source_url, raw_text
)
select
  location->'market'->>'market_id',
  nullif(observation->>'date', '')::date,
  coalesce(nullif(observation->>'source_type', ''), 'ncei_ghcnd'),
  location->'station'->>'id',
  location->'station'->>'name',
  nullif(observation->>'tmax_f', '')::double precision,
  coalesce(
    nullif(observation->>'source_url', ''),
    'https://www.ncei.noaa.gov/cdo-web/webservices/v2'
  ),
  coalesce(observation->'raw_text', observation)
from payload, jsonb_array_elements(doc->'locations') as location
cross join jsonb_array_elements(coalesce(location->'observations', '[]'::jsonb)) as observation
on conflict (market_id, observation_date, source_type) do update set
  station_id = excluded.station_id,
  station_name = excluded.station_name,
  tmax_f = excluded.tmax_f,
  source_url = excluded.source_url,
  raw_text = excluded.raw_text,
  updated_at = now();
"""


PRELIMINARY_SQL = r"""
with payload as (
  select doc from _codex_ingest_payload
)
insert into app.daily_observations (
  market_id, observation_date, source_type, station_id, station_name, tmax_f, source_url, raw_text
)
select
  row->>'market_id',
  nullif(row->>'forecast_date', '')::date,
  'preliminary_intraday_max',
  null,
  case
    when jsonb_array_length(coalesce(row->'stations', '[]'::jsonb)) > 0
      then array_to_string(array(select jsonb_array_elements_text(row->'stations')), ', ')
    else null
  end,
  nullif(row->>'preliminary_high_f', '')::double precision,
  nullif(row->>'provider', ''),
  row
from payload, jsonb_array_elements(coalesce(doc->'rows', '[]'::jsonb)) as row
where row ? 'market_id'
  and row ? 'forecast_date'
  and row ? 'preliminary_high_f'
on conflict (market_id, observation_date, source_type) do update set
  station_id = excluded.station_id,
  station_name = excluded.station_name,
  tmax_f = excluded.tmax_f,
  source_url = excluded.source_url,
  raw_text = excluded.raw_text,
  updated_at = now();
"""


def resolve_connection(database_url: str | None = None) -> tuple[str, str]:
    psql = require_psql()
    resolved_url = require_database_url(database_url)
    return psql, resolved_url


def sync_reference_tables(psql: str, database_url: str) -> None:
    upsert_reference_tables(psql, database_url)


def sync_kalshi_payload(psql: str, database_url: str, payload: Any) -> None:
    run_json_ingest(psql, database_url, KALSHI_SQL, payload)


def sync_weather_payload(psql: str, database_url: str, payload: Any) -> None:
    run_json_ingest(psql, database_url, WEATHER_SQL, payload)


def sync_scored_payload(psql: str, database_url: str, payload: Any) -> None:
    run_json_ingest(psql, database_url, SCORED_SQL, payload)


def sync_bets_payload(psql: str, database_url: str, payload: Any) -> None:
    run_json_ingest(psql, database_url, BETS_SQL, payload)


def sync_bankroll_payload(psql: str, database_url: str, payload: Any) -> None:
    run_json_ingest(psql, database_url, BANKROLL_SQL, payload)


def sync_history_payload(psql: str, database_url: str, payload: Any) -> None:
    run_json_ingest(psql, database_url, HISTORY_SQL, payload)


def sync_preliminary_payload(psql: str, database_url: str, payload: Any) -> None:
    run_json_ingest(psql, database_url, PRELIMINARY_SQL, payload)


def sync_all_default_artifacts(psql: str, database_url: str, *, apply_schema: bool = True) -> None:
    if apply_schema:
        run_sql_file(psql, database_url, DB_SCHEMA_PATH)

    sync_reference_tables(psql, database_url)
    sync_kalshi_payload(psql, database_url, load_json(KALSHI_PATH))
    sync_weather_payload(psql, database_url, load_json(WEATHER_NOAA_PATH))
    sync_weather_payload(psql, database_url, load_json(WEATHER_OPEN_METEO_PATH))
    sync_history_payload(psql, database_url, load_json(HISTORY_PATH))
    if PRELIMINARY_PATH.exists():
        sync_preliminary_payload(psql, database_url, load_json(PRELIMINARY_PATH))
    sync_scored_payload(psql, database_url, load_json(SCORED_PATH))
    sync_bets_payload(psql, database_url, load_json(BETS_PATH))
    sync_bankroll_payload(psql, database_url, load_json(BANKROLL_PATH))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", help="Postgres connection string. Falls back to DATABASE_URL.")
    parser.add_argument(
        "--skip-schema",
        action="store_true",
        help="Skip applying db/schema.sql before loading data.",
    )
    args = parser.parse_args()

    try:
        psql, database_url = resolve_connection(args.database_url)
    except RuntimeError as error:
        print(str(error), file=sys.stderr)
        return 1

    try:
        sync_all_default_artifacts(psql, database_url, apply_schema=not args.skip_schema)
    except subprocess.CalledProcessError as error:
        print(f"psql command failed with exit code {error.returncode}", file=sys.stderr)
        return error.returncode
    except FileNotFoundError as error:
        print(f"Missing required file: {error}", file=sys.stderr)
        return 1

    print("Loaded current JSON artifacts into Postgres")
    print("Applied schema, reference tables, Kalshi snapshots, weather snapshots, NOAA history, scored markets, bets, and bankroll history")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
