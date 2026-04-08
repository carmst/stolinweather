#!/usr/bin/env python3
"""Compact older Postgres snapshot data into daily rollups and prune raw rows."""

from __future__ import annotations

import argparse
import subprocess
import sys

from load_json_to_postgres import DB_SCHEMA_PATH, resolve_connection, run_sql, run_sql_file


ROLLUP_SQL_TEMPLATE = r"""
begin;

with cutoff as (
  select (current_date - interval '{retention_days} days')::date as retention_cutoff
),
source_rows as (
  select
    date(kms.pulled_at at time zone 'UTC') as summary_date,
    kms.ticker,
    kms.event_ticker,
    kms.pulled_at,
    kms.yes_bid_dollars,
    kms.yes_ask_dollars,
    kms.no_bid_dollars,
    kms.no_ask_dollars,
    kms.last_price_dollars,
    kms.implied_probability,
    kms.volume,
    kms.volume_24h
  from app.kalshi_market_snapshots kms
  join cutoff c on date(kms.pulled_at at time zone 'UTC') < c.retention_cutoff
),
aggregated as (
  select
    summary_date,
    ticker,
    min(event_ticker) as event_ticker,
    count(*)::integer as snapshot_count,
    min(pulled_at) as first_pulled_at,
    max(pulled_at) as last_pulled_at
  from source_rows
  group by summary_date, ticker
),
last_row as (
  select distinct on (summary_date, ticker)
    summary_date,
    ticker,
    event_ticker,
    yes_bid_dollars,
    yes_ask_dollars,
    no_bid_dollars,
    no_ask_dollars,
    last_price_dollars,
    implied_probability,
    volume,
    volume_24h
  from source_rows
  order by summary_date, ticker, pulled_at desc
)
insert into app.kalshi_market_daily_rollups (
  summary_date, ticker, event_ticker, snapshot_count, first_pulled_at, last_pulled_at,
  last_yes_bid_dollars, last_yes_ask_dollars, last_no_bid_dollars, last_no_ask_dollars,
  last_last_price_dollars, last_implied_probability, last_volume, last_volume_24h
)
select
  a.summary_date,
  a.ticker,
  a.event_ticker,
  a.snapshot_count,
  a.first_pulled_at,
  a.last_pulled_at,
  l.yes_bid_dollars,
  l.yes_ask_dollars,
  l.no_bid_dollars,
  l.no_ask_dollars,
  l.last_price_dollars,
  l.implied_probability,
  l.volume,
  l.volume_24h
from aggregated a
join last_row l using (summary_date, ticker)
on conflict (summary_date, ticker) do update set
  event_ticker = excluded.event_ticker,
  snapshot_count = excluded.snapshot_count,
  first_pulled_at = excluded.first_pulled_at,
  last_pulled_at = excluded.last_pulled_at,
  last_yes_bid_dollars = excluded.last_yes_bid_dollars,
  last_yes_ask_dollars = excluded.last_yes_ask_dollars,
  last_no_bid_dollars = excluded.last_no_bid_dollars,
  last_no_ask_dollars = excluded.last_no_ask_dollars,
  last_last_price_dollars = excluded.last_last_price_dollars,
  last_implied_probability = excluded.last_implied_probability,
  last_volume = excluded.last_volume,
  last_volume_24h = excluded.last_volume_24h,
  updated_at = now();

with cutoff as (
  select (current_date - interval '{retention_days} days')::date as retention_cutoff
),
daily_extremes as (
  select
    ws.provider,
    ws.market_id,
    date(ws.pulled_at at time zone 'UTC') as summary_date,
    max(wdf.temperature_2m_max) as max_daily_temperature_2m_max,
    min(wdf.temperature_2m_min) as min_daily_temperature_2m_min
  from app.weather_snapshots ws
  join cutoff c on date(ws.pulled_at at time zone 'UTC') < c.retention_cutoff
  left join app.weather_daily_forecasts wdf on wdf.snapshot_id = ws.id
  group by ws.provider, ws.market_id, date(ws.pulled_at at time zone 'UTC')
),
source_rows as (
  select
    ws.provider,
    ws.market_id,
    date(ws.pulled_at at time zone 'UTC') as summary_date,
    ws.pulled_at,
    ws.current_json
  from app.weather_snapshots ws
  join cutoff c on date(ws.pulled_at at time zone 'UTC') < c.retention_cutoff
),
aggregated as (
  select
    provider,
    market_id,
    summary_date,
    count(*)::integer as snapshot_count,
    min(pulled_at) as first_pulled_at,
    max(pulled_at) as last_pulled_at
  from source_rows
  group by provider, market_id, summary_date
),
last_row as (
  select distinct on (provider, market_id, summary_date)
    provider,
    market_id,
    summary_date,
    current_json
  from source_rows
  order by provider, market_id, summary_date, pulled_at desc
)
insert into app.weather_snapshot_daily_rollups (
  summary_date, provider, market_id, snapshot_count, first_pulled_at, last_pulled_at,
  last_temperature_2m, last_dew_point_2m, last_relative_humidity_2m, last_pressure_msl,
  last_precipitation, last_wind_speed_10m, last_wind_gusts_10m, max_daily_temperature_2m_max,
  min_daily_temperature_2m_min
)
select
  a.summary_date,
  a.provider,
  a.market_id,
  a.snapshot_count,
  a.first_pulled_at,
  a.last_pulled_at,
  nullif(l.current_json->>'temperature_2m', '')::double precision,
  nullif(l.current_json->>'dew_point_2m', '')::double precision,
  nullif(l.current_json->>'relative_humidity_2m', '')::double precision,
  nullif(l.current_json->>'pressure_msl', '')::double precision,
  nullif(l.current_json->>'precipitation', '')::double precision,
  nullif(l.current_json->>'wind_speed_10m', '')::double precision,
  nullif(l.current_json->>'wind_gusts_10m', '')::double precision,
  d.max_daily_temperature_2m_max,
  d.min_daily_temperature_2m_min
from aggregated a
join last_row l using (provider, market_id, summary_date)
left join daily_extremes d using (provider, market_id, summary_date)
on conflict (summary_date, provider, market_id) do update set
  snapshot_count = excluded.snapshot_count,
  first_pulled_at = excluded.first_pulled_at,
  last_pulled_at = excluded.last_pulled_at,
  last_temperature_2m = excluded.last_temperature_2m,
  last_dew_point_2m = excluded.last_dew_point_2m,
  last_relative_humidity_2m = excluded.last_relative_humidity_2m,
  last_pressure_msl = excluded.last_pressure_msl,
  last_precipitation = excluded.last_precipitation,
  last_wind_speed_10m = excluded.last_wind_speed_10m,
  last_wind_gusts_10m = excluded.last_wind_gusts_10m,
  max_daily_temperature_2m_max = excluded.max_daily_temperature_2m_max,
  min_daily_temperature_2m_min = excluded.min_daily_temperature_2m_min,
  updated_at = now();

with cutoff as (
  select (current_date - interval '{retention_days} days')::date as retention_cutoff
),
source_rows as (
  select
    date(sms.pulled_at at time zone 'UTC') as summary_date,
    sms.ticker,
    sms.event_ticker,
    sms.pulled_at,
    sms.forecast_date,
    sms.lead_bucket,
    sms.adjusted_forecast_max_f,
    sms.forecast_sigma_f,
    sms.model_probability,
    sms.edge
  from app.scored_market_snapshots sms
  join cutoff c on date(sms.pulled_at at time zone 'UTC') < c.retention_cutoff
),
aggregated as (
  select
    summary_date,
    ticker,
    min(event_ticker) as event_ticker,
    count(*)::integer as snapshot_count,
    min(pulled_at) as first_pulled_at,
    max(pulled_at) as last_pulled_at
  from source_rows
  group by summary_date, ticker
),
last_row as (
  select distinct on (summary_date, ticker)
    summary_date,
    ticker,
    event_ticker,
    forecast_date,
    lead_bucket,
    adjusted_forecast_max_f,
    forecast_sigma_f,
    model_probability,
    edge
  from source_rows
  order by summary_date, ticker, pulled_at desc
)
insert into app.scored_market_daily_rollups (
  summary_date, ticker, event_ticker, snapshot_count, first_pulled_at, last_pulled_at,
  forecast_date, lead_bucket, last_adjusted_forecast_max_f, last_forecast_sigma_f,
  last_model_probability, last_edge
)
select
  a.summary_date,
  a.ticker,
  a.event_ticker,
  a.snapshot_count,
  a.first_pulled_at,
  a.last_pulled_at,
  l.forecast_date,
  l.lead_bucket,
  l.adjusted_forecast_max_f,
  l.forecast_sigma_f,
  l.model_probability,
  l.edge
from aggregated a
join last_row l using (summary_date, ticker)
on conflict (summary_date, ticker) do update set
  event_ticker = excluded.event_ticker,
  snapshot_count = excluded.snapshot_count,
  first_pulled_at = excluded.first_pulled_at,
  last_pulled_at = excluded.last_pulled_at,
  forecast_date = excluded.forecast_date,
  lead_bucket = excluded.lead_bucket,
  last_adjusted_forecast_max_f = excluded.last_adjusted_forecast_max_f,
  last_forecast_sigma_f = excluded.last_forecast_sigma_f,
  last_model_probability = excluded.last_model_probability,
  last_edge = excluded.last_edge,
  updated_at = now();

delete from app.changed_entities
where processed_at is not null
  and detected_at < now() - interval '{processed_days} days';

delete from app.scored_market_snapshots
where pulled_at < now() - interval '{retention_days} days';

delete from app.kalshi_market_snapshots
where pulled_at < now() - interval '{retention_days} days';

delete from app.weather_snapshots
where pulled_at < now() - interval '{retention_days} days';

commit;
"""


def build_sql(retention_days: int, processed_days: int) -> str:
    return ROLLUP_SQL_TEMPLATE.format(
        retention_days=int(retention_days),
        processed_days=int(processed_days),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", help="Postgres connection string. Falls back to DATABASE_URL.")
    parser.add_argument(
        "--retention-days",
        type=int,
        default=14,
        help="Keep raw kalshi/weather/scored snapshots newer than this many days.",
    )
    parser.add_argument(
        "--processed-change-retention-days",
        type=int,
        default=7,
        help="Keep processed work queue rows newer than this many days.",
    )
    parser.add_argument(
        "--init-db-schema",
        action="store_true",
        help="Apply db/schema.sql before compacting and pruning.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.retention_days < 1:
        print("--retention-days must be at least 1", file=sys.stderr)
        return 1

    try:
        psql, database_url = resolve_connection(args.database_url)
        if args.init_db_schema:
            run_sql_file(psql, database_url, DB_SCHEMA_PATH)
        run_sql(psql, database_url, build_sql(args.retention_days, args.processed_change_retention_days))
    except (RuntimeError, subprocess.CalledProcessError) as error:
        print(f"Postgres retention error: {error}", file=sys.stderr)
        return 1

    print(f"Compacted old snapshots into daily rollups and pruned raw data older than {args.retention_days} days")
    print(f"Processed change records older than {args.processed_change_retention_days} days were also pruned")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
