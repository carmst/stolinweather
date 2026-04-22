-- Core schema for the weather / Kalshi pipeline.
-- Designed for Postgres/Supabase/Neon.

create extension if not exists pgcrypto;

create schema if not exists app;

-- Reference data

create table if not exists app.market_locations (
  market_id text primary key,
  city text not null,
  contract text not null,
  event_type text not null,
  latitude double precision not null,
  longitude double precision not null,
  timezone text not null,
  kalshi_series text[] not null default '{}',
  location_aliases text[] not null default '{}',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists app.settlement_sources (
  series_ticker text primary key,
  market_id text not null references app.market_locations(market_id),
  city text not null,
  kalshi_market_title text not null,
  status text not null default 'todo',
  kalshi_source_label text,
  kalshi_source_url text,
  settlement_station_name text,
  settlement_station_id text,
  settlement_station_latitude double precision,
  settlement_station_longitude double precision,
  settlement_dataset text,
  settlement_product text,
  notes text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Kalshi entities

create table if not exists app.kalshi_events (
  event_ticker text primary key,
  series_ticker text not null,
  series_title text,
  series_slug text,
  event_date date,
  close_time timestamptz,
  open_time timestamptz,
  status text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists app.kalshi_markets (
  ticker text primary key,
  event_ticker text not null references app.kalshi_events(event_ticker),
  series_ticker text not null,
  title text not null,
  subtitle text,
  market_type text,
  strike_type text,
  floor_strike double precision,
  cap_strike double precision,
  functional_strike double precision,
  custom_strike text,
  status text,
  result text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists app.kalshi_market_snapshots (
  id uuid primary key default gen_random_uuid(),
  pulled_at timestamptz not null,
  ticker text not null references app.kalshi_markets(ticker),
  event_ticker text not null references app.kalshi_events(event_ticker),
  yes_bid_dollars numeric(10,4),
  yes_ask_dollars numeric(10,4),
  no_bid_dollars numeric(10,4),
  no_ask_dollars numeric(10,4),
  last_price_dollars numeric(10,4),
  implied_probability numeric(10,4),
  volume numeric(18,4),
  volume_24h numeric(18,4),
  state_hash text not null,
  inserted_at timestamptz not null default now(),
  unique (ticker, pulled_at)
);

create index if not exists kalshi_market_snapshots_ticker_pulled_at_idx
  on app.kalshi_market_snapshots (ticker, pulled_at desc);

create index if not exists kalshi_market_snapshots_pulled_at_idx
  on app.kalshi_market_snapshots (pulled_at desc);

-- Weather snapshots

create table if not exists app.weather_snapshots (
  id uuid primary key default gen_random_uuid(),
  provider text not null,
  pulled_at timestamptz not null,
  market_id text not null references app.market_locations(market_id),
  location text not null,
  latitude double precision not null,
  longitude double precision not null,
  timezone text not null,
  current_json jsonb not null,
  units_json jsonb not null,
  provider_meta_json jsonb,
  state_hash text not null,
  inserted_at timestamptz not null default now(),
  unique (provider, market_id, pulled_at)
);

create index if not exists weather_snapshots_provider_market_pulled_at_idx
  on app.weather_snapshots (provider, market_id, pulled_at desc);

create table if not exists app.weather_hourly_forecasts (
  snapshot_id uuid not null references app.weather_snapshots(id) on delete cascade,
  forecast_time timestamptz not null,
  temperature_2m double precision,
  dew_point_2m double precision,
  relative_humidity_2m double precision,
  pressure_msl double precision,
  precipitation double precision,
  cloud_cover double precision,
  wind_speed_10m double precision,
  wind_gusts_10m double precision,
  probability_of_precipitation double precision,
  short_forecast text,
  primary key (snapshot_id, forecast_time)
);

create table if not exists app.weather_daily_forecasts (
  snapshot_id uuid not null references app.weather_snapshots(id) on delete cascade,
  forecast_date date not null,
  temperature_2m_max double precision,
  temperature_2m_min double precision,
  precipitation_sum double precision,
  wind_speed_10m_max double precision,
  wind_gusts_10m_max double precision,
  primary key (snapshot_id, forecast_date)
);

-- Historical and settlement observations

create table if not exists app.daily_observations (
  id uuid primary key default gen_random_uuid(),
  market_id text not null references app.market_locations(market_id),
  observation_date date not null,
  source_type text not null, -- ncei_ghcnd, nws_cli, manual
  station_id text,
  station_name text,
  tmax_f double precision,
  source_url text,
  raw_text text,
  inserted_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (market_id, observation_date, source_type)
);

create index if not exists daily_observations_market_date_idx
  on app.daily_observations (market_id, observation_date desc);

-- Model scoring

create table if not exists app.scored_market_snapshots (
  id uuid primary key default gen_random_uuid(),
  pulled_at timestamptz not null,
  ticker text not null references app.kalshi_markets(ticker),
  event_ticker text not null references app.kalshi_events(event_ticker),
  market_id text references app.market_locations(market_id),
  forecast_date date,
  matched_location text,
  matched_latitude double precision,
  matched_longitude double precision,
  forecast_max_f double precision,
  forecast_min_f double precision,
  adjusted_forecast_max_f double precision,
  forecast_sigma_f double precision,
  noaa_forecast_max_f double precision,
  open_meteo_forecast_max_f double precision,
  forecast_source_spread_f double precision,
  lead_bucket text,
  model_probability numeric(10,4),
  edge numeric(10,4),
  signal_short text,
  market_context text,
  model_signal text,
  weather_market_id text,
  scoring_version text not null default 'temperature-scorer-v1',
  state_hash text not null,
  inserted_at timestamptz not null default now(),
  unique (ticker, pulled_at)
);

create index if not exists scored_market_snapshots_ticker_pulled_at_idx
  on app.scored_market_snapshots (ticker, pulled_at desc);

create index if not exists scored_market_snapshots_pulled_at_idx
  on app.scored_market_snapshots (pulled_at desc);

create index if not exists scored_market_snapshots_forecast_market_pulled_at_idx
  on app.scored_market_snapshots (forecast_date desc, market_id, weather_market_id, pulled_at desc);

create table if not exists app.latest_marketplace_contracts (
  ticker text primary key references app.kalshi_markets(ticker),
  pulled_at timestamptz not null,
  forecast_date date not null,
  adjusted_forecast_max_f double precision,
  forecast_sigma_f double precision,
  noaa_forecast_max_f double precision,
  open_meteo_forecast_max_f double precision,
  forecast_source_spread_f double precision,
  model_probability numeric(10,4),
  edge numeric(10,4),
  signal_short text,
  market_context text,
  model_signal text,
  lead_bucket text,
  title text not null,
  subtitle text,
  series_ticker text,
  floor_strike double precision,
  cap_strike double precision,
  functional_strike double precision,
  custom_strike text,
  market_status text,
  event_date date,
  close_time timestamptz,
  market_id text references app.market_locations(market_id),
  city text,
  event_type text,
  timezone text,
  price_pulled_at timestamptz,
  yes_bid_dollars numeric(10,4),
  yes_ask_dollars numeric(10,4),
  no_bid_dollars numeric(10,4),
  no_ask_dollars numeric(10,4),
  last_price_dollars numeric(10,4),
  implied_probability numeric(10,4),
  volume numeric(18,4),
  volume_24h numeric(18,4),
  search_text text not null default '',
  refreshed_at timestamptz not null default now()
);

create index if not exists latest_marketplace_contracts_date_sort_idx
  on app.latest_marketplace_contracts (forecast_date, city, floor_strike, ticker);

create index if not exists latest_marketplace_contracts_refreshed_at_idx
  on app.latest_marketplace_contracts (refreshed_at desc);

-- Compact rollups for long-term retention

create table if not exists app.kalshi_market_daily_rollups (
  summary_date date not null,
  ticker text not null references app.kalshi_markets(ticker),
  event_ticker text not null references app.kalshi_events(event_ticker),
  snapshot_count integer not null,
  first_pulled_at timestamptz not null,
  last_pulled_at timestamptz not null,
  last_yes_bid_dollars numeric(10,4),
  last_yes_ask_dollars numeric(10,4),
  last_no_bid_dollars numeric(10,4),
  last_no_ask_dollars numeric(10,4),
  last_last_price_dollars numeric(10,4),
  last_implied_probability numeric(10,4),
  last_volume numeric(18,4),
  last_volume_24h numeric(18,4),
  updated_at timestamptz not null default now(),
  primary key (summary_date, ticker)
);

create index if not exists kalshi_market_daily_rollups_ticker_date_idx
  on app.kalshi_market_daily_rollups (ticker, summary_date desc);

create table if not exists app.weather_snapshot_daily_rollups (
  summary_date date not null,
  provider text not null,
  market_id text not null references app.market_locations(market_id),
  snapshot_count integer not null,
  first_pulled_at timestamptz not null,
  last_pulled_at timestamptz not null,
  last_temperature_2m double precision,
  last_dew_point_2m double precision,
  last_relative_humidity_2m double precision,
  last_pressure_msl double precision,
  last_precipitation double precision,
  last_wind_speed_10m double precision,
  last_wind_gusts_10m double precision,
  max_daily_temperature_2m_max double precision,
  min_daily_temperature_2m_min double precision,
  updated_at timestamptz not null default now(),
  primary key (summary_date, provider, market_id)
);

create index if not exists weather_snapshot_daily_rollups_market_date_idx
  on app.weather_snapshot_daily_rollups (market_id, provider, summary_date desc);

create table if not exists app.scored_market_daily_rollups (
  summary_date date not null,
  ticker text not null references app.kalshi_markets(ticker),
  event_ticker text not null references app.kalshi_events(event_ticker),
  snapshot_count integer not null,
  first_pulled_at timestamptz not null,
  last_pulled_at timestamptz not null,
  forecast_date date,
  lead_bucket text,
  last_adjusted_forecast_max_f double precision,
  last_forecast_sigma_f double precision,
  last_model_probability numeric(10,4),
  last_edge numeric(10,4),
  updated_at timestamptz not null default now(),
  primary key (summary_date, ticker)
);

create index if not exists scored_market_daily_rollups_ticker_date_idx
  on app.scored_market_daily_rollups (ticker, summary_date desc);

-- Change detection / work queue

create table if not exists app.changed_entities (
  id uuid primary key default gen_random_uuid(),
  entity_type text not null, -- market, weather, score
  entity_key text not null,
  source text not null,
  detected_at timestamptz not null default now(),
  processed_at timestamptz,
  payload jsonb not null default '{}'::jsonb
);

create index if not exists changed_entities_unprocessed_idx
  on app.changed_entities (processed_at, detected_at)
  where processed_at is null;

-- Daily bets and bankroll

create table if not exists app.daily_bet_runs (
  target_date date primary key,
  generated_at timestamptz not null,
  starting_bankroll numeric(12,4) not null,
  risk_pct_per_city numeric(8,6) not null,
  stake_per_city_dollars numeric(12,4) not null,
  bet_count integer not null,
  created_at timestamptz not null default now()
);

create table if not exists app.daily_bets (
  id uuid primary key default gen_random_uuid(),
  target_date date not null references app.daily_bet_runs(target_date) on delete cascade,
  logged_at timestamptz not null,
  weather_market_id text references app.market_locations(market_id),
  city text not null,
  series_ticker text,
  event_ticker text,
  ticker text references app.kalshi_markets(ticker),
  title text not null,
  strike_type text,
  floor_strike double precision,
  cap_strike double precision,
  forecast_date date not null,
  lead_bucket text,
  forecast_max_f double precision,
  adjusted_forecast_max_f double precision,
  forecast_sigma_f double precision,
  recommended_side text not null,
  model_win_probability numeric(10,4) not null,
  yes_probability numeric(10,4) not null,
  kalshi_yes_probability numeric(10,4),
  contract_cost numeric(10,4),
  expected_value numeric(10,4),
  expected_return numeric(10,4),
  bankroll_at_bet numeric(12,4) not null,
  risk_pct_of_bankroll numeric(8,6) not null,
  stake_dollars numeric(12,4) not null,
  contract_count numeric(18,6),
  max_profit_dollars numeric(12,4),
  expected_value_dollars numeric(12,4),
  yes_ask_dollars numeric(10,4),
  no_ask_dollars numeric(10,4),
  volume numeric(18,4),
  close_time timestamptz,
  signal text,
  status text not null default 'pending',
  resolved_at timestamptz,
  observed_high_f double precision,
  contract_yes_outcome boolean,
  bet_won boolean,
  pnl_per_contract numeric(10,4),
  pnl_dollars numeric(12,4),
  inserted_at timestamptz not null default now(),
  unique (target_date, city, recommended_side)
);

create index if not exists daily_bets_target_date_idx
  on app.daily_bets (target_date desc);

alter table app.daily_bets
  drop constraint if exists daily_bets_target_date_city_key;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'app.daily_bets'::regclass
      and conname = 'daily_bets_target_date_city_recommended_side_key'
  ) then
    alter table app.daily_bets
      add constraint daily_bets_target_date_city_recommended_side_key
      unique (target_date, city, recommended_side);
  end if;
end $$;

create table if not exists app.kalshi_settlement_reconciliations (
  id uuid primary key default gen_random_uuid(),
  target_date date not null,
  reconciled_at timestamptz not null,
  weather_market_id text references app.market_locations(market_id),
  city text not null,
  ticker text not null references app.kalshi_markets(ticker),
  event_ticker text,
  recommended_side text not null,
  title text not null,
  observed_high_f double precision,
  computed_contract_yes_outcome boolean,
  kalshi_contract_yes_outcome boolean,
  kalshi_status text,
  kalshi_result text,
  kalshi_market_payload jsonb not null default '{}'::jsonb,
  alignment_status text not null,
  notes text,
  inserted_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (target_date, ticker, recommended_side)
);

create index if not exists kalshi_settlement_reconciliations_date_idx
  on app.kalshi_settlement_reconciliations (target_date desc);

create table if not exists app.bankroll_history (
  target_date date primary key,
  bankroll_start numeric(12,4) not null,
  bankroll_end numeric(12,4) not null,
  risk_pct_per_city numeric(8,6) not null,
  stake_per_city_dollars numeric(12,4) not null,
  bet_count integer not null,
  resolved_bet_count integer not null,
  pending_bet_count integer not null,
  wins integer not null,
  losses integer not null,
  total_staked_dollars numeric(12,4) not null,
  realized_pnl_dollars numeric(12,4) not null,
  roi_on_staked numeric(10,4),
  generated_at timestamptz not null default now()
);

-- Forecast error training rows

create table if not exists app.forecast_error_training_rows (
  id uuid primary key default gen_random_uuid(),
  market_id text not null references app.market_locations(market_id),
  location text not null,
  pulled_at timestamptz not null,
  forecast_date date not null,
  month integer not null,
  lead_hours numeric(10,2) not null,
  lead_bucket text not null,
  forecast_max_f double precision not null,
  observed_max_f double precision not null,
  error_f double precision not null,
  inserted_at timestamptz not null default now(),
  unique (market_id, pulled_at, forecast_date)
);

create index if not exists forecast_error_training_rows_market_date_idx
  on app.forecast_error_training_rows (market_id, forecast_date desc);
