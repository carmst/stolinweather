from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from django.core.cache import cache
from django.db import connections


logger = logging.getLogger(__name__)

HISTORY_DAYS = (7, 14, 30, 60, 90)
DEFAULT_HISTORY_DAYS = 30
HISTORY_CACHE_SECONDS = int(os.environ.get("DJANGO_HISTORY_CACHE_SECONDS", "300"))

HISTORY_SQL = """
with scored_source as (
  select
    coalesce(sms.market_id, sms.weather_market_id, ml_map.market_id) as market_id,
    sms.forecast_date,
    sms.pulled_at,
    sms.lead_bucket,
    sms.adjusted_forecast_max_f
  from app.scored_market_snapshots sms
  join app.kalshi_markets km on km.ticker = sms.ticker
  left join app.market_locations ml_map
    on km.series_ticker = any(ml_map.kalshi_series)
  where sms.forecast_date is not null
    and sms.adjusted_forecast_max_f is not null
    and sms.forecast_date >= current_date - (%s * interval '1 day')
    and (%s = '' or coalesce(sms.market_id, sms.weather_market_id, ml_map.market_id) = %s)
),
actuals as (
  select
    dobs.market_id,
    dobs.observation_date,
    coalesce(
      max(dobs.tmax_f) filter (where dobs.source_type <> 'preliminary_intraday_max'),
      max(dobs.tmax_f)
    ) as observed_max_f
  from app.daily_observations dobs
  group by dobs.market_id, dobs.observation_date
),
recent_rows as (
  select
    ss.market_id,
    ml.city,
    ml.latitude,
    ml.longitude,
    ss.forecast_date,
    ss.pulled_at,
    ss.lead_bucket,
    ss.adjusted_forecast_max_f as forecast_max_f,
    actuals.observed_max_f,
    ss.adjusted_forecast_max_f - actuals.observed_max_f as error_f,
    row_number() over (
      partition by ss.market_id, ss.forecast_date
      order by ss.pulled_at desc
    ) as rn
  from scored_source ss
  join app.market_locations ml on ml.market_id = ss.market_id
  join actuals on actuals.market_id = ss.market_id and actuals.observation_date = ss.forecast_date
  where ss.pulled_at <= make_timestamptz(
    extract(year from ss.forecast_date)::int,
    extract(month from ss.forecast_date)::int,
    extract(day from ss.forecast_date)::int,
    8,
    0,
    0,
    ml.timezone
  )
),
latest_rows as (
  select *
  from recent_rows
  where rn = 1
),
market_summary as (
  select
    market_id,
    city,
    latitude,
    longitude,
    count(*)::int as sample_count,
    avg(abs(error_f)) as mae_f,
    avg(error_f) as bias_f,
    max(forecast_date) as latest_date
  from latest_rows
  group by market_id, city, latitude, longitude
),
latest_market as (
  select distinct on (market_id)
    market_id,
    forecast_date,
    forecast_max_f,
    observed_max_f,
    error_f,
    lead_bucket
  from latest_rows
  order by market_id, forecast_date desc
)
select
  ms.market_id,
  ms.city,
  ms.latitude,
  ms.longitude,
  ms.sample_count,
  ms.mae_f,
  ms.bias_f,
  ms.latest_date,
  lm.forecast_max_f as latest_forecast_max_f,
  lm.observed_max_f as latest_observed_max_f,
  lm.error_f as latest_error_f,
  lm.lead_bucket as latest_lead_bucket
from market_summary ms
left join latest_market lm on lm.market_id = ms.market_id
order by ms.mae_f asc nulls last, ms.city asc;
"""

HISTORY_SERIES_SQL = """
with scored_source as (
  select
    coalesce(sms.market_id, sms.weather_market_id, ml_map.market_id) as market_id,
    sms.forecast_date,
    sms.pulled_at,
    sms.lead_bucket,
    sms.adjusted_forecast_max_f
  from app.scored_market_snapshots sms
  join app.kalshi_markets km on km.ticker = sms.ticker
  left join app.market_locations ml_map
    on km.series_ticker = any(ml_map.kalshi_series)
  where sms.forecast_date is not null
    and sms.adjusted_forecast_max_f is not null
    and sms.forecast_date >= current_date - (%s * interval '1 day')
    and coalesce(sms.market_id, sms.weather_market_id, ml_map.market_id) = %s
),
actuals as (
  select
    dobs.market_id,
    dobs.observation_date,
    coalesce(
      max(dobs.tmax_f) filter (where dobs.source_type <> 'preliminary_intraday_max'),
      max(dobs.tmax_f)
    ) as observed_max_f
  from app.daily_observations dobs
  group by dobs.market_id, dobs.observation_date
),
ranked_rows as (
  select
    ss.market_id,
    ml.city,
    ss.forecast_date,
    ss.pulled_at,
    ss.lead_bucket,
    ss.adjusted_forecast_max_f as forecast_max_f,
    actuals.observed_max_f,
    ss.adjusted_forecast_max_f - actuals.observed_max_f as error_f,
    row_number() over (
      partition by ss.market_id, ss.forecast_date
      order by ss.pulled_at desc
    ) as rn
  from scored_source ss
  join app.market_locations ml on ml.market_id = ss.market_id
  join actuals on actuals.market_id = ss.market_id and actuals.observation_date = ss.forecast_date
  where ss.pulled_at <= make_timestamptz(
    extract(year from ss.forecast_date)::int,
    extract(month from ss.forecast_date)::int,
    extract(day from ss.forecast_date)::int,
    8,
    0,
    0,
    ml.timezone
  )
)
select
  market_id,
  city,
  forecast_date,
  pulled_at,
  lead_bucket,
  forecast_max_f,
  observed_max_f,
  error_f
from ranked_rows
where rn = 1
order by forecast_date asc;
"""

HISTORY_AGGREGATE_SERIES_SQL = """
with scored_source as (
  select
    coalesce(sms.market_id, sms.weather_market_id, ml_map.market_id) as market_id,
    sms.forecast_date,
    sms.pulled_at,
    sms.lead_bucket,
    sms.adjusted_forecast_max_f
  from app.scored_market_snapshots sms
  join app.kalshi_markets km on km.ticker = sms.ticker
  left join app.market_locations ml_map
    on km.series_ticker = any(ml_map.kalshi_series)
  where sms.forecast_date is not null
    and sms.adjusted_forecast_max_f is not null
    and sms.forecast_date >= current_date - (%s * interval '1 day')
),
actuals as (
  select
    dobs.market_id,
    dobs.observation_date,
    coalesce(
      max(dobs.tmax_f) filter (where dobs.source_type <> 'preliminary_intraday_max'),
      max(dobs.tmax_f)
    ) as observed_max_f
  from app.daily_observations dobs
  group by dobs.market_id, dobs.observation_date
),
ranked_rows as (
  select
    ss.market_id,
    ss.forecast_date,
    ss.pulled_at,
    ss.lead_bucket,
    ss.adjusted_forecast_max_f as forecast_max_f,
    actuals.observed_max_f,
    ss.adjusted_forecast_max_f - actuals.observed_max_f as error_f,
    row_number() over (
      partition by ss.market_id, ss.forecast_date
      order by ss.pulled_at desc
    ) as rn
  from scored_source ss
  join app.market_locations ml on ml.market_id = ss.market_id
  join actuals on actuals.market_id = ss.market_id and actuals.observation_date = ss.forecast_date
  where ss.pulled_at <= make_timestamptz(
    extract(year from ss.forecast_date)::int,
    extract(month from ss.forecast_date)::int,
    extract(day from ss.forecast_date)::int,
    8,
    0,
    0,
    ml.timezone
  )
),
latest_rows as (
  select *
  from ranked_rows
  where rn = 1
)
select
  '' as market_id,
  'All Cities' as city,
  forecast_date,
  max(pulled_at) as pulled_at,
  count(*)::text || ' cities' as lead_bucket,
  avg(forecast_max_f) as forecast_max_f,
  avg(observed_max_f) as observed_max_f,
  avg(error_f) as error_f
from latest_rows
group by forecast_date
order by forecast_date asc;
"""


@dataclass(frozen=True)
class HistoryFilters:
    days: int = DEFAULT_HISTORY_DAYS
    market_id: str = ""


def get_history_context(filters: HistoryFilters) -> dict[str, Any]:
    started_at = time.perf_counter()
    days = filters.days if filters.days in HISTORY_DAYS else DEFAULT_HISTORY_DAYS
    selected_market_id = filters.market_id.strip()

    summary_rows = _cached_rows(
        f"markets:history:summary:{days}:all",
        HISTORY_SQL,
        params=(days, "", ""),
        log_label="history_summary_fetch",
    )
    markets = [_normalize_market(row) for row in summary_rows]

    if selected_market_id:
        series_rows = _cached_rows(
            f"markets:history:series:{days}:{selected_market_id}",
            HISTORY_SERIES_SQL,
            params=(days, selected_market_id),
            log_label="history_series_fetch",
        )
    else:
        series_rows = _cached_rows(
            f"markets:history:series:{days}:all",
            HISTORY_AGGREGATE_SERIES_SQL,
            params=(days,),
            log_label="history_aggregate_series_fetch",
        )

    selected_market = next((market for market in markets if market["market_id"] == selected_market_id), None)
    series = [_normalize_series_row(row) for row in series_rows]
    chart = _build_history_chart(series)
    log_rows = list(reversed(series[-12:]))
    aggregate_mae = _average([market["mae"] for market in markets])
    aggregate_bias = _average([market["bias"] for market in markets])
    positive_bias_count = sum(1 for market in markets if market["bias"] is not None and market["bias"] > 0)

    logger.info(
        "history_context days=%s selected_market_id=%s markets=%s series_rows=%s total_ms=%.1f",
        days,
        selected_market_id,
        len(markets),
        len(series),
        _elapsed_ms(started_at),
    )

    return {
        "days": days,
        "day_options": HISTORY_DAYS,
        "selected_market_id": selected_market_id,
        "selected_market": selected_market,
        "markets": markets,
        "series": series,
        "log_rows": log_rows,
        "chart": chart,
        "market_count": len(markets),
        "series_count": len(series),
        "aggregate_mae": aggregate_mae,
        "aggregate_mae_display": _format_temp(aggregate_mae),
        "aggregate_bias": aggregate_bias,
        "aggregate_bias_display": _format_signed_temp(aggregate_bias),
        "positive_bias_count": positive_bias_count,
        "updated_at": max((market["latest_date"] for market in markets if market["latest_date"]), default=None),
    }


def _fetch_rows(sql: str, *, params: tuple[Any, ...], log_label: str) -> list[dict[str, Any]]:
    started_at = time.perf_counter()
    with connections["weather"].cursor() as cursor:
        execute_started_at = time.perf_counter()
        cursor.execute(sql, params)
        execute_ms = _elapsed_ms(execute_started_at)
        columns = [column[0] for column in cursor.description]
        raw_rows = cursor.fetchall()

    rows = [dict(zip(columns, row)) for row in raw_rows]
    logger.info("%s rows=%s execute_ms=%.1f total_ms=%.1f", log_label, len(rows), execute_ms, _elapsed_ms(started_at))
    return rows


def _cached_rows(cache_key: str, sql: str, *, params: tuple[Any, ...], log_label: str) -> list[dict[str, Any]]:
    cached = cache.get(cache_key)
    if cached is not None:
        logger.info("%s cache_hit=true rows=%s", log_label, len(cached))
        return cached
    rows = _fetch_rows(sql, params=params, log_label=log_label)
    cache.set(cache_key, rows, HISTORY_CACHE_SECONDS)
    return rows


def _normalize_market(row: dict[str, Any]) -> dict[str, Any]:
    mae = _float_or_none(row.get("mae_f"))
    bias = _float_or_none(row.get("bias_f"))
    error = _float_or_none(row.get("latest_error_f"))
    return {
        **row,
        "market_id": row.get("market_id") or "",
        "city": row.get("city") or "Unknown",
        "map_x": _map_x(_float_or_none(row.get("longitude"))),
        "map_y": _map_y(_float_or_none(row.get("latitude"))),
        "sample_count": int(row.get("sample_count") or 0),
        "mae": mae,
        "bias": bias,
        "latest_error": error,
        "latest_date": row.get("latest_date"),
        "mae_display": _format_temp(mae),
        "bias_display": _format_signed_temp(bias),
        "latest_forecast_display": _format_temp(_float_or_none(row.get("latest_forecast_max_f"))),
        "latest_observed_display": _format_temp(_float_or_none(row.get("latest_observed_max_f"))),
        "latest_error_display": _format_signed_temp(error),
        "status": _status_for_error(error),
        "status_class": _status_class(error),
        "pulse_radius": _pulse_radius(mae),
    }


def _normalize_series_row(row: dict[str, Any]) -> dict[str, Any]:
    forecast = _float_or_none(row.get("forecast_max_f"))
    observed = _float_or_none(row.get("observed_max_f"))
    error = _float_or_none(row.get("error_f"))
    return {
        **row,
        "date_key": _date_key(row.get("forecast_date")),
        "forecast": forecast,
        "observed": observed,
        "error": error,
        "forecast_display": _format_temp(forecast),
        "observed_display": _format_temp(observed),
        "error_display": _format_signed_temp(error),
        "status": _status_for_error(error),
    }


def _build_history_chart(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"available": False, "message": "No historical rows matched this city and window yet."}

    values = [value for row in rows for value in (row["forecast"], row["observed"]) if value is not None]
    if not values:
        return {"available": False, "message": "Historical rows do not include model and observed highs yet."}

    width = 980
    height = 330
    padding = {"top": 26, "right": 28, "bottom": 42, "left": 56}
    min_temp = int(min(values) - 3)
    max_temp = int(max(values) + 3)
    if min_temp == max_temp:
        max_temp += 1

    def x_for(index: int) -> float:
        if len(rows) == 1:
            return width / 2
        return padding["left"] + (index / (len(rows) - 1)) * (width - padding["left"] - padding["right"])

    def y_for(value: float) -> float:
        return padding["top"] + ((max_temp - value) / (max_temp - min_temp)) * (height - padding["top"] - padding["bottom"])

    forecast_points = [(x_for(index), y_for(row["forecast"])) for index, row in enumerate(rows) if row["forecast"] is not None]
    observed_points = [(x_for(index), y_for(row["observed"])) for index, row in enumerate(rows) if row["observed"] is not None]
    y_ticks = [{"y": y_for(temp), "label": f"{temp}F"} for temp in _chart_ticks(min_temp, max_temp)]
    tick_indexes = sorted({0, len(rows) // 2, len(rows) - 1})
    x_ticks = [{"x": x_for(index), "label": rows[index]["date_key"][5:]} for index in tick_indexes if rows[index].get("date_key")]

    return {
        "available": True,
        "width": width,
        "height": height,
        "forecast_path": _path_for_points(forecast_points),
        "observed_path": _path_for_points(observed_points),
        "y_ticks": y_ticks,
        "x_ticks": x_ticks,
        "min_temp": min_temp,
        "max_temp": max_temp,
    }


def _path_for_points(points: list[tuple[float, float]]) -> str:
    return " ".join(f"{'M' if index == 0 else 'L'} {x:.2f} {y:.2f}" for index, (x, y) in enumerate(points))


def _chart_ticks(min_temp: int, max_temp: int) -> list[int]:
    step = max(1, round((max_temp - min_temp) / 4))
    ticks = list(range(min_temp, max_temp + 1, step))
    if ticks[-1] != max_temp:
        ticks.append(max_temp)
    return ticks


def _map_x(longitude: float | None) -> float:
    if longitude is None:
        return 50.0
    return max(6.0, min(94.0, ((longitude + 125.0) / 59.0) * 100.0))


def _map_y(latitude: float | None) -> float:
    if latitude is None:
        return 50.0
    return max(8.0, min(92.0, ((50.0 - latitude) / 26.0) * 100.0))


def _pulse_radius(mae: float | None) -> float:
    if mae is None:
        return 8.0
    return max(8.0, min(20.0, 7.0 + mae * 1.8))


def _status_for_error(error: float | None) -> str:
    if error is None:
        return "PENDING"
    if abs(error) <= 1.5:
        return "TIGHT"
    if error > 0:
        return "WARM"
    return "COOL"


def _status_class(error: float | None) -> str:
    if error is None:
        return "text-on-surface-variant border-outline-variant"
    if abs(error) <= 1.5:
        return "text-primary border-primary/40 bg-primary/10"
    if error > 0:
        return "text-secondary border-secondary/40 bg-secondary/10"
    return "text-sky-300 border-sky-300/40 bg-sky-400/10"


def _average(values: list[float | None]) -> float | None:
    usable = [value for value in values if value is not None]
    return sum(usable) / len(usable) if usable else None


def _date_key(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value or "")[:10]


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_temp(value: float | None) -> str:
    return "--" if value is None else f"{value:.1f}F"


def _format_signed_temp(value: float | None) -> str:
    if value is None:
        return "--"
    sign = "+" if value >= 0 else "-"
    return f"{sign}{abs(value):.1f}F"


def _elapsed_ms(started_at: float) -> float:
    return (time.perf_counter() - started_at) * 1000
