from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from django.db import connections
from django.utils import timezone


logger = logging.getLogger(__name__)
REPO_DIR = Path(__file__).resolve().parents[3]
WATCHLIST_CONFIG_PATH = REPO_DIR / "config" / "watchlist.json"
WATCHLIST_CONFIDENCE_THRESHOLD = 0.87
RESOLVED_BETS_DIR = REPO_DIR / "output" / "bets" / "resolved"
DEPLOY_EDGE_STRATEGY_PATH = REPO_DIR / "deploy_data" / "bets" / "latest_edge_strategy_summary.json"
EDGE_STRATEGY_STARTING_BANKROLL = 100.0
EDGE_STRATEGY_RISK_PCT = 0.01
EDGE_STRATEGY_LOCAL_TZ = ZoneInfo("America/New_York")
WEATHER_FORECAST_PATHS = [
    REPO_DIR / "output" / "weather" / "latest_forecasts_noaa.json",
    REPO_DIR / "output" / "weather" / "latest_forecasts.json",
    REPO_DIR / "output" / "weather" / "latest_forecasts_visual_crossing.json",
    REPO_DIR / "deploy_data" / "weather" / "latest_forecasts_noaa.json",
    REPO_DIR / "deploy_data" / "weather" / "latest_forecasts.json",
    REPO_DIR / "deploy_data" / "weather" / "latest_forecasts_visual_crossing.json",
]
SERIES_SLUG_MAP = {
    "KXHIGHAUS": "highest-temperature-in-austin",
    "KXHIGHCHI": "highest-temperature-in-chicago",
    "KXHIGHDEN": "highest-temperature-in-denver",
    "KXHIGHLAX": "highest-temperature-in-los-angeles",
    "KXHIGHMIA": "highest-temperature-in-miami",
    "KXHIGHNY": "highest-temperature-in-nyc",
    "KXHIGHPHIL": "highest-temperature-in-philadelphia",
    "KXHIGHTATL": "highest-temperature-in-atlanta",
    "KXHIGHTBOS": "highest-temperature-in-boston",
    "KXHIGHTDAL": "highest-temperature-in-dallas",
    "KXHIGHTDC": "highest-temperature-in-washington-dc",
    "KXHIGHTHOU": "highest-temperature-in-houston",
    "KXHIGHTLV": "highest-temperature-in-las-vegas",
    "KXHIGHTMIN": "highest-temperature-in-minneapolis",
    "KXHIGHTNOLA": "highest-temperature-in-new-orleans",
    "KXHIGHTOKC": "highest-temperature-in-oklahoma-city",
    "KXHIGHTPHX": "highest-temperature-in-phoenix",
    "KXHIGHTSATX": "highest-temperature-in-san-antonio",
    "KXHIGHTSEA": "highest-temperature-in-seattle",
    "KXHIGHTSFO": "highest-temperature-in-san-francisco",
}

MARKETPLACE_SQL = """
with metadata as (
  select
    min(forecast_date)::date as today_key,
    (min(forecast_date) + interval '1 day')::date as tomorrow_key,
    (min(forecast_date) + (%s * interval '1 day'))::date as active_date,
    count(*) as total_contract_count,
    max(pulled_at) as updated_at
  from app.latest_marketplace_contracts
)
select
  lmc.*,
  md.today_key as _today_key,
  md.tomorrow_key as _tomorrow_key,
  md.active_date as _active_date,
  md.total_contract_count as _total_contract_count,
  md.updated_at as _updated_at
from app.latest_marketplace_contracts lmc
cross join metadata md
where lmc.forecast_date = md.active_date
  and (%s = '' or lmc.search_text like %s)
order by lmc.forecast_date asc, lmc.city asc nulls last, lmc.floor_strike asc nulls last, lmc.ticker asc
limit 500;
"""

MARKETPLACE_METADATA_SQL = """
select
  min(forecast_date)::date as today_key,
  (min(forecast_date) + interval '1 day')::date as tomorrow_key,
  (min(forecast_date) + (%s * interval '1 day'))::date as active_date,
  count(*) as total_contract_count,
  max(pulled_at) as updated_at
from app.latest_marketplace_contracts;
"""

MARKET_DETAIL_SQL = """
select *
from app.latest_marketplace_contracts
where ticker = %s
limit 1;
"""

WATCHLIST_SQL = """
with metadata as (
  select
    min(forecast_date)::date as today_key,
    (min(forecast_date) + interval '1 day')::date as tomorrow_key,
    (min(forecast_date) + (%s * interval '1 day'))::date as active_date,
    count(*) as total_contract_count,
    max(pulled_at) as updated_at
  from app.latest_marketplace_contracts
)
select
  lmc.*,
  md.today_key as _today_key,
  md.tomorrow_key as _tomorrow_key,
  md.active_date as _active_date,
  md.total_contract_count as _total_contract_count,
  md.updated_at as _updated_at
from app.latest_marketplace_contracts lmc
cross join metadata md
where lmc.forecast_date = md.active_date
order by lmc.city asc nulls last, lmc.floor_strike asc nulls last, lmc.ticker asc;
"""

HOURLY_FORECAST_SQL = """
with latest_snapshots as (
  select distinct on (ws.provider)
    ws.id,
    ws.provider,
    ws.pulled_at,
    ws.timezone
  from app.weather_snapshots ws
  where ws.market_id = %s
  order by ws.provider, ws.pulled_at desc
)
select
  ls.provider,
  ls.pulled_at,
  whf.forecast_time,
  (whf.forecast_time at time zone ls.timezone) as local_forecast_time,
  whf.temperature_2m
from latest_snapshots ls
join app.weather_hourly_forecasts whf on whf.snapshot_id = ls.id
where (whf.forecast_time at time zone ls.timezone)::date = %s
  and whf.temperature_2m is not null
order by ls.provider, whf.forecast_time;
"""


@dataclass(frozen=True)
class MarketplaceFilters:
    day: str = "today"
    search: str = ""
    edge_only: bool = False
    side: str = "yes"


def get_marketplace_context(filters: MarketplaceFilters) -> dict[str, Any]:
    started_at = time.perf_counter()
    day_offset = 1 if filters.day == "tomorrow" else 0
    search = filters.search.strip().lower()
    raw_rows = _fetch_rows(
        MARKETPLACE_SQL,
        params=(day_offset, search, f"%{search}%"),
    )
    metadata = _marketplace_metadata_from_rows(raw_rows) or _fetch_marketplace_metadata(day_offset)

    normalize_started_at = time.perf_counter()
    rows = [_with_marketplace_side_fields(_normalize_row(row), "yes") for row in raw_rows]
    normalize_ms = _elapsed_ms(normalize_started_at)

    today_key = _date_key(metadata.get("today_key")) or date.today().isoformat()
    tomorrow_key = _date_key(metadata.get("tomorrow_key")) or _add_days(today_key, 1)
    active_date = _date_key(metadata.get("active_date")) or (tomorrow_key if filters.day == "tomorrow" else today_key)
    total_contract_count = int(metadata.get("total_contract_count") or 0)

    logger.info(
        "marketplace_context day=%s search=%r returned_rows=%s total_contract_count=%s normalize_ms=%.1f total_ms=%.1f",
        filters.day,
        filters.search,
        len(rows),
        total_contract_count,
        normalize_ms,
        _elapsed_ms(started_at),
    )

    return {
        "contracts": rows,
        "contract_count": len(rows),
        "total_contract_count": total_contract_count,
        "day": filters.day,
        "side": "yes",
        "side_display": "YES",
        "search": filters.search,
        "today_key": today_key,
        "tomorrow_key": tomorrow_key,
        "active_date": active_date,
        "updated_at": metadata.get("updated_at"),
        "positive_yes_count": sum(1 for row in rows if row["yes_ev"] is not None and row["yes_ev"] > 0),
        "positive_side_count": sum(1 for row in rows if row["selected_side_ev"] is not None and row["selected_side_ev"] > 0),
        "high_confidence_count": sum(1 for row in rows if row.get("selected_side_has_backtested_edge")),
        "high_confidence_threshold": WATCHLIST_CONFIDENCE_THRESHOLD,
        "high_confidence_threshold_display": _format_percent(WATCHLIST_CONFIDENCE_THRESHOLD),
    }


def get_market_detail_context(ticker: str) -> dict[str, Any] | None:
    started_at = time.perf_counter()
    normalized_ticker = ticker.strip().upper()
    raw_rows = _fetch_rows(
        MARKET_DETAIL_SQL,
        params=(normalized_ticker,),
        log_label="market_detail_fetch",
    )
    if raw_rows:
        row = _normalize_row(raw_rows[0])
        row["hourly_chart"] = _build_hourly_chart(row)
        logger.info(
            "market_detail_context ticker=%s raw_rows=%s found=true total_ms=%.1f",
            normalized_ticker,
            len(raw_rows),
            _elapsed_ms(started_at),
        )
        return {"contract": row}
    logger.info(
        "market_detail_context ticker=%s raw_rows=%s found=false total_ms=%.1f",
        normalized_ticker,
        len(raw_rows),
        _elapsed_ms(started_at),
    )
    return None


def get_edge_ticker_context(day: str = "today", limit: int = 16) -> dict[str, Any]:
    started_at = time.perf_counter()
    day_offset = 1 if day == "tomorrow" else 0
    raw_rows = _fetch_rows(WATCHLIST_SQL, params=(day_offset,), log_label="home_edge_ticker_fetch")
    normalized_rows = [_normalize_row(row) for row in raw_rows]
    edge_rows: list[dict[str, Any]] = []
    for side in ("yes", "no"):
        side_rows = [
            _with_marketplace_side_fields(row, side)
            for row in _select_best_contracts_by_city(normalized_rows, side)
        ]
        edge_rows.extend(row for row in side_rows if row.get("selected_side_has_backtested_edge"))

    edge_rows.sort(
        key=lambda row: (
            -(row.get("selected_side_ev") or -999),
            row.get("location") or "",
            row.get("selected_side") or "",
        )
    )
    selected_rows = edge_rows[:limit]
    ticker_items = [_edge_ticker_item(row) for row in selected_rows]
    logger.info(
        "home_edge_ticker day=%s candidates=%s edges=%s returned=%s total_ms=%.1f",
        day,
        len(normalized_rows),
        len(edge_rows),
        len(ticker_items),
        _elapsed_ms(started_at),
    )
    return {
        "edge_ticker_items": ticker_items,
        "edge_ticker_count": len(edge_rows),
        "edge_strategy": _build_edge_strategy_summary(len(edge_rows)),
    }


def get_watchlist_context(filters: MarketplaceFilters) -> dict[str, Any]:
    started_at = time.perf_counter()
    day_offset = 1 if filters.day == "tomorrow" else 0
    config = _load_watchlist_config()
    raw_rows = _fetch_rows(WATCHLIST_SQL, params=(day_offset,), log_label="watchlist_fetch")
    metadata = _marketplace_metadata_from_rows(raw_rows) or _fetch_marketplace_metadata(day_offset)

    normalized_rows = [_normalize_row(row) for row in raw_rows]
    side = filters.side if filters.side in {"yes", "no"} else "yes"
    all_selections = [
        _with_marketplace_side_fields(row, side)
        for row in _select_best_contracts_by_city(normalized_rows, side)
    ]
    high_confidence_count = sum(1 for row in all_selections if row.get("selected_side_has_backtested_edge"))
    selections = (
        [row for row in all_selections if row.get("selected_side_has_backtested_edge")]
        if filters.edge_only
        else all_selections
    )

    total_probability = sum(row["selected_side_probability"] or 0 for row in selections)
    avg_probability = total_probability / len(selections) if selections else 0
    positive_side_count = sum(1 for row in selections if row["selected_side_ev"] is not None and row["selected_side_ev"] > 0)

    today_key = _date_key(metadata.get("today_key")) or date.today().isoformat()
    tomorrow_key = _date_key(metadata.get("tomorrow_key")) or _add_days(today_key, 1)
    active_date = _date_key(metadata.get("active_date")) or (tomorrow_key if filters.day == "tomorrow" else today_key)

    logger.info(
        "watchlist_context day=%s side=%s edge_only=%s candidates=%s selected=%s total_ms=%.1f",
        filters.day,
        side,
        str(filters.edge_only).lower(),
        len(normalized_rows),
        len(selections),
        _elapsed_ms(started_at),
    )

    return {
        "rows": selections,
        "row_count": len(selections),
        "unfiltered_row_count": len(all_selections),
        "candidate_count": len(normalized_rows),
        "positive_yes_count": sum(1 for row in selections if row["yes_ev"] is not None and row["yes_ev"] > 0),
        "positive_side_count": positive_side_count,
        "high_confidence_count": high_confidence_count,
        "high_confidence_threshold": WATCHLIST_CONFIDENCE_THRESHOLD,
        "high_confidence_threshold_display": _format_percent(WATCHLIST_CONFIDENCE_THRESHOLD),
        "avg_probability_display": _format_percent(avg_probability),
        "watchlist_locations": config["locations"],
        "watchlist_tickers": config["tickers"],
        "day": filters.day,
        "side": side,
        "side_display": side.upper(),
        "edge_only": filters.edge_only,
        "today_key": today_key,
        "tomorrow_key": tomorrow_key,
        "active_date": active_date,
        "updated_at": metadata.get("updated_at"),
    }


def _fetch_rows(
    sql: str = MARKETPLACE_SQL,
    *,
    params: tuple[Any, ...] | None = None,
    log_label: str = "marketplace_fetch",
) -> list[dict[str, Any]]:
    started_at = time.perf_counter()
    with connections["weather"].cursor() as cursor:
        execute_started_at = time.perf_counter()
        if params is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, params)
        execute_ms = _elapsed_ms(execute_started_at)

        fetch_started_at = time.perf_counter()
        columns = [column[0] for column in cursor.description]
        raw_rows = cursor.fetchall()
        fetch_ms = _elapsed_ms(fetch_started_at)

    hydrate_started_at = time.perf_counter()
    rows = [dict(zip(columns, row)) for row in raw_rows]
    hydrate_ms = _elapsed_ms(hydrate_started_at)
    logger.info(
        "%s rows=%s execute_ms=%.1f fetch_ms=%.1f hydrate_ms=%.1f total_ms=%.1f",
        log_label,
        len(rows),
        execute_ms,
        fetch_ms,
        hydrate_ms,
        _elapsed_ms(started_at),
    )
    return rows


def _fetch_marketplace_metadata(day_offset: int) -> dict[str, Any]:
    rows = _fetch_rows(
        MARKETPLACE_METADATA_SQL,
        params=(day_offset,),
        log_label="marketplace_metadata_fetch",
    )
    return rows[0] if rows else {}


def _marketplace_metadata_from_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    row = rows[0]
    return {
        "today_key": row.get("_today_key"),
        "tomorrow_key": row.get("_tomorrow_key"),
        "active_date": row.get("_active_date"),
        "total_contract_count": row.get("_total_contract_count"),
        "updated_at": row.get("_updated_at"),
    }


def _build_hourly_chart(contract: dict[str, Any]) -> dict[str, Any]:
    market_id = contract.get("market_id")
    forecast_date = contract.get("forecast_date")
    if not market_id or not forecast_date:
        return {"available": False, "series": [], "paths": [], "ticks": [], "message": "Hourly forecast data is not linked to this contract yet."}

    rows = _fetch_hourly_rows_from_json(str(market_id), forecast_date, str(contract.get("timezone") or "America/New_York"))
    if not rows:
        rows = _fetch_rows(
            HOURLY_FORECAST_SQL,
            params=(market_id, forecast_date),
            log_label="hourly_forecast_fetch",
        )
    if not rows:
        return {"available": False, "series": [], "paths": [], "ticks": [], "message": "No hourly forecast rows matched this market and date yet."}

    series_by_provider: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        local_time = row.get("local_forecast_time")
        temperature = _float_or_none(row.get("temperature_2m"))
        if local_time is None or temperature is None:
            continue
        local_hour = local_time.hour + local_time.minute / 60 if isinstance(local_time, datetime) else None
        if local_hour is None:
            continue
        series_by_provider.setdefault(str(row.get("provider") or "provider"), []).append(
            {
                "hour": local_hour,
                "temperature": temperature,
                "label": f"{int(local_hour):02d}:00",
            }
        )

    series = [
        {
            "provider": provider,
            "label": _provider_label(provider),
            "color": _provider_color(provider),
            "points": points,
        }
        for provider, points in sorted(series_by_provider.items())
        if points
    ]
    if not series:
        return {"available": False, "series": [], "paths": [], "ticks": [], "message": "Hourly forecast rows did not include usable temperature points."}

    all_temps = [point["temperature"] for item in series for point in item["points"]]
    model_high = contract.get("model_high")
    if model_high is not None:
        all_temps.append(model_high)
    min_temp = int(min(all_temps) - 2)
    max_temp = int(max(all_temps) + 3)
    if min_temp == max_temp:
        max_temp += 1

    width = 1000
    height = 340
    padding = {"top": 28, "right": 30, "bottom": 44, "left": 58}
    temp_domain = max_temp - min_temp

    def x_for(hour: float) -> float:
        return padding["left"] + (max(0, min(23, hour)) / 23) * (width - padding["left"] - padding["right"])

    def y_for(temp: float) -> float:
        return padding["top"] + ((max_temp - temp) / temp_domain) * (height - padding["top"] - padding["bottom"])

    paths = []
    for item in series:
        path = " ".join(
            f"{'M' if index == 0 else 'L'} {x_for(point['hour']):.2f} {y_for(point['temperature']):.2f}"
            for index, point in enumerate(item["points"])
        )
        paths.append({**item, "path": path})

    y_ticks = [
        {
            "y": y_for(temp),
            "label": f"{temp}F",
        }
        for temp in _chart_ticks(min_temp, max_temp)
    ]
    x_ticks = [
        {
            "x": x_for(hour),
            "label": label,
        }
        for hour, label in [(0, "12a"), (6, "6a"), (12, "12p"), (18, "6p"), (23, "11p")]
    ]
    model_high_line = None
    if model_high is not None:
        model_high_line = {
            "y": y_for(model_high),
            "label": f"model {model_high:.1f}F",
        }
    current_hour_line = None
    current_hour = contract.get("current_hour")
    if isinstance(current_hour, (int, float)) and 0 <= current_hour <= 23:
        current_hour_line = {"x": x_for(float(current_hour))}

    return {
        "available": True,
        "width": width,
        "height": height,
        "series": series,
        "paths": paths,
        "y_ticks": y_ticks,
        "x_ticks": x_ticks,
        "model_high_line": model_high_line,
        "current_hour_line": current_hour_line,
        "min_temp": min_temp,
        "max_temp": max_temp,
    }


def _fetch_hourly_rows_from_json(market_id: str, forecast_date: date, timezone_name: str) -> list[dict[str, Any]]:
    rows = []
    seen_providers = set()
    for path in WEATHER_FORECAST_PATHS:
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if not isinstance(payload, list):
            continue
        for snapshot in payload:
            market = snapshot.get("market") or {}
            if market.get("market_id") != market_id:
                continue
            provider = snapshot.get("provider") or path.stem
            if provider in seen_providers:
                continue
            seen_providers.add(provider)
            snapshot_timezone = market.get("timezone") or timezone_name
            for hourly in snapshot.get("hourly") or []:
                local_time = _parse_hourly_local_time(hourly.get("time"), snapshot_timezone)
                if not local_time or local_time.date() != forecast_date:
                    continue
                rows.append(
                    {
                        "provider": provider,
                        "pulled_at": snapshot.get("pulled_at"),
                        "forecast_time": hourly.get("time"),
                        "local_forecast_time": local_time,
                        "temperature_2m": hourly.get("temperature_2m"),
                    }
                )
    logger.info("hourly_forecast_json rows=%s market_id=%s forecast_date=%s", len(rows), market_id, forecast_date)
    return rows


def _parse_hourly_local_time(value: Any, timezone_name: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        try:
            return parsed.replace(tzinfo=ZoneInfo(timezone_name))
        except Exception:
            return parsed
    try:
        return parsed.astimezone(ZoneInfo(timezone_name))
    except Exception:
        return parsed


def _chart_ticks(min_temp: int, max_temp: int) -> list[int]:
    step = max(1, round((max_temp - min_temp) / 4))
    ticks = list(range(min_temp, max_temp + 1, step))
    if ticks[-1] != max_temp:
        ticks.append(max_temp)
    return ticks


def _provider_label(provider: str) -> str:
    labels = {
        "noaa-nws": "NOAA",
        "open-meteo": "Open-Meteo",
        "visual-crossing": "Visual Crossing",
    }
    return labels.get(provider, provider)


def _provider_color(provider: str) -> str:
    colors = {
        "noaa-nws": "#f0f8fc",
        "open-meteo": "#a4acb0",
        "visual-crossing": "#93f1fd",
    }
    return colors.get(provider, "#6debfd")


def _load_watchlist_config() -> dict[str, list[str]]:
    try:
        import json

        with WATCHLIST_CONFIG_PATH.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, ValueError):
        payload = {}

    return {
        "locations": [str(item) for item in payload.get("locations", []) if item],
        "tickers": [str(item).upper() for item in payload.get("tickers", []) if item],
    }


def _select_model_high_matches(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best_by_city: dict[str, dict[str, Any]] = {}
    for row in rows:
        city = row.get("location") or "Unknown"
        candidate = {
            **row,
            "model_high_match_distance": _model_high_match_distance(row),
            "model_high_floored": _model_high_floored(row),
            "model_high_settlement_band": _model_high_settlement_band(row),
        }
        existing = best_by_city.get(city)
        if existing is None or _watchlist_sort_key(candidate) > _watchlist_sort_key(existing):
            best_by_city[city] = candidate
    return sorted(best_by_city.values(), key=lambda row: (-(row.get("yes_ev") or -999), row.get("location") or ""))


def _select_best_contracts_by_city(rows: list[dict[str, Any]], side: str) -> list[dict[str, Any]]:
    best_by_city: dict[str, dict[str, Any]] = {}
    for row in rows:
        city = row.get("location") or "Unknown"
        current = best_by_city.get(city)
        if current is None or _marketplace_side_sort_key(row, side) > _marketplace_side_sort_key(current, side):
            best_by_city[city] = row
    return sorted(
        best_by_city.values(),
        key=lambda row: (-(_side_metric(row, side, "ev") or -999), row.get("location") or ""),
    )


def _marketplace_side_sort_key(row: dict[str, Any], side: str) -> tuple[float, float, float, int, str]:
    ev = _side_metric(row, side, "ev")
    probability = _side_metric(row, side, "probability")
    cost = _side_metric(row, side, "cost")
    distance = row.get("model_high_match_distance")
    if distance is None:
        distance = _model_high_match_distance(row)
    return (
        float(ev if ev is not None else -999),
        float(probability if probability is not None else -999),
        -float(cost if cost is not None else 999),
        -int(distance if distance is not None else 999),
        str(row.get("ticker") or ""),
    )


def _side_metric(row: dict[str, Any], side: str, metric: str) -> float | None:
    keys = {
        "yes": {
            "probability": "model_probability",
            "cost": "yes_ask",
            "ev": "yes_ev",
        },
        "no": {
            "probability": "no_probability",
            "cost": "no_ask",
            "ev": "no_ev",
        },
    }
    key = keys[side][metric]
    value = row.get(key)
    return float(value) if isinstance(value, (int, float)) else None


def _with_marketplace_side_fields(row: dict[str, Any], side: str) -> dict[str, Any]:
    probability = _side_metric(row, side, "probability")
    cost = _side_metric(row, side, "cost")
    ev = _side_metric(row, side, "ev")
    has_edge = (
        probability is not None
        and probability >= WATCHLIST_CONFIDENCE_THRESHOLD
        and ev is not None
        and ev > 0
    )
    return {
        **row,
        "model_high_match_distance": _model_high_match_distance(row),
        "model_high_floored": _model_high_floored(row),
        "model_high_settlement_band": _model_high_settlement_band(row),
        "selected_side": side,
        "selected_side_display": side.upper(),
        "selected_side_is_yes": side == "yes",
        "selected_side_is_no": side == "no",
        "selected_side_probability": probability,
        "selected_side_probability_width": 0 if probability is None else max(0, min(100, round(probability * 100))),
        "selected_side_probability_display": _format_percent(probability),
        "selected_side_cost": cost,
        "selected_side_cost_display": _format_dollars(cost),
        "selected_side_ev": ev,
        "selected_side_ev_display": _format_ev(ev),
        "selected_side_has_backtested_edge": has_edge,
        "selected_side_action_label": f"EXECUTE {side.upper()}" if ev is not None and ev > 0 else f"INSPECT {side.upper()}",
    }


def _edge_ticker_item(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "location": row.get("location") or "Unknown",
        "contract_label": row.get("contract_label") or "Temperature contract",
        "side": row.get("selected_side_display") or "PASS",
        "probability": row.get("selected_side_probability_display") or "--",
        "cost": row.get("selected_side_cost_display") or "--",
        "ev": row.get("selected_side_ev_display") or "--",
        "theme": row.get("theme") or "high",
        "detail_url": row.get("detail_url") or "#",
    }


def _build_edge_strategy_summary(active_signal_count: int) -> dict[str, Any]:
    bankroll = EDGE_STRATEGY_STARTING_BANKROLL
    total_bets = 0
    wins = 0
    losses = 0
    total_staked = 0.0
    days: list[dict[str, Any]] = []

    if not RESOLVED_BETS_DIR.exists():
        return _load_deploy_edge_strategy_summary(active_signal_count)

    for path in sorted(RESOLVED_BETS_DIR.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if not _is_clean_8am_snapshot(payload):
            continue

        selected_bets = _select_edge_strategy_bets(payload.get("bets") or [])
        if not selected_bets:
            continue

        day_wins = 0
        day_losses = 0
        day_pnl = 0.0
        stake = bankroll * EDGE_STRATEGY_RISK_PCT

        for bet in selected_bets:
            cost = _float_or_none(bet.get("contract_cost"))
            if cost is None or cost <= 0:
                continue
            won = bool(bet.get("bet_won"))
            pnl = (stake / cost) * (1 - cost) if won else -stake
            bankroll += pnl
            day_pnl += pnl
            total_staked += stake
            total_bets += 1
            if won:
                wins += 1
                day_wins += 1
            else:
                losses += 1
                day_losses += 1

        if day_wins or day_losses:
            days.append(
                {
                    "date": payload.get("target_date") or path.stem,
                    "bets": day_wins + day_losses,
                    "wins": day_wins,
                    "losses": day_losses,
                    "pnl_display": _format_signed_dollars(day_pnl),
                    "bankroll_display": _format_dollars(bankroll),
                    "positive": day_pnl >= 0,
                }
            )

    pnl = bankroll - EDGE_STRATEGY_STARTING_BANKROLL
    roi = pnl / total_staked if total_staked else None
    win_rate = wins / total_bets if total_bets else None
    return {
        "active_signal_count": active_signal_count,
        "selected_contract_count": total_bets,
        "day_count": len(days),
        "wins": wins,
        "losses": losses,
        "win_loss_display": f"{wins}-{losses}" if total_bets else "--",
        "win_rate_display": _format_percent(win_rate),
        "roi_display": _format_signed_percent(roi),
        "return_rate_display": _format_signed_percent((bankroll / EDGE_STRATEGY_STARTING_BANKROLL) - 1),
        "pnl_display": _format_signed_dollars(pnl),
        "starting_bankroll_display": _format_dollars(EDGE_STRATEGY_STARTING_BANKROLL),
        "bankroll_display": _format_dollars(bankroll),
        "threshold_display": _format_percent(WATCHLIST_CONFIDENCE_THRESHOLD),
        "risk_display": _format_percent(EDGE_STRATEGY_RISK_PCT),
        "days": days[-6:],
    }


def _load_deploy_edge_strategy_summary(active_signal_count: int) -> dict[str, Any]:
    try:
        payload = json.loads(DEPLOY_EDGE_STRATEGY_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return _empty_edge_strategy(active_signal_count)
    return {
        **_empty_edge_strategy(active_signal_count),
        **payload,
        "active_signal_count": active_signal_count,
    }


def _empty_edge_strategy(active_signal_count: int) -> dict[str, Any]:
    return {
        "active_signal_count": active_signal_count,
        "selected_contract_count": 0,
        "day_count": 0,
        "wins": 0,
        "losses": 0,
        "win_loss_display": "--",
        "win_rate_display": "--",
        "roi_display": "--",
        "return_rate_display": "--",
        "pnl_display": "--",
        "starting_bankroll_display": _format_dollars(EDGE_STRATEGY_STARTING_BANKROLL),
        "bankroll_display": _format_dollars(EDGE_STRATEGY_STARTING_BANKROLL),
        "threshold_display": _format_percent(WATCHLIST_CONFIDENCE_THRESHOLD),
        "risk_display": _format_percent(EDGE_STRATEGY_RISK_PCT),
        "days": [],
    }


def _is_clean_8am_snapshot(payload: dict[str, Any]) -> bool:
    generated_at = _parse_utc_datetime(payload.get("generated_at"))
    if generated_at is None:
        bets = payload.get("bets") or []
        generated_at = _parse_utc_datetime(bets[0].get("logged_at")) if bets else None
    if generated_at is None:
        return False
    local = generated_at.astimezone(EDGE_STRATEGY_LOCAL_TZ)
    local_hour = local.hour + local.minute / 60
    return 7.5 <= local_hour <= 8.5


def _select_edge_strategy_bets(bets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best_by_city: dict[str, dict[str, Any]] = {}
    for bet in bets:
        if bet.get("status") != "resolved" or bet.get("bet_won") is None:
            continue
        probability = _float_or_none(bet.get("model_win_probability"))
        expected_value = _float_or_none(bet.get("expected_value"))
        cost = _float_or_none(bet.get("contract_cost"))
        if (
            probability is None
            or probability < WATCHLIST_CONFIDENCE_THRESHOLD
            or expected_value is None
            or expected_value <= 0
            or cost is None
            or cost <= 0
        ):
            continue
        key = str(bet.get("weather_market_id") or bet.get("city") or bet.get("ticker") or "")
        current = best_by_city.get(key)
        if current is None or _edge_bet_sort_key(bet) > _edge_bet_sort_key(current):
            best_by_city[key] = bet
    return sorted(best_by_city.values(), key=lambda bet: (str(bet.get("city") or ""), str(bet.get("ticker") or "")))


def _edge_bet_sort_key(bet: dict[str, Any]) -> tuple[float, float, float, str]:
    return (
        _float_or_none(bet.get("expected_value")) or -999,
        _float_or_none(bet.get("model_win_probability")) or -999,
        -(_float_or_none(bet.get("contract_cost")) or 999),
        str(bet.get("ticker") or ""),
    )


def _watchlist_sort_key(row: dict[str, Any]) -> tuple[float, int, float, float]:
    distance = row.get("model_high_match_distance")
    match_score = -float(distance if distance is not None else 9999)
    kind_score = _model_high_match_kind_score(row)
    forecast_date = row.get("forecast_date") if isinstance(row.get("forecast_date"), date) else date.max
    date_score = -float(forecast_date.toordinal())
    ev_score = float(row.get("yes_ev") or -999)
    return match_score, kind_score, date_score, ev_score


def _model_high_floored(row: dict[str, Any]) -> int | None:
    model_high = row.get("model_high")
    if model_high is None:
        return None
    return int(float(f"{model_high:.1f}") // 1)


def _model_high_match_distance(row: dict[str, Any]) -> float | None:
    forecast = _model_high_floored(row)
    if forecast is None:
        return None

    floor = _float_or_none(row.get("floor_strike"))
    cap = _float_or_none(row.get("cap_strike"))
    if floor is not None and cap is not None:
        if floor <= forecast <= cap:
            return 0
        return min(abs(forecast - floor), abs(forecast - cap))
    if cap is not None:
        return 0 if forecast < cap else abs(forecast - cap)
    if floor is not None:
        return 0 if forecast > floor else abs(forecast - floor)
    return None


def _model_high_match_kind_score(row: dict[str, Any]) -> int:
    if row.get("floor_strike") is not None and row.get("cap_strike") is not None:
        return 3
    if row.get("floor_strike") is not None or row.get("cap_strike") is not None:
        return 2
    return 1


def _model_high_settlement_band(row: dict[str, Any]) -> str | None:
    floor = _float_or_none(row.get("floor_strike"))
    cap = _float_or_none(row.get("cap_strike"))
    if floor is not None and cap is not None:
        return f"{_format_strike(floor)}-{_format_strike(cap)}F floor bucket"
    if cap is not None:
        return f"<{_format_strike(cap)}F floor bucket"
    if floor is not None:
        return f">{_format_strike(floor)}F floor bucket"
    return None


def _elapsed_ms(started_at: float) -> float:
    return (time.perf_counter() - started_at) * 1000


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    model_probability = _decimal_to_float(row.get("model_probability"))
    yes_ask = _decimal_to_float(row.get("yes_ask_dollars"))
    no_ask = _decimal_to_float(row.get("no_ask_dollars"))
    implied_probability = _decimal_to_float(row.get("implied_probability"))
    yes_ev = _yes_ev(model_probability, yes_ask)
    no_probability = None if model_probability is None else 1.0 - model_probability
    no_ev = _yes_ev(no_probability, no_ask)
    side_label, side_probability, side_ev, side_cost = _edge_side_metrics(
        model_probability,
        yes_ask,
        yes_ev,
        no_probability,
        no_ask,
        no_ev,
    )
    timezone_name = row.get("timezone") or "America/New_York"
    current_hour = _local_hour(timezone_name)
    model_high = _float_or_none(row.get("adjusted_forecast_max_f"))
    noaa_high = _float_or_none(row.get("noaa_forecast_max_f"))
    open_meteo_high = _float_or_none(row.get("open_meteo_forecast_max_f"))
    contract_label = _contract_label(row)
    is_low = "low" in f"{row.get('event_type') or ''} {row.get('title') or ''}".lower()

    return {
        **row,
        "location": row.get("city") or "Unknown",
        "contract_label": contract_label,
        "date_key": _date_key(row.get("forecast_date") or row.get("event_date")),
        "model_probability": model_probability,
        "model_probability_width": 0 if model_probability is None else max(0, min(100, round(model_probability * 100))),
        "model_probability_display": _format_percent(model_probability),
        "no_probability": no_probability,
        "no_probability_width": 0 if no_probability is None else max(0, min(100, round(no_probability * 100))),
        "no_probability_display": _format_percent(no_probability),
        "market_probability": implied_probability,
        "market_probability_display": _format_percent(implied_probability),
        "yes_ask": yes_ask,
        "yes_cost_display": _format_dollars(yes_ask),
        "no_ask": no_ask,
        "no_cost_display": _format_dollars(no_ask),
        "yes_ev": yes_ev,
        "yes_ev_display": _format_ev(yes_ev),
        "no_ev": no_ev,
        "no_ev_display": _format_ev(no_ev),
        "watchlist_side": side_label,
        "watchlist_side_display": side_label.upper() if side_label else "PASS",
        "watchlist_side_is_yes": side_label == "yes",
        "watchlist_side_is_no": side_label == "no",
        "watchlist_side_probability": side_probability,
        "watchlist_side_probability_display": _format_percent(side_probability),
        "watchlist_side_ev": side_ev,
        "watchlist_side_ev_display": _format_ev(side_ev),
        "watchlist_side_cost": side_cost,
        "watchlist_side_cost_display": _format_dollars(side_cost),
        "is_backtested_edge": (
            side_probability is not None
            and side_probability >= WATCHLIST_CONFIDENCE_THRESHOLD
            and side_ev is not None
            and side_ev > 0
        ),
        "is_high_confidence_watchlist": (
            side_probability is not None
            and side_probability >= WATCHLIST_CONFIDENCE_THRESHOLD
            and side_ev is not None
            and side_ev > 0
        ),
        "model_high": model_high,
        "model_high_display": _format_temp(model_high, digits=1),
        "noaa_high_display": _format_temp(noaa_high, digits=0),
        "open_meteo_high_display": _format_temp(open_meteo_high, digits=1),
        "theme": "low" if is_low else "high",
        "theme_icon": "ac_unit" if is_low else "device_thermostat",
        "current_hour": current_hour,
        "current_hour_display": f"{int(current_hour):02d}:00 local",
        "bar_heights": _marketplace_bar_heights(current_hour),
        "action_label": "EXECUTE YES" if yes_ev is not None and yes_ev > 0 else "INSPECT YES",
        "edge_action_label": f"EXECUTE {side_label.upper()}" if side_label and side_ev is not None and side_ev > 0 else "INSPECT",
        "detail_url": f"/marketplace/{row.get('ticker')}/",
        "kalshi_url": _kalshi_url(row),
        "signal_summary": _signal_summary(row, model_high, noaa_high, open_meteo_high),
    }


def _edge_side_metrics(
    yes_probability: float | None,
    yes_cost: float | None,
    yes_ev: float | None,
    no_probability: float | None,
    no_cost: float | None,
    no_ev: float | None,
) -> tuple[str | None, float | None, float | None, float | None]:
    candidates = [
        ("yes", yes_probability, yes_ev, yes_cost),
        ("no", no_probability, no_ev, no_cost),
    ]
    priced = [
        candidate
        for candidate in candidates
        if candidate[1] is not None and candidate[2] is not None and candidate[3] is not None
    ]
    if not priced:
        return None, None, None, None
    return max(priced, key=lambda candidate: (candidate[2] or -999, candidate[1] or 0))


def _contract_label(row: dict[str, Any]) -> str:
    floor = _float_or_none(row.get("floor_strike"))
    cap = _float_or_none(row.get("cap_strike"))
    custom = row.get("custom_strike")

    if floor is not None and cap is not None:
        return f"{_format_strike(floor)}° to {_format_strike(cap)}°"
    if floor is not None:
        return f"{_format_strike(floor)}° or above"
    if cap is not None:
        return f"{_format_strike(cap)}° or below"
    if custom:
        return str(custom)
    return "Temperature contract"


def _signal_summary(row: dict[str, Any], model_high: float | None, noaa_high: float | None, open_meteo_high: float | None) -> str:
    pieces = []
    if row.get("signal_short"):
        pieces.append(str(row["signal_short"]))
    if model_high is not None and noaa_high is not None:
        delta = model_high - noaa_high
        if abs(delta) >= 0.5:
            pieces.append(f"Model is {abs(delta):.1f}F {'warmer' if delta > 0 else 'cooler'} than NOAA")
    if open_meteo_high is not None and noaa_high is not None:
        spread = open_meteo_high - noaa_high
        if abs(spread) >= 1.5:
            pieces.append(f"Open-Meteo is {abs(spread):.1f}F {'warmer' if spread > 0 else 'cooler'} than NOAA")
    return " • ".join(pieces[:2]) or "Forecast signal still settling."


def _kalshi_url(row: dict[str, Any]) -> str:
    ticker = str(row.get("ticker") or "").upper()
    event_ticker = ticker.rsplit("-", 1)[0] if "-" in ticker else ""
    series_ticker = str(row.get("series_ticker") or (event_ticker.split("-", 1)[0] if event_ticker else "")).upper()
    series_slug = SERIES_SLUG_MAP.get(series_ticker) or _slugify(row.get("location") or row.get("title"))
    if not series_ticker or not series_slug or not event_ticker:
        return "https://kalshi.com/markets"
    return f"https://kalshi.com/markets/{series_ticker.lower()}/{series_slug}/{event_ticker.lower()}"


def _slugify(value: Any) -> str:
    text = str(value or "").lower()
    pieces = []
    previous_dash = False
    for character in text:
        if character.isalnum():
            pieces.append(character)
            previous_dash = False
        elif not previous_dash:
            pieces.append("-")
            previous_dash = True
    return "".join(pieces).strip("-")


def _marketplace_bar_heights(current_hour: float) -> list[dict[str, Any]]:
    buckets = [
        ("12a", 0, 18),
        ("4a", 4, 26),
        ("8a", 8, 44),
        ("12p", 12, 100),
        ("4p", 16, 72),
        ("8p", 20, 34),
    ]
    return [
        {
            "label": label,
            "height": height,
            "active": hour <= current_hour < (buckets[index + 1][1] if index + 1 < len(buckets) else 24),
        }
        for index, (label, hour, height) in enumerate(buckets)
    ]


def _local_hour(timezone_name: str) -> float:
    try:
        now = timezone.now().astimezone(ZoneInfo(timezone_name))
    except Exception:
        now = timezone.now().astimezone(ZoneInfo("America/New_York"))
    return now.hour + now.minute / 60


def _yes_ev(model_probability: float | None, yes_ask: float | None) -> float | None:
    if model_probability is None or yes_ask is None:
        return None
    # YES EV per contract: p * (1 - cost) - (1 - p) * cost, which simplifies to p - cost.
    return model_probability - yes_ask


def _date_key(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if value:
        return str(value)[:10]
    return None


def _add_days(date_key: str, days: int) -> str | None:
    try:
        parsed = datetime.fromisoformat(f"{date_key}T00:00:00").date()
    except ValueError:
        return None
    return (parsed + timedelta(days=days)).isoformat()


def _decimal_to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _parse_utc_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=ZoneInfo("UTC"))
    return parsed


def _format_percent(value: float | None) -> str:
    return "--" if value is None else f"{value * 100:.1f}%"


def _format_signed_percent(value: float | None) -> str:
    if value is None:
        return "--"
    sign = "+" if value >= 0 else "-"
    return f"{sign}{abs(value) * 100:.1f}%"


def _format_dollars(value: float | None) -> str:
    return "--" if value is None else f"${value:.2f}"


def _format_signed_dollars(value: float | None) -> str:
    if value is None:
        return "--"
    sign = "+" if value >= 0 else "-"
    return f"{sign}${abs(value):.2f}"


def _format_ev(value: float | None) -> str:
    if value is None:
        return "--"
    sign = "+" if value >= 0 else "-"
    return f"{sign}${abs(value):.2f}"


def _format_temp(value: float | None, *, digits: int) -> str:
    return "--" if value is None else f"{value:.{digits}f}F"


def _format_strike(value: float) -> str:
    return str(int(value)) if value.is_integer() else f"{value:.1f}"
