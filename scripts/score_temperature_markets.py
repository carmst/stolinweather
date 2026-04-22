#!/usr/bin/env python3
"""Match Kalshi temperature markets to weather snapshots and score them."""

from __future__ import annotations

import json
import math
import re
import subprocess
import sys
import argparse
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from load_json_to_postgres import DB_SCHEMA_PATH, resolve_connection, run_sql_file, sync_scored_payload
from weather_feature_utils import build_combined_features, floor_checkpoint, is_morning_same_day_checkpoint, predict_residual, provider_prefix


ROOT = Path(__file__).resolve().parents[1]
KALSHI_PATH = ROOT / "output" / "kalshi" / "latest_markets.json"
WEATHER_PATH = ROOT / "output" / "weather" / "latest_forecasts.json"
NOAA_WEATHER_PATH = ROOT / "output" / "weather" / "latest_forecasts_noaa.json"
VISUAL_CROSSING_WEATHER_PATH = ROOT / "output" / "weather" / "latest_forecasts_visual_crossing.json"
CALIBRATION_PATH = ROOT / "output" / "models" / "temperature_calibration.json"
FORECAST_ERROR_MODEL_PATH = ROOT / "output" / "models" / "forecast_error_model.json"
HIGH_TEMP_RESIDUAL_MODEL_PATH = ROOT / "output" / "models" / "high_temp_residual_model.json"
PRELIMINARY_HIGHS_PATH = ROOT / "output" / "preliminary" / "latest_preliminary_daily_highs.json"
OUTPUT_DIR = ROOT / "output" / "models"
LATEST_PATH = OUTPUT_DIR / "latest_scored_markets.json"
SNAPSHOT_DIR = OUTPUT_DIR / "snapshots"

TITLE_DATE_RE = re.compile(r"on ([A-Za-z]{3} \d{1,2}, \d{4})\?")


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_weather_snapshots(path: Path) -> list[dict[str, Any]]:
    if path.exists():
        return load_json(path)
    return []


def load_calibration() -> dict[str, Any]:
    if CALIBRATION_PATH.exists():
        return load_json(CALIBRATION_PATH)
    return {"locations": {}}


def load_forecast_error_model() -> dict[str, Any]:
    if FORECAST_ERROR_MODEL_PATH.exists():
        return load_json(FORECAST_ERROR_MODEL_PATH)
    return {"locations": {}}


def load_high_temp_residual_model() -> dict[str, Any]:
    if HIGH_TEMP_RESIDUAL_MODEL_PATH.exists():
        return load_json(HIGH_TEMP_RESIDUAL_MODEL_PATH)
    return {"active": False}


def load_preliminary_highs() -> dict[tuple[str, str], dict[str, Any]]:
    if not PRELIMINARY_HIGHS_PATH.exists():
        return {}
    payload = load_json(PRELIMINARY_HIGHS_PATH)
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for row in payload.get("rows", []):
        market_id = row.get("market_id")
        forecast_date = row.get("forecast_date")
        if market_id and forecast_date:
            rows[(market_id, forecast_date)] = row
    return rows


def ensure_output_dirs() -> None:
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)


def extract_series_key(market: dict[str, Any]) -> str | None:
    event_ticker = market.get("event_ticker") or ""
    if "-" in event_ticker:
        return event_ticker.split("-", 1)[0]
    return market.get("series_ticker")


def find_weather_snapshot(
    weather_snapshots: list[dict[str, Any]], series_key: str | None
) -> dict[str, Any] | None:
    if not series_key:
        return None

    for snapshot in weather_snapshots:
        market = snapshot.get("market", {})
        if series_key in market.get("kalshi_series", []):
            return snapshot
    return None


def weather_mapping(snapshot: dict[str, Any]) -> dict[str, Any]:
    market = snapshot.get("market", {})
    return {
        "location": market.get("location"),
        "latitude": market.get("latitude"),
        "longitude": market.get("longitude"),
    }


def parse_market_date(title: str) -> str | None:
    match = TITLE_DATE_RE.search(title or "")
    if not match:
        return None

    parsed = datetime.strptime(match.group(1), "%b %d, %Y")
    return parsed.strftime("%Y-%m-%d")


def get_forecast_row(snapshot: dict[str, Any], target_date: str) -> dict[str, Any] | None:
    for row in snapshot.get("daily", []):
        if row.get("date") == target_date:
            return row
    return None


def pick_primary_snapshot(
    noaa_snapshots: list[dict[str, Any]],
    open_meteo_snapshots: list[dict[str, Any]],
    visual_crossing_snapshots: list[dict[str, Any]],
    series_key: str | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None]:
    noaa_snapshot = find_weather_snapshot(noaa_snapshots, series_key)
    open_meteo_snapshot = find_weather_snapshot(open_meteo_snapshots, series_key)
    visual_crossing_snapshot = find_weather_snapshot(visual_crossing_snapshots, series_key)
    return (
        noaa_snapshot or visual_crossing_snapshot or open_meteo_snapshot,
        open_meteo_snapshot,
        visual_crossing_snapshot,
    )


def median(values: list[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def logistic(value: float) -> float:
    return 1.0 / (1.0 + math.exp(-value))


def normal_cdf(value: float, mean: float, stddev: float) -> float:
    if stddev <= 0:
        return 1.0 if value >= mean else 0.0
    z_score = (value - mean) / (stddev * math.sqrt(2.0))
    return 0.5 * (1.0 + math.erf(z_score))


def estimate_sigma(snapshot: dict[str, Any], daily_row: dict[str, Any]) -> float:
    current = snapshot.get("current", {})
    wind = float(current.get("wind_speed_10m") or 0.0)
    gust = float(current.get("wind_gusts_10m") or 0.0)
    diurnal_range = abs(
        float(daily_row.get("temperature_2m_max") or 0.0)
        - float(daily_row.get("temperature_2m_min") or 0.0)
    )

    base_sigma = 2.5
    wind_component = min(2.0, wind / 20.0 + max(0.0, gust - wind) / 25.0)
    range_component = min(1.5, diurnal_range / 30.0)
    return base_sigma + wind_component + range_component


def apply_source_spread_adjustment(sigma: float, source_values: list[float]) -> float:
    if len(source_values) < 2:
        return sigma
    spread = max(source_values) - min(source_values)
    return round(sigma + min(2.0, spread * 0.35), 2)


def same_day_intraday_sigma(
    *,
    sigma: float,
    snapshot: dict[str, Any],
    forecast_date: str,
    forecast_max: float,
    source_values: list[float],
    feature_row: dict[str, Any] | None,
    preliminary_high: float | None,
) -> tuple[float, str | None]:
    pulled_at = datetime.fromisoformat(snapshot["pulled_at"].replace("Z", "+00:00"))
    tz_name = snapshot.get("market", {}).get("timezone", "UTC")
    local_pulled_at = pulled_at.astimezone(ZoneInfo(tz_name))
    if local_pulled_at.date().isoformat() != forecast_date:
        return sigma, None

    local_hour = local_pulled_at.hour + (local_pulled_at.minute / 60.0)
    if local_hour < 8:
        return sigma, None

    if local_hour >= 16:
        live_sigma = 1.1
    elif local_hour >= 14:
        live_sigma = 1.35
    elif local_hour >= 12:
        live_sigma = 1.65
    else:
        live_sigma = 2.1

    hours_to_peak = None
    if feature_row:
        raw_hours_to_peak = feature_row.get("blended_hours_to_forecast_high")
        if isinstance(raw_hours_to_peak, (int, float)):
            hours_to_peak = float(raw_hours_to_peak)
            if hours_to_peak <= -2:
                live_sigma = min(live_sigma, 0.95)
            elif hours_to_peak <= 0:
                live_sigma = min(live_sigma, 1.05)
            elif hours_to_peak <= 2:
                live_sigma = min(live_sigma, 1.2)
            elif hours_to_peak <= 4:
                live_sigma = min(live_sigma, 1.55)

    if preliminary_high is not None:
        remaining_to_forecast = forecast_max - preliminary_high
        if remaining_to_forecast <= 0.5:
            live_sigma = min(live_sigma, 0.95 if hours_to_peak is not None and hours_to_peak <= 1 else 1.1)
        elif remaining_to_forecast <= 1.5 and hours_to_peak is not None and hours_to_peak <= 2:
            live_sigma = min(live_sigma, 1.1)

    spread = max(source_values) - min(source_values) if len(source_values) >= 2 else 0.0
    live_sigma += min(0.6, spread * 0.12)
    live_sigma = max(0.75, live_sigma)
    tightened = round(min(sigma, live_sigma), 2)
    if tightened == sigma:
        return sigma, None

    reason = f"intraday {local_hour:.1f}h"
    if hours_to_peak is not None:
        reason += f", {hours_to_peak:.1f}h to forecast high"
    if preliminary_high is not None:
        reason += f", prelim {preliminary_high:.1f}F"
    if spread:
        reason += f", provider spread {spread:.1f}F"
    return tightened, reason


def calibrated_sigma(
    snapshot: dict[str, Any], daily_row: dict[str, Any], forecast_date: str, calibration: dict[str, Any]
) -> float:
    heuristic = estimate_sigma(snapshot, daily_row)
    location = snapshot.get("market", {}).get("location")
    location_stats = calibration.get("locations", {}).get(location, {})
    climatology_stats = calibration_stats_for_date(location_stats, forecast_date)

    if not climatology_stats:
        return heuristic

    climatology_sigma = float(climatology_stats.get("sigma_f", heuristic))
    return round((0.65 * heuristic) + (0.35 * climatology_sigma), 2)


def date_bucket_keys(forecast_date: str) -> tuple[str, str]:
    parsed = datetime.strptime(forecast_date, "%Y-%m-%d").date()
    return str(int(parsed.isocalendar().week)), str(parsed.month)


def calibration_stats_for_date(location_stats: dict[str, Any], forecast_date: str) -> dict[str, Any] | None:
    week_key, month_key = date_bucket_keys(forecast_date)
    return location_stats.get("weekly", {}).get(week_key) or location_stats.get("monthly", {}).get(month_key)


def forecast_target_timestamp(snapshot: dict[str, Any], forecast_date: str) -> datetime:
    tz_name = snapshot.get("market", {}).get("timezone", "UTC")
    target = datetime.strptime(forecast_date, "%Y-%m-%d").date()
    local = datetime.combine(target, time(15, 0), tzinfo=ZoneInfo(tz_name))
    return local.astimezone(timezone.utc)


def lead_bucket_for_snapshot(snapshot: dict[str, Any], forecast_date: str) -> str:
    pulled_at = datetime.fromisoformat(snapshot["pulled_at"].replace("Z", "+00:00"))
    lead_hours = (forecast_target_timestamp(snapshot, forecast_date) - pulled_at).total_seconds() / 3600.0
    if lead_hours < 18:
        return "same_day"
    if lead_hours < 42:
        return "next_day"
    if lead_hours < 66:
        return "day_2"
    return "day_3_plus"


def apply_forecast_error_adjustment(
    adjusted_mean: float,
    sigma: float,
    location: str,
    forecast_date: str,
    lead_bucket: str,
    error_model: dict[str, Any],
) -> tuple[float, float, dict[str, Any] | None]:
    week_key, month_key = date_bucket_keys(forecast_date)
    weekly_stats = error_model.get("weekly_locations", {}).get(location, {}).get(week_key, {}).get(lead_bucket)
    monthly_stats = error_model.get("locations", {}).get(location, {}).get(month_key, {}).get(lead_bucket)
    bucket_stats = weekly_stats or monthly_stats
    if not bucket_stats:
        return adjusted_mean, sigma, None

    mean_error = float(bucket_stats.get("mean_error_f", 0.0))
    if weekly_stats and monthly_stats:
        weekly_count = float(weekly_stats.get("count") or 0)
        weekly_mean = float(weekly_stats.get("mean_error_f", 0.0))
        monthly_mean = float(monthly_stats.get("mean_error_f", 0.0))
        blend_weight = weekly_count / (weekly_count + 500.0)
        blended_mean = (blend_weight * weekly_mean) + ((1.0 - blend_weight) * monthly_mean)
        max_weekly_delta = 0.75
        mean_error = max(monthly_mean - max_weekly_delta, min(monthly_mean + max_weekly_delta, blended_mean))
        bucket_stats = {
            **weekly_stats,
            "bucket_source": "weekly_shrunk_to_month",
            "raw_weekly_mean_error_f": weekly_mean,
            "monthly_mean_error_f": monthly_mean,
            "mean_error_f": round(mean_error, 2),
        }
    sigma_error = float(bucket_stats.get("sigma_error_f", sigma))
    corrected_mean = round(adjusted_mean - mean_error, 2)
    corrected_sigma = round((0.7 * sigma) + (0.3 * sigma_error), 2)
    return corrected_mean, corrected_sigma, bucket_stats


def climatology_adjusted_mean(
    forecast_max: float, location: str, forecast_date: str, calibration: dict[str, Any]
) -> tuple[float, dict[str, Any] | None]:
    location_stats = calibration.get("locations", {}).get(location, {})
    month_stats = calibration_stats_for_date(location_stats, forecast_date)
    if not month_stats:
        return forecast_max, None

    climatology_mean = float(month_stats["mean_high_f"])
    climatology_sigma = float(month_stats["sigma_f"])
    anomaly = forecast_max - climatology_mean

    # Pull extreme anomalies slightly back toward climatology so thin-tail contracts
    # do not dominate the board from a single forecast print.
    shrink_factor = 0.82 if abs(anomaly) > climatology_sigma else 0.9
    adjusted_mean = climatology_mean + (anomaly * shrink_factor)
    return round(adjusted_mean, 2), month_stats


def predict_probability(
    *,
    strike_type: str | None,
    floor_strike: Any,
    cap_strike: Any,
    forecast_max: float,
    sigma: float,
    observed_floor: float | None = None,
) -> float | None:
    denominator = 1.0
    if observed_floor is not None:
        denominator = max(0.0001, 1.0 - normal_cdf(observed_floor, forecast_max, sigma))

    # Kalshi high-temperature contracts are selected against integer high
    # buckets. Use floor-style bins so 84.9F still belongs to the 83-84 bucket.
    if strike_type == "greater" and floor_strike is not None:
        lower = float(floor_strike) + 1.0
        if observed_floor is not None and observed_floor >= lower:
            return 1.0
        return (1.0 - normal_cdf(lower, forecast_max, sigma)) / denominator

    if strike_type == "less" and cap_strike is not None:
        upper = float(cap_strike)
        if observed_floor is not None:
            if observed_floor >= upper:
                return 0.0
            return max(0.0, normal_cdf(upper, forecast_max, sigma) - normal_cdf(observed_floor, forecast_max, sigma)) / denominator
        return normal_cdf(upper, forecast_max, sigma)

    if strike_type == "between" and floor_strike is not None and cap_strike is not None:
        lower_bound = float(floor_strike)
        upper_bound = float(cap_strike) + 1.0
        if observed_floor is not None:
            if observed_floor >= upper_bound:
                return 0.0
            lower_bound = max(lower_bound, observed_floor)
        upper = normal_cdf(upper_bound, forecast_max, sigma)
        lower = normal_cdf(lower_bound, forecast_max, sigma)
        return max(0.0, upper - lower) / denominator

    return None


def offsetless_open_meteo_is_utc(provider: str | None, forecast_timezone: str | None) -> bool:
    return provider == "open-meteo" and (forecast_timezone or "UTC").upper() in {"UTC", "GMT"}


def parse_hourly_time(
    value: Any,
    tz_name: str,
    provider: str | None = None,
    forecast_timezone: str | None = None,
) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(
            tzinfo=timezone.utc
            if offsetless_open_meteo_is_utc(provider, forecast_timezone)
            else ZoneInfo(tz_name)
        )
    return parsed.astimezone(ZoneInfo(tz_name))


def contract_path_bounds(strike_type: str | None, floor_strike: Any, cap_strike: Any) -> tuple[float | None, float | None]:
    if strike_type == "greater" and floor_strike is not None:
        return float(floor_strike) + 1.0, None
    if strike_type == "less" and cap_strike is not None:
        return None, float(cap_strike)
    if strike_type == "between" and floor_strike is not None and cap_strike is not None:
        return float(floor_strike), float(cap_strike) + 1.0
    return None, None


def hourly_temperatures_for_date(
    snapshot: dict[str, Any] | None,
    target_date: str,
    *,
    remaining_only: bool,
) -> list[float]:
    if not snapshot:
        return []

    tz_name = snapshot.get("market", {}).get("timezone", "UTC")
    provider = snapshot.get("provider")
    forecast_timezone = snapshot.get("forecast_timezone")
    pulled_at = datetime.fromisoformat(snapshot["pulled_at"].replace("Z", "+00:00")).astimezone(ZoneInfo(tz_name))
    rows: list[float] = []
    for row in snapshot.get("hourly", []):
        local_time = parse_hourly_time(row.get("time"), tz_name, provider, forecast_timezone)
        if not local_time or local_time.date().isoformat() != target_date:
            continue
        if remaining_only and local_time < pulled_at:
            continue
        temp = row.get("temperature_2m")
        if isinstance(temp, (int, float)):
            rows.append(float(temp))
    return rows


def hourly_path_pressure(
    *,
    provider_snapshots: dict[str, dict[str, Any]],
    target_date: str,
    lead_bucket: str,
    strike_type: str | None,
    floor_strike: Any,
    cap_strike: Any,
) -> dict[str, Any] | None:
    lower, upper = contract_path_bounds(strike_type, floor_strike, cap_strike)
    if lower is None and upper is None:
        return None

    remaining_only = lead_bucket == "same_day"
    provider_rows = []
    for provider, snapshot in provider_snapshots.items():
        temps = hourly_temperatures_for_date(snapshot, target_date, remaining_only=remaining_only)
        if not temps:
            continue

        if upper is not None:
            violation_distances = [max(0.0, temp - upper) for temp in temps]
            violating_hours = sum(1 for distance in violation_distances if distance > 0)
            distance_pressure = sum(violation_distances)
            max_gap = max(violation_distances) if violation_distances else 0.0
        else:
            max_temp = max(temps)
            max_gap = max(0.0, (lower or 0.0) - max_temp)
            violating_hours = len(temps) if max_gap > 0 else 0
            distance_pressure = max_gap * max(1, len(temps) / 4.0)

        provider_rows.append(
            {
                "provider": provider,
                "hour_count": len(temps),
                "violating_hours": violating_hours,
                "distance_pressure_f": round(distance_pressure, 2),
                "max_gap_f": round(max_gap, 2),
                "max_hourly_forecast_f": round(max(temps), 2),
            }
        )

    if not provider_rows:
        return None

    total_hours = sum(row["hour_count"] for row in provider_rows)
    total_violations = sum(row["violating_hours"] for row in provider_rows)
    total_distance = sum(row["distance_pressure_f"] for row in provider_rows)
    pressure_score = (total_violations / max(1, total_hours)) + (total_distance / max(1.0, total_hours * 5.0))

    return {
        "hour_count": total_hours,
        "violating_hour_count": total_violations,
        "distance_pressure_f": round(total_distance, 2),
        "pressure_score": round(pressure_score, 4),
        "providers": provider_rows,
    }


def apply_hourly_path_pressure(model_prob: float, pressure: dict[str, Any] | None) -> float:
    if not pressure:
        return model_prob

    pressure_score = float(pressure.get("pressure_score") or 0.0)
    if pressure_score <= 0:
        return model_prob

    penalty = min(0.85, pressure_score * 0.55)
    return max(0.001, min(0.999, model_prob * (1.0 - penalty)))


def pick_yes_pricing(market: dict[str, Any], model_prob: float) -> tuple[str, float | None, float]:
    cost_candidates = [
        market.get("yes_ask_dollars"),
        market.get("last_price_dollars"),
        market.get("yes_bid_dollars"),
        market.get("implied_probability"),
    ]
    contract_cost = next(
        (value for value in cost_candidates if isinstance(value, (int, float))),
        None,
    )
    recommended_side = "yes" if isinstance(contract_cost, (int, float)) and model_prob > contract_cost else "pass"
    return recommended_side, contract_cost, model_prob


def compute_city_model_rank_score(
    market: dict[str, Any],
    *,
    model_prob: float,
    edge: float,
    adjusted_mean: float,
    sigma: float,
) -> tuple[float, str, float | None, float | None]:
    recommended_side, contract_cost, win_prob = pick_yes_pricing(market, model_prob)
    expected_value = (win_prob - contract_cost) if isinstance(contract_cost, (int, float)) else None
    conviction = abs(model_prob - 0.5)
    tradable = isinstance(contract_cost, (int, float)) and 0.03 <= contract_cost <= 0.97

    strike_type = market.get("strike_type")
    floor_strike = market.get("floor_strike")
    cap_strike = market.get("cap_strike")
    strike_gap = 0.0
    if strike_type == "greater" and floor_strike is not None:
        strike_gap = adjusted_mean - float(floor_strike)
    elif strike_type == "less" and cap_strike is not None:
        strike_gap = float(cap_strike) - adjusted_mean
    elif strike_type == "between" and floor_strike is not None and cap_strike is not None:
        low = float(floor_strike)
        high = float(cap_strike)
        if low <= adjusted_mean <= high:
            strike_gap = min(adjusted_mean - low, high - adjusted_mean)
        elif adjusted_mean < low:
            strike_gap = adjusted_mean - low
        else:
            strike_gap = high - adjusted_mean

    normalized_gap = strike_gap / sigma if sigma > 0 else 0.0
    value_component = (expected_value if expected_value is not None else -1.0) * 180.0
    edge_component = edge * 100.0
    conviction_component = conviction * 50.0
    gap_component = normalized_gap * 12.0
    tradable_component = 8.0 if tradable else -25.0
    score = round(
        value_component +
        edge_component +
        conviction_component +
        gap_component +
        tradable_component,
        2,
    )
    return score, recommended_side, contract_cost, expected_value


def build_signal_comment(
    *,
    matched_location: str,
    matched_lat: float,
    matched_lon: float,
    forecast_max: float,
    adjusted_mean: float,
    sigma: float,
    climatology_mean: float | None,
    lead_bucket: str,
    historical_bias: float | None,
) -> str:
    rounded_sigma = f"{sigma:.1f}"
    climatology_text = (
        f"; climatology mean high {climatology_mean:.1f}F" if climatology_mean is not None else ""
    )
    bias_text = f"; lead-bucket {lead_bucket} bias {historical_bias:+.1f}F" if historical_bias is not None else ""
    return (
        f"Matched weather grid for {matched_location} "
        f"({matched_lat:.4f}, {matched_lon:.4f}); forecast high {forecast_max:.1f}F "
        f"(adjusted {adjusted_mean:.1f}F) with {rounded_sigma}F sigma"
        f"{climatology_text}"
        f"{bias_text}"
    )


def build_market_context_comment(
    *,
    noaa_forecast_max: float | None,
    open_meteo_forecast_max: float | None,
    visual_crossing_forecast_max: float | None,
    consensus_forecast_max: float,
    adjusted_mean: float,
    floor_strike: Any,
    cap_strike: Any,
    strike_type: str | None,
    lead_bucket: str,
) -> str:
    lead_label = (
        "today" if lead_bucket == "same_day" else "tomorrow" if lead_bucket == "next_day" else "later"
    )
    source_bits = []

    if isinstance(noaa_forecast_max, (int, float)):
        source_bits.append(f"NOAA {noaa_forecast_max:.0f}F")
    if isinstance(open_meteo_forecast_max, (int, float)):
        source_bits.append(f"Open-Meteo {open_meteo_forecast_max:.0f}F")
    if isinstance(visual_crossing_forecast_max, (int, float)):
        source_bits.append(f"Visual Crossing {visual_crossing_forecast_max:.0f}F")

    spread_text = ""
    available = [
        float(value)
        for value in (noaa_forecast_max, open_meteo_forecast_max, visual_crossing_forecast_max)
        if isinstance(value, (int, float))
    ]
    if len(available) >= 2:
        spread = round(max(available) - min(available), 1)
        if spread < 0.5:
            spread_text = "; forecast sources aligned"
        else:
            spread_text = f"; provider spread {spread:.0f}F"

    target_text = f"Consensus {consensus_forecast_max:.0f}F, adjusted model {adjusted_mean:.0f}F"
    if strike_type == "greater" and floor_strike is not None:
        target_text += f" vs {float(floor_strike):.0f}F yes cutoff"
    elif strike_type == "less" and cap_strike is not None:
        target_text += f" vs {float(cap_strike):.0f}F yes cutoff"
    elif strike_type == "between" and floor_strike is not None and cap_strike is not None:
        target_text += f" vs {float(floor_strike):.0f}-{float(cap_strike):.0f}F yes range"

    if source_bits:
        return f"{' | '.join(source_bits)}{spread_text}; {target_text} {lead_label}"
    return f"{target_text} {lead_label}"


def build_concise_signal(
    *, strike_type: str | None, floor_strike: Any, cap_strike: Any, adjusted_mean: float, lead_bucket: str
) -> str:
    if lead_bucket == "same_day":
        lead_label = "today"
    elif lead_bucket == "next_day":
        lead_label = "tomorrow"
    elif lead_bucket == "day_2":
        lead_label = "in 2 days"
    else:
        lead_label = "later"

    if strike_type == "greater" and floor_strike is not None:
        delta = adjusted_mean - float(floor_strike)
        direction = "above" if delta >= 0 else "below"
        return f"Forecast {abs(delta):.0f}F {direction} {float(floor_strike):.0f}F threshold {lead_label}"

    if strike_type == "less" and cap_strike is not None:
        delta = adjusted_mean - float(cap_strike)
        direction = "below" if delta <= 0 else "above"
        return f"Forecast {abs(delta):.0f}F {direction} {float(cap_strike):.0f}F threshold {lead_label}"

    if strike_type == "between" and floor_strike is not None and cap_strike is not None:
        low = float(floor_strike)
        high = float(cap_strike)
        if low <= adjusted_mean <= high:
            return f"Forecast inside {low:.0f}-{high:.0f}F range {lead_label}"
        if adjusted_mean < low:
            return f"Forecast {low - adjusted_mean:.0f}F below {low:.0f}-{high:.0f}F range {lead_label}"
        return f"Forecast {adjusted_mean - high:.0f}F above {low:.0f}-{high:.0f}F range {lead_label}"

    return f"Forecast near {adjusted_mean:.0f}F {lead_label}"


def score_markets(
    kalshi_payload: dict[str, Any],
    noaa_snapshots: list[dict[str, Any]],
    open_meteo_snapshots: list[dict[str, Any]],
    visual_crossing_snapshots: list[dict[str, Any]],
    calibration: dict[str, Any],
    error_model: dict[str, Any],
    high_temp_residual_model: dict[str, Any],
    preliminary_highs: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    scored = []

    for market in kalshi_payload.get("markets", []):
        series_key = extract_series_key(market)
        noaa_snapshot = find_weather_snapshot(noaa_snapshots, series_key)
        open_meteo_snapshot = find_weather_snapshot(open_meteo_snapshots, series_key)
        visual_crossing_snapshot = find_weather_snapshot(visual_crossing_snapshots, series_key)
        weather_snapshot = noaa_snapshot or visual_crossing_snapshot or open_meteo_snapshot
        if not weather_snapshot:
            continue

        target_date = parse_market_date(market.get("title", ""))
        if not target_date:
            continue

        forecast_row = get_forecast_row(weather_snapshot, target_date)
        if not forecast_row:
            continue

        open_meteo_row = get_forecast_row(open_meteo_snapshot, target_date) if open_meteo_snapshot else None
        visual_crossing_row = (
            get_forecast_row(visual_crossing_snapshot, target_date) if visual_crossing_snapshot else None
        )

        noaa_forecast_max = (
            round(float(forecast_row.get("temperature_2m_max")), 2)
            if forecast_row.get("temperature_2m_max") is not None
            else None
        )
        open_meteo_forecast_max = (
            round(float(open_meteo_row.get("temperature_2m_max")), 2)
            if open_meteo_row and open_meteo_row.get("temperature_2m_max") is not None
            else None
        )
        visual_crossing_forecast_max = (
            round(float(visual_crossing_row.get("temperature_2m_max")), 2)
            if visual_crossing_row and visual_crossing_row.get("temperature_2m_max") is not None
            else None
        )
        source_values = [
            float(value)
            for value in (noaa_forecast_max, open_meteo_forecast_max, visual_crossing_forecast_max)
            if value is not None
        ]
        if not source_values:
            continue
        forecast_max = round(median(source_values), 2)

        mapping = weather_mapping(weather_snapshot)
        location = mapping.get("location") or weather_snapshot.get("market", {}).get("location")
        adjusted_mean, month_stats = climatology_adjusted_mean(
            float(forecast_max), location, target_date, calibration
        )
        sigma = calibrated_sigma(weather_snapshot, forecast_row, target_date, calibration)
        sigma = apply_source_spread_adjustment(sigma, source_values)
        lead_bucket = lead_bucket_for_snapshot(weather_snapshot, target_date)
        adjusted_mean, sigma, bucket_stats = apply_forecast_error_adjustment(
            adjusted_mean, sigma, location, target_date, lead_bucket, error_model
        )
        market_id = weather_snapshot.get("market", {}).get("market_id")
        preliminary_row = preliminary_highs.get((market_id, target_date)) if market_id else None
        preliminary_high = (
            float(preliminary_row["preliminary_high_f"])
            if preliminary_row and isinstance(preliminary_row.get("preliminary_high_f"), (int, float))
            else None
        )
        provider_snapshots = {
            provider_prefix(snapshot.get("provider")): snapshot
            for snapshot in (noaa_snapshot, open_meteo_snapshot, visual_crossing_snapshot)
            if snapshot
        }
        feature_row = None
        if provider_snapshots:
            latest_pulled_at = max(
                snapshot["pulled_at"]
                for snapshot in provider_snapshots.values()
                if snapshot.get("pulled_at")
            )
            feature_row = build_combined_features(
                market_id=market_id,
                location=location,
                forecast_date=target_date,
                checkpoint_at=floor_checkpoint(latest_pulled_at),
                provider_snapshots=provider_snapshots,
            )
        feature_model_adjustment = None
        if high_temp_residual_model.get("active") and feature_row and is_morning_same_day_checkpoint(feature_row):
            feature_model_adjustment = predict_residual(feature_row, high_temp_residual_model)
            if feature_model_adjustment is not None:
                adjusted_mean = round(float(forecast_max) + feature_model_adjustment, 2)
        if preliminary_high is not None:
            adjusted_mean = round(max(adjusted_mean, preliminary_high), 2)
        sigma, intraday_sigma_reason = same_day_intraday_sigma(
            sigma=sigma,
            snapshot=weather_snapshot,
            forecast_date=target_date,
            forecast_max=float(forecast_max),
            source_values=source_values,
            feature_row=feature_row,
            preliminary_high=preliminary_high,
        )
        model_prob = predict_probability(
            strike_type=market.get("strike_type"),
            floor_strike=market.get("floor_strike"),
            cap_strike=market.get("cap_strike"),
            forecast_max=adjusted_mean,
            sigma=sigma,
            observed_floor=preliminary_high if lead_bucket == "same_day" else None,
        )
        if model_prob is None:
            continue
        raw_model_prob = model_prob
        path_pressure = hourly_path_pressure(
            provider_snapshots=provider_snapshots,
            target_date=target_date,
            lead_bucket=lead_bucket,
            strike_type=market.get("strike_type"),
            floor_strike=market.get("floor_strike"),
            cap_strike=market.get("cap_strike"),
        )
        model_prob = apply_hourly_path_pressure(model_prob, path_pressure)

        kalshi_prob = market.get("implied_probability")
        if kalshi_prob is None:
            continue

        edge = model_prob - float(kalshi_prob)
        city_model_rank_score, recommended_side, side_contract_cost, side_expected_value = compute_city_model_rank_score(
            market,
            model_prob=model_prob,
            edge=edge,
            adjusted_mean=adjusted_mean,
            sigma=sigma,
        )
        climatology_mean = float(month_stats["mean_high_f"]) if month_stats else None
        historical_bias = float(bucket_stats["mean_error_f"]) if bucket_stats else None
        forecast_source_spread = (
            round(max(source_values) - min(source_values), 2)
            if len(source_values) >= 2
            else None
        )

        scored.append(
            {
                **market,
                "model_probability": round(model_prob, 4),
                "raw_model_probability": round(raw_model_prob, 4),
                "edge": round(edge, 4),
                "city_model_rank_score": city_model_rank_score,
                "model_recommended_side": recommended_side,
                "model_contract_cost": round(side_contract_cost, 4) if isinstance(side_contract_cost, (int, float)) else None,
                "model_expected_value": round(side_expected_value, 4) if isinstance(side_expected_value, (int, float)) else None,
                "hourly_path_pressure": path_pressure,
                "hourly_path_pressure_score": path_pressure.get("pressure_score") if path_pressure else None,
                "hourly_path_violation_hours": path_pressure.get("violating_hour_count") if path_pressure else None,
                "hourly_path_hours": path_pressure.get("hour_count") if path_pressure else None,
                "hourly_path_distance_pressure_f": path_pressure.get("distance_pressure_f") if path_pressure else None,
                "forecast_date": target_date,
                "forecast_max_f": forecast_max,
                "noaa_forecast_max_f": noaa_forecast_max,
                "open_meteo_forecast_max_f": open_meteo_forecast_max,
                "visual_crossing_forecast_max_f": visual_crossing_forecast_max,
                "forecast_source_spread_f": forecast_source_spread,
                "adjusted_forecast_max_f": adjusted_mean,
                "feature_model_adjustment_f": round(feature_model_adjustment, 3) if feature_model_adjustment is not None else None,
                "feature_model_active": bool(high_temp_residual_model.get("active")),
                "preliminary_high_f": round(preliminary_high, 2) if preliminary_high is not None else None,
                "preliminary_high_observed_at": preliminary_row.get("max_observed_at") if preliminary_row else None,
                "intraday_sigma_reason": intraday_sigma_reason,
                "forecast_min_f": forecast_row.get("temperature_2m_min"),
                "forecast_sigma_f": round(sigma, 2),
                "lead_bucket": lead_bucket,
                "matched_location": mapping.get("location") or weather_snapshot.get("market", {}).get("location"),
                "matched_latitude": mapping.get("latitude") or weather_snapshot.get("market", {}).get("latitude"),
                "matched_longitude": mapping.get("longitude") or weather_snapshot.get("market", {}).get("longitude"),
                "weather_market_id": weather_snapshot.get("market", {}).get("market_id"),
                "signal_short": build_concise_signal(
                    strike_type=market.get("strike_type"),
                    floor_strike=market.get("floor_strike"),
                    cap_strike=market.get("cap_strike"),
                    adjusted_mean=adjusted_mean,
                    lead_bucket=lead_bucket,
                ),
                "market_context": build_market_context_comment(
                    noaa_forecast_max=noaa_forecast_max,
                    open_meteo_forecast_max=open_meteo_forecast_max,
                    visual_crossing_forecast_max=visual_crossing_forecast_max,
                    consensus_forecast_max=forecast_max,
                    adjusted_mean=adjusted_mean,
                    floor_strike=market.get("floor_strike"),
                    cap_strike=market.get("cap_strike"),
                    strike_type=market.get("strike_type"),
                    lead_bucket=lead_bucket,
                ),
                "model_signal": build_signal_comment(
                    matched_location=location,
                    matched_lat=mapping.get("latitude") or weather_snapshot.get("market", {}).get("latitude"),
                    matched_lon=mapping.get("longitude") or weather_snapshot.get("market", {}).get("longitude"),
                    forecast_max=float(forecast_max),
                    adjusted_mean=adjusted_mean,
                    sigma=sigma,
                    climatology_mean=climatology_mean,
                    lead_bucket=lead_bucket,
                    historical_bias=historical_bias,
                ),
            }
        )

    scored.sort(key=lambda item: item.get("close_time") or "")

    return {
        "pulled_at": kalshi_payload.get("pulled_at"),
        "source": "temperature-scorer-v1",
        "weather_snapshot_count": len(noaa_snapshots) or len(open_meteo_snapshots),
        "noaa_snapshot_count": len(noaa_snapshots),
        "open_meteo_snapshot_count": len(open_meteo_snapshots),
        "visual_crossing_snapshot_count": len(visual_crossing_snapshots),
        "market_count": len(scored),
        "markets": scored,
    }


def write_outputs(payload: dict[str, Any]) -> Path:
    ensure_output_dirs()
    with LATEST_PATH.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")

    day = (payload.get("pulled_at") or "unknown-date")[:10]
    snapshot_path = SNAPSHOT_DIR / f"{day}.jsonl"
    with snapshot_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload))
        handle.write("\n")
    return snapshot_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sync-db", action="store_true", help="Also upsert the scored payload into Postgres.")
    parser.add_argument("--database-url", help="Postgres connection string. Falls back to DATABASE_URL.")
    parser.add_argument(
        "--init-db-schema",
        action="store_true",
        help="Apply db/schema.sql before syncing to Postgres.",
    )
    args = parser.parse_args()

    try:
        kalshi_payload = load_json(KALSHI_PATH)
        noaa_snapshots = load_weather_snapshots(NOAA_WEATHER_PATH)
        open_meteo_snapshots = load_weather_snapshots(WEATHER_PATH)
        visual_crossing_snapshots = load_weather_snapshots(VISUAL_CROSSING_WEATHER_PATH)
        calibration = load_calibration()
        error_model = load_forecast_error_model()
        high_temp_residual_model = load_high_temp_residual_model()
        preliminary_highs = load_preliminary_highs()
    except FileNotFoundError as error:
        print(f"Missing input file: {error}", file=sys.stderr)
        return 1

    payload = score_markets(
        kalshi_payload,
        noaa_snapshots,
        open_meteo_snapshots,
        visual_crossing_snapshots,
        calibration,
        error_model,
        high_temp_residual_model,
        preliminary_highs,
    )
    snapshot_path = write_outputs(payload)

    print(f"Scored {payload['market_count']} temperature markets")
    print(f"Latest file: {LATEST_PATH}")
    print(f"History file: {snapshot_path}")

    if args.sync_db:
        try:
            psql, database_url = resolve_connection(args.database_url)
            if args.init_db_schema:
                run_sql_file(psql, database_url, DB_SCHEMA_PATH)
            sync_scored_payload(psql, database_url, payload)
        except (RuntimeError, subprocess.CalledProcessError) as error:
            print(f"Postgres sync error: {error}", file=sys.stderr)
            return 1
        print("Synced scored markets to Postgres")

    if payload["markets"]:
        first = payload["markets"][0]
        print(
            f"First match: {first['title']} | forecast high {first['forecast_max_f']}F | "
            f"Kalshi {first['implied_probability']:.2f} | model {first['model_probability']:.2f}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
