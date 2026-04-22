"""Feature extraction helpers for high-temperature forecast models."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from statistics import mean, median
from typing import Any
from zoneinfo import ZoneInfo


PROVIDER_PREFIXES = {
    "noaa-nws": "noaa",
    "noaa": "noaa",
    "open-meteo": "open_meteo",
    "visual-crossing": "visual_crossing",
}

TEXT_FLAG_TERMS = {
    "clear": ("clear", "sunny"),
    "cloud": ("cloud", "overcast"),
    "rain": ("rain", "shower"),
    "storm": ("storm", "thunder"),
    "fog": ("fog", "haze", "mist"),
    "wind": ("wind", "breezy", "gust"),
    "snow": ("snow",),
}

NUMERIC_FEATURE_KEYS = (
    "blended_forecast_max_f",
    "provider_spread_f",
    "source_count",
    "month",
    "day_of_year",
    "lead_hours",
    "local_checkpoint_hour",
    "blended_hours_to_forecast_high",
    "noaa_daily_max_f",
    "noaa_daily_min_f",
    "noaa_daily_precip_sum",
    "noaa_daily_wind_max",
    "noaa_current_temp_f",
    "noaa_current_dew_point_f",
    "noaa_current_humidity_pct",
    "noaa_current_pressure",
    "noaa_current_precip",
    "noaa_current_cloud_cover",
    "noaa_current_wind_speed",
    "noaa_current_wind_gust",
    "noaa_hourly_temp_max_f",
    "noaa_hourly_temp_mean_f",
    "noaa_hourly_dew_point_mean_f",
    "noaa_hourly_humidity_mean_pct",
    "noaa_hourly_pressure_mean",
    "noaa_hourly_precip_sum",
    "noaa_hourly_cloud_cover_mean",
    "noaa_hourly_wind_speed_max",
    "noaa_hourly_wind_gust_max",
    "noaa_hourly_pop_max",
    "noaa_hours_to_forecast_high",
    "noaa_forecast_high_local_hour",
    "open_meteo_daily_max_f",
    "open_meteo_daily_min_f",
    "open_meteo_daily_precip_sum",
    "open_meteo_daily_wind_max",
    "open_meteo_current_temp_f",
    "open_meteo_current_dew_point_f",
    "open_meteo_current_humidity_pct",
    "open_meteo_current_pressure",
    "open_meteo_current_precip",
    "open_meteo_current_cloud_cover",
    "open_meteo_current_wind_speed",
    "open_meteo_current_wind_gust",
    "open_meteo_hourly_temp_max_f",
    "open_meteo_hourly_temp_mean_f",
    "open_meteo_hourly_dew_point_mean_f",
    "open_meteo_hourly_humidity_mean_pct",
    "open_meteo_hourly_pressure_mean",
    "open_meteo_hourly_precip_sum",
    "open_meteo_hourly_cloud_cover_mean",
    "open_meteo_hourly_wind_speed_max",
    "open_meteo_hourly_wind_gust_max",
    "open_meteo_hours_to_forecast_high",
    "open_meteo_forecast_high_local_hour",
    "visual_crossing_daily_max_f",
    "visual_crossing_daily_min_f",
    "visual_crossing_daily_precip_sum",
    "visual_crossing_daily_wind_max",
    "visual_crossing_current_temp_f",
    "visual_crossing_current_dew_point_f",
    "visual_crossing_current_humidity_pct",
    "visual_crossing_current_pressure",
    "visual_crossing_current_precip",
    "visual_crossing_current_cloud_cover",
    "visual_crossing_current_wind_speed",
    "visual_crossing_current_wind_gust",
    "visual_crossing_hourly_temp_max_f",
    "visual_crossing_hourly_temp_mean_f",
    "visual_crossing_hourly_dew_point_mean_f",
    "visual_crossing_hourly_humidity_mean_pct",
    "visual_crossing_hourly_pressure_mean",
    "visual_crossing_hourly_precip_sum",
    "visual_crossing_hourly_cloud_cover_mean",
    "visual_crossing_hourly_wind_speed_max",
    "visual_crossing_hourly_wind_gust_max",
    "visual_crossing_hours_to_forecast_high",
    "visual_crossing_forecast_high_local_hour",
    "text_clear",
    "text_cloud",
    "text_rain",
    "text_storm",
    "text_fog",
    "text_wind",
    "text_snow",
)


def safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def floor_checkpoint(value: str, minutes: int = 10) -> str:
    dt = parse_timestamp(value).astimezone(timezone.utc)
    floored_minute = (dt.minute // minutes) * minutes
    floored = dt.replace(minute=floored_minute, second=0, microsecond=0)
    return floored.isoformat().replace("+00:00", "Z")


def provider_prefix(provider: str | None) -> str:
    return PROVIDER_PREFIXES.get(provider or "", (provider or "unknown").replace("-", "_"))


def get_daily_row(snapshot: dict[str, Any], forecast_date: str) -> dict[str, Any] | None:
    for row in snapshot.get("daily", []):
        if row.get("date") == forecast_date:
            return row
    return None


def forecast_target_timestamp(snapshot: dict[str, Any], forecast_date: str) -> datetime:
    tz_name = snapshot.get("market", {}).get("timezone", "UTC")
    local_date = datetime.strptime(forecast_date, "%Y-%m-%d").date()
    local = datetime(local_date.year, local_date.month, local_date.day, 15, 0, tzinfo=ZoneInfo(tz_name))
    return local.astimezone(timezone.utc)


def local_checkpoint_hour(snapshot: dict[str, Any], checkpoint_at: str) -> float:
    tz_name = snapshot.get("market", {}).get("timezone", "UTC")
    local = parse_timestamp(checkpoint_at).astimezone(ZoneInfo(tz_name))
    return round(local.hour + (local.minute / 60.0), 2)


def lead_hours(snapshot: dict[str, Any], forecast_date: str, checkpoint_at: str) -> float:
    checkpoint = parse_timestamp(checkpoint_at)
    return round((forecast_target_timestamp(snapshot, forecast_date) - checkpoint).total_seconds() / 3600.0, 2)


def lead_bucket(hours: float) -> str:
    if hours < 18:
        return "same_day"
    if hours < 42:
        return "next_day"
    if hours < 66:
        return "day_2"
    return "day_3_plus"


def is_morning_same_day_checkpoint(row: dict[str, Any]) -> bool:
    lead = safe_float(row.get("lead_hours"))
    local_hour = safe_float(row.get("local_checkpoint_hour"))
    return (
        row.get("lead_bucket") == "same_day"
        and lead is not None
        and local_hour is not None
        and lead >= 4
        and 5 <= local_hour <= 10
    )


def offsetless_open_meteo_is_utc(provider: str | None, forecast_timezone: str | None) -> bool:
    return provider == "open-meteo" and (forecast_timezone or "UTC").upper() in {"UTC", "GMT"}


def parse_hourly_time(
    row: dict[str, Any],
    tz_name: str,
    provider: str | None = None,
    forecast_timezone: str | None = None,
) -> datetime | None:
    value = row.get("time")
    if not value:
        return None
    try:
        dt = parse_timestamp(value)
    except ValueError:
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(
            tzinfo=timezone.utc
            if offsetless_open_meteo_is_utc(provider, forecast_timezone)
            else ZoneInfo(tz_name)
        )
    return dt


def local_date_from_hourly_time(
    row: dict[str, Any],
    tz_name: str,
    provider: str | None = None,
    forecast_timezone: str | None = None,
) -> str | None:
    dt = parse_hourly_time(row, tz_name, provider, forecast_timezone)
    if dt is None:
        return None
    return dt.astimezone(ZoneInfo(tz_name)).date().isoformat()


def hourly_rows_for_date(snapshot: dict[str, Any], forecast_date: str) -> list[dict[str, Any]]:
    tz_name = snapshot.get("market", {}).get("timezone", "UTC")
    provider = snapshot.get("provider")
    forecast_timezone = snapshot.get("forecast_timezone")
    return [
        row
        for row in snapshot.get("hourly", [])
        if local_date_from_hourly_time(row, tz_name, provider, forecast_timezone) == forecast_date
    ]


def values(rows: list[dict[str, Any]], key: str) -> list[float]:
    return [parsed for row in rows if (parsed := safe_float(row.get(key))) is not None]


def maybe_mean(items: list[float]) -> float | None:
    return round(mean(items), 3) if items else None


def maybe_max(items: list[float]) -> float | None:
    return round(max(items), 3) if items else None


def maybe_sum(items: list[float]) -> float | None:
    return round(sum(items), 3) if items else None


def hourly_peak_timing_features(
    hourly_rows: list[dict[str, Any]],
    tz_name: str,
    checkpoint_at: str,
    provider: str | None = None,
    forecast_timezone: str | None = None,
) -> tuple[float | None, float | None]:
    peak: tuple[float, datetime] | None = None
    for row in hourly_rows:
        temp = safe_float(row.get("temperature_2m"))
        timestamp = parse_hourly_time(row, tz_name, provider, forecast_timezone)
        if temp is None or timestamp is None:
            continue
        if peak is None or temp > peak[0]:
            peak = (temp, timestamp)
    if peak is None:
        return None, None

    checkpoint = parse_timestamp(checkpoint_at)
    peak_time = peak[1]
    if checkpoint.tzinfo is None:
        checkpoint = checkpoint.replace(tzinfo=timezone.utc)
    hours_to_peak = round((peak_time.astimezone(timezone.utc) - checkpoint.astimezone(timezone.utc)).total_seconds() / 3600.0, 2)
    local_peak_time = peak_time.astimezone(ZoneInfo(tz_name))
    local_peak_hour = round(local_peak_time.hour + (local_peak_time.minute / 60.0), 2)
    return hours_to_peak, local_peak_hour


def text_flags(snapshot: dict[str, Any], hourly_rows: list[dict[str, Any]]) -> dict[str, int]:
    text_parts = []
    current = snapshot.get("current", {})
    for key in ("text_description", "conditions", "short_forecast"):
        if current.get(key):
            text_parts.append(str(current[key]))
    for row in hourly_rows:
        if row.get("short_forecast"):
            text_parts.append(str(row["short_forecast"]))
    text = " ".join(text_parts).lower()
    return {
        f"text_{name}": int(any(term in text for term in terms))
        for name, terms in TEXT_FLAG_TERMS.items()
    }


def provider_features(snapshot: dict[str, Any], forecast_date: str, checkpoint_at: str) -> dict[str, Any] | None:
    daily = get_daily_row(snapshot, forecast_date)
    if not daily:
        return None

    prefix = provider_prefix(snapshot.get("provider"))
    current = snapshot.get("current", {})
    hourly = hourly_rows_for_date(snapshot, forecast_date)
    tz_name = snapshot.get("market", {}).get("timezone", "UTC")
    provider = snapshot.get("provider")
    forecast_timezone = snapshot.get("forecast_timezone")
    hours_to_peak, local_peak_hour = hourly_peak_timing_features(
        hourly, tz_name, checkpoint_at, provider, forecast_timezone
    )
    features: dict[str, Any] = {
        f"{prefix}_daily_max_f": safe_float(daily.get("temperature_2m_max")),
        f"{prefix}_daily_min_f": safe_float(daily.get("temperature_2m_min")),
        f"{prefix}_daily_precip_sum": safe_float(daily.get("precipitation_sum")),
        f"{prefix}_daily_wind_max": safe_float(daily.get("wind_speed_10m_max")),
        f"{prefix}_daily_wind_gust_max": safe_float(daily.get("wind_gusts_10m_max")),
        f"{prefix}_current_temp_f": safe_float(current.get("temperature_2m")),
        f"{prefix}_current_dew_point_f": safe_float(current.get("dew_point_2m")),
        f"{prefix}_current_humidity_pct": safe_float(current.get("relative_humidity_2m")),
        f"{prefix}_current_pressure": safe_float(current.get("pressure_msl")),
        f"{prefix}_current_precip": safe_float(current.get("precipitation")),
        f"{prefix}_current_cloud_cover": safe_float(current.get("cloud_cover")),
        f"{prefix}_current_wind_speed": safe_float(current.get("wind_speed_10m")),
        f"{prefix}_current_wind_gust": safe_float(current.get("wind_gusts_10m")),
        f"{prefix}_hourly_temp_max_f": maybe_max(values(hourly, "temperature_2m")),
        f"{prefix}_hourly_temp_mean_f": maybe_mean(values(hourly, "temperature_2m")),
        f"{prefix}_hourly_dew_point_mean_f": maybe_mean(values(hourly, "dew_point_2m")),
        f"{prefix}_hourly_humidity_mean_pct": maybe_mean(values(hourly, "relative_humidity_2m")),
        f"{prefix}_hourly_pressure_mean": maybe_mean(values(hourly, "pressure_msl")),
        f"{prefix}_hourly_precip_sum": maybe_sum(values(hourly, "precipitation")),
        f"{prefix}_hourly_cloud_cover_mean": maybe_mean(values(hourly, "cloud_cover")),
        f"{prefix}_hourly_wind_speed_max": maybe_max(values(hourly, "wind_speed_10m")),
        f"{prefix}_hourly_wind_gust_max": maybe_max(values(hourly, "wind_gusts_10m")),
        f"{prefix}_hourly_pop_max": maybe_max(values(hourly, "probability_of_precipitation")),
        f"{prefix}_hours_to_forecast_high": hours_to_peak,
        f"{prefix}_forecast_high_local_hour": local_peak_hour,
    }
    features.update(text_flags(snapshot, hourly))
    return features


def build_combined_features(
    *,
    market_id: str,
    location: str | None,
    forecast_date: str,
    checkpoint_at: str,
    provider_snapshots: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    provider_rows: dict[str, dict[str, Any]] = {}
    source_values: list[float] = []
    peak_hour_values: list[float] = []
    text_flags_merged = {f"text_{name}": 0 for name in TEXT_FLAG_TERMS}
    reference_snapshot = next(iter(provider_snapshots.values()), None)
    if not reference_snapshot:
        return None

    for provider, snapshot in provider_snapshots.items():
        features = provider_features(snapshot, forecast_date, checkpoint_at)
        if not features:
            continue
        provider_rows[provider] = features
        prefix = provider_prefix(provider)
        daily_max = features.get(f"{prefix}_daily_max_f")
        if daily_max is not None:
            source_values.append(float(daily_max))
        hours_to_peak = features.get(f"{prefix}_hours_to_forecast_high")
        if hours_to_peak is not None:
            peak_hour_values.append(float(hours_to_peak))
        for key in text_flags_merged:
            text_flags_merged[key] = max(text_flags_merged[key], int(features.get(key) or 0))

    if not source_values:
        return None

    hours = lead_hours(reference_snapshot, forecast_date, checkpoint_at)
    forecast_dt = datetime.strptime(forecast_date, "%Y-%m-%d")
    row: dict[str, Any] = {
        "market_id": market_id,
        "location": location,
        "checkpoint_at": checkpoint_at,
        "forecast_date": forecast_date,
        "month": forecast_dt.month,
        "day_of_year": int(forecast_dt.strftime("%j")),
        "lead_hours": hours,
        "lead_bucket": lead_bucket(hours),
        "local_checkpoint_hour": local_checkpoint_hour(reference_snapshot, checkpoint_at),
        "source_count": len(source_values),
        "blended_forecast_max_f": round(float(median(source_values)), 2),
        "provider_spread_f": round(max(source_values) - min(source_values), 2) if len(source_values) > 1 else 0.0,
        "blended_hours_to_forecast_high": round(float(median(peak_hour_values)), 2) if peak_hour_values else None,
    }
    for features in provider_rows.values():
        row.update(features)
    row.update(text_flags_merged)
    return row


def model_feature_vector(row: dict[str, Any], feature_order: list[str], means: dict[str, float], scales: dict[str, float]) -> list[float]:
    vector = []
    for feature in feature_order:
        if feature.startswith("market_id="):
            vector.append(1.0 if feature.split("=", 1)[1] == row.get("market_id") else 0.0)
        elif feature.startswith("lead_bucket="):
            vector.append(1.0 if feature.split("=", 1)[1] == row.get("lead_bucket") else 0.0)
        else:
            value = safe_float(row.get(feature))
            if value is None:
                value = means.get(feature, 0.0)
            scale = scales.get(feature, 1.0) or 1.0
            vector.append((float(value) - means.get(feature, 0.0)) / scale)
    return vector


def predict_residual(row: dict[str, Any], model: dict[str, Any]) -> float | None:
    if model.get("model_type") == "shrunken_bias_v1":
        return predict_shrunken_bias_residual(row, model)

    feature_order = model.get("feature_order") or []
    weights = model.get("weights") or []
    if not feature_order or not weights or len(feature_order) != len(weights):
        return None
    vector = model_feature_vector(row, feature_order, model.get("means", {}), model.get("scales", {}))
    intercept = float(model.get("intercept", 0.0))
    return intercept + sum(float(weight) * value for weight, value in zip(weights, vector))


def _bias_group_key(row: dict[str, Any], group_name: str) -> str | None:
    if group_name == "market_id":
        return row.get("market_id")
    if group_name == "market_week":
        market_id = row.get("market_id")
        forecast_date = row.get("forecast_date")
        if not market_id or not forecast_date:
            return None
        week = parse_timestamp(f"{forecast_date}T00:00:00+00:00").date().isocalendar().week
        return f"{market_id}|{int(week)}"
    if group_name == "market_month":
        market_id = row.get("market_id")
        month = safe_float(row.get("month"))
        if not market_id or month is None:
            return None
        return f"{market_id}|{int(month)}"
    return None


def predict_shrunken_bias_residual(row: dict[str, Any], model: dict[str, Any]) -> float | None:
    window = model.get("checkpoint_window") or {}
    local_hour = safe_float(row.get("local_checkpoint_hour"))
    lead_hours = safe_float(row.get("lead_hours"))
    if row.get("lead_bucket") != model.get("lead_bucket", "same_day"):
        return None
    if local_hour is None or lead_hours is None:
        return None
    if local_hour < float(window.get("min_hour", 0)) or local_hour > float(window.get("max_hour", 24)):
        return None
    if lead_hours < float(model.get("min_lead_hours", 0)):
        return None

    global_residual = float(model.get("global_residual_f") or 0.0)
    group_weights = model.get("group_weights") or {}
    group_tables = model.get("group_tables") or {}
    weighted_corrections = []
    for group_name, weight in group_weights.items():
        key = _bias_group_key(row, group_name)
        table = group_tables.get(group_name) or {}
        correction = table.get(key or "", {}).get("correction_f", global_residual)
        weighted_corrections.append((float(weight), float(correction)))

    if weighted_corrections:
        residual = sum(weight * correction for weight, correction in weighted_corrections) / sum(weight for weight, _ in weighted_corrections)
    else:
        residual = global_residual

    clip = safe_float(model.get("correction_clip_f"))
    if clip is not None:
        residual = max(-clip, min(clip, residual))
    return residual
