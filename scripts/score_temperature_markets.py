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


ROOT = Path(__file__).resolve().parents[1]
KALSHI_PATH = ROOT / "output" / "kalshi" / "latest_markets.json"
WEATHER_PATH = ROOT / "output" / "weather" / "latest_forecasts.json"
NOAA_WEATHER_PATH = ROOT / "output" / "weather" / "latest_forecasts_noaa.json"
CALIBRATION_PATH = ROOT / "output" / "models" / "temperature_calibration.json"
FORECAST_ERROR_MODEL_PATH = ROOT / "output" / "models" / "forecast_error_model.json"
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
    series_key: str | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    noaa_snapshot = find_weather_snapshot(noaa_snapshots, series_key)
    open_meteo_snapshot = find_weather_snapshot(open_meteo_snapshots, series_key)
    return (noaa_snapshot or open_meteo_snapshot), open_meteo_snapshot


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


def calibrated_sigma(
    snapshot: dict[str, Any], daily_row: dict[str, Any], forecast_date: str, calibration: dict[str, Any]
) -> float:
    heuristic = estimate_sigma(snapshot, daily_row)
    location = snapshot.get("market", {}).get("location")
    month_key = str(int(forecast_date[5:7]))
    location_stats = calibration.get("locations", {}).get(location, {})
    month_stats = location_stats.get("monthly", {}).get(month_key)

    if not month_stats:
        return heuristic

    climatology_sigma = float(month_stats.get("sigma_f", heuristic))
    return round((0.65 * heuristic) + (0.35 * climatology_sigma), 2)


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
    month_key = str(int(forecast_date[5:7]))
    bucket_stats = (
        error_model.get("locations", {})
        .get(location, {})
        .get(month_key, {})
        .get(lead_bucket)
    )
    if not bucket_stats:
        return adjusted_mean, sigma, None

    mean_error = float(bucket_stats.get("mean_error_f", 0.0))
    sigma_error = float(bucket_stats.get("sigma_error_f", sigma))
    corrected_mean = round(adjusted_mean - mean_error, 2)
    corrected_sigma = round((0.7 * sigma) + (0.3 * sigma_error), 2)
    return corrected_mean, corrected_sigma, bucket_stats


def climatology_adjusted_mean(
    forecast_max: float, location: str, forecast_date: str, calibration: dict[str, Any]
) -> tuple[float, dict[str, Any] | None]:
    month_key = str(int(forecast_date[5:7]))
    month_stats = (
        calibration.get("locations", {})
        .get(location, {})
        .get("monthly", {})
        .get(month_key)
    )
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
    *, strike_type: str | None, floor_strike: Any, cap_strike: Any, forecast_max: float, sigma: float
) -> float | None:
    if strike_type == "greater" and floor_strike is not None:
        return 1.0 - normal_cdf(float(floor_strike), forecast_max, sigma)

    if strike_type == "less" and cap_strike is not None:
        return normal_cdf(float(cap_strike), forecast_max, sigma)

    if strike_type == "between" and floor_strike is not None and cap_strike is not None:
        upper = normal_cdf(float(cap_strike), forecast_max, sigma)
        lower = normal_cdf(float(floor_strike), forecast_max, sigma)
        return max(0.0, upper - lower)

    return None


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

    spread_text = ""
    if isinstance(noaa_forecast_max, (int, float)) and isinstance(open_meteo_forecast_max, (int, float)):
        spread = round(float(open_meteo_forecast_max) - float(noaa_forecast_max), 1)
        if abs(spread) < 0.5:
            spread_text = "; forecast sources aligned"
        elif spread > 0:
            spread_text = f"; Open-Meteo is {spread:.0f}F hotter than NOAA"
        else:
            spread_text = f"; Open-Meteo is {abs(spread):.0f}F cooler than NOAA"

    target_text = f"Adjusted model {adjusted_mean:.0f}F"
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
    calibration: dict[str, Any],
    error_model: dict[str, Any],
) -> dict[str, Any]:
    scored = []

    for market in kalshi_payload.get("markets", []):
        series_key = extract_series_key(market)
        weather_snapshot, open_meteo_snapshot = pick_primary_snapshot(
            noaa_snapshots, open_meteo_snapshots, series_key
        )
        if not weather_snapshot:
            continue

        target_date = parse_market_date(market.get("title", ""))
        if not target_date:
            continue

        forecast_row = get_forecast_row(weather_snapshot, target_date)
        if not forecast_row:
            continue

        open_meteo_row = get_forecast_row(open_meteo_snapshot, target_date) if open_meteo_snapshot else None

        forecast_max = forecast_row.get("temperature_2m_max")
        if forecast_max is None:
            continue

        mapping = weather_mapping(weather_snapshot)
        location = mapping.get("location") or weather_snapshot.get("market", {}).get("location")
        adjusted_mean, month_stats = climatology_adjusted_mean(
            float(forecast_max), location, target_date, calibration
        )
        sigma = calibrated_sigma(weather_snapshot, forecast_row, target_date, calibration)
        lead_bucket = lead_bucket_for_snapshot(weather_snapshot, target_date)
        adjusted_mean, sigma, bucket_stats = apply_forecast_error_adjustment(
            adjusted_mean, sigma, location, target_date, lead_bucket, error_model
        )
        model_prob = predict_probability(
            strike_type=market.get("strike_type"),
            floor_strike=market.get("floor_strike"),
            cap_strike=market.get("cap_strike"),
            forecast_max=adjusted_mean,
            sigma=sigma,
        )
        if model_prob is None:
            continue

        kalshi_prob = market.get("implied_probability")
        if kalshi_prob is None:
            continue

        edge = model_prob - float(kalshi_prob)
        climatology_mean = float(month_stats["mean_high_f"]) if month_stats else None
        historical_bias = float(bucket_stats["mean_error_f"]) if bucket_stats else None
        open_meteo_forecast_max = (
            round(float(open_meteo_row.get("temperature_2m_max")), 2)
            if open_meteo_row and open_meteo_row.get("temperature_2m_max") is not None
            else None
        )
        noaa_forecast_max = round(float(forecast_max), 2)
        forecast_source_spread = (
            round(open_meteo_forecast_max - noaa_forecast_max, 2)
            if open_meteo_forecast_max is not None
            else None
        )

        scored.append(
            {
                **market,
                "model_probability": round(model_prob, 4),
                "edge": round(edge, 4),
                "forecast_date": target_date,
                "forecast_max_f": noaa_forecast_max,
                "noaa_forecast_max_f": noaa_forecast_max,
                "open_meteo_forecast_max_f": open_meteo_forecast_max,
                "forecast_source_spread_f": forecast_source_spread,
                "adjusted_forecast_max_f": adjusted_mean,
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
        calibration = load_calibration()
        error_model = load_forecast_error_model()
    except FileNotFoundError as error:
        print(f"Missing input file: {error}", file=sys.stderr)
        return 1

    payload = score_markets(kalshi_payload, noaa_snapshots, open_meteo_snapshots, calibration, error_model)
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
