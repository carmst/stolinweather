#!/usr/bin/env python3
"""Backtest daily high forecast error by checkpoint hour, city, and provider."""

from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

from weather_feature_utils import is_morning_same_day_checkpoint, predict_residual, safe_float


ROOT = Path(__file__).resolve().parents[1]
TRAINING_PATH = ROOT / "output" / "models" / "forecast_training_features.json"
MODEL_PATH = ROOT / "output" / "models" / "high_temp_residual_model.json"
OUTPUT_JSON_PATH = ROOT / "output" / "models" / "high_temp_checkpoint_backtest.json"
OUTPUT_CSV_PATH = ROOT / "output" / "models" / "high_temp_checkpoint_backtest.csv"

PROVIDER_FORECAST_COLUMNS = {
    "blend": "blended_forecast_max_f",
    "noaa": "noaa_daily_max_f",
    "open_meteo": "open_meteo_daily_max_f",
    "visual_crossing": "visual_crossing_daily_max_f",
}


def mae(errors: list[float]) -> float | None:
    if not errors:
        return None
    return sum(abs(error) for error in errors) / len(errors)


def rmse(errors: list[float]) -> float | None:
    if not errors:
        return None
    return math.sqrt(sum(error * error for error in errors) / len(errors))


def summarize(errors: list[float]) -> dict[str, Any]:
    absolute_errors = [abs(error) for error in errors]
    within_one = sum(1 for error in absolute_errors if error <= 1.0)
    hot_bias_count = sum(1 for error in errors if error > 0)
    cold_bias_count = sum(1 for error in errors if error < 0)
    signed_bias = mean(errors) if errors else None
    return {
        "row_count": len(errors),
        "signed_bias_f": round(signed_bias, 3) if signed_bias is not None else None,
        "median_signed_error_f": round(median(errors), 3) if errors else None,
        "bias_direction": "too_hot" if signed_bias and signed_bias > 0 else "too_cold" if signed_bias and signed_bias < 0 else "neutral",
        "hot_bias_count": hot_bias_count,
        "cold_bias_count": cold_bias_count,
        "hot_bias_rate": round(hot_bias_count / len(errors), 3) if errors else None,
        "cold_bias_rate": round(cold_bias_count / len(errors), 3) if errors else None,
        "within_1f_count": within_one,
        "within_1f_rate": round(within_one / len(errors), 3) if errors else None,
        "mae_f": round(mae(errors), 3) if errors else None,
        "rmse_f": round(rmse(errors), 3) if errors else None,
    }


def checkpoint_hour(row: dict[str, Any]) -> int | None:
    value = safe_float(row.get("local_checkpoint_hour"))
    if value is None:
        return None
    return int(value)


def candidate_model_forecast(row: dict[str, Any], model: dict[str, Any]) -> float | None:
    if not model:
        return None
    if not is_morning_same_day_checkpoint(row):
        return None
    residual = predict_residual(row, model)
    blended = safe_float(row.get("blended_forecast_max_f"))
    if residual is None or blended is None:
        return None
    return blended + residual


def append_error(
    groups: dict[tuple[str, str, str], list[float]],
    *,
    group: str,
    key: str,
    row: dict[str, Any],
    forecast: float | None,
) -> None:
    actual = safe_float(row.get("observed_max_f"))
    if actual is None or forecast is None:
        return
    groups[(group, key, "all")].append(float(forecast) - actual)
    groups[(group, key, row.get("market_id") or "unknown")].append(float(forecast) - actual)


def main() -> int:
    training_payload = json.loads(TRAINING_PATH.read_text())
    model = json.loads(MODEL_PATH.read_text()) if MODEL_PATH.exists() else {}
    rows = [
        row
        for row in training_payload.get("rows", [])
        if row.get("observed_max_f") is not None and row.get("blended_forecast_max_f") is not None
    ]

    groups: dict[tuple[str, str, str], list[float]] = defaultdict(list)

    for row in rows:
        hour = checkpoint_hour(row)
        if hour is None:
            continue
        hour_key = f"{hour:02d}:00"
        for provider, column in PROVIDER_FORECAST_COLUMNS.items():
            forecast = safe_float(row.get(column))
            append_error(groups, group=f"hour:{provider}", key=hour_key, row=row, forecast=forecast)
        if model:
            append_error(
                groups,
                group="hour:candidate_model",
                key=hour_key,
                row=row,
                forecast=candidate_model_forecast(row, model),
            )

        for provider, column in PROVIDER_FORECAST_COLUMNS.items():
            forecast = safe_float(row.get(column))
            append_error(groups, group=f"lead_bucket:{provider}", key=row.get("lead_bucket") or "unknown", row=row, forecast=forecast)
        if model:
            append_error(
                groups,
                group="lead_bucket:candidate_model",
                key=row.get("lead_bucket") or "unknown",
                row=row,
                forecast=candidate_model_forecast(row, model),
            )

    summaries = []
    for (group, key, market_id), errors in sorted(groups.items()):
        summary = summarize(errors)
        summaries.append(
            {
                "group": group,
                "key": key,
                "market_id": market_id,
                **summary,
            }
        )

    directional_bias_candidates = sorted(
        [
            row
            for row in summaries
            if row["market_id"] != "all"
            and row["row_count"] >= 20
            and row["signed_bias_f"] is not None
            and abs(row["signed_bias_f"]) >= 1.0
        ],
        key=lambda row: (-abs(row["signed_bias_f"]), -row["row_count"]),
    )[:25]

    all_market_directional_bias = sorted(
        [
            row
            for row in summaries
            if row["market_id"] == "all"
            and row["row_count"] >= 20
            and row["signed_bias_f"] is not None
        ],
        key=lambda row: (-abs(row["signed_bias_f"]), -row["row_count"]),
    )[:25]

    payload = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source": "high-temp-checkpoint-backtest-v2",
        "training_row_count": len(rows),
        "candidate_model_active": bool(model.get("active")),
        "candidate_model_source": model.get("source"),
        "error_convention": "signed error = forecast_high_f - observed_high_f; positive means forecast/model was too hot, negative means too cold",
        "directional_bias_candidates": directional_bias_candidates,
        "all_market_directional_bias": all_market_directional_bias,
        "summaries": summaries,
    }

    OUTPUT_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    with OUTPUT_CSV_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "group",
                "key",
                "market_id",
                "row_count",
                "signed_bias_f",
                "median_signed_error_f",
                "bias_direction",
                "hot_bias_count",
                "cold_bias_count",
                "hot_bias_rate",
                "cold_bias_rate",
                "within_1f_count",
                "within_1f_rate",
                "mae_f",
                "rmse_f",
            ],
        )
        writer.writeheader()
        writer.writerows(summaries)

    print(f"Wrote checkpoint backtest JSON: {OUTPUT_JSON_PATH}")
    print(f"Wrote checkpoint backtest CSV: {OUTPUT_CSV_PATH}")
    print(f"Training rows: {len(rows)}")
    print(f"Candidate model active: {bool(model.get('active'))}")
    print("Largest all-market signed biases:")
    for row in all_market_directional_bias[:10]:
        print(
            f"  {row['group']} {row['key']} | n={row['row_count']} | "
            f"signed bias={row['signed_bias_f']}F ({row['bias_direction']}) | "
            f"hot={row['hot_bias_rate']} cold={row['cold_bias_rate']}"
        )
    print("Largest city/provider signed biases:")
    for row in directional_bias_candidates[:10]:
        print(
            f"  {row['market_id']} {row['group']} {row['key']} | n={row['row_count']} | "
            f"signed bias={row['signed_bias_f']}F ({row['bias_direction']}) | "
            f"hot={row['hot_bias_rate']} cold={row['cold_bias_rate']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
