#!/usr/bin/env python3
"""Train a lightweight residual model for daily high-temperature forecasts."""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median

from weather_feature_utils import NUMERIC_FEATURE_KEYS, is_morning_same_day_checkpoint, model_feature_vector, safe_float


ROOT = Path(__file__).resolve().parents[1]
TRAINING_PATH = ROOT / "output" / "models" / "forecast_training_features.json"
OUTPUT_PATH = ROOT / "output" / "models" / "high_temp_residual_model.json"
REPORT_PATH = ROOT / "output" / "models" / "high_temp_residual_model_report.json"


def is_training_candidate(row: dict) -> bool:
    """Keep the first live model aligned with the morning-checkpoint use case."""
    return is_morning_same_day_checkpoint(row)


def rmse(errors: list[float]) -> float | None:
    if not errors:
        return None
    return math.sqrt(sum(error * error for error in errors) / len(errors))


def mae(errors: list[float]) -> float | None:
    if not errors:
        return None
    return sum(abs(error) for error in errors) / len(errors)


def summarize_errors(errors: list[float]) -> dict:
    if not errors:
        return {
            "row_count": 0,
            "mae_f": None,
            "rmse_f": None,
            "signed_bias_f": None,
            "median_signed_error_f": None,
            "within_1f_rate": None,
            "within_2f_rate": None,
        }
    return {
        "row_count": len(errors),
        "mae_f": round(mae(errors), 3),
        "rmse_f": round(rmse(errors), 3),
        "signed_bias_f": round(mean(errors), 3),
        "median_signed_error_f": round(median(errors), 3),
        "within_1f_rate": round(sum(1 for error in errors if abs(error) <= 1.0) / len(errors), 3),
        "within_2f_rate": round(sum(1 for error in errors if abs(error) <= 2.0) / len(errors), 3),
    }


def build_feature_order(rows: list[dict]) -> list[str]:
    market_ids = sorted({row["market_id"] for row in rows if row.get("market_id")})
    lead_buckets = sorted({row["lead_bucket"] for row in rows if row.get("lead_bucket")})
    numeric = [key for key in NUMERIC_FEATURE_KEYS if any(safe_float(row.get(key)) is not None for row in rows)]
    return numeric + [f"market_id={market_id}" for market_id in market_ids] + [f"lead_bucket={bucket}" for bucket in lead_buckets]


def stats_for_features(rows: list[dict], feature_order: list[str]) -> tuple[dict[str, float], dict[str, float]]:
    means: dict[str, float] = {}
    scales: dict[str, float] = {}
    for feature in feature_order:
        if feature.startswith(("market_id=", "lead_bucket=")):
            continue
        values = [safe_float(row.get(feature)) for row in rows]
        present = [float(value) for value in values if value is not None]
        if not present:
            means[feature] = 0.0
            scales[feature] = 1.0
            continue
        avg = mean(present)
        variance = sum((value - avg) ** 2 for value in present) / max(1, len(present) - 1)
        means[feature] = avg
        scales[feature] = math.sqrt(variance) or 1.0
    return means, scales


def predict(intercept: float, weights: list[float], vector: list[float]) -> float:
    return intercept + sum(weight * value for weight, value in zip(weights, vector))


def split_chronologically(rows: list[dict]) -> tuple[list[dict], list[dict], list[str]]:
    dates = sorted({row["forecast_date"] for row in rows if row.get("forecast_date")})
    if len(dates) < 4:
        return rows, [], []
    validation_date_count = min(3, max(1, len(dates) // 4))
    validation_dates = dates[-validation_date_count:]
    train_rows = [row for row in rows if row["forecast_date"] not in validation_dates]
    validation_rows = [row for row in rows if row["forecast_date"] in validation_dates]
    if len(train_rows) < 100 or len(validation_rows) < 20:
        return rows, [], []
    return train_rows, validation_rows, validation_dates


def train_ridge(rows: list[dict], feature_order: list[str], means: dict[str, float], scales: dict[str, float]) -> tuple[float, list[float]]:
    vectors = [model_feature_vector(row, feature_order, means, scales) for row in rows]
    targets = [float(row["residual_observed_minus_blended_f"]) for row in rows]
    intercept = mean(targets) if targets else 0.0
    weights = [0.0] * len(feature_order)
    learning_rate = 0.015
    l2 = 0.002

    for _ in range(450):
        intercept_grad = 0.0
        weight_grads = [0.0] * len(weights)
        for vector, target in zip(vectors, targets):
            error = predict(intercept, weights, vector) - target
            intercept_grad += error
            for i, value in enumerate(vector):
                weight_grads[i] += error * value
        n = max(1, len(vectors))
        intercept -= learning_rate * intercept_grad / n
        for i in range(len(weights)):
            weight_grad = (weight_grads[i] / n) + (l2 * weights[i])
            weights[i] -= learning_rate * weight_grad

    return intercept, weights


def evaluate(rows: list[dict], feature_order: list[str], means: dict[str, float], scales: dict[str, float], intercept: float, weights: list[float]) -> dict:
    baseline_errors = []
    model_errors = []
    for row in rows:
        actual = float(row["observed_max_f"])
        blended = float(row["blended_forecast_max_f"])
        residual = predict(intercept, weights, model_feature_vector(row, feature_order, means, scales))
        adjusted = blended + residual
        baseline_errors.append(blended - actual)
        model_errors.append(adjusted - actual)
    return {
        "row_count": len(rows),
        "baseline": summarize_errors(baseline_errors),
        "model": summarize_errors(model_errors),
        "baseline_mae_f": round(mae(baseline_errors), 3) if baseline_errors else None,
        "baseline_rmse_f": round(rmse(baseline_errors), 3) if baseline_errors else None,
        "model_mae_f": round(mae(model_errors), 3) if model_errors else None,
        "model_rmse_f": round(rmse(model_errors), 3) if model_errors else None,
    }


def evaluate_by_market(rows: list[dict], feature_order: list[str], means: dict[str, float], scales: dict[str, float], intercept: float, weights: list[float]) -> list[dict]:
    errors_by_market: dict[str, dict[str, list[float]]] = {}
    for row in rows:
        actual = float(row["observed_max_f"])
        blended = float(row["blended_forecast_max_f"])
        residual = predict(intercept, weights, model_feature_vector(row, feature_order, means, scales))
        adjusted = blended + residual
        market_id = row.get("market_id") or "unknown"
        bucket = errors_by_market.setdefault(market_id, {"baseline": [], "model": []})
        bucket["baseline"].append(blended - actual)
        bucket["model"].append(adjusted - actual)

    summaries = []
    for market_id, groups in errors_by_market.items():
        if len(groups["model"]) < 10:
            continue
        baseline = summarize_errors(groups["baseline"])
        model = summarize_errors(groups["model"])
        summaries.append(
            {
                "market_id": market_id,
                "row_count": model["row_count"],
                "baseline_mae_f": baseline["mae_f"],
                "model_mae_f": model["mae_f"],
                "mae_delta_f": round(model["mae_f"] - baseline["mae_f"], 3)
                if model["mae_f"] is not None and baseline["mae_f"] is not None
                else None,
                "model_signed_bias_f": model["signed_bias_f"],
            }
        )
    return sorted(summaries, key=lambda row: (row["mae_delta_f"] is None, row["mae_delta_f"] or 0, row["market_id"]))


def top_feature_weights(feature_order: list[str], weights: list[float], limit: int = 20) -> list[dict]:
    ranked = sorted(
        zip(feature_order, weights),
        key=lambda item: abs(item[1]),
        reverse=True,
    )[:limit]
    return [{"feature": feature, "weight": round(weight, 6)} for feature, weight in ranked]


def group_key(row: dict, group_name: str) -> str:
    if group_name == "market_id":
        return row.get("market_id") or "unknown"
    if group_name == "market_week":
        forecast_date = datetime.strptime(row["forecast_date"], "%Y-%m-%d").date()
        return f"{row.get('market_id') or 'unknown'}|{int(forecast_date.isocalendar().week)}"
    if group_name == "market_month":
        return f"{row.get('market_id') or 'unknown'}|{int(row.get('month') or 0)}"
    raise ValueError(f"Unknown group: {group_name}")


def build_group_table(rows: list[dict], group_name: str, global_residual: float, shrinkage_k: float) -> dict[str, dict]:
    grouped: dict[str, list[float]] = {}
    for row in rows:
        residual = float(row["observed_max_f"]) - float(row["blended_forecast_max_f"])
        grouped.setdefault(group_key(row, group_name), []).append(residual)

    table = {}
    for key, values in grouped.items():
        avg = mean(values)
        n = len(values)
        correction = (n / (n + shrinkage_k)) * avg + (shrinkage_k / (n + shrinkage_k)) * global_residual
        table[key] = {
            "row_count": n,
            "mean_residual_f": round(avg, 6),
            "correction_f": round(correction, 6),
        }
    return table


def predict_shrunken_bias(row: dict, model: dict) -> float | None:
    window = model.get("checkpoint_window", {})
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
    weighted = []
    for group_name, weight in group_weights.items():
        key = group_key(row, group_name)
        table = group_tables.get(group_name) or {}
        correction = table.get(key, {}).get("correction_f", global_residual)
        weighted.append((float(weight), float(correction)))
    if not weighted:
        residual = global_residual
    else:
        residual = sum(weight * correction for weight, correction in weighted) / sum(weight for weight, _ in weighted)

    clip = safe_float(model.get("correction_clip_f"))
    if clip is not None:
        residual = max(-clip, min(clip, residual))
    return residual


def train_shrunken_bias_model(train_rows: list[dict], *, shrinkage_k: float = 100.0) -> dict:
    residuals = [float(row["observed_max_f"]) - float(row["blended_forecast_max_f"]) for row in train_rows]
    global_residual = mean(residuals) if residuals else 0.0
    return {
        "model_type": "shrunken_bias_v1",
        "source": "high-temp-residual-shrunken-bias-v1",
        "target": "observed_high_f_minus_blended_forecast_high_f",
        "lead_bucket": "same_day",
        "min_lead_hours": 4,
        "checkpoint_window": {"min_hour": 7.5, "max_hour": 8.5, "label": "latest row around 8am local"},
        "shrinkage_k": shrinkage_k,
        "correction_clip_f": 4.0,
        "global_residual_f": round(global_residual, 6),
        "group_weights": {"market_id": 0.55, "market_week": 0.45},
        "group_tables": {
            "market_id": build_group_table(train_rows, "market_id", global_residual, shrinkage_k),
            "market_week": build_group_table(train_rows, "market_week", global_residual, shrinkage_k),
        },
    }


def evaluate_shrunken_bias(rows: list[dict], model: dict) -> dict:
    baseline_errors = []
    model_errors = []
    skipped = 0
    for row in rows:
        actual = float(row["observed_max_f"])
        blended = float(row["blended_forecast_max_f"])
        residual = predict_shrunken_bias(row, model)
        if residual is None:
            skipped += 1
            continue
        baseline_errors.append(blended - actual)
        model_errors.append(blended + residual - actual)
    return {
        "row_count": len(model_errors),
        "skipped_row_count": skipped,
        "baseline": summarize_errors(baseline_errors),
        "model": summarize_errors(model_errors),
        "baseline_mae_f": round(mae(baseline_errors), 3) if baseline_errors else None,
        "baseline_rmse_f": round(rmse(baseline_errors), 3) if baseline_errors else None,
        "model_mae_f": round(mae(model_errors), 3) if model_errors else None,
        "model_rmse_f": round(rmse(model_errors), 3) if model_errors else None,
    }


def main() -> int:
    payload = json.loads(TRAINING_PATH.read_text())
    all_rows = [
        row
        for row in payload.get("rows", [])
        if row.get("observed_max_f") is not None and row.get("blended_forecast_max_f") is not None
    ]
    rows = [row for row in all_rows if is_training_candidate(row)]
    exact_8am_rows = [
        row
        for row in rows
        if (hour := safe_float(row.get("local_checkpoint_hour"))) is not None and 7.5 <= hour <= 8.5
    ]
    if not rows:
        raise SystemExit("No morning-checkpoint model-ready training rows found.")
    if not exact_8am_rows:
        raise SystemExit("No 8am checkpoint model-ready training rows found.")

    train_rows, holdout_rows, holdout_dates = split_chronologically(exact_8am_rows)

    feature_order = build_feature_order(train_rows)
    means, scales = stats_for_features(train_rows, feature_order)
    intercept, weights = train_ridge(train_rows, feature_order, means, scales)

    ridge_train_metrics = evaluate(train_rows, feature_order, means, scales, intercept, weights)
    ridge_holdout_metrics = evaluate(holdout_rows, feature_order, means, scales, intercept, weights) if holdout_rows else None
    bias_model = train_shrunken_bias_model(train_rows, shrinkage_k=100.0)
    train_metrics = evaluate_shrunken_bias(train_rows, bias_model)
    holdout_metrics = evaluate_shrunken_bias(holdout_rows, bias_model) if holdout_rows else None
    active = bool(
        holdout_metrics
        and holdout_metrics["model_mae_f"] is not None
        and holdout_metrics["baseline_mae_f"] is not None
        and holdout_metrics["model_mae_f"] <= holdout_metrics["baseline_mae_f"]
    )

    model = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "active": active,
        "activation_rule": "active only when chronological holdout model MAE is <= blended forecast baseline MAE",
        "training_filter": "same_day rows, local checkpoint hour 7.5-8.5, lead_hours >= 4",
        "input_row_count": len(all_rows),
        "candidate_row_count": len(rows),
        "exact_8am_candidate_row_count": len(exact_8am_rows),
        "training_row_count": len(train_rows),
        "holdout_date": holdout_dates[-1] if holdout_dates else None,
        "holdout_dates": holdout_dates,
        "holdout_row_count": len(holdout_rows),
        **bias_model,
        "train_metrics": train_metrics,
        "holdout_metrics": holdout_metrics,
        "ridge_candidate": {
            "source": "high-temp-residual-ridge-v1",
            "feature_order": feature_order,
            "means": {key: round(value, 6) for key, value in means.items()},
            "scales": {key: round(value, 6) for key, value in scales.items()},
            "intercept": round(intercept, 6),
            "weights": [round(weight, 6) for weight in weights],
            "train_metrics": ridge_train_metrics,
            "holdout_metrics": ridge_holdout_metrics,
            "top_feature_weights": top_feature_weights(feature_order, weights),
        },
    }
    OUTPUT_PATH.write_text(json.dumps(model, indent=2) + "\n", encoding="utf-8")
    report = {
        "generated_at": model["generated_at"],
        "source": "high-temp-residual-model-report-v1",
        "model_path": str(OUTPUT_PATH.relative_to(ROOT)),
        "training_path": str(TRAINING_PATH.relative_to(ROOT)),
        "candidate_row_count": len(rows),
        "exact_8am_candidate_row_count": len(exact_8am_rows),
        "training_row_count": len(train_rows),
        "holdout_dates": holdout_dates,
        "train_metrics": train_metrics,
        "holdout_metrics": holdout_metrics,
        "model_type": model["model_type"],
        "global_residual_f": model["global_residual_f"],
        "group_weights": model["group_weights"],
        "ridge_candidate": model["ridge_candidate"],
        "holdout_market_metrics": evaluate_by_market(holdout_rows, feature_order, means, scales, intercept, weights)[:40]
        if holdout_rows
        else [],
    }
    REPORT_PATH.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote high-temp residual model: {model['model_type']}")
    print(f"Training rows: {len(train_rows)}")
    if holdout_rows:
        print(f"Holdout dates: {', '.join(holdout_dates)} ({len(holdout_rows)} rows)")
        print(f"Holdout baseline MAE: {model['holdout_metrics']['baseline_mae_f']}F")
        print(f"Holdout model MAE: {model['holdout_metrics']['model_mae_f']}F")
        print(f"Active for scoring: {model['active']}")
    print(f"Model file: {OUTPUT_PATH}")
    print(f"Report file: {REPORT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
