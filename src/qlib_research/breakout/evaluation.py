"""Evaluation helpers for breakout event models."""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd


def evaluate_breakout_scores(
    scored_events: pd.DataFrame,
    *,
    score_column: str = "signal_score",
    return_column: str | None = None,
    split_column: str | None = "dataset_split",
    top_percentiles: Iterable[float] = (5, 10, 20),
) -> dict[str, object]:
    """Compute event-model metrics with stable legacy keys."""

    if scored_events.empty:
        return {
            "event_count": 0,
            "evaluated_count": 0,
            "rank_ic": None,
            "top_quantile_mean_return": None,
            "top_quantile_hit_rate": None,
            "splits": {},
        }

    frame = scored_events.copy()
    if return_column is None:
        candidates = [column for column in frame.columns if str(column).startswith("future_return_")]
        return_column = candidates[0] if candidates else ""
    metrics = _metrics_for_frame(frame, score_column, return_column, top_percentiles)
    metrics["event_count"] = int(len(frame))
    metrics["evaluated_count"] = int(metrics.get("n_samples") or 0)
    metrics["rank_ic"] = metrics.get("spearman")
    metrics["top_quantile_mean_return"] = metrics.get("top20_mean_return")
    metrics["top_quantile_hit_rate"] = metrics.get("top20_hit_rate")

    splits: dict[str, object] = {}
    if split_column and split_column in frame.columns:
        for split_name, split_frame in frame.groupby(split_column, dropna=False):
            splits[str(split_name)] = _metrics_for_frame(split_frame, score_column, return_column, top_percentiles)
    metrics["splits"] = splits
    metrics["score_distribution"] = _distribution(frame.get(score_column))
    metrics["return_distribution"] = _distribution(frame.get(return_column) if return_column in frame else None)
    metrics["annual_slices"] = _annual_slices(frame, score_column, return_column)
    metrics["symbol_slices"] = _symbol_slices(frame, score_column, return_column)
    metrics["overfit_ratio"] = _overfit_ratio(splits)
    return _json_ready(metrics)


def feature_importance_frame(model: object, feature_columns: list[str]) -> pd.DataFrame:
    """Return LightGBM gain/split importance if available."""

    if not feature_columns:
        return pd.DataFrame(columns=["feature", "importance_gain", "importance_split"])
    gain = _importance(model, "gain", len(feature_columns))
    split = _importance(model, "split", len(feature_columns))
    frame = pd.DataFrame(
        {
            "feature": feature_columns,
            "importance_gain": gain,
            "importance_split": split,
        }
    )
    return frame.sort_values(["importance_gain", "importance_split"], ascending=False).reset_index(drop=True)


def _metrics_for_frame(frame: pd.DataFrame, score_column: str, return_column: str, top_percentiles: Iterable[float]) -> dict[str, object]:
    if score_column not in frame.columns or return_column not in frame.columns:
        return {"n_samples": 0, "spearman": None, "rmse": None, "mae": None}
    valid = frame[[score_column, return_column]].apply(pd.to_numeric, errors="coerce").dropna()
    if valid.empty:
        return {"n_samples": 0, "spearman": None, "rmse": None, "mae": None}
    score = valid[score_column]
    ret = valid[return_column]
    out: dict[str, object] = {
        "n_samples": int(len(valid)),
        "spearman": _finite_or_none(score.corr(ret, method="spearman")),
        "rmse": _finite_or_none(np.sqrt(((score - ret) ** 2).mean())),
        "mae": _finite_or_none((score - ret).abs().mean()),
    }
    sorted_valid = valid.sort_values(score_column, ascending=False)
    for pct in top_percentiles:
        tag = str(int(pct)) if float(pct).is_integer() else str(pct).replace(".", "_")
        top_count = max(1, int(np.ceil(len(sorted_valid) * float(pct) / 100.0)))
        top = sorted_valid.head(top_count)
        out[f"top{tag}_mean_return"] = _finite_or_none(top[return_column].mean())
        out[f"top{tag}_hit_rate"] = _finite_or_none((top[return_column] > 0).mean())
    bottom_count = max(1, int(np.ceil(len(sorted_valid) * 0.2)))
    bottom = sorted_valid.tail(bottom_count)
    out["bottom20_mean_return"] = _finite_or_none(bottom[return_column].mean())
    top20 = out.get("top20_mean_return")
    bottom20 = out.get("bottom20_mean_return")
    out["long_short_spread"] = _finite_or_none(float(top20) - float(bottom20)) if top20 is not None and bottom20 is not None else None
    return out


def _annual_slices(frame: pd.DataFrame, score_column: str, return_column: str) -> list[dict[str, object]]:
    if "event_date" not in frame.columns:
        return []
    result = frame.copy()
    result["_year"] = pd.to_datetime(result["event_date"], errors="coerce").dt.year
    rows: list[dict[str, object]] = []
    for year, group in result.dropna(subset=["_year"]).groupby("_year"):
        metrics = _metrics_for_frame(group, score_column, return_column, (20,))
        rows.append({"year": int(year), **metrics})
    return rows


def _symbol_slices(frame: pd.DataFrame, score_column: str, return_column: str, limit: int = 30) -> list[dict[str, object]]:
    if "code" not in frame.columns:
        return []
    rows: list[dict[str, object]] = []
    for code, group in frame.groupby("code"):
        if len(group) < 5:
            continue
        metrics = _metrics_for_frame(group, score_column, return_column, (20,))
        rows.append({"code": str(code), **metrics})
    return sorted(rows, key=lambda item: int(item.get("n_samples") or 0), reverse=True)[:limit]


def _distribution(series: pd.Series | None) -> dict[str, object]:
    if series is None:
        return {}
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return {}
    return {
        "count": int(len(values)),
        "min": _finite_or_none(values.min()),
        "p05": _finite_or_none(values.quantile(0.05)),
        "p25": _finite_or_none(values.quantile(0.25)),
        "median": _finite_or_none(values.median()),
        "p75": _finite_or_none(values.quantile(0.75)),
        "p95": _finite_or_none(values.quantile(0.95)),
        "max": _finite_or_none(values.max()),
        "mean": _finite_or_none(values.mean()),
        "std": _finite_or_none(values.std()),
    }


def _overfit_ratio(splits: dict[str, object]) -> float | None:
    train = splits.get("train") if isinstance(splits, dict) else None
    test = splits.get("test") or splits.get("valid") if isinstance(splits, dict) else None
    if not isinstance(train, dict) or not isinstance(test, dict):
        return None
    train_ic = train.get("spearman")
    test_ic = test.get("spearman")
    try:
        if train_ic is None or test_ic in {None, 0}:
            return None
        return _finite_or_none(float(train_ic) / float(test_ic))
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _importance(model: object, importance_type: str, size: int) -> list[float]:
    method = getattr(model, "feature_importance", None)
    if not callable(method):
        return [0.0] * size
    try:
        values = method(importance_type=importance_type)
    except TypeError:
        try:
            values = method(importance_type)
        except Exception:
            return [0.0] * size
    except Exception:
        return [0.0] * size
    parsed = [float(item) for item in list(values)[:size]]
    if len(parsed) < size:
        parsed.extend([0.0] * (size - len(parsed)))
    return parsed


def _json_ready(value: object) -> object:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, np.generic):
        return _json_ready(value.item())
    if isinstance(value, float):
        return value if np.isfinite(value) else None
    return value


def _finite_or_none(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(parsed):
        return None
    return parsed
