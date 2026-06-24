"""Model evaluation metrics: Spearman, TopK, win rate, overfit detection."""

from __future__ import annotations

import math
from typing import Iterable

import numpy as np
import pandas as pd
from scipy import stats


_DEFAULT_TOP_PERCENTILES: list[float] = [5.0, 10.0, 20.0]
_MIN_SAMPLES = 10


def _safe_spearman(x: np.ndarray, y: np.ndarray) -> tuple[float, float]:
    """Compute Spearman correlation, returning (corr, p-value).

    Returns ``(nan, nan)`` when there are too few samples or when one of the
    inputs is constant.
    """
    if len(x) < 2 or len(y) < 2:
        return float("nan"), float("nan")
    if np.all(x == x[0]) or np.all(y == y[0]):
        return float("nan"), float("nan")
    corr, pvalue = stats.spearmanr(x, y)
    if isinstance(corr, float) and math.isnan(corr):
        return float("nan"), float("nan")
    return float(corr), float(pvalue)


def _topk_metrics(
    sorted_targets: np.ndarray,
    pct: float,
) -> tuple[float, float]:
    """Return ``(mean_return, win_rate)`` for the top ``pct`` percent rows.

    Assumes ``sorted_targets`` is already sorted in descending prediction order.
    """
    n = len(sorted_targets)
    if n == 0:
        return float("nan"), float("nan")
    k = max(1, int(round(n * pct / 100.0)))
    head = sorted_targets[:k]
    if len(head) == 0:
        return float("nan"), float("nan")
    return float(np.mean(head)), float(np.mean(head > 0))


def _bottom_pct_mean(sorted_targets: np.ndarray, pct: float) -> float:
    """Mean target value among the bottom ``pct`` percent (lowest predictions)."""
    n = len(sorted_targets)
    if n == 0:
        return float("nan")
    k = max(1, int(round(n * pct / 100.0)))
    tail = sorted_targets[-k:]
    if len(tail) == 0:
        return float("nan")
    return float(np.mean(tail))


def _ic_summary(
    df: pd.DataFrame,
    target_col: str,
    pred_col: str,
) -> tuple[float, float, float]:
    """Compute mean / std / IR of daily cross-sectional rank IC."""
    if "date" not in df.columns:
        return float("nan"), float("nan"), float("nan")

    daily_ics: list[float] = []
    for _, group in df.groupby("date"):
        if len(group) < 2:
            continue
        y = group[target_col].to_numpy(dtype=float)
        p = group[pred_col].to_numpy(dtype=float)
        mask = ~(np.isnan(y) | np.isnan(p))
        if mask.sum() < 2:
            continue
        corr, _ = _safe_spearman(p[mask], y[mask])
        if not math.isnan(corr):
            daily_ics.append(corr)

    if not daily_ics:
        return float("nan"), float("nan"), float("nan")

    arr = np.asarray(daily_ics, dtype=float)
    ic_mean = float(np.mean(arr))
    ic_std = float(np.std(arr, ddof=1)) if len(arr) > 1 else float("nan")
    if ic_std and not math.isnan(ic_std) and ic_std > 0:
        icir = ic_mean / ic_std
    else:
        icir = float("nan")
    return ic_mean, ic_std, icir


def evaluate_predictions(
    predictions: pd.DataFrame,
    target_col: str = "future_return",
    pred_col: str = "pred",
    top_percentiles: list[float] | None = None,
) -> dict[str, float]:
    """Comprehensive evaluation of predictions vs. realised returns.

    See module docstring for the full list of returned metrics.
    """
    if top_percentiles is None:
        top_percentiles = list(_DEFAULT_TOP_PERCENTILES)

    metrics: dict[str, float] = {}

    if predictions is None or len(predictions) == 0:
        metrics["spearman_corr"] = float("nan")
        metrics["spearman_pvalue"] = float("nan")
        metrics["rmse"] = float("nan")
        metrics["mae"] = float("nan")
        for pct in top_percentiles:
            tag = _pct_tag(pct)
            metrics[f"top{tag}_return"] = float("nan")
            metrics[f"top{tag}_winrate"] = float("nan")
        metrics["bottom20_return"] = float("nan")
        metrics["long_short_spread"] = float("nan")
        metrics["ic_mean"] = float("nan")
        metrics["ic_std"] = float("nan")
        metrics["icir"] = float("nan")
        metrics["n_samples"] = 0.0
        return metrics

    df = predictions[[c for c in predictions.columns if c is not None]].copy()
    df = df.dropna(subset=[target_col, pred_col])
    n = len(df)
    metrics["n_samples"] = float(n)

    if n < _MIN_SAMPLES:
        # Too few samples for stable metrics; emit NaN placeholders.
        metrics["spearman_corr"] = float("nan")
        metrics["spearman_pvalue"] = float("nan")
        metrics["rmse"] = float("nan") if n == 0 else _rmse(df[target_col], df[pred_col])
        metrics["mae"] = float("nan") if n == 0 else _mae(df[target_col], df[pred_col])
        for pct in top_percentiles:
            tag = _pct_tag(pct)
            metrics[f"top{tag}_return"] = float("nan")
            metrics[f"top{tag}_winrate"] = float("nan")
        metrics["bottom20_return"] = float("nan")
        metrics["long_short_spread"] = float("nan")
        metrics["ic_mean"] = float("nan")
        metrics["ic_std"] = float("nan")
        metrics["icir"] = float("nan")
        return metrics

    y = df[target_col].to_numpy(dtype=float)
    p = df[pred_col].to_numpy(dtype=float)

    corr, pvalue = _safe_spearman(p, y)
    metrics["spearman_corr"] = corr
    metrics["spearman_pvalue"] = pvalue
    metrics["rmse"] = _rmse(df[target_col], df[pred_col])
    metrics["mae"] = _mae(df[target_col], df[pred_col])

    # Sort by prediction descending for TopK analysis.
    order = np.argsort(-p, kind="stable")
    sorted_targets = y[order]

    for pct in top_percentiles:
        tag = _pct_tag(pct)
        mean_ret, winrate = _topk_metrics(sorted_targets, pct)
        metrics[f"top{tag}_return"] = mean_ret
        metrics[f"top{tag}_winrate"] = winrate

    bottom20 = _bottom_pct_mean(sorted_targets, 20.0)
    metrics["bottom20_return"] = bottom20
    top20 = metrics.get("top20_return", float("nan"))
    if math.isnan(top20) or math.isnan(bottom20):
        metrics["long_short_spread"] = float("nan")
    else:
        metrics["long_short_spread"] = top20 - bottom20

    ic_mean, ic_std, icir = _ic_summary(df, target_col, pred_col)
    metrics["ic_mean"] = ic_mean
    metrics["ic_std"] = ic_std
    metrics["icir"] = icir

    return metrics


def _pct_tag(pct: float) -> str:
    """Render a percentile as the tag used in metric keys (e.g. 5.0 -> '5')."""
    if float(pct).is_integer():
        return str(int(pct))
    # Replace '.' with '_' to keep keys identifier-friendly.
    return str(pct).replace(".", "_")


def _rmse(target: pd.Series, pred: pd.Series) -> float:
    diff = target.to_numpy(dtype=float) - pred.to_numpy(dtype=float)
    if len(diff) == 0:
        return float("nan")
    return float(np.sqrt(np.mean(diff ** 2)))


def _mae(target: pd.Series, pred: pd.Series) -> float:
    diff = target.to_numpy(dtype=float) - pred.to_numpy(dtype=float)
    if len(diff) == 0:
        return float("nan")
    return float(np.mean(np.abs(diff)))


def compute_overfit_ratio(
    train_metrics: dict[str, float],
    test_metrics: dict[str, float],
    metric_key: str = "spearman_corr",
) -> float:
    """Return ``train_metric / test_metric`` for the requested key.

    A ratio > 2 typically indicates significant overfitting. ``NaN`` is returned
    when either metric is missing, ``NaN`` or when the test metric is zero.
    """
    train_val = train_metrics.get(metric_key)
    test_val = test_metrics.get(metric_key)
    if train_val is None or test_val is None:
        return float("nan")
    if isinstance(train_val, float) and math.isnan(train_val):
        return float("nan")
    if isinstance(test_val, float) and math.isnan(test_val):
        return float("nan")
    if test_val == 0:
        return float("nan")
    return float(train_val) / float(test_val)
