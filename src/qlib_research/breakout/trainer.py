"""LightGBM model training pipeline with time-based splitting."""

from __future__ import annotations

import json
import logging
import pickle
from datetime import datetime
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
import pandas as pd

from qlib_research.breakout.config import TrainConfig, TrainResult
from qlib_research.breakout.evaluator import evaluate_predictions
from qlib_research.breakout.features import ALL_FEATURE_COLUMNS

logger = logging.getLogger(__name__)


_DEFAULT_OUTPUT_DIR = Path("artifacts/breakout/models")
_TARGET_COLUMN = "future_return"


def train_breakout_model(
    features_df: pd.DataFrame,
    labels_df: pd.DataFrame,
    config: TrainConfig | None = None,
    output_dir: Path | None = None,
) -> TrainResult:
    """Train a LightGBM regression model for breakout scoring.

    See module docstring of the original task spec for full pipeline details.
    """
    if config is None:
        config = TrainConfig()

    output_dir = Path(output_dir) if output_dir is not None else _DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # 1. Merge features + labels on index
    # ------------------------------------------------------------------
    merged = _merge_features_labels(features_df, labels_df)

    # 2. Drop rows with NaN target
    merged = merged.dropna(subset=[_TARGET_COLUMN]).reset_index(drop=True)
    if merged.empty:
        raise ValueError("No rows with valid future_return after dropping NaN targets")

    # 3. Time-based split
    train_df, valid_df, test_df = _time_split(
        merged, train_end=config.train_end, valid_end=config.valid_end
    )
    if train_df.empty:
        raise ValueError(f"Train set empty (train_end={config.train_end!r})")
    if valid_df.empty:
        raise ValueError(
            f"Validation set empty (train_end={config.train_end!r}, "
            f"valid_end={config.valid_end!r})"
        )

    feature_cols = [c for c in ALL_FEATURE_COLUMNS if c in merged.columns]
    if not feature_cols:
        raise ValueError(
            "None of ALL_FEATURE_COLUMNS were found in features_df; cannot train"
        )

    X_train = train_df[feature_cols].apply(pd.to_numeric, errors="coerce")
    y_train = train_df[_TARGET_COLUMN].astype(float)
    X_valid = valid_df[feature_cols].apply(pd.to_numeric, errors="coerce")
    y_valid = valid_df[_TARGET_COLUMN].astype(float)

    # ------------------------------------------------------------------
    # 4. LightGBM datasets
    # ------------------------------------------------------------------
    train_set = lgb.Dataset(
        X_train.values, label=y_train.values, feature_name=feature_cols, free_raw_data=False
    )
    valid_set = lgb.Dataset(
        X_valid.values,
        label=y_valid.values,
        feature_name=feature_cols,
        reference=train_set,
        free_raw_data=False,
    )

    params = _build_lgb_params(config)

    # ------------------------------------------------------------------
    # 5. Train with early stopping on validation RMSE
    # ------------------------------------------------------------------
    callbacks = [
        lgb.early_stopping(stopping_rounds=int(config.early_stopping_rounds), verbose=False),
        lgb.log_evaluation(period=0),
    ]
    booster = lgb.train(
        params=params,
        train_set=train_set,
        num_boost_round=int(config.num_boost_round),
        valid_sets=[train_set, valid_set],
        valid_names=["train", "valid"],
        callbacks=callbacks,
    )

    # ------------------------------------------------------------------
    # 6. Evaluate
    # ------------------------------------------------------------------
    train_metrics = _predict_and_evaluate(booster, train_df, feature_cols)
    valid_metrics = _predict_and_evaluate(booster, valid_df, feature_cols)
    test_metrics = (
        _predict_and_evaluate(booster, test_df, feature_cols)
        if not test_df.empty
        else {}
    )

    metrics: dict[str, float] = {}
    metrics.update({f"train_{k}": v for k, v in train_metrics.items()})
    metrics.update({f"valid_{k}": v for k, v in valid_metrics.items()})
    metrics.update({f"test_{k}": v for k, v in test_metrics.items()})
    # Convenience top-level keys mirror the test-set view; fall back to valid
    # when the test split is empty so that downstream code (and the unit tests)
    # can rely on ``metrics["spearman_corr"]`` being present.
    primary = test_metrics if test_metrics else valid_metrics
    for key, value in primary.items():
        metrics[key] = value

    feature_importance = _feature_importance(booster, feature_cols)

    # ------------------------------------------------------------------
    # 7. Persist model + metadata
    # ------------------------------------------------------------------
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_path = output_dir / f"breakout_lgbm_{timestamp}.pkl"
    metadata_path = output_dir / f"breakout_lgbm_{timestamp}.json"

    with open(model_path, "wb") as f:
        pickle.dump(booster, f)

    metadata = {
        "model_path": str(model_path),
        "created_at": timestamp,
        "config": _config_to_dict(config),
        "lgb_params": params,
        "feature_columns": feature_cols,
        "best_iteration": int(getattr(booster, "best_iteration", 0) or 0),
        "train_samples": int(len(train_df)),
        "valid_samples": int(len(valid_df)),
        "test_samples": int(len(test_df)),
        "metrics": _jsonify(metrics),
        "feature_importance": _jsonify(
            feature_importance.to_dict(orient="records")
        ),
    }
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    return TrainResult(
        model_path=model_path,
        feature_importance=feature_importance,
        metrics=metrics,
        train_samples=int(len(train_df)),
        valid_samples=int(len(valid_df)),
        config=config,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _merge_features_labels(
    features_df: pd.DataFrame,
    labels_df: pd.DataFrame,
) -> pd.DataFrame:
    """Combine features and labels along their index, preserving meta columns."""
    if features_df is None or labels_df is None:
        raise ValueError("features_df and labels_df must not be None")
    if len(features_df) != len(labels_df):
        raise ValueError(
            f"features_df ({len(features_df)} rows) and labels_df "
            f"({len(labels_df)} rows) must align by index"
        )
    if _TARGET_COLUMN not in labels_df.columns:
        raise ValueError(f"labels_df missing required column {_TARGET_COLUMN!r}")
    if "date" not in labels_df.columns:
        raise ValueError("labels_df must contain a 'date' column for time-based split")

    # Avoid duplicate columns; prefer labels_df values for any overlap so that
    # symbol/date come from the labels frame.
    feature_only = [c for c in features_df.columns if c not in labels_df.columns]
    merged = pd.concat(
        [features_df[feature_only].reset_index(drop=True), labels_df.reset_index(drop=True)],
        axis=1,
    )
    merged["date"] = pd.to_datetime(merged["date"])
    return merged


def _time_split(
    df: pd.DataFrame,
    train_end: str,
    valid_end: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split ``df`` by date column into train / valid / test frames."""
    train_end_ts = pd.Timestamp(train_end)
    valid_end_ts = pd.Timestamp(valid_end)
    if valid_end_ts < train_end_ts:
        raise ValueError(
            f"valid_end ({valid_end!r}) must be >= train_end ({train_end!r})"
        )

    dates = df["date"]
    # Handle timezone-aware dates (e.g. datetime64[us, UTC] from FinanceDataHub)
    if hasattr(dates.dt, "tz") and dates.dt.tz is not None:
        train_end_ts = train_end_ts.tz_localize(dates.dt.tz)
        valid_end_ts = valid_end_ts.tz_localize(dates.dt.tz)
    train = df[dates <= train_end_ts]
    valid = df[(dates > train_end_ts) & (dates <= valid_end_ts)]
    test = df[dates > valid_end_ts]
    return train.reset_index(drop=True), valid.reset_index(drop=True), test.reset_index(drop=True)


def _build_lgb_params(config: TrainConfig) -> dict[str, Any]:
    """Translate a TrainConfig into LightGBM training params."""
    params: dict[str, Any] = {
        "objective": config.objective,
        "metric": "rmse",
        "num_leaves": int(config.num_leaves),
        "learning_rate": float(config.learning_rate),
        "feature_fraction": float(config.feature_fraction),
        "lambda_l1": float(config.lambda_l1),
        "lambda_l2": float(config.lambda_l2),
        "min_data_in_leaf": int(config.min_data_in_leaf),
        "verbose": -1,
    }
    return params


def _predict_and_evaluate(
    booster: lgb.Booster,
    split_df: pd.DataFrame,
    feature_cols: list[str],
) -> dict[str, float]:
    """Predict on a split and run the shared evaluation pipeline."""
    if split_df.empty:
        return {}
    X = split_df[feature_cols].apply(pd.to_numeric, errors="coerce")
    preds = booster.predict(X.values)
    eval_df = pd.DataFrame(
        {
            "date": split_df["date"].values if "date" in split_df.columns else pd.NaT,
            "symbol": split_df["symbol"].values if "symbol" in split_df.columns else "",
            _TARGET_COLUMN: split_df[_TARGET_COLUMN].astype(float).values,
            "pred": np.asarray(preds, dtype=float),
        }
    )
    return evaluate_predictions(eval_df, target_col=_TARGET_COLUMN, pred_col="pred")


def _feature_importance(
    booster: lgb.Booster,
    feature_cols: list[str],
) -> pd.DataFrame:
    """Build a tidy feature-importance DataFrame using gain-based importance."""
    gain = booster.feature_importance(importance_type="gain")
    split = booster.feature_importance(importance_type="split")
    df = pd.DataFrame(
        {
            "feature": feature_cols,
            "gain": gain,
            "split": split,
        }
    )
    df = df.sort_values("gain", ascending=False).reset_index(drop=True)
    return df


def _config_to_dict(config: TrainConfig) -> dict[str, Any]:
    """Serialize a TrainConfig dataclass to a JSON-safe dict."""
    return {
        "train_end": config.train_end,
        "valid_end": config.valid_end,
        "num_leaves": config.num_leaves,
        "learning_rate": config.learning_rate,
        "early_stopping_rounds": config.early_stopping_rounds,
        "num_boost_round": config.num_boost_round,
        "feature_fraction": config.feature_fraction,
        "lambda_l1": config.lambda_l1,
        "lambda_l2": config.lambda_l2,
        "min_data_in_leaf": config.min_data_in_leaf,
        "objective": config.objective,
    }


def _jsonify(value: Any) -> Any:
    """Recursively coerce numpy / pandas scalars into JSON-friendly types."""
    if isinstance(value, dict):
        return {k: _jsonify(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonify(v) for v in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        v = float(value)
        return None if np.isnan(v) else v
    if isinstance(value, float):
        return None if np.isnan(value) else value
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    return value
