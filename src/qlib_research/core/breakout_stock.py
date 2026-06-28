"""Stock breakout event research pipeline.

The module keeps the v1 breakout workflow independent from the existing
weekly cross-sectional workflow: callers provide stock OHLCV frames, and the
functions return event, label, feature, and signal frames that can be published
as normal QlibResearch artifacts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

import numpy as np
import pandas as pd

from qlib_research.breakout.evaluation import evaluate_breakout_scores
from qlib_research.breakout.features import ALL_FEATURE_COLUMNS, FEATURE_GROUPS, build_stock_breakout_features


@dataclass(frozen=True)
class StockBreakoutConfig:
    """Configuration for stock-only breakout research v1."""

    lookback_window: int = 60
    consolidation_window: int = 20
    momentum_windows: tuple[int, ...] = (5, 10, 20)
    volume_window: int = 20
    breakout_pct: float = 0.0
    min_volume_ratio: float = 1.0
    label_horizon_days: int = 13
    success_return_pct: float = 0.03
    min_history_days: int = 80
    extra_feature_columns: tuple[str, ...] = field(default_factory=tuple)


REQUIRED_PRICE_COLUMNS = {"code", "date", "open", "high", "low", "close"}
OPTIONAL_NUMERIC_COLUMNS = ("volume", "vol", "amount", "turnover_rate", "circ_mv", "total_mv")


def normalize_stock_price_frame(price_frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize a stock OHLCV frame into sorted, numeric research input."""

    if price_frame.empty:
        return pd.DataFrame(columns=sorted(REQUIRED_PRICE_COLUMNS | {"volume"}))
    frame = price_frame.copy()
    rename_map = {
        "symbol": "code",
        "instrument": "code",
        "datetime": "date",
        "time": "date",
        "trade_date": "date",
        "vol": "volume",
    }
    frame = frame.rename(columns={k: v for k, v in rename_map.items() if k in frame.columns and v not in frame.columns})
    missing = REQUIRED_PRICE_COLUMNS - set(frame.columns)
    if missing:
        raise ValueError(f"Stock price frame missing required columns: {sorted(missing)}")

    frame["code"] = frame["code"].astype(str).str.strip().str.upper()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce", utc=True).dt.tz_localize(None).dt.normalize()
    frame = frame.dropna(subset=["code", "date"]).copy()
    for column in ("open", "high", "low", "close", *OPTIONAL_NUMERIC_COLUMNS):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if "volume" not in frame.columns:
        frame["volume"] = np.nan
    frame = frame.dropna(subset=["open", "high", "low", "close"])
    return frame.sort_values(["code", "date"]).reset_index(drop=True)


def detect_stock_breakout_events(
    price_frame: pd.DataFrame,
    config: StockBreakoutConfig | None = None,
) -> pd.DataFrame:
    """Detect long breakout candidates using only data available at event date."""

    cfg = config or StockBreakoutConfig()
    frame = normalize_stock_price_frame(price_frame)
    if frame.empty:
        return _empty_events()

    events: list[pd.DataFrame] = []
    for code, group in frame.groupby("code", sort=True):
        group = group.sort_values("date").reset_index(drop=True).copy()
        prior_high = group["high"].rolling(cfg.lookback_window, min_periods=cfg.lookback_window).max().shift(1)
        prior_low = group["low"].rolling(cfg.consolidation_window, min_periods=cfg.consolidation_window).min().shift(1)
        volume_ma = group["volume"].rolling(cfg.volume_window, min_periods=max(3, cfg.volume_window // 2)).mean().shift(1)
        volume_ratio = group["volume"] / volume_ma.replace(0, np.nan)
        consolidation_range = (prior_high - prior_low) / prior_high.replace(0, np.nan)
        breakout_strength = group["close"] / prior_high.replace(0, np.nan) - 1.0
        enough_history = group.index >= max(cfg.min_history_days, cfg.lookback_window)
        mask = (
            enough_history
            & prior_high.notna()
            & (group["close"] > prior_high * (1.0 + cfg.breakout_pct))
            & (volume_ratio.fillna(np.inf) >= cfg.min_volume_ratio)
        )
        if not mask.any():
            continue
        event_frame = pd.DataFrame(
            {
                "event_id": [
                    f"stock_breakout:{code}:{pd.Timestamp(date).date().isoformat()}"
                    for date in group.loc[mask, "date"].tolist()
                ],
                "asset_class": "stock",
                "code": code,
                "event_date": group.loc[mask, "date"].dt.date.astype(str).to_numpy(),
                "side": "long",
                "frequency": "D",
                "candidate_rule": "close_above_prior_high",
                "entry_price": group.loc[mask, "close"].to_numpy(),
                "ref_high": prior_high.loc[mask].to_numpy(),
                "ref_low": prior_low.loc[mask].to_numpy(),
                "breakout_strength": breakout_strength.loc[mask].to_numpy(),
                "volume_ratio": volume_ratio.loc[mask].to_numpy(),
                "consolidation_range": consolidation_range.loc[mask].to_numpy(),
            }
        )
        events.append(event_frame)
    if not events:
        return _empty_events()
    return pd.concat(events, ignore_index=True).sort_values(["event_date", "code"]).reset_index(drop=True)


def label_stock_breakout_events(
    price_frame: pd.DataFrame,
    events: pd.DataFrame,
    config: StockBreakoutConfig | None = None,
) -> pd.DataFrame:
    """Attach forward labels for detected stock breakout events."""

    cfg = config or StockBreakoutConfig()
    if events.empty:
        return events.copy()
    frame = normalize_stock_price_frame(price_frame)
    forward_rows: list[pd.DataFrame] = []
    for code, group in frame.groupby("code", sort=True):
        group = group.sort_values("date").reset_index(drop=True).copy()
        group["future_close"] = group["close"].shift(-cfg.label_horizon_days)
        group["future_high"] = group["high"].shift(-1).rolling(cfg.label_horizon_days, min_periods=1).max().shift(-(cfg.label_horizon_days - 1))
        group["future_low"] = group["low"].shift(-1).rolling(cfg.label_horizon_days, min_periods=1).min().shift(-(cfg.label_horizon_days - 1))
        forward_rows.append(group[["code", "date", "future_close", "future_high", "future_low"]])
    forward = pd.concat(forward_rows, ignore_index=True) if forward_rows else pd.DataFrame()
    result = events.copy()
    result["_event_date"] = pd.to_datetime(result["event_date"], errors="coerce")
    result = result.merge(forward, left_on=["code", "_event_date"], right_on=["code", "date"], how="left").drop(columns=["date", "_event_date"])
    entry = pd.to_numeric(result["entry_price"], errors="coerce")
    ref_high = pd.to_numeric(result["ref_high"], errors="coerce")
    result[f"future_return_{cfg.label_horizon_days}d"] = pd.to_numeric(result["future_close"], errors="coerce") / entry - 1.0
    result[f"mfe_return_{cfg.label_horizon_days}d"] = pd.to_numeric(result["future_high"], errors="coerce") / entry - 1.0
    result[f"mae_return_{cfg.label_horizon_days}d"] = pd.to_numeric(result["future_low"], errors="coerce") / entry - 1.0
    result[f"label_success_{cfg.label_horizon_days}d"] = (
        pd.to_numeric(result["future_close"], errors="coerce") >= ref_high * (1.0 + cfg.success_return_pct)
    ).astype("Int64")
    result.loc[result["future_close"].isna(), f"label_success_{cfg.label_horizon_days}d"] = pd.NA
    return result


def build_stock_breakout_feature_panel(
    price_frame: pd.DataFrame,
    events: pd.DataFrame,
    config: StockBreakoutConfig | None = None,
) -> pd.DataFrame:
    """Build event-level features bounded by event-date information."""

    cfg = config or StockBreakoutConfig()
    if events.empty:
        result = events.copy()
        for column in ALL_FEATURE_COLUMNS:
            if column not in result.columns:
                result[column] = pd.Series(dtype=float)
        return result
    frame = normalize_stock_price_frame(price_frame)
    result = build_stock_breakout_features(frame, events, extra_feature_columns=cfg.extra_feature_columns)
    return result


def build_stock_breakout_research_frame(
    price_frame: pd.DataFrame,
    config: StockBreakoutConfig | None = None,
) -> pd.DataFrame:
    """Run candidate detection, labeling, and feature extraction for stocks."""

    cfg = config or StockBreakoutConfig()
    events = detect_stock_breakout_events(price_frame, cfg)
    labeled = label_stock_breakout_events(price_frame, events, cfg)
    return build_stock_breakout_feature_panel(price_frame, labeled, cfg)


def stock_breakout_feature_columns(frame: pd.DataFrame) -> list[str]:
    """Return numeric model feature columns from an event feature frame."""

    excluded_prefixes = ("future_", "label_", "mfe_", "mae_")
    excluded = {
        "event_id",
        "asset_class",
        "code",
        "event_date",
        "side",
        "frequency",
        "candidate_rule",
    }
    columns: list[str] = []
    preferred = [column for column in ALL_FEATURE_COLUMNS if column in frame.columns]
    legacy = [
        "entry_price",
        "ref_high",
        "ref_low",
        "breakout_strength",
        "volume_ratio",
        "consolidation_range",
        "volatility_20d",
        "volume_ratio_20d",
        "range_20d",
        "return_5d",
        "return_10d",
        "return_20d",
        "ma_ratio_5d",
        "ma_ratio_10d",
        "ma_ratio_20d",
    ]
    for column in [*legacy, *preferred, *frame.columns.tolist()]:
        if column in excluded or column.startswith(excluded_prefixes):
            continue
        if column not in frame.columns:
            continue
        if pd.api.types.is_numeric_dtype(frame[column]) and column not in columns:
            columns.append(column)
    return columns


def build_stock_breakout_signal_frame(
    scored_events: pd.DataFrame,
    model_id: str,
    feature_date: str | None = None,
    score_column: str = "signal_score",
    config: StockBreakoutConfig | None = None,
) -> pd.DataFrame:
    """Convert scored breakout events into the stable strategy signal contract."""

    cfg = config or StockBreakoutConfig()
    if scored_events.empty:
        return pd.DataFrame(
            columns=[
                "event_id",
                "model_id",
                "code",
                "signal_date",
                "feature_date",
                "signal_type",
                "side",
                "signal_score",
                f"pred_return_{cfg.label_horizon_days}d",
                "entry_price",
                "ref_high",
                "ref_low",
            ]
        )
    frame = scored_events.copy()
    if score_column not in frame.columns:
        label_col = f"future_return_{cfg.label_horizon_days}d"
        frame[score_column] = pd.to_numeric(frame.get(label_col), errors="coerce")
    output = pd.DataFrame(
        {
            "event_id": frame["event_id"],
            "model_id": model_id,
            "code": frame["code"],
            "signal_date": frame["event_date"],
            "feature_date": feature_date or frame["event_date"],
            "signal_type": "breakout",
            "side": frame.get("side", "long"),
            "signal_score": pd.to_numeric(frame[score_column], errors="coerce"),
            f"pred_return_{cfg.label_horizon_days}d": pd.to_numeric(frame[score_column], errors="coerce"),
            "entry_price": pd.to_numeric(frame.get("entry_price"), errors="coerce"),
            "ref_high": pd.to_numeric(frame.get("ref_high"), errors="coerce"),
            "ref_low": pd.to_numeric(frame.get("ref_low"), errors="coerce"),
        }
    )
    return output.sort_values(["signal_date", "code"]).reset_index(drop=True)


def evaluate_stock_breakout_scores(
    scored_events: pd.DataFrame,
    score_column: str = "signal_score",
    return_column: str | None = None,
    top_quantile: float = 0.2,
) -> dict[str, object]:
    """Compute compact event-model metrics for offline promotion checks."""

    top_percent = max(1.0, min(100.0, float(top_quantile) * 100.0))
    return evaluate_breakout_scores(
        scored_events,
        score_column=score_column,
        return_column=return_column,
        top_percentiles=(5, 10, top_percent),
    )


def train_lightgbm_stock_breakout_model(
    feature_frame: pd.DataFrame,
    label_column: str,
    feature_columns: Iterable[str] | None = None,
    *,
    valid_frame: pd.DataFrame | None = None,
    model_params: dict | None = None,
    num_boost_round: int = 500,
    early_stopping_rounds: int | None = 30,
    random_state: int = 42,
):
    """Train a LightGBM regressor on an event feature frame."""

    import lightgbm as lgb

    columns = list(feature_columns or stock_breakout_feature_columns(feature_frame))
    if not columns:
        raise ValueError("No numeric stock breakout feature columns available for training")
    if label_column not in feature_frame.columns:
        raise ValueError(f"Label column not found: {label_column}")
    train = feature_frame[columns + [label_column]].copy()
    for column in columns + [label_column]:
        train[column] = pd.to_numeric(train[column], errors="coerce")
    train = train.dropna(subset=[label_column])
    if train.empty:
        raise ValueError("No labeled stock breakout rows available for training")
    model_input = train[columns]
    dataset = lgb.Dataset(model_input, label=train[label_column], feature_name=columns, free_raw_data=False)
    params = {
        "objective": "regression",
        "metric": "rmse",
        "learning_rate": 0.03,
        "num_leaves": 63,
        "seed": random_state,
        "verbosity": -1,
    }
    params.update(model_params or {})
    valid_sets = [dataset]
    valid_names = ["train"]
    callbacks = []
    if valid_frame is not None and not valid_frame.empty:
        valid = valid_frame[columns + [label_column]].copy()
        for column in columns + [label_column]:
            valid[column] = pd.to_numeric(valid[column], errors="coerce")
        valid = valid.dropna(subset=[label_column])
        if not valid.empty:
            valid_dataset = lgb.Dataset(valid[columns], label=valid[label_column], feature_name=columns, reference=dataset, free_raw_data=False)
            valid_sets.append(valid_dataset)
            valid_names.append("valid")
            if early_stopping_rounds:
                callbacks.append(lgb.early_stopping(stopping_rounds=int(early_stopping_rounds), verbose=False))
    callbacks.append(lgb.log_evaluation(period=0))
    model = lgb.train(
        params,
        train_set=dataset,
        num_boost_round=int(num_boost_round),
        valid_sets=valid_sets,
        valid_names=valid_names,
        callbacks=callbacks,
    )
    return model, columns


def score_stock_breakout_events(
    model,
    feature_frame: pd.DataFrame,
    feature_columns: Iterable[str],
    score_column: str = "signal_score",
) -> pd.DataFrame:
    """Score stock breakout events with a fitted model."""

    columns = list(feature_columns)
    result = feature_frame.copy()
    if result.empty:
        result[score_column] = []
        return result
    model_input = result[columns].apply(pd.to_numeric, errors="coerce")
    result[score_column] = model.predict(model_input)
    return result


def _empty_events() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "event_id",
            "asset_class",
            "code",
            "event_date",
            "side",
            "frequency",
            "candidate_rule",
            "entry_price",
            "ref_high",
            "ref_low",
            "breakout_strength",
            "volume_ratio",
            "consolidation_range",
        ]
    )


def _finite_or_none(value: float | int | None) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(parsed):
        return None
    return parsed
