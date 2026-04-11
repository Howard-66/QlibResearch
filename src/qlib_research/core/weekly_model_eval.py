"""
Rolling weekly evaluation helpers for qlib-based stock ranking models.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
import itertools
import math
from pathlib import Path
import time
from typing import Any, Callable, Sequence

import pandas as pd

from qlib_research.core.backtest import BacktestEngine
from qlib_research.core.portfolio import BacktestTradingConfig
from qlib_research.core.qlib_pipeline import (
    FEATURE_GROUP_COLUMNS,
    LABEL_COLUMN,
    apply_industry_normalization,
    build_training_frame,
    compute_feature_fill_values,
    get_outlier_audit_candidates,
    suppress_external_output,
)


SLICE_FLAG_COLUMNS = {
    "csi300": "in_csi300",
    "csi500": "in_csi500",
}

COARSE_TUNING_CANDIDATES = (
    {"learning_rate": 0.03, "num_leaves": 15, "min_data_in_leaf": 20, "feature_fraction": 0.6, "lambda_l2": 0, "num_boost_round": 100},
    {"learning_rate": 0.03, "num_leaves": 31, "min_data_in_leaf": 50, "feature_fraction": 0.8, "lambda_l2": 1, "num_boost_round": 200},
    {"learning_rate": 0.03, "num_leaves": 63, "min_data_in_leaf": 100, "feature_fraction": 1.0, "lambda_l2": 5, "num_boost_round": 400},
    {"learning_rate": 0.05, "num_leaves": 15, "min_data_in_leaf": 50, "feature_fraction": 0.8, "lambda_l2": 0, "num_boost_round": 200},
    {"learning_rate": 0.05, "num_leaves": 31, "min_data_in_leaf": 20, "feature_fraction": 1.0, "lambda_l2": 1, "num_boost_round": 400},
    {"learning_rate": 0.05, "num_leaves": 63, "min_data_in_leaf": 50, "feature_fraction": 0.6, "lambda_l2": 5, "num_boost_round": 100},
    {"learning_rate": 0.1, "num_leaves": 15, "min_data_in_leaf": 100, "feature_fraction": 1.0, "lambda_l2": 1, "num_boost_round": 100},
    {"learning_rate": 0.1, "num_leaves": 31, "min_data_in_leaf": 50, "feature_fraction": 0.6, "lambda_l2": 5, "num_boost_round": 200},
    {"learning_rate": 0.1, "num_leaves": 63, "min_data_in_leaf": 20, "feature_fraction": 0.8, "lambda_l2": 0, "num_boost_round": 400},
    {"learning_rate": 0.03, "num_leaves": 31, "min_data_in_leaf": 100, "feature_fraction": 0.6, "lambda_l2": 5, "num_boost_round": 200},
    {"learning_rate": 0.05, "num_leaves": 15, "min_data_in_leaf": 20, "feature_fraction": 1.0, "lambda_l2": 5, "num_boost_round": 400},
    {"learning_rate": 0.1, "num_leaves": 63, "min_data_in_leaf": 100, "feature_fraction": 0.8, "lambda_l2": 1, "num_boost_round": 100},
)

TUNING_VALUE_SPACE = {
    "learning_rate": [0.03, 0.05, 0.1],
    "num_leaves": [15, 31, 63],
    "min_data_in_leaf": [20, 50, 100],
    "feature_fraction": [0.6, 0.8, 1.0],
    "lambda_l2": [0, 1, 5],
    "num_boost_round": [100, 200, 400],
}


@dataclass(frozen=True)
class ModelRecipe:
    name: str
    feature_columns: Sequence[str]
    feature_groups: Sequence[str] = field(default_factory=tuple)
    industry_normalization: str = "none"
    model_params: dict[str, Any] = field(default_factory=dict)


@dataclass
class RecipeEvaluation:
    recipe: ModelRecipe
    summary: pd.DataFrame
    slice_summary: pd.DataFrame
    details: pd.DataFrame
    slice_details: pd.DataFrame
    predictions: pd.DataFrame
    equity_curve: pd.DataFrame


@dataclass
class PreparedModelInput:
    qlib_frame: pd.DataFrame
    used_features: list[str]
    segments: dict[str, tuple[str, str]]


@dataclass
class EvaluationRuntimeCache:
    evaluation_cache: dict[tuple[Any, ...], RecipeEvaluation] = field(default_factory=dict)
    normalized_panel_cache: dict[tuple[Any, ...], pd.DataFrame] = field(default_factory=dict)
    prepared_input_cache: dict[tuple[Any, ...], PreparedModelInput] = field(default_factory=dict)
    normalization_feature_columns: tuple[str, ...] | None = None


@dataclass(frozen=True)
class BacktestPriceFrames:
    execution_price: pd.DataFrame
    mark_price: pd.DataFrame
    stop_price: pd.DataFrame | None = None


def default_weekly_net_backtest_config() -> BacktestTradingConfig:
    return BacktestTradingConfig(
        broker_commission_rate=0.0002,
        exchange_fee_rate=0.0000341,
        transfer_fee_rate=0.00001,
        stamp_duty_sell_rate=0.0005,
        impact_cost_rate=0.0005,
        min_commission=5.0,
        trade_unit=100,
        stop_loss_mode="off",
        stop_loss_threshold=0.08,
    )


ProgressCallback = Callable[[dict[str, Any]], None]


def _emit_progress(
    progress_callback: ProgressCallback | None,
    event: str,
    progress_context: dict[str, Any] | None = None,
    **payload: Any,
) -> None:
    if progress_callback is None:
        return
    event_payload = dict(progress_context or {})
    event_payload.update(payload)
    event_payload["event"] = event
    progress_callback(event_payload)


def _float_or_nan(value: Any) -> float:
    if pd.isna(value):
        return math.nan
    return float(value)


def _metric_or_neg_inf(value: Any) -> float:
    return -math.inf if pd.isna(value) else float(value)


def _recipe_cache_key(
    recipe: ModelRecipe,
    eval_dates: Sequence[pd.Timestamp],
    topk: int,
    train_weeks: int,
    valid_weeks: int,
) -> tuple[Any, ...]:
    sorted_params = tuple(sorted(recipe.model_params.items()))
    return (
        tuple(recipe.feature_columns),
        tuple(recipe.feature_groups),
        recipe.industry_normalization,
        sorted_params,
        tuple(pd.Timestamp(date).isoformat() for date in eval_dates),
        topk,
        train_weeks,
        valid_weeks,
    )


def select_evaluation_dates(
    panel: pd.DataFrame,
    train_weeks: int,
    valid_weeks: int,
    eval_count: int,
    step_weeks: int = 1,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[pd.Timestamp]:
    """Select eligible rolling evaluation dates."""
    label_ready_dates = sorted(pd.to_datetime(panel.loc[panel[LABEL_COLUMN].notna(), "datetime"]).dropna().unique())
    if start_date is not None:
        start_ts = pd.Timestamp(start_date)
        label_ready_dates = [date for date in label_ready_dates if date >= start_ts]
    if end_date is not None:
        end_ts = pd.Timestamp(end_date)
        label_ready_dates = [date for date in label_ready_dates if date <= end_ts]

    minimum_history = train_weeks + valid_weeks
    eligible = [
        pd.Timestamp(date)
        for index, date in enumerate(label_ready_dates)
        if index >= minimum_history
    ]
    if step_weeks > 1 and eligible:
        eligible = list(reversed(list(reversed(eligible))[::step_weeks]))
    if eval_count > 0:
        eligible = eligible[-eval_count:]
    return eligible


def build_rolling_segments(
    panel: pd.DataFrame,
    feature_date: pd.Timestamp,
    train_weeks: int,
    valid_weeks: int,
    label_ready_dates: Sequence[pd.Timestamp] | None = None,
) -> dict[str, tuple[str, str]]:
    """Build a fixed-length rolling train/valid/test split for one feature date."""
    effective_dates = label_ready_dates
    if effective_dates is None:
        effective_dates = sorted(pd.to_datetime(panel.loc[panel[LABEL_COLUMN].notna(), "datetime"]).dropna().unique())
    history = [pd.Timestamp(date) for date in effective_dates if date < feature_date]
    required = train_weeks + valid_weeks
    if len(history) < required:
        raise ValueError(
            f"Not enough history before {feature_date.date()} "
            f"(required {required} weekly dates, got {len(history)})"
        )

    history = history[-required:]
    train_dates = history[:train_weeks]
    valid_dates = history[train_weeks:]
    return {
        "train": (str(train_dates[0].date()), str(train_dates[-1].date())),
        "valid": (str(valid_dates[0].date()), str(valid_dates[-1].date())),
        "test": (str(feature_date.date()), str(feature_date.date())),
    }


def _resolve_runtime_cache(
    cache: dict[tuple[Any, ...], RecipeEvaluation] | None = None,
    runtime_cache: EvaluationRuntimeCache | None = None,
) -> EvaluationRuntimeCache:
    if runtime_cache is not None:
        if cache is not None and runtime_cache.evaluation_cache is not cache:
            runtime_cache.evaluation_cache = cache
        return runtime_cache
    return EvaluationRuntimeCache(evaluation_cache=cache if cache is not None else {})


def get_or_prepare_normalized_panel(
    panel: pd.DataFrame,
    recipe: ModelRecipe,
    runtime_cache: EvaluationRuntimeCache | None = None,
    progress_callback: ProgressCallback | None = None,
    progress_context: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, tuple[Any, ...]]:
    runtime_cache = runtime_cache or EvaluationRuntimeCache()
    normalization_columns = tuple(runtime_cache.normalization_feature_columns or tuple(recipe.feature_columns))
    cache_key = (recipe.industry_normalization, normalization_columns)
    _emit_progress(
        progress_callback,
        "recipe_preprocess_start",
        progress_context=progress_context,
        recipe_name=recipe.name,
        feature_count=len(recipe.feature_columns),
        normalization_method=recipe.industry_normalization,
    )
    if cache_key in runtime_cache.normalized_panel_cache:
        _emit_progress(
            progress_callback,
            "recipe_preprocess_cache_hit",
            progress_context=progress_context,
            recipe_name=recipe.name,
            feature_count=len(recipe.feature_columns),
            normalization_method=recipe.industry_normalization,
        )
        return runtime_cache.normalized_panel_cache[cache_key], cache_key

    normalized_panel = apply_industry_normalization(
        panel,
        feature_columns=normalization_columns,
        method=recipe.industry_normalization,
    )
    runtime_cache.normalized_panel_cache[cache_key] = normalized_panel
    _emit_progress(
        progress_callback,
        "recipe_preprocess_complete",
        progress_context=progress_context,
        recipe_name=recipe.name,
        feature_count=len(recipe.feature_columns),
        normalization_method=recipe.industry_normalization,
    )
    return normalized_panel, cache_key


def get_or_prepare_model_input(
    panel: pd.DataFrame,
    feature_date: pd.Timestamp,
    feature_columns: Sequence[str],
    train_weeks: int,
    valid_weeks: int,
    normalization_cache_key: tuple[Any, ...],
    runtime_cache: EvaluationRuntimeCache | None = None,
    progress_callback: ProgressCallback | None = None,
    progress_context: dict[str, Any] | None = None,
) -> PreparedModelInput:
    runtime_cache = runtime_cache or EvaluationRuntimeCache()
    cache_key = (
        normalization_cache_key,
        tuple(feature_columns),
        pd.Timestamp(feature_date).isoformat(),
        train_weeks,
        valid_weeks,
    )
    if cache_key in runtime_cache.prepared_input_cache:
        _emit_progress(
            progress_callback,
            "eval_input_cache_hit",
            progress_context=progress_context,
            feature_date=str(pd.Timestamp(feature_date).date()),
            feature_count=len(feature_columns),
        )
        return runtime_cache.prepared_input_cache[cache_key]

    _emit_progress(
        progress_callback,
        "eval_input_prepare_start",
        progress_context=progress_context,
        feature_date=str(pd.Timestamp(feature_date).date()),
        feature_count=len(feature_columns),
    )
    training_panel = panel.loc[pd.to_datetime(panel["datetime"]) <= feature_date].copy()
    label_ready_dates = sorted(pd.to_datetime(training_panel.loc[training_panel[LABEL_COLUMN].notna(), "datetime"]).dropna().unique())
    segments = build_rolling_segments(
        training_panel,
        feature_date,
        train_weeks=train_weeks,
        valid_weeks=valid_weeks,
        label_ready_dates=label_ready_dates,
    )
    train_start, train_end = segments["train"]
    train_panel = training_panel.loc[
        pd.to_datetime(training_panel["datetime"]).between(pd.Timestamp(train_start), pd.Timestamp(train_end))
    ].copy()
    fill_values = compute_feature_fill_values(train_panel, feature_columns=feature_columns)
    qlib_frame, used_features = build_training_frame(
        training_panel,
        feature_columns=feature_columns,
        label_column=LABEL_COLUMN,
        fill_values=fill_values,
    )
    prepared_input = PreparedModelInput(
        qlib_frame=qlib_frame,
        used_features=used_features,
        segments=segments,
    )
    runtime_cache.prepared_input_cache[cache_key] = prepared_input
    _emit_progress(
        progress_callback,
        "eval_input_prepare_complete",
        progress_context=progress_context,
        feature_date=str(pd.Timestamp(feature_date).date()),
        feature_count=len(feature_columns),
        used_feature_count=len(used_features),
    )
    return prepared_input


def fit_predict_one_date(
    panel: pd.DataFrame,
    feature_date: pd.Timestamp,
    feature_columns: Sequence[str],
    train_weeks: int = 260,
    valid_weeks: int = 52,
    num_boost_round: int = 200,
    early_stopping_rounds: int = 50,
    model_params: dict[str, Any] | None = None,
    prepared_input: PreparedModelInput | None = None,
) -> pd.DataFrame:
    """Fit one rolling model and predict one feature date."""
    with suppress_external_output():
        from qlib.contrib.model.gbdt import LGBModel
        from qlib.data.dataset import DatasetH
        from qlib.data.dataset.handler import DataHandlerLP
        from qlib.data.dataset.loader import StaticDataLoader

    if prepared_input is None:
        training_panel = panel.loc[pd.to_datetime(panel["datetime"]) <= feature_date].copy()
        segments = build_rolling_segments(
            training_panel,
            feature_date,
            train_weeks=train_weeks,
            valid_weeks=valid_weeks,
        )
        train_start, train_end = segments["train"]
        train_panel = training_panel.loc[
            pd.to_datetime(training_panel["datetime"]).between(pd.Timestamp(train_start), pd.Timestamp(train_end))
        ].copy()
        fill_values = compute_feature_fill_values(train_panel, feature_columns=feature_columns)
        qlib_frame, used_features = build_training_frame(
            training_panel,
            feature_columns=feature_columns,
            label_column=LABEL_COLUMN,
            fill_values=fill_values,
        )
    else:
        qlib_frame = prepared_input.qlib_frame
        used_features = prepared_input.used_features
        segments = prepared_input.segments

    loader = StaticDataLoader(qlib_frame)
    handler = DataHandlerLP(data_loader=loader, infer_processors=[], learn_processors=[])
    dataset = DatasetH(handler=handler, segments=segments)
    effective_params = {
        "loss": "mse",
        "num_boost_round": num_boost_round,
        "early_stopping_rounds": early_stopping_rounds,
    }
    if model_params:
        effective_params.update(model_params)

    model = LGBModel(**effective_params)
    with suppress_external_output():
        model.fit(dataset, verbose_eval=False)
        prediction = model.predict(dataset, segment="test")

    score_frame = prediction.reset_index()
    score_frame = score_frame.rename(columns={0: "score"})
    if "score" not in score_frame.columns:
        last_col = score_frame.columns[-1]
        score_frame = score_frame.rename(columns={last_col: "score"})
    score_frame["datetime"] = pd.to_datetime(score_frame["datetime"])
    score_frame["instrument"] = score_frame["instrument"].astype(str)
    score_frame["used_feature_count"] = len(used_features)
    return score_frame


def evaluate_prediction_frame(
    prediction_frame: pd.DataFrame,
    topk: int,
) -> dict[str, float]:
    """Calculate signal-quality metrics for one evaluation date."""
    result = prediction_frame.dropna(subset=["score"]).copy()
    if result.empty:
        return {
            "coverage": 0.0,
            "rank_ic": math.nan,
            "ic": math.nan,
            "topk_mean_return_4w": math.nan,
            "topk_mean_excess_return_4w": math.nan,
            "topk_hit_rate": math.nan,
            "universe_mean_return_4w": math.nan,
            "universe_mean_excess_return_4w": math.nan,
        }

    rank_ic = result["score"].corr(result[LABEL_COLUMN], method="spearman")
    ic = result["score"].corr(result[LABEL_COLUMN], method="pearson")
    top = result.sort_values("score", ascending=False).head(topk)
    universe_mean_return = result["future_return_4w"].mean()
    universe_mean_excess = result[LABEL_COLUMN].mean()
    return {
        "coverage": float(len(result)),
        "rank_ic": _float_or_nan(rank_ic),
        "ic": _float_or_nan(ic),
        "topk_mean_return_4w": _float_or_nan(top["future_return_4w"].mean()),
        "topk_mean_excess_return_4w": _float_or_nan(top[LABEL_COLUMN].mean()),
        "topk_hit_rate": _float_or_nan((top["future_return_4w"] > 0).mean()),
        "universe_mean_return_4w": _float_or_nan(universe_mean_return),
        "universe_mean_excess_return_4w": _float_or_nan(universe_mean_excess),
    }


def run_strategy_backtest(
    price_frame: pd.DataFrame,
    positions: pd.DataFrame,
    execution_price_frame: pd.DataFrame | None = None,
    stop_price_frame: pd.DataFrame | None = None,
    initial_capital: float = 1_000_000.0,
    trading_config: BacktestTradingConfig | None = None,
) -> tuple[pd.DataFrame, dict[str, float]]:
    """Run a weekly equal-weight backtest from predicted positions."""
    engine = BacktestEngine(initial_capital=initial_capital, trading_config=trading_config)
    engine.run(
        price_frame,
        positions,
        execution_price_data=execution_price_frame,
        stop_price_data=stop_price_frame,
    )
    history = pd.DataFrame(engine.portfolio.history)
    if history.empty:
        return history, {}

    history["date"] = pd.to_datetime(history["date"])
    history = history.set_index("date").sort_index()
    history["weekly_return"] = history["total_value"].pct_change().fillna(0.0)
    total_return = (history["total_value"].iloc[-1] / initial_capital) - 1.0
    days = max((history.index[-1] - history.index[0]).days, 1)
    cagr = (1.0 + total_return) ** (365.25 / days) - 1.0
    history["cummax"] = history["total_value"].cummax()
    history["drawdown"] = (history["total_value"] - history["cummax"]) / history["cummax"]
    weekly_vol = history["weekly_return"].std()
    weekly_sharpe = 0.0 if weekly_vol == 0 or pd.isna(weekly_vol) else history["weekly_return"].mean() / weekly_vol * math.sqrt(52)
    metrics = {
        "strategy_total_return": float(total_return),
        "strategy_cagr": float(cagr),
        "strategy_max_drawdown": float(history["drawdown"].min()),
        "strategy_weekly_sharpe": float(weekly_sharpe),
    }
    for column in (
        "gross_trade_value",
        "total_cost",
        "commission",
        "exchange_fee",
        "transfer_fee",
        "stamp_duty",
        "impact_cost",
    ):
        if column in history.columns:
            metrics[f"strategy_{column}"] = float(history[column].sum())
    if "turnover_ratio" in history.columns:
        metrics["strategy_turnover_ratio_mean"] = float(history["turnover_ratio"].mean())
        metrics["strategy_turnover_ratio_total"] = float(history["turnover_ratio"].sum())
    if "trade_count" in history.columns:
        metrics["strategy_trade_count"] = float(history["trade_count"].sum())
    return history, metrics


def summarize_details(
    details: pd.DataFrame,
    group_columns: Sequence[str] = ("recipe",),
) -> pd.DataFrame:
    """Aggregate per-date metrics into a summary frame."""
    summaries: list[dict[str, float | str]] = []
    if details.empty:
        return pd.DataFrame()

    group_keys = list(group_columns)
    for group_values, frame in details.groupby(group_keys, dropna=False):
        if not isinstance(group_values, tuple):
            group_values = (group_values,)
        rank_ic_mean = frame["rank_ic"].mean()
        rank_ic_std = frame["rank_ic"].std()
        ic_mean = frame["ic"].mean()
        ic_std = frame["ic"].std()
        row: dict[str, float | str] = {
            column: value
            for column, value in zip(group_keys, group_values)
        }
        row.update(
            {
                "evaluation_dates": int(frame["feature_date"].nunique()),
                "coverage_mean": float(frame["coverage"].mean()),
                "rank_ic_mean": _float_or_nan(rank_ic_mean),
                "rank_ic_std": _float_or_nan(rank_ic_std),
                "rank_ic_ir": _float_or_nan(rank_ic_mean / rank_ic_std) if rank_ic_std and not pd.isna(rank_ic_std) else math.nan,
                "ic_mean": _float_or_nan(ic_mean),
                "ic_std": _float_or_nan(ic_std),
                "topk_mean_return_4w": _float_or_nan(frame["topk_mean_return_4w"].mean()),
                "topk_mean_excess_return_4w": _float_or_nan(frame["topk_mean_excess_return_4w"].mean()),
                "topk_hit_rate": _float_or_nan(frame["topk_hit_rate"].mean()),
                "universe_mean_return_4w": _float_or_nan(frame["universe_mean_return_4w"].mean()),
                "used_feature_count": int(frame["used_feature_count"].max()),
            }
        )
        summaries.append(row)
    return pd.DataFrame(summaries).sort_values(group_keys).reset_index(drop=True)


def build_signal_matrix(
    predictions: pd.DataFrame,
    price_frame: pd.DataFrame,
    topk: int,
    execution_lag_steps: int = 0,
) -> pd.DataFrame:
    """Build weekly equal-weight signals for backtesting."""
    all_symbols = list(price_frame.columns)
    signal = pd.DataFrame(0.0, index=price_frame.index, columns=all_symbols)
    if not all_symbols or predictions.empty or price_frame.empty:
        return signal

    price_index = pd.Index(pd.to_datetime(price_frame.index))
    for feature_date, frame in predictions.groupby("feature_date"):
        feature_timestamp = pd.Timestamp(feature_date)
        if feature_timestamp not in price_index:
            continue
        execution_position = price_index.get_loc(feature_timestamp)
        if not isinstance(execution_position, int):
            continue
        execution_position += int(execution_lag_steps)
        if execution_position < 0 or execution_position >= len(price_index):
            continue
        execution_date = price_index[execution_position]
        tradable_symbols = set(
            price_frame.loc[execution_date].dropna().index.astype(str)
        ) if execution_date in price_frame.index else set()
        top = frame.loc[frame["instrument"].astype(str).isin(tradable_symbols)].copy()
        top = top.sort_values("score", ascending=False).head(topk)
        if top.empty:
            continue
        weight = 1.0 / len(top)
        for symbol in top["instrument"]:
            signal.loc[execution_date, str(symbol)] = weight
    return signal


def attach_future_labels_to_prediction(
    score_frame: pd.DataFrame,
    panel: pd.DataFrame,
    feature_date: pd.Timestamp,
) -> pd.DataFrame:
    """Attach realized labels for one feature date to model predictions."""
    columns = ["instrument", "future_return_4w", LABEL_COLUMN, "close"]
    for extra in ("in_csi300", "in_csi500", "l1_name", "l2_name", "l3_name"):
        if extra in panel.columns:
            columns.append(extra)
    realized = panel.loc[pd.to_datetime(panel["datetime"]) == feature_date, columns].copy()
    realized["instrument"] = realized["instrument"].astype(str)
    merged = score_frame.merge(realized, on="instrument", how="left")
    merged["feature_date"] = feature_date
    return merged


def prepare_backtest_price_frame(price_frame: pd.DataFrame) -> pd.DataFrame:
    """Prepare weekly prices for backtesting by forward-filling temporary gaps."""
    prepared = price_frame.sort_index().copy()
    prepared = prepared.apply(pd.to_numeric, errors="coerce")
    prepared = prepared.ffill()
    prepared = prepared.dropna(axis=1, how="all")
    return prepared


def prepare_execution_price_frame(price_frame: pd.DataFrame) -> pd.DataFrame:
    """Prepare execution prices without forward-filling suspended or missing trade dates."""
    prepared = price_frame.sort_index().copy()
    prepared = prepared.apply(pd.to_numeric, errors="coerce")
    prepared = prepared.dropna(axis=1, how="all")
    return prepared


def ensure_output_dir(path: str | Path) -> Path:
    output_dir = Path(path).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def build_price_frame(
    panel: pd.DataFrame,
    eval_dates: Sequence[pd.Timestamp],
    symbols: Sequence[str] | None = None,
) -> pd.DataFrame:
    frame = panel.loc[panel["datetime"].isin(eval_dates), ["datetime", "instrument", "close"]].copy()
    if symbols:
        frame = frame.loc[frame["instrument"].astype(str).isin({str(symbol) for symbol in symbols})]
    price_frame = frame.pivot(index="datetime", columns="instrument", values="close").sort_index()
    return prepare_backtest_price_frame(price_frame)


def build_backtest_price_frames(
    panel: pd.DataFrame,
    eval_dates: Sequence[pd.Timestamp],
    symbols: Sequence[str] | None = None,
) -> BacktestPriceFrames:
    frame = panel.loc[panel["datetime"].isin(eval_dates)].copy()
    if symbols:
        frame = frame.loc[frame["instrument"].astype(str).isin({str(symbol) for symbol in symbols})]

    open_frame = frame.pivot(index="datetime", columns="instrument", values="open").sort_index()
    close_frame = frame.pivot(index="datetime", columns="instrument", values="close").sort_index()
    low_source = "low" if "low" in frame.columns else "close"
    low_frame = frame.pivot(index="datetime", columns="instrument", values=low_source).sort_index()

    return BacktestPriceFrames(
        execution_price=prepare_execution_price_frame(open_frame),
        mark_price=prepare_backtest_price_frame(close_frame),
        stop_price=prepare_execution_price_frame(low_frame),
    )


def _summary_row(summary: pd.DataFrame) -> dict[str, Any]:
    return {} if summary.empty else summary.iloc[0].to_dict()


def _slice_row_map(slice_summary: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if slice_summary.empty:
        return {}
    return {
        str(row["slice"]): row.to_dict()
        for _, row in slice_summary.iterrows()
    }


def objective_tuple(evaluation: RecipeEvaluation) -> tuple[float, float, float, float]:
    main = _summary_row(evaluation.summary)
    slices = _slice_row_map(evaluation.slice_summary)
    worst_slice_rank_ic_ir = min(
        _metric_or_neg_inf(slices.get(slice_name, {}).get("rank_ic_ir"))
        for slice_name in SLICE_FLAG_COLUMNS
    ) if slices else -math.inf
    return (
        _metric_or_neg_inf(main.get("rank_ic_ir")),
        worst_slice_rank_ic_ir,
        _metric_or_neg_inf(main.get("topk_mean_excess_return_4w")),
        _metric_or_neg_inf(main.get("strategy_max_drawdown")),
    )


def compare_recipe_evaluations(
    candidate: RecipeEvaluation,
    baseline: RecipeEvaluation,
) -> dict[str, float | bool]:
    candidate_main = _summary_row(candidate.summary)
    baseline_main = _summary_row(baseline.summary)
    candidate_slices = _slice_row_map(candidate.slice_summary)
    baseline_slices = _slice_row_map(baseline.slice_summary)

    slice_rank_ic_ir_deltas = []
    slice_drawdown_deltas = []
    for slice_name in SLICE_FLAG_COLUMNS:
        candidate_slice = candidate_slices.get(slice_name, {})
        baseline_slice = baseline_slices.get(slice_name, {})
        slice_rank_ic_ir_deltas.append(
            _float_or_nan(candidate_slice.get("rank_ic_ir")) - _float_or_nan(baseline_slice.get("rank_ic_ir"))
        )
        slice_drawdown_deltas.append(
            _float_or_nan(candidate_slice.get("strategy_max_drawdown")) - _float_or_nan(baseline_slice.get("strategy_max_drawdown"))
        )

    rank_ic_ir_delta = _float_or_nan(candidate_main.get("rank_ic_ir")) - _float_or_nan(baseline_main.get("rank_ic_ir"))
    topk_excess_delta = _float_or_nan(candidate_main.get("topk_mean_excess_return_4w")) - _float_or_nan(baseline_main.get("topk_mean_excess_return_4w"))
    drawdown_delta = _float_or_nan(candidate_main.get("strategy_max_drawdown")) - _float_or_nan(baseline_main.get("strategy_max_drawdown"))
    return {
        "rank_ic_mean_delta": _float_or_nan(candidate_main.get("rank_ic_mean")) - _float_or_nan(baseline_main.get("rank_ic_mean")),
        "rank_ic_ir_delta": rank_ic_ir_delta,
        "topk_mean_excess_return_4w_delta": topk_excess_delta,
        "strategy_total_return_delta": _float_or_nan(candidate_main.get("strategy_total_return")) - _float_or_nan(baseline_main.get("strategy_total_return")),
        "strategy_max_drawdown_delta": drawdown_delta,
        "worst_slice_rank_ic_ir_delta": min(slice_rank_ic_ir_deltas) if slice_rank_ic_ir_deltas else math.nan,
        "worst_slice_drawdown_delta": min(slice_drawdown_deltas) if slice_drawdown_deltas else math.nan,
    }


def passes_prune_gate(
    candidate: RecipeEvaluation,
    baseline: RecipeEvaluation,
) -> tuple[bool, dict[str, float | bool]]:
    deltas = compare_recipe_evaluations(candidate, baseline)
    rank_ok = (
        deltas["rank_ic_ir_delta"] >= 0
        or (
            deltas["rank_ic_ir_delta"] >= -0.01
            and deltas["strategy_max_drawdown_delta"] >= 0.01
        )
    )
    slice_ok = deltas["worst_slice_rank_ic_ir_delta"] >= 0
    topk_ok = deltas["topk_mean_excess_return_4w_delta"] >= -0.001
    accepted = bool(rank_ok and slice_ok and topk_ok)
    deltas["accepted"] = accepted
    return accepted, deltas


def passes_promotion_gate(
    candidate: RecipeEvaluation,
    baseline: RecipeEvaluation,
) -> tuple[bool, dict[str, float | bool]]:
    candidate_main = _summary_row(candidate.summary)
    baseline_main = _summary_row(baseline.summary)
    candidate_slices = _slice_row_map(candidate.slice_summary)
    baseline_slices = _slice_row_map(baseline.slice_summary)

    feature_reduction = 1.0 - (
        _float_or_nan(candidate_main.get("used_feature_count")) /
        max(_float_or_nan(baseline_main.get("used_feature_count")), 1.0)
    )
    rank_ok = (
        _float_or_nan(candidate_main.get("rank_ic_mean")) >= _float_or_nan(baseline_main.get("rank_ic_mean"))
        and _float_or_nan(candidate_main.get("rank_ic_ir")) >= _float_or_nan(baseline_main.get("rank_ic_ir"))
    )
    topk_ok = _float_or_nan(candidate_main.get("topk_mean_excess_return_4w")) >= _float_or_nan(baseline_main.get("topk_mean_excess_return_4w"))
    drawdown_ok = _float_or_nan(candidate_main.get("strategy_max_drawdown")) >= (_float_or_nan(baseline_main.get("strategy_max_drawdown")) - 0.02)
    slice_rank_ok = True
    slice_drawdown_ok = True
    for slice_name in SLICE_FLAG_COLUMNS:
        candidate_slice = candidate_slices.get(slice_name, {})
        baseline_slice = baseline_slices.get(slice_name, {})
        slice_rank_ok = slice_rank_ok and (
            _float_or_nan(candidate_slice.get("rank_ic_ir")) >= _float_or_nan(baseline_slice.get("rank_ic_ir"))
        )
        slice_drawdown_ok = slice_drawdown_ok and (
            _float_or_nan(candidate_slice.get("strategy_max_drawdown")) >= (_float_or_nan(baseline_slice.get("strategy_max_drawdown")) - 0.02)
        )
    reduction_ok = feature_reduction >= 0.25
    passed = bool(rank_ok and topk_ok and drawdown_ok and slice_rank_ok and slice_drawdown_ok and reduction_ok)
    return passed, {
        "promotion_gate_passed": passed,
        "feature_reduction_ratio": feature_reduction,
        "rank_ok": rank_ok,
        "topk_ok": topk_ok,
        "drawdown_ok": drawdown_ok,
        "slice_rank_ok": slice_rank_ok,
        "slice_drawdown_ok": slice_drawdown_ok,
        "reduction_ok": reduction_ok,
    }


def prefilter_feature_columns(
    panel: pd.DataFrame,
    feature_columns: Sequence[str],
    missing_threshold: float = 0.35,
    near_constant_threshold: float = 1e-8,
    corr_threshold: float = 0.9,
) -> tuple[list[str], pd.DataFrame, pd.DataFrame]:
    """
    Remove unusable features before research iterations.
    """
    numeric_frame = panel.copy()
    numeric_frame["datetime"] = pd.to_datetime(numeric_frame["datetime"])
    stats_rows: list[dict[str, Any]] = []
    kept_features: list[str] = []
    available_columns = [column for column in feature_columns if column in numeric_frame.columns]
    for column in available_columns:
        series = pd.to_numeric(numeric_frame[column], errors="coerce")
        missing_ratio = float(series.isna().mean())
        overall_std = float(series.std(skipna=True)) if series.notna().any() else math.nan
        if series.notna().any():
            cross_section_std = numeric_frame.assign(_value=series).groupby("datetime")["_value"].std().dropna()
            median_cross_std = float(cross_section_std.median()) if not cross_section_std.empty else math.nan
        else:
            median_cross_std = math.nan
        keep = (
            missing_ratio <= missing_threshold
            and pd.notna(overall_std)
            and overall_std > near_constant_threshold
            and pd.notna(median_cross_std)
            and median_cross_std > near_constant_threshold
        )
        stats_rows.append(
            {
                "feature": column,
                "missing_ratio": missing_ratio,
                "overall_std": overall_std,
                "median_cross_section_std": median_cross_std,
                "keep": keep,
            }
        )
        if keep:
            kept_features.append(column)

    stats = pd.DataFrame(stats_rows).sort_values(["keep", "missing_ratio", "feature"], ascending=[False, True, True]).reset_index(drop=True)
    corr_rows: list[dict[str, Any]] = []
    if len(kept_features) >= 2:
        corr_frame = numeric_frame[kept_features].apply(pd.to_numeric, errors="coerce")
        corr_frame = corr_frame.fillna(corr_frame.median(numeric_only=True))
        corr_matrix = corr_frame.corr().abs()
        for left, right in itertools.combinations(kept_features, 2):
            corr_value = float(corr_matrix.loc[left, right])
            if corr_value > corr_threshold:
                corr_rows.append(
                    {
                        "left_feature": left,
                        "right_feature": right,
                        "abs_corr": corr_value,
                    }
                )
    corr_marks = pd.DataFrame(corr_rows).sort_values(["abs_corr", "left_feature", "right_feature"], ascending=[False, True, True]).reset_index(drop=True) if corr_rows else pd.DataFrame(columns=["left_feature", "right_feature", "abs_corr"])
    return kept_features, stats, corr_marks


def build_feature_redundancy_report(
    panel: pd.DataFrame,
    feature_columns: Sequence[str],
    *,
    start_date: str | pd.Timestamp | None = None,
    end_date: str | pd.Timestamp | None = None,
    corr_threshold: float = 0.9,
) -> pd.DataFrame:
    window = panel.copy()
    window["datetime"] = pd.to_datetime(window["datetime"])
    if start_date is not None:
        window = window.loc[window["datetime"] >= pd.Timestamp(start_date)]
    if end_date is not None:
        window = window.loc[window["datetime"] <= pd.Timestamp(end_date)]

    available = [column for column in feature_columns if column in window.columns]
    if len(available) < 2 or window.empty:
        return pd.DataFrame(columns=["left_feature", "right_feature", "abs_corr", "cluster_id", "window_start", "window_end"])

    corr_frame = window[available].apply(pd.to_numeric, errors="coerce")
    corr_frame = corr_frame.fillna(corr_frame.median(numeric_only=True))
    corr_matrix = corr_frame.corr().abs()

    parent = {column: column for column in available}

    def _find(node: str) -> str:
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    def _union(left: str, right: str) -> None:
        left_root = _find(left)
        right_root = _find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    pair_rows: list[dict[str, Any]] = []
    for left, right in itertools.combinations(available, 2):
        corr_value = float(corr_matrix.loc[left, right])
        if corr_value <= corr_threshold:
            continue
        _union(left, right)
        pair_rows.append(
            {
                "left_feature": left,
                "right_feature": right,
                "abs_corr": corr_value,
            }
        )

    if not pair_rows:
        return pd.DataFrame(columns=["left_feature", "right_feature", "abs_corr", "cluster_id", "window_start", "window_end"])

    cluster_ids: dict[str, str] = {}
    cluster_index = 1
    for column in available:
        root = _find(column)
        if root not in cluster_ids:
            cluster_ids[root] = f"cluster_{cluster_index:02d}"
            cluster_index += 1

    window_start = str(window["datetime"].min().date()) if not window.empty else None
    window_end = str(window["datetime"].max().date()) if not window.empty else None
    for row in pair_rows:
        row["cluster_id"] = cluster_ids[_find(row["left_feature"])]
        row["window_start"] = window_start
        row["window_end"] = window_end
    return pd.DataFrame(pair_rows).sort_values(
        ["cluster_id", "abs_corr", "left_feature", "right_feature"],
        ascending=[True, False, True, True],
    ).reset_index(drop=True)


def build_feature_outlier_audit(
    panel: pd.DataFrame,
    feature_columns: Sequence[str],
    *,
    start_date: str | pd.Timestamp | None = None,
    end_date: str | pd.Timestamp | None = None,
) -> pd.DataFrame:
    window = panel.copy()
    window["datetime"] = pd.to_datetime(window["datetime"])
    if start_date is not None:
        window = window.loc[window["datetime"] >= pd.Timestamp(start_date)]
    if end_date is not None:
        window = window.loc[window["datetime"] <= pd.Timestamp(end_date)]

    audit_columns = get_outlier_audit_candidates(feature_columns)
    rows: list[dict[str, Any]] = []
    for column in audit_columns:
        if column not in window.columns:
            continue
        series = pd.to_numeric(window[column], errors="coerce").dropna()
        if series.empty:
            continue
        q1 = float(series.quantile(0.25))
        q50 = float(series.quantile(0.50))
        q99 = float(series.quantile(0.99))
        q01 = float(series.quantile(0.01))
        iqr = float(series.quantile(0.75) - series.quantile(0.25))
        if iqr > 0:
            lower = q1 - 1.5 * iqr
            upper = float(series.quantile(0.75)) + 1.5 * iqr
            extreme_ratio = float(((series < lower) | (series > upper)).mean())
        else:
            extreme_ratio = 0.0
        rows.append(
            {
                "feature": column,
                "non_null_count": int(series.shape[0]),
                "p01": q01,
                "p50": q50,
                "p99": q99,
                "iqr": iqr,
                "extreme_ratio": extreme_ratio,
                "window_start": str(window["datetime"].min().date()) if not window.empty else None,
                "window_end": str(window["datetime"].max().date()) if not window.empty else None,
            }
        )
    return pd.DataFrame(rows).sort_values(["extreme_ratio", "feature"], ascending=[False, True]).reset_index(drop=True) if rows else pd.DataFrame(
        columns=["feature", "non_null_count", "p01", "p50", "p99", "iqr", "extreme_ratio", "window_start", "window_end"]
    )


def build_model_params(
    base_params: dict[str, Any] | None,
    num_boost_round: int,
    early_stopping_rounds: int,
) -> dict[str, Any]:
    params = {
        "num_boost_round": num_boost_round,
        "early_stopping_rounds": early_stopping_rounds,
    }
    if base_params:
        params.update(base_params)
    return params


def evaluate_recipe(
    panel: pd.DataFrame,
    recipe: ModelRecipe,
    eval_dates: Sequence[pd.Timestamp],
    topk: int,
    train_weeks: int,
    valid_weeks: int,
    cache: dict[tuple[Any, ...], RecipeEvaluation] | None = None,
    runtime_cache: EvaluationRuntimeCache | None = None,
    progress_callback: ProgressCallback | None = None,
    progress_context: dict[str, Any] | None = None,
) -> RecipeEvaluation:
    runtime_cache = _resolve_runtime_cache(cache, runtime_cache)
    cache = runtime_cache.evaluation_cache
    key = _recipe_cache_key(
        recipe=recipe,
        eval_dates=eval_dates,
        topk=topk,
        train_weeks=train_weeks,
        valid_weeks=valid_weeks,
    )
    if key in cache:
        _emit_progress(
            progress_callback,
            "recipe_evaluation_cache_hit",
            progress_context=progress_context,
            recipe_name=recipe.name,
            feature_count=len(recipe.feature_columns),
            eval_date_total=len(eval_dates),
        )
        return cache[key]

    normalized_panel, normalization_cache_key = get_or_prepare_normalized_panel(
        panel,
        recipe=recipe,
        runtime_cache=runtime_cache,
        progress_callback=progress_callback,
        progress_context=progress_context,
    )

    all_predictions: list[pd.DataFrame] = []
    all_details: list[pd.DataFrame] = []
    all_slice_details: list[pd.DataFrame] = []
    eval_started_at = time.monotonic()
    total_eval_dates = len(eval_dates)

    _emit_progress(
        progress_callback,
        "recipe_evaluation_start",
        progress_context=progress_context,
        recipe_name=recipe.name,
        feature_count=len(recipe.feature_columns),
        eval_date_total=total_eval_dates,
    )

    for eval_date_index, feature_date in enumerate(eval_dates, start=1):
        _emit_progress(
            progress_callback,
            "eval_date_start",
            progress_context=progress_context,
            recipe_name=recipe.name,
            feature_count=len(recipe.feature_columns),
            eval_date_index=eval_date_index,
            eval_date_total=total_eval_dates,
            feature_date=str(pd.Timestamp(feature_date).date()),
        )
        prepared_input = get_or_prepare_model_input(
            panel=normalized_panel,
            feature_date=feature_date,
            feature_columns=recipe.feature_columns,
            train_weeks=train_weeks,
            valid_weeks=valid_weeks,
            normalization_cache_key=normalization_cache_key,
            runtime_cache=runtime_cache,
            progress_callback=progress_callback,
            progress_context=progress_context,
        )
        score_frame = fit_predict_one_date(
            panel=normalized_panel,
            feature_date=feature_date,
            feature_columns=recipe.feature_columns,
            train_weeks=train_weeks,
            valid_weeks=valid_weeks,
            model_params=recipe.model_params,
            num_boost_round=int(recipe.model_params.get("num_boost_round", 200)),
            early_stopping_rounds=int(recipe.model_params.get("early_stopping_rounds", 50)),
            prepared_input=prepared_input,
        )
        merged = attach_future_labels_to_prediction(score_frame, normalized_panel, feature_date)
        merged["recipe"] = recipe.name
        all_predictions.append(merged)

        metrics = evaluate_prediction_frame(merged, topk=topk)
        metrics.update(
            {
                "recipe": recipe.name,
                "feature_date": pd.Timestamp(feature_date),
                "used_feature_count": int(merged["used_feature_count"].max()),
            }
        )
        all_details.append(pd.DataFrame([metrics]))

        for slice_name, flag_column in SLICE_FLAG_COLUMNS.items():
            if flag_column not in merged.columns:
                continue
            slice_frame = merged.loc[merged[flag_column].fillna(False)].copy()
            slice_metrics = evaluate_prediction_frame(slice_frame, topk=topk)
            slice_metrics.update(
                {
                    "recipe": recipe.name,
                    "slice": slice_name,
                    "feature_date": pd.Timestamp(feature_date),
                    "used_feature_count": int(merged["used_feature_count"].max()),
                }
            )
            all_slice_details.append(pd.DataFrame([slice_metrics]))
        _emit_progress(
            progress_callback,
            "eval_date_complete",
            progress_context=progress_context,
            recipe_name=recipe.name,
            feature_count=len(recipe.feature_columns),
            eval_date_index=eval_date_index,
            eval_date_total=total_eval_dates,
            feature_date=str(pd.Timestamp(feature_date).date()),
            elapsed_seconds=time.monotonic() - eval_started_at,
        )

    predictions = pd.concat(all_predictions, ignore_index=True)
    details = pd.concat(all_details, ignore_index=True).sort_values(["recipe", "feature_date"]).reset_index(drop=True)
    slice_details = pd.concat(all_slice_details, ignore_index=True).sort_values(["recipe", "slice", "feature_date"]).reset_index(drop=True) if all_slice_details else pd.DataFrame()

    summary = summarize_details(details)
    slice_summary = summarize_details(slice_details, group_columns=("recipe", "slice")) if not slice_details.empty else pd.DataFrame()

    price_frame = build_price_frame(normalized_panel, eval_dates=eval_dates)

    equity_frames: list[pd.DataFrame] = []
    summary_metrics: list[dict[str, Any]] = []
    signal_data = build_signal_matrix(predictions, price_frame, topk=topk)
    equity_curve, metrics = run_strategy_backtest(price_frame, signal_data)
    if not equity_curve.empty:
        equity_frame = equity_curve.reset_index().rename(columns={"date": "datetime"})
        equity_frame["recipe"] = recipe.name
        equity_frame["slice"] = "all"
        equity_frames.append(equity_frame)
    summary_metrics.append({"recipe": recipe.name, **metrics})

    slice_metrics_rows: list[dict[str, Any]] = []
    for slice_name, flag_column in SLICE_FLAG_COLUMNS.items():
        if flag_column not in predictions.columns:
            continue
        slice_predictions = predictions.loc[predictions[flag_column].fillna(False)].copy()
        if slice_predictions.empty:
            slice_metrics_rows.append({"recipe": recipe.name, "slice": slice_name})
            continue
        slice_symbols = sorted(slice_predictions["instrument"].astype(str).unique().tolist())
        slice_price_frame = build_price_frame(normalized_panel, eval_dates=eval_dates, symbols=slice_symbols)
        slice_signal_data = build_signal_matrix(slice_predictions, slice_price_frame, topk=topk)
        slice_equity_curve, slice_metrics = run_strategy_backtest(slice_price_frame, slice_signal_data)
        if not slice_equity_curve.empty:
            slice_equity = slice_equity_curve.reset_index().rename(columns={"date": "datetime"})
            slice_equity["recipe"] = recipe.name
            slice_equity["slice"] = slice_name
            equity_frames.append(slice_equity)
        slice_metrics_rows.append({"recipe": recipe.name, "slice": slice_name, **slice_metrics})

    summary = summary.merge(pd.DataFrame(summary_metrics), on="recipe", how="left").reset_index(drop=True)
    if not slice_summary.empty:
        slice_summary = slice_summary.merge(pd.DataFrame(slice_metrics_rows), on=["recipe", "slice"], how="left").reset_index(drop=True)

    evaluation = RecipeEvaluation(
        recipe=recipe,
        summary=summary,
        slice_summary=slice_summary,
        details=details,
        slice_details=slice_details,
        predictions=predictions,
        equity_curve=pd.concat(equity_frames, ignore_index=True) if equity_frames else pd.DataFrame(),
    )
    cache[key] = evaluation
    _emit_progress(
        progress_callback,
        "recipe_evaluation_complete",
        progress_context=progress_context,
        recipe_name=recipe.name,
        feature_count=len(recipe.feature_columns),
        eval_date_total=total_eval_dates,
        elapsed_seconds=time.monotonic() - eval_started_at,
        evaluation=evaluation,
    )
    return evaluation


def _micro_candidates(best_params: dict[str, Any]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen = set()
    for key, values in TUNING_VALUE_SPACE.items():
        if key not in best_params:
            continue
        current_value = best_params[key]
        if current_value not in values:
            continue
        index = values.index(current_value)
        neighbor_indices = {index}
        if index > 0:
            neighbor_indices.add(index - 1)
        if index < len(values) - 1:
            neighbor_indices.add(index + 1)
        for neighbor_index in sorted(neighbor_indices):
            candidate = dict(best_params)
            candidate[key] = values[neighbor_index]
            serialized = tuple(sorted(candidate.items()))
            if serialized in seen:
                continue
            seen.add(serialized)
            candidates.append(candidate)
            if len(candidates) >= 6:
                return candidates
    return candidates[:6]


def tune_recipe(
    panel: pd.DataFrame,
    recipe: ModelRecipe,
    eval_dates: Sequence[pd.Timestamp],
    topk: int,
    train_weeks: int,
    valid_weeks: int,
    cache: dict[tuple[Any, ...], RecipeEvaluation] | None = None,
    runtime_cache: EvaluationRuntimeCache | None = None,
    coarse_limit: int | None = None,
    micro_limit: int | None = None,
    progress_callback: ProgressCallback | None = None,
    progress_context: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], pd.DataFrame, RecipeEvaluation]:
    runtime_cache = _resolve_runtime_cache(cache, runtime_cache)
    cache = runtime_cache.evaluation_cache
    tuning_rows: list[dict[str, Any]] = []
    best_eval: RecipeEvaluation | None = None
    best_params: dict[str, Any] | None = None

    for phase, candidates in (
        ("coarse", COARSE_TUNING_CANDIDATES),
        ("micro", []),
    ):
        if phase == "micro":
            if best_params is None:
                break
            candidates = _micro_candidates(best_params)
            if micro_limit is not None:
                candidates = candidates[:micro_limit]
        elif coarse_limit is not None:
            candidates = candidates[:coarse_limit]
        _emit_progress(
            progress_callback,
            "tune_phase_start",
            progress_context=progress_context,
            recipe_name=recipe.name,
            feature_count=len(recipe.feature_columns),
            phase=phase,
            candidate_total=len(candidates),
        )
        for index, params in enumerate(candidates, start=1):
            candidate_recipe = replace(recipe, model_params={**params, "early_stopping_rounds": 50})
            candidate_context = dict(progress_context or {})
            candidate_context.update(
                {
                    "phase": phase,
                    "candidate_index": index,
                    "candidate_total": len(candidates),
                }
            )
            _emit_progress(
                progress_callback,
                "candidate_start",
                progress_context=candidate_context,
                recipe_name=candidate_recipe.name,
                feature_count=len(candidate_recipe.feature_columns),
                params=params,
            )
            evaluation = evaluate_recipe(
                panel=panel,
                recipe=candidate_recipe,
                eval_dates=eval_dates,
                topk=topk,
                train_weeks=train_weeks,
                valid_weeks=valid_weeks,
                cache=cache,
                runtime_cache=runtime_cache,
                progress_callback=progress_callback,
                progress_context=candidate_context,
            )
            main_row = _summary_row(evaluation.summary)
            slices = _slice_row_map(evaluation.slice_summary)
            row = {
                "phase": phase,
                "candidate_index": index,
                **params,
                "rank_ic_mean": main_row.get("rank_ic_mean"),
                "rank_ic_ir": main_row.get("rank_ic_ir"),
                "topk_mean_excess_return_4w": main_row.get("topk_mean_excess_return_4w"),
                "strategy_max_drawdown": main_row.get("strategy_max_drawdown"),
                "worst_slice_rank_ic_ir": min(
                    _metric_or_neg_inf(slices.get(slice_name, {}).get("rank_ic_ir"))
                    for slice_name in SLICE_FLAG_COLUMNS
                ) if slices else math.nan,
            }
            tuning_rows.append(row)
            is_best_so_far = best_eval is None or objective_tuple(evaluation) > objective_tuple(best_eval)
            if is_best_so_far:
                best_eval = evaluation
                best_params = {**params, "early_stopping_rounds": 50}
            _emit_progress(
                progress_callback,
                "candidate_complete",
                progress_context=candidate_context,
                recipe_name=candidate_recipe.name,
                feature_count=len(candidate_recipe.feature_columns),
                params=params,
                row=row,
                is_best_so_far=is_best_so_far,
                evaluation=evaluation,
            )

    if best_eval is None or best_params is None:
        raise ValueError("Failed to tune recipe: no candidate evaluations were produced")

    return best_params, pd.DataFrame(tuning_rows), best_eval


def prune_feature_groups(
    panel: pd.DataFrame,
    base_recipe: ModelRecipe,
    base_evaluation: RecipeEvaluation,
    eval_dates: Sequence[pd.Timestamp],
    topk: int,
    train_weeks: int,
    valid_weeks: int,
    cache: dict[tuple[Any, ...], RecipeEvaluation] | None = None,
    runtime_cache: EvaluationRuntimeCache | None = None,
    max_rounds: int | None = None,
    candidate_limit: int | None = None,
    progress_callback: ProgressCallback | None = None,
    progress_context: dict[str, Any] | None = None,
) -> tuple[ModelRecipe, RecipeEvaluation, pd.DataFrame]:
    runtime_cache = _resolve_runtime_cache(cache, runtime_cache)
    cache = runtime_cache.evaluation_cache
    current_recipe = base_recipe
    current_evaluation = base_evaluation
    current_groups = [group for group in base_recipe.feature_groups if any(feature in current_recipe.feature_columns for feature in FEATURE_GROUP_COLUMNS.get(group, ()))]
    log_rows: list[dict[str, Any]] = []
    round_index = 1

    while True:
        if max_rounds is not None and round_index > max_rounds:
            break
        attempts: list[tuple[RecipeEvaluation, dict[str, Any], str]] = []
        candidate_groups = list(current_groups)
        if candidate_limit is not None:
            candidate_groups = candidate_groups[:candidate_limit]
        _emit_progress(
            progress_callback,
            "prune_round_start",
            progress_context=progress_context,
            prune_kind="group",
            round=round_index,
            recipe_name=current_recipe.name,
            feature_count=len(current_recipe.feature_columns),
            candidate_total=len(candidate_groups),
        )
        for candidate_index, group_name in enumerate(candidate_groups, start=1):
            group_features = [feature for feature in FEATURE_GROUP_COLUMNS.get(group_name, ()) if feature in current_recipe.feature_columns]
            if not group_features or len(group_features) >= len(current_recipe.feature_columns):
                continue
            candidate_features = tuple(feature for feature in current_recipe.feature_columns if feature not in group_features)
            candidate_group_set = tuple(group for group in current_groups if group != group_name)
            candidate_recipe = ModelRecipe(
                name=f"{base_recipe.name}__group_prune__{group_name}",
                feature_columns=candidate_features,
                feature_groups=candidate_group_set,
                industry_normalization=current_recipe.industry_normalization,
                model_params=current_recipe.model_params,
            )
            candidate_context = dict(progress_context or {})
            candidate_context.update(
                {
                    "prune_kind": "group",
                    "round": round_index,
                    "candidate_index": candidate_index,
                    "candidate_total": len(candidate_groups),
                    "removed_group": group_name,
                }
            )
            candidate_evaluation = evaluate_recipe(
                panel=panel,
                recipe=candidate_recipe,
                eval_dates=eval_dates,
                topk=topk,
                train_weeks=train_weeks,
                valid_weeks=valid_weeks,
                cache=cache,
                runtime_cache=runtime_cache,
                progress_callback=progress_callback,
                progress_context=candidate_context,
            )
            accepted, deltas = passes_prune_gate(candidate_evaluation, current_evaluation)
            attempt_row = {
                "round": round_index,
                "removed_group": group_name,
                "removed_features": ",".join(group_features),
                "feature_count_before": len(current_recipe.feature_columns),
                "feature_count_after": len(candidate_features),
                **deltas,
                "selected": False,
            }
            attempts.append(
                (
                    candidate_evaluation,
                    attempt_row,
                    group_name,
                )
            )
            _emit_progress(
                progress_callback,
                "prune_candidate_complete",
                progress_context=candidate_context,
                recipe_name=candidate_recipe.name,
                feature_count=len(candidate_recipe.feature_columns),
                accepted=accepted,
                row=attempt_row,
                evaluation=candidate_evaluation,
            )

        if not attempts:
            break

        accepted_attempts = [attempt for attempt in attempts if attempt[1]["accepted"]]
        if not accepted_attempts:
            log_rows.extend(row for _, row, _ in attempts)
            break

        best_eval, best_row, selected_group = max(
            accepted_attempts,
            key=lambda item: objective_tuple(item[0]),
        )
        best_row["selected"] = True
        log_rows.extend(row for _, row, _ in attempts)

        current_evaluation = best_eval
        current_recipe = ModelRecipe(
            name=f"{base_recipe.name}__group_pruned_round_{round_index}",
            feature_columns=best_eval.recipe.feature_columns,
            feature_groups=tuple(group for group in current_groups if group != selected_group),
            industry_normalization=current_recipe.industry_normalization,
            model_params=current_recipe.model_params,
        )
        _emit_progress(
            progress_callback,
            "prune_round_selected",
            progress_context=progress_context,
            prune_kind="group",
            round=round_index,
            recipe_name=current_recipe.name,
            feature_count=len(current_recipe.feature_columns),
            row=best_row,
            evaluation=best_eval,
        )
        current_groups = list(current_recipe.feature_groups)
        round_index += 1

    return current_recipe, current_evaluation, pd.DataFrame(log_rows)


def prune_features(
    panel: pd.DataFrame,
    base_recipe: ModelRecipe,
    base_evaluation: RecipeEvaluation,
    eval_dates: Sequence[pd.Timestamp],
    topk: int,
    train_weeks: int,
    valid_weeks: int,
    cache: dict[tuple[Any, ...], RecipeEvaluation] | None = None,
    runtime_cache: EvaluationRuntimeCache | None = None,
    max_rounds: int | None = None,
    candidate_limit: int | None = None,
    progress_callback: ProgressCallback | None = None,
    progress_context: dict[str, Any] | None = None,
) -> tuple[ModelRecipe, RecipeEvaluation, list[dict[str, Any]]]:
    runtime_cache = _resolve_runtime_cache(cache, runtime_cache)
    cache = runtime_cache.evaluation_cache
    current_recipe = base_recipe
    current_evaluation = base_evaluation
    log_rows: list[dict[str, Any]] = []
    round_index = 1

    while len(current_recipe.feature_columns) > 1:
        if max_rounds is not None and round_index > max_rounds:
            break
        attempts: list[tuple[RecipeEvaluation, dict[str, Any], str]] = []
        candidate_features = list(current_recipe.feature_columns)
        if candidate_limit is not None:
            candidate_features = candidate_features[:candidate_limit]
        _emit_progress(
            progress_callback,
            "prune_round_start",
            progress_context=progress_context,
            prune_kind="feature",
            round=round_index,
            recipe_name=current_recipe.name,
            feature_count=len(current_recipe.feature_columns),
            candidate_total=len(candidate_features),
        )
        for candidate_index, feature_name in enumerate(candidate_features, start=1):
            candidate_feature_set = tuple(feature for feature in current_recipe.feature_columns if feature != feature_name)
            candidate_groups = tuple(
                group_name
                for group_name in current_recipe.feature_groups
                if any(feature in candidate_feature_set for feature in FEATURE_GROUP_COLUMNS.get(group_name, ()))
            )
            candidate_recipe = ModelRecipe(
                name=f"{base_recipe.name}__feature_prune__{feature_name}",
                feature_columns=candidate_feature_set,
                feature_groups=candidate_groups,
                industry_normalization=current_recipe.industry_normalization,
                model_params=current_recipe.model_params,
            )
            candidate_context = dict(progress_context or {})
            candidate_context.update(
                {
                    "prune_kind": "feature",
                    "round": round_index,
                    "candidate_index": candidate_index,
                    "candidate_total": len(candidate_features),
                    "removed_feature": feature_name,
                }
            )
            candidate_evaluation = evaluate_recipe(
                panel=panel,
                recipe=candidate_recipe,
                eval_dates=eval_dates,
                topk=topk,
                train_weeks=train_weeks,
                valid_weeks=valid_weeks,
                cache=cache,
                runtime_cache=runtime_cache,
                progress_callback=progress_callback,
                progress_context=candidate_context,
            )
            accepted, deltas = passes_prune_gate(candidate_evaluation, current_evaluation)
            attempt_row = {
                "round": round_index,
                "removed_feature": feature_name,
                "feature_count_before": len(current_recipe.feature_columns),
                "feature_count_after": len(candidate_feature_set),
                **deltas,
                "selected": False,
            }
            attempts.append(
                (
                    candidate_evaluation,
                    attempt_row,
                    feature_name,
                )
            )
            _emit_progress(
                progress_callback,
                "prune_candidate_complete",
                progress_context=candidate_context,
                recipe_name=candidate_recipe.name,
                feature_count=len(candidate_recipe.feature_columns),
                accepted=accepted,
                row=attempt_row,
                evaluation=candidate_evaluation,
            )

        if not attempts:
            break

        accepted_attempts = [attempt for attempt in attempts if attempt[1]["accepted"]]
        if not accepted_attempts:
            log_rows.extend(row for _, row, _ in attempts)
            break

        best_eval, best_row, selected_feature = max(
            accepted_attempts,
            key=lambda item: objective_tuple(item[0]),
        )
        best_row["selected"] = True
        log_rows.extend(row for _, row, _ in attempts)

        current_evaluation = best_eval
        current_recipe = ModelRecipe(
            name=f"{base_recipe.name}__feature_pruned_round_{round_index}",
            feature_columns=tuple(feature for feature in current_recipe.feature_columns if feature != selected_feature),
            feature_groups=tuple(
                group_name
                for group_name in current_recipe.feature_groups
                if any(feature in best_eval.recipe.feature_columns for feature in FEATURE_GROUP_COLUMNS.get(group_name, ()))
            ),
            industry_normalization=current_recipe.industry_normalization,
            model_params=current_recipe.model_params,
        )
        _emit_progress(
            progress_callback,
            "prune_round_selected",
            progress_context=progress_context,
            prune_kind="feature",
            round=round_index,
            recipe_name=current_recipe.name,
            feature_count=len(current_recipe.feature_columns),
            row=best_row,
            evaluation=best_eval,
        )
        round_index += 1

    return current_recipe, current_evaluation, log_rows
