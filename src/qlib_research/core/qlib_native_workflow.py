"""
Scriptable qlib-native weekly research workflow.

The notebook can stay focused on display and commentary while this module owns
the actual research pipeline: panel preparation, signal training, diagnostics,
native backtests, validation comparisons, and artifact persistence.
"""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from dataclasses import asdict, dataclass, field
from dataclasses import replace
import json
from multiprocessing import get_context
import os
from pathlib import Path
import random
import time
from typing import Any, Callable, Iterable, Literal, Sequence

import lightgbm as lgb
import numpy as np
import pandas as pd

from qlib_research.core.portfolio import BacktestTradingConfig
from qlib_research.core.index_benchmark import (
    build_universe_benchmark_frame,
    build_universe_benchmark_series,
    resolve_benchmark_components,
)
from qlib_research.core.notebook_workflow import (
    filter_panel_by_universe_profile,
    groups_for_features,
    load_feature_config,
)
from qlib_research.core.qlib_native_backtest import (
    build_native_portfolio_diagnostics,
    build_native_quote_frame,
    build_native_signal_frame,
    build_signal_diagnostics,
    ensure_minimal_qlib_provider,
    run_native_backtest_analysis,
    select_topk_with_buffer,
)
from qlib_research.core.qlib_pipeline import (
    apply_industry_normalization,
    compose_feature_columns,
    get_normalized_feature_candidates,
    init_qlib_runtime,
    load_panel_dataframe,
    normalize_feature_name_list,
    resolve_feature_columns,
)
from qlib_research.core.weekly_feature_panel import (
    PanelEnrichmentScope,
    ensure_panel_enrichment,
    export_weekly_feature_panel,
    load_feature_panel_enrichment_scope,
)
from qlib_research.io.artifacts import build_portfolio_targets, publish_portfolio_targets, publish_score_snapshot

from qlib_research.core.weekly_model_eval import (
    build_backtest_price_frames,
    build_feature_outlier_audit,
    build_feature_redundancy_report,
    default_weekly_net_backtest_config,
    prefilter_feature_columns,
    run_strategy_backtest,
    summarize_details,
)


RAW_LABEL_COLUMN_MAP = {
    "excess_4w": "label_excess_return_4w",
    "excess_8w": "label_excess_return_8w",
}

MONTH_HEATMAP_COLUMNS = [f"{month:02d}" for month in range(1, 13)]
NATIVE_WORKFLOW_SUMMARY_SCHEMA_VERSION = 2

RESEARCH_DEFAULT_FEATURE_GROUPS = (
    "technical_core",
    "technical_flow",
    "valuation_absolute",
    "valuation_percentile",
    "industry_valuation_context",
    "quality_summary",
    "buffett_moat",
    "fscore_components",
    "ttm_profitability",
    "macro_cycle_flags",
    "derived_momentum",
    "derived_volatility",
    "derived_liquidity",
    "derived_valuation_delta",
    "derived_quality_delta",
    "industry_relative",
    "macro_interactions",
)

SignalObjective = Literal[
    "mse_regression",
    "mae_regression",
    "huber_regression",
    "binary_top_quintile",
    "grouped_rank",
]

LabelRecipe = Literal[
    "excess_4w",
    "excess_8w",
    "blended_excess_4w_8w",
]

RunExportMode = Literal["always", "auto_if_missing", "never"]


@dataclass(frozen=True)
class NativeResearchRecipe:
    name: str
    signal_objective: SignalObjective = "huber_regression"
    label_recipe: LabelRecipe = "blended_excess_4w_8w"
    feature_groups: tuple[str, ...] = field(default_factory=lambda: RESEARCH_DEFAULT_FEATURE_GROUPS)
    included_features: tuple[str, ...] = field(default_factory=tuple)
    excluded_features: tuple[str, ...] = field(default_factory=tuple)
    industry_normalization: str = "l1_weekly_robust"
    model_params: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class NativeWorkflowConfig:
    universe_profile: str = "csi300"
    panel_path: str | Path = "artifacts/panels/csi300_weekly.parquet"
    execution_panel_path: str | Path | None = None
    output_dir: str | Path = "artifacts/native_workflow/csi300"
    task_description: str | None = None
    start_date: str | None = "2016-01-01"
    end_date: str | None = None
    batch_size: int = 200
    run_export: RunExportMode = "auto_if_missing"
    topk: int = 10
    train_weeks: int = 260
    valid_weeks: int = 52
    eval_count: int = 52
    rolling_recent_weeks: int = 52
    step_weeks: int = 1
    walk_forward_enabled: bool = True
    walk_forward_start_date: str | None = "2016-01-01"
    walk_forward_end_date: str | None = None
    walk_forward_train_weeks: int = 260
    walk_forward_valid_weeks: int = 52
    walk_forward_step_weeks: int = 1
    walk_forward_eval_count: int = 0
    benchmark_mode: str = "auto"
    signal_objective: SignalObjective = "huber_regression"
    label_recipe: LabelRecipe = "blended_excess_4w_8w"
    rebalance_interval_weeks: int = 1
    hold_buffer_rank: int | None = None
    universe_exit_policy: str = "retain_quotes_for_existing_positions"
    min_liquidity_filter: float = 0.0
    min_score_spread: float = 0.0
    industry_max_weight: float | None = None
    diagnostics_enabled: bool = True
    run_validation_comparison: bool = True
    validation_execution_lag_steps: int = 1
    validation_only_tradable: bool = False
    validation_risk_degree: float = 1.0
    native_risk_degree: float = 0.95
    native_only_tradable: bool = True
    account: float = 1_000_000.0
    seed: int = 42
    reproducibility_mode: Literal["balanced", "strict"] = "balanced"
    recipe_parallel_workers: int = 1
    model_num_threads: int | None = None
    publish_model: bool = False
    feature_spec_path: str | Path | None = None
    feature_groups: tuple[str, ...] = field(default_factory=lambda: RESEARCH_DEFAULT_FEATURE_GROUPS)
    included_features: tuple[str, ...] = field(default_factory=tuple)
    excluded_features: tuple[str, ...] = field(default_factory=tuple)


@dataclass
class NativeRecipeArtifacts:
    recipe: NativeResearchRecipe
    latest_score_frame: pd.DataFrame
    prediction_bundles: dict[str, dict[str, Any]]
    native_results: dict[str, Any]
    validation_results: dict[str, dict[str, Any]]
    executor_comparison_summary: pd.DataFrame
    signal_diagnostics: pd.DataFrame
    portfolio_diagnostics: pd.DataFrame
    slice_regime_summary: pd.DataFrame
    feature_prefilter_stats: pd.DataFrame
    feature_corr_candidates: pd.DataFrame
    feature_redundancy: pd.DataFrame
    feature_outlier_audit: pd.DataFrame
    used_feature_columns: list[str]
    native_provider_dir: Path
    benchmark_frames: dict[str, pd.DataFrame]
    native_summary: pd.DataFrame


def apply_research_seed(seed: int) -> None:
    random.seed(int(seed))
    np.random.seed(int(seed))


def build_model_params(
    seed: int,
    reproducibility_mode: str,
    signal_objective: SignalObjective,
    overrides: dict[str, Any] | None = None,
    num_threads: int | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "learning_rate": 0.05,
        "num_leaves": 31,
        "min_data_in_leaf": 50,
        "feature_fraction": 0.8,
        "lambda_l2": 1.0,
        "num_boost_round": 300,
        "early_stopping_rounds": 50,
        "verbosity": -1,
        "seed": int(seed),
        "feature_fraction_seed": int(seed),
        "bagging_seed": int(seed),
        "data_random_seed": int(seed),
    }
    if reproducibility_mode == "strict":
        params.update({"deterministic": True, "num_threads": 1})
    elif num_threads is not None:
        params["num_threads"] = max(int(num_threads), 1)
    objective_defaults: dict[SignalObjective, dict[str, Any]] = {
        "mse_regression": {"objective": "regression", "metric": "l2"},
        "mae_regression": {"objective": "regression_l1", "metric": "l1"},
        "huber_regression": {"objective": "huber", "metric": "huber"},
        "binary_top_quintile": {"objective": "binary", "metric": "binary_logloss", "is_unbalance": True},
        "grouped_rank": {"objective": "lambdarank", "metric": "ndcg", "ndcg_eval_at": [10]},
    }
    params.update(objective_defaults[signal_objective])
    if overrides:
        params.update(overrides)
    return params


NativeWorkflowProgressCallback = Callable[[dict[str, Any]], None]


def _emit_native_workflow_progress(
    progress_callback: NativeWorkflowProgressCallback | None,
    event: str,
    **payload: Any,
) -> None:
    if progress_callback is None:
        return
    progress_callback({"event": event, **payload})


PARALLEL_PROGRESS_HEARTBEAT_SECONDS = 30.0


def _build_parallel_recipe_heartbeat(
    pending_futures: set[Any],
    future_to_meta: dict[Any, dict[str, Any]],
    *,
    completed: int,
    total: int,
    now: float,
    max_listed_recipes: int = 3,
) -> dict[str, Any]:
    active_rows: list[dict[str, Any]] = []
    for future in pending_futures:
        meta = future_to_meta.get(future)
        if meta is None:
            continue
        elapsed = max(0.0, now - float(meta["started_at"]))
        active_rows.append(
            {
                "recipe": str(meta["recipe"]),
                "index": int(meta["index"]),
                "elapsed": elapsed,
            }
        )
    active_rows.sort(key=lambda row: row["elapsed"], reverse=True)
    listed = active_rows[:max_listed_recipes]
    return {
        "active_recipe_count": len(active_rows),
        "active_recipes": [row["recipe"] for row in listed],
        "active_recipe_elapsed_seconds": [row["elapsed"] for row in listed],
        "oldest_recipe": listed[0]["recipe"] if listed else None,
        "oldest_recipe_elapsed": listed[0]["elapsed"] if listed else 0.0,
        "completed": completed,
        "total": total,
    }


def _resolve_recipe_parallel_workers(requested_workers: int, recipe_count: int) -> int:
    if recipe_count <= 1:
        return 1
    normalized_requested = int(requested_workers)
    if normalized_requested == 0:
        return max(1, min(recipe_count, os.cpu_count() or recipe_count))
    return max(1, min(recipe_count, normalized_requested))


def _resolve_model_num_threads(
    requested_threads: int | None,
    *,
    recipe_parallel_workers: int,
    reproducibility_mode: str,
) -> int | None:
    if requested_threads is not None:
        return max(int(requested_threads), 1)
    if reproducibility_mode == "strict":
        return 1
    if recipe_parallel_workers <= 1:
        return None
    cpu_total = os.cpu_count() or recipe_parallel_workers
    return max(1, cpu_total // recipe_parallel_workers)


def _compact_native_recipe_artifacts(artifacts: NativeRecipeArtifacts) -> NativeRecipeArtifacts:
    return NativeRecipeArtifacts(
        recipe=artifacts.recipe,
        latest_score_frame=artifacts.latest_score_frame,
        prediction_bundles={},
        native_results={},
        validation_results={},
        executor_comparison_summary=artifacts.executor_comparison_summary,
        signal_diagnostics=artifacts.signal_diagnostics,
        portfolio_diagnostics=artifacts.portfolio_diagnostics,
        slice_regime_summary=artifacts.slice_regime_summary,
        feature_prefilter_stats=artifacts.feature_prefilter_stats,
        feature_corr_candidates=artifacts.feature_corr_candidates,
        feature_redundancy=artifacts.feature_redundancy,
        feature_outlier_audit=artifacts.feature_outlier_audit,
        used_feature_columns=artifacts.used_feature_columns,
        native_provider_dir=artifacts.native_provider_dir,
        benchmark_frames={},
        native_summary=artifacts.native_summary,
    )


def _run_native_recipe_job(
    config: NativeWorkflowConfig,
    recipe: NativeResearchRecipe,
) -> tuple[str, NativeRecipeArtifacts]:
    return recipe.name, _compact_native_recipe_artifacts(run_native_recipe(config, recipe))


def build_native_recipe_registry(config: NativeWorkflowConfig) -> dict[str, NativeResearchRecipe]:
    feature_spec = load_feature_config(config.feature_spec_path)
    spec_feature_groups = normalize_feature_name_list(feature_spec.get("selected_feature_groups"))
    spec_selected_features = normalize_feature_name_list(feature_spec.get("selected_features"))
    baseline_feature_groups = tuple(spec_feature_groups or groups_for_features(spec_selected_features) or config.feature_groups)
    baseline_included_features = tuple(spec_selected_features or normalize_feature_name_list(config.included_features))
    baseline_excluded_features = tuple(
        normalize_feature_name_list(
            (
                *normalize_feature_name_list(feature_spec.get("excluded_features")),
                *normalize_feature_name_list(config.excluded_features),
            )
        )
    )
    baseline_industry_normalization = str(
        feature_spec.get("industry_normalization")
        or feature_spec.get("normalization_policy", {}).get("default")
        or "l1_weekly_robust"
    )
    baseline_model_params = dict(feature_spec.get("tuned_params") or {})

    baseline = NativeResearchRecipe(
        name="baseline",
        signal_objective=config.signal_objective,
        label_recipe=config.label_recipe,
        feature_groups=baseline_feature_groups,
        included_features=baseline_included_features,
        excluded_features=baseline_excluded_features,
        industry_normalization=baseline_industry_normalization,
        model_params=baseline_model_params,
    )
    registry = {
        baseline.name: baseline,
        "mae_4w": NativeResearchRecipe(
            name="mae_4w",
            signal_objective="mae_regression",
            label_recipe="excess_4w",
            feature_groups=baseline.feature_groups,
            included_features=baseline.included_features,
            excluded_features=baseline.excluded_features,
            industry_normalization=baseline.industry_normalization,
            model_params=baseline.model_params,
        ),
        "binary_4w": NativeResearchRecipe(
            name="binary_4w",
            signal_objective="binary_top_quintile",
            label_recipe="excess_4w",
            feature_groups=baseline.feature_groups,
            included_features=baseline.included_features,
            excluded_features=baseline.excluded_features,
            industry_normalization=baseline.industry_normalization,
            model_params=baseline.model_params,
        ),
        "rank_blended": NativeResearchRecipe(
            name="rank_blended",
            signal_objective="grouped_rank",
            label_recipe="blended_excess_4w_8w",
            feature_groups=baseline.feature_groups,
            included_features=baseline.included_features,
            excluded_features=baseline.excluded_features,
            industry_normalization=baseline.industry_normalization,
            model_params=baseline.model_params,
        ),
        "huber_8w": NativeResearchRecipe(
            name="huber_8w",
            signal_objective="huber_regression",
            label_recipe="excess_8w",
            feature_groups=baseline.feature_groups,
            included_features=baseline.included_features,
            excluded_features=baseline.excluded_features,
            industry_normalization=baseline.industry_normalization,
            model_params=baseline.model_params,
        ),
    }
    return registry


def _resolve_paths(config: NativeWorkflowConfig) -> tuple[Path, Path | None, Path]:
    panel_path = Path(config.panel_path).expanduser().resolve()
    execution_path = Path(config.execution_panel_path).expanduser().resolve() if config.execution_panel_path else None
    output_dir = Path(config.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return panel_path, execution_path, output_dir


def _ensure_panel(
    path: Path,
    *,
    universe_profile: str,
    start_date: str | None,
    end_date: str | None,
    batch_size: int,
    run_export: RunExportMode,
    filter_to_universe_membership: bool,
    enrichment_scope: PanelEnrichmentScope,
    task_description: str | None = None,
) -> pd.DataFrame:
    if run_export == "always" or (run_export == "auto_if_missing" and not path.exists()):
        export_weekly_feature_panel(
            output_path=path,
            universe_profile=universe_profile,
            start_date=start_date,
            end_date=end_date,
            batch_size=batch_size,
            filter_to_universe_membership=filter_to_universe_membership,
            enrichment_scope=enrichment_scope,
            task_description=task_description,
        )
    if not path.exists():
        raise FileNotFoundError(f"Feature panel not found: {path}")
    panel = load_panel_dataframe(path)
    panel.attrs["enrichment_scope"] = load_feature_panel_enrichment_scope(path, panel=panel)
    return panel


def _materialize_panel_artifact(
    path: Path,
    *,
    universe_profile: str,
    start_date: str | None,
    end_date: str | None,
    batch_size: int,
    run_export: RunExportMode,
    filter_to_universe_membership: bool,
    enrichment_scope: PanelEnrichmentScope,
    task_description: str | None = None,
) -> Path:
    if run_export == "always" or (run_export == "auto_if_missing" and not path.exists()):
        export_weekly_feature_panel(
            output_path=path,
            universe_profile=universe_profile,
            start_date=start_date,
            end_date=end_date,
            batch_size=batch_size,
            filter_to_universe_membership=filter_to_universe_membership,
            enrichment_scope=enrichment_scope,
            task_description=task_description,
        )
    if not path.exists():
        raise FileNotFoundError(f"Feature panel not found: {path}")
    return path


def _prepare_execution_panel(config: NativeWorkflowConfig, execution_path: Path | None) -> tuple[pd.DataFrame, Path | None]:
    if config.universe_exit_policy != "retain_quotes_for_existing_positions":
        return pd.DataFrame(), execution_path
    if execution_path is None:
        execution_path = Path(config.output_dir).expanduser().resolve() / f"{config.universe_profile}_execution_panel.parquet"
    execution_panel = _ensure_panel(
        execution_path,
        universe_profile=config.universe_profile,
        start_date=config.start_date,
        end_date=config.end_date,
        batch_size=config.batch_size,
        run_export=config.run_export,
        filter_to_universe_membership=False,
        enrichment_scope="none",
        task_description=config.task_description,
    )
    return execution_panel, execution_path


def _prime_parallel_workflow_inputs(config: NativeWorkflowConfig) -> NativeWorkflowConfig:
    """
    Materialize shared panel inputs before spawning parallel recipe workers.

    Without this preflight step, each worker can race to create the same
    execution panel path under ``auto_if_missing`` / ``always`` export modes,
    which may leave readers observing a partially-written parquet file.
    """
    panel_path, execution_path, output_dir = _resolve_paths(config)
    _materialize_panel_artifact(
        panel_path,
        universe_profile=config.universe_profile,
        start_date=config.start_date,
        end_date=config.end_date,
        batch_size=config.batch_size,
        run_export=config.run_export,
        filter_to_universe_membership=True,
        enrichment_scope="research_full",
        task_description=config.task_description,
    )
    if config.universe_exit_policy == "retain_quotes_for_existing_positions":
        if execution_path is None:
            execution_path = output_dir / f"{config.universe_profile}_execution_panel.parquet"
        _materialize_panel_artifact(
            execution_path,
            universe_profile=config.universe_profile,
            start_date=config.start_date,
            end_date=config.end_date,
            batch_size=config.batch_size,
            run_export=config.run_export,
            filter_to_universe_membership=False,
            enrichment_scope="none",
            task_description=config.task_description,
        )
    return replace(
        config,
        panel_path=panel_path,
        execution_panel_path=execution_path,
        output_dir=output_dir,
        run_export="never",
    )


def _winsorize_series(series: pd.Series, lower: float = 0.02, upper: float = 0.98) -> pd.Series:
    if series.notna().sum() <= 2:
        return series
    lower_value = series.quantile(lower)
    upper_value = series.quantile(upper)
    return series.clip(lower=lower_value, upper=upper_value)


def _zscore_series(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    std = numeric.std()
    if pd.isna(std) or std == 0:
        return numeric * 0.0
    return (numeric - numeric.mean()) / std


def prepare_modeling_panel(
    panel: pd.DataFrame,
    label_recipe: LabelRecipe,
    signal_objective: SignalObjective,
) -> pd.DataFrame:
    result = ensure_panel_enrichment(panel, "research_full")
    result["datetime"] = pd.to_datetime(result["datetime"])
    if label_recipe == "blended_excess_4w_8w":
        result["model_label_raw"] = (
            pd.to_numeric(result["label_excess_return_4w"], errors="coerce")
            + pd.to_numeric(result["label_excess_return_8w"], errors="coerce")
        ) / 2.0
    else:
        raw_column = RAW_LABEL_COLUMN_MAP[label_recipe]
        result["model_label_raw"] = pd.to_numeric(result[raw_column], errors="coerce")

    grouped = result.groupby("datetime", group_keys=False)["model_label_raw"]
    if signal_objective == "binary_top_quintile":
        def _binary_label(values: pd.Series) -> pd.Series:
            numeric = pd.to_numeric(values, errors="coerce")
            if numeric.notna().sum() == 0:
                return numeric
            ranked = numeric.rank(pct=True, method="average")
            labeled = (ranked >= 0.8).astype(float)
            labeled[numeric.isna()] = np.nan
            return labeled
        result["model_label"] = grouped.transform(_binary_label)
    elif signal_objective == "grouped_rank":
        def _rank_bucket(values: pd.Series) -> pd.Series:
            numeric = pd.to_numeric(values, errors="coerce")
            if numeric.notna().sum() < 5:
                return numeric.rank(method="first")
            ranked = numeric.rank(method="first")
            buckets = pd.qcut(ranked, 5, labels=False, duplicates="drop").astype(float)
            buckets[numeric.isna()] = np.nan
            return buckets
        result["model_label"] = grouped.transform(_rank_bucket)
    else:
        result["model_label"] = grouped.transform(_winsorize_series)
        result["model_label"] = result.groupby("datetime", group_keys=False)["model_label"].transform(_zscore_series)

    return result


def select_evaluation_dates_for_label(
    panel: pd.DataFrame,
    label_column: str,
    train_weeks: int,
    valid_weeks: int,
    eval_count: int,
    recent_weeks: int | None = None,
    step_weeks: int = 1,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[pd.Timestamp]:
    label_ready_dates = sorted(pd.to_datetime(panel.loc[panel[label_column].notna(), "datetime"]).dropna().unique())
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
    if recent_weeks is not None and int(recent_weeks) > 0 and eligible:
        window_anchor = eligible[-1] - pd.Timedelta(weeks=max(int(recent_weeks) - 1, 0))
        eligible = [date for date in eligible if date >= window_anchor]
    if step_weeks > 1 and eligible:
        eligible = list(reversed(list(reversed(eligible))[::step_weeks]))
    if eval_count > 0:
        eligible = eligible[-eval_count:]
    return eligible


def build_segments_for_label(
    panel: pd.DataFrame,
    feature_date: pd.Timestamp,
    label_column: str,
    train_weeks: int,
    valid_weeks: int,
) -> dict[str, tuple[str, str]]:
    effective_dates = sorted(pd.to_datetime(panel.loc[panel[label_column].notna(), "datetime"]).dropna().unique())
    history = [pd.Timestamp(date) for date in effective_dates if date < feature_date]
    required = train_weeks + valid_weeks
    if len(history) < required:
        raise ValueError(
            f"Not enough history before {feature_date.date()} for {label_column}: required {required}, got {len(history)}"
        )
    history = history[-required:]
    train_dates = history[:train_weeks]
    valid_dates = history[train_weeks:]
    return {
        "train": (str(train_dates[0].date()), str(train_dates[-1].date())),
        "valid": (str(valid_dates[0].date()), str(valid_dates[-1].date())),
        "test": (str(pd.Timestamp(feature_date).date()), str(pd.Timestamp(feature_date).date())),
    }


def _segment_frame(frame: pd.DataFrame, segment: tuple[str, str]) -> pd.DataFrame:
    start_date, end_date = pd.Timestamp(segment[0]), pd.Timestamp(segment[1])
    return frame.loc[(frame["datetime"] >= start_date) & (frame["datetime"] <= end_date)].copy()


def _prepare_model_matrices(
    frame: pd.DataFrame,
    feature_columns: Sequence[str],
) -> tuple[pd.DataFrame, list[str]]:
    feature_candidates = [column for column in feature_columns if column in frame.columns]
    if not feature_candidates:
        raise ValueError("No feature columns available for model training")
    feature_frame = frame[feature_candidates].apply(pd.to_numeric, errors="coerce")
    valid_rows = feature_frame.notna().any(axis=1)
    if not valid_rows.any():
        raise ValueError("No valid feature rows available for model training")
    filtered = frame.loc[valid_rows].copy()
    feature_frame = feature_frame.loc[valid_rows]
    filtered[feature_candidates] = feature_frame
    return filtered, feature_candidates


def train_lightgbm_model_for_date(
    panel: pd.DataFrame,
    *,
    feature_date: pd.Timestamp,
    feature_columns: Sequence[str],
    label_column: str,
    signal_objective: SignalObjective,
    model_params: dict[str, Any],
    train_weeks: int,
    valid_weeks: int,
) -> tuple[lgb.Booster, pd.DataFrame, list[str], pd.DataFrame]:
    training_panel = panel.loc[pd.to_datetime(panel["datetime"]) <= pd.Timestamp(feature_date)].copy()
    prepared, used_features = _prepare_model_matrices(training_panel, feature_columns)
    segments = build_segments_for_label(
        prepared,
        feature_date=pd.Timestamp(feature_date),
        label_column="model_label_raw",
        train_weeks=train_weeks,
        valid_weeks=valid_weeks,
    )
    train_frame = _segment_frame(prepared, segments["train"])
    valid_frame = _segment_frame(prepared, segments["valid"])
    test_frame = _segment_frame(prepared, segments["test"])
    train_frame = train_frame.loc[pd.to_numeric(train_frame[label_column], errors="coerce").notna()].copy()
    valid_frame = valid_frame.loc[pd.to_numeric(valid_frame[label_column], errors="coerce").notna()].copy()
    if train_frame.empty or valid_frame.empty or test_frame.empty:
        raise ValueError(f"Insufficient train/valid/test rows for feature date {pd.Timestamp(feature_date).date()}")
    medians = train_frame[used_features].apply(pd.to_numeric, errors="coerce").median(numeric_only=True).fillna(0.0)
    train_x = train_frame[used_features].apply(pd.to_numeric, errors="coerce").fillna(medians)
    valid_x = valid_frame[used_features].apply(pd.to_numeric, errors="coerce").fillna(medians)
    test_x = test_frame[used_features].apply(pd.to_numeric, errors="coerce").fillna(medians)
    train_y = pd.to_numeric(train_frame[label_column], errors="coerce")
    valid_y = pd.to_numeric(valid_frame[label_column], errors="coerce")

    params = dict(model_params)
    num_boost_round = int(params.pop("num_boost_round", 300))
    early_stopping_rounds = int(params.pop("early_stopping_rounds", 50))
    callbacks = [
        lgb.early_stopping(stopping_rounds=early_stopping_rounds, verbose=False),
        lgb.log_evaluation(period=0),
    ]

    train_dataset_kwargs: dict[str, Any] = {}
    valid_dataset_kwargs: dict[str, Any] = {}
    if signal_objective == "grouped_rank":
        train_group = train_frame.groupby("datetime").size().tolist()
        valid_group = valid_frame.groupby("datetime").size().tolist()
        train_dataset_kwargs["group"] = train_group
        valid_dataset_kwargs["group"] = valid_group

    train_set = lgb.Dataset(train_x, label=train_y, free_raw_data=False, **train_dataset_kwargs)
    valid_set = lgb.Dataset(valid_x, label=valid_y, free_raw_data=False, reference=train_set, **valid_dataset_kwargs)
    booster = lgb.train(
        params,
        train_set=train_set,
        valid_sets=[valid_set],
        num_boost_round=num_boost_round,
        callbacks=callbacks,
    )
    prediction = booster.predict(test_x, num_iteration=booster.best_iteration)
    score_frame = test_frame[["datetime", "instrument"]].copy()
    score_frame["score"] = pd.to_numeric(prediction, errors="coerce")
    importance_frame = pd.DataFrame(
        {
            "feature": used_features,
            "importance_gain": booster.feature_importance(importance_type="gain"),
            "importance_split": booster.feature_importance(importance_type="split"),
            "feature_date": pd.Timestamp(feature_date),
        }
    ).sort_values(["importance_gain", "importance_split", "feature"], ascending=[False, False, True])
    return booster, score_frame, used_features, importance_frame


def attach_prediction_metadata(
    score_frame: pd.DataFrame,
    panel: pd.DataFrame,
    feature_date: pd.Timestamp,
) -> pd.DataFrame:
    columns = [
        "instrument",
        "open",
        "close",
        "volume",
        "amount",
        "future_return_4w",
        "future_return_8w",
        "label_excess_return_4w",
        "label_excess_return_8w",
        "model_label_raw",
        "model_label",
        "in_csi300",
        "in_csi500",
        "l1_name",
        "l2_name",
        "l3_name",
        "macro_phase",
        "macro_industry_match",
    ]
    available_columns = [column for column in columns if column in panel.columns]
    realized = panel.loc[pd.to_datetime(panel["datetime"]) == pd.Timestamp(feature_date), available_columns].copy()
    realized["instrument"] = realized["instrument"].astype(str)
    merged = score_frame.merge(realized, on="instrument", how="left")
    merged["feature_date"] = pd.Timestamp(feature_date)
    return merged


def evaluate_prediction_frame_extended(prediction_frame: pd.DataFrame, topk: int) -> dict[str, float]:
    result = prediction_frame.dropna(subset=["score"]).copy()
    if result.empty:
        return {
            "coverage": 0.0,
            "rank_ic": np.nan,
            "ic": np.nan,
            "topk_mean_return_4w": np.nan,
            "topk_mean_excess_return_4w": np.nan,
            "topk_hit_rate": np.nan,
            "universe_mean_return_4w": np.nan,
            "universe_mean_excess_return_4w": np.nan,
            "score_dispersion": np.nan,
            "score_unique_count": 0.0,
            "topk_unique_score_ratio": np.nan,
        }
    result = result.sort_values("score", ascending=False)
    top = result.head(topk)
    rank_ic = result["score"].corr(result["label_excess_return_4w"], method="spearman") if "label_excess_return_4w" in result.columns else np.nan
    ic = result["score"].corr(result["label_excess_return_4w"], method="pearson") if "label_excess_return_4w" in result.columns else np.nan
    return {
        "coverage": float(len(result)),
        "rank_ic": float(rank_ic) if pd.notna(rank_ic) else np.nan,
        "ic": float(ic) if pd.notna(ic) else np.nan,
        "topk_mean_return_4w": float(pd.to_numeric(top.get("future_return_4w"), errors="coerce").mean()),
        "topk_mean_excess_return_4w": float(pd.to_numeric(top.get("label_excess_return_4w"), errors="coerce").mean()),
        "topk_hit_rate": float((pd.to_numeric(top.get("future_return_4w"), errors="coerce") > 0).mean()),
        "universe_mean_return_4w": float(pd.to_numeric(result.get("future_return_4w"), errors="coerce").mean()),
        "universe_mean_excess_return_4w": float(pd.to_numeric(result.get("label_excess_return_4w"), errors="coerce").mean()),
        "score_dispersion": float(pd.to_numeric(result["score"], errors="coerce").std()),
        "score_unique_count": float(result["score"].nunique()),
        "topk_unique_score_ratio": float(top["score"].nunique() / max(min(topk, len(top)), 1)),
    }


def collect_prediction_bundle(
    bundle_name: str,
    panel: pd.DataFrame,
    *,
    eval_dates: Sequence[pd.Timestamp],
    feature_columns: Sequence[str],
    recipe: NativeResearchRecipe,
    config: NativeWorkflowConfig,
) -> dict[str, Any]:
    if not eval_dates:
        return {
            "bundle": bundle_name,
            "eval_dates": [],
            "predictions": pd.DataFrame(),
            "details": pd.DataFrame(),
            "summary": pd.DataFrame(),
            "feature_importance": pd.DataFrame(),
        }

    model_params = build_model_params(
        seed=config.seed,
        reproducibility_mode=config.reproducibility_mode,
        signal_objective=recipe.signal_objective,
        overrides=recipe.model_params,
        num_threads=config.model_num_threads,
    )
    prediction_frames: list[pd.DataFrame] = []
    detail_rows: list[dict[str, Any]] = []
    feature_importance_frames: list[pd.DataFrame] = []
    for feature_date in eval_dates:
        _booster, score_frame, used_features, importance_frame = train_lightgbm_model_for_date(
            panel,
            feature_date=pd.Timestamp(feature_date),
            feature_columns=feature_columns,
            label_column="model_label",
            signal_objective=recipe.signal_objective,
            model_params=model_params,
            train_weeks=config.train_weeks if bundle_name == "rolling" else config.walk_forward_train_weeks,
            valid_weeks=config.valid_weeks if bundle_name == "rolling" else config.walk_forward_valid_weeks,
        )
        realized_frame = attach_prediction_metadata(score_frame, panel, pd.Timestamp(feature_date))
        metrics = evaluate_prediction_frame_extended(realized_frame, topk=config.topk)
        detail_rows.append(
            {
                "bundle": bundle_name,
                "feature_date": pd.Timestamp(feature_date),
                "used_feature_count": len(used_features),
                "signal_objective": recipe.signal_objective,
                "label_recipe": recipe.label_recipe,
                **metrics,
            }
        )
        prediction_frames.append(realized_frame.assign(bundle=bundle_name, signal_objective=recipe.signal_objective, label_recipe=recipe.label_recipe))
        feature_importance_frames.append(importance_frame.assign(bundle=bundle_name))

    detail_frame = pd.DataFrame(detail_rows)
    summary_frame = summarize_details(detail_frame, group_columns=("bundle",))
    prediction_frame = pd.concat(prediction_frames, ignore_index=True)
    feature_importance = pd.concat(feature_importance_frames, ignore_index=True)
    return {
        "bundle": bundle_name,
        "eval_dates": list(eval_dates),
        "predictions": prediction_frame,
        "details": detail_frame,
        "summary": summary_frame,
        "feature_importance": feature_importance,
    }


def build_buffered_signal_matrix(
    predictions: pd.DataFrame,
    execution_price_frame: pd.DataFrame,
    *,
    topk: int,
    hold_buffer_rank: int | None = None,
    rebalance_interval_weeks: int = 1,
    min_liquidity_filter: float = 0.0,
    min_score_spread: float = 0.0,
    industry_max_weight: float | None = None,
    execution_lag_steps: int = 0,
) -> pd.DataFrame:
    signal = pd.DataFrame(0.0, index=execution_price_frame.index, columns=execution_price_frame.columns)
    if predictions.empty or execution_price_frame.empty:
        return signal
    price_index = pd.Index(pd.to_datetime(execution_price_frame.index))
    current_holdings: list[str] = []
    previous_weights: dict[str, float] = {}
    for step, (feature_date, frame) in enumerate(predictions.groupby("feature_date")):
        feature_date = pd.Timestamp(feature_date)
        if feature_date not in price_index:
            continue
        execution_position = price_index.get_loc(feature_date)
        if not isinstance(execution_position, int):
            continue
        execution_position += int(execution_lag_steps)
        if execution_position < 0 or execution_position >= len(price_index):
            continue
        execution_date = price_index[execution_position]
        tradable_series = execution_price_frame.loc[execution_date]
        buyable_codes = tradable_series[tradable_series.notna()].index.astype(str).tolist()
        if step % max(int(rebalance_interval_weeks), 1) == 0 or not current_holdings:
            selected = select_topk_with_buffer(
                frame.set_index("instrument"),
                current_holdings=current_holdings,
                topk=topk,
                hold_buffer_rank=hold_buffer_rank,
                min_liquidity_filter=min_liquidity_filter,
                min_score_spread=min_score_spread,
                industry_max_weight=industry_max_weight,
                buyable_codes=buyable_codes,
            )
            current_holdings = selected
            previous_weights = {code: 1.0 / len(selected) for code in selected} if selected else {}
        if previous_weights:
            for code, weight in previous_weights.items():
                if code in signal.columns:
                    signal.loc[execution_date, code] = weight
    return signal


def _build_native_report_frame(report_normal: pd.DataFrame, initial_capital: float) -> pd.DataFrame:
    frame = report_normal.copy().reset_index().rename(columns={"index": "datetime"})
    frame["datetime"] = pd.to_datetime(frame["datetime"])
    frame["gross_return"] = pd.to_numeric(frame["return"], errors="coerce").fillna(0.0)
    frame["cost"] = pd.to_numeric(frame["cost"], errors="coerce").fillna(0.0)
    frame["net_return"] = frame["gross_return"] - frame["cost"]
    frame["benchmark_return"] = pd.to_numeric(frame.get("bench"), errors="coerce").fillna(0.0)
    frame["gross_value"] = float(initial_capital) * (1.0 + frame["gross_return"]).cumprod()
    frame["net_value"] = float(initial_capital) * (1.0 + frame["net_return"]).cumprod()
    frame["benchmark_value"] = float(initial_capital) * (1.0 + frame["benchmark_return"]).cumprod()
    frame["relative_drawdown"] = (frame["net_value"] / frame["net_value"].cummax()) - 1.0
    frame["benchmark_excess_drawdown"] = (
        (frame["net_value"] / frame["benchmark_value"].replace(0.0, np.nan))
        / (frame["net_value"] / frame["benchmark_value"].replace(0.0, np.nan)).cummax()
    ) - 1.0
    return frame


def _compute_native_performance_metrics(report_frame: pd.DataFrame, *, account: float) -> dict[str, Any]:
    empty_metrics = {
        "period_start": None,
        "period_end": None,
        "period_count": 0,
        "net_total_return": np.nan,
        "annualized_return": np.nan,
        "annualized_volatility": np.nan,
        "sharpe_ratio": np.nan,
        "win_rate": np.nan,
        "max_drawdown": np.nan,
        "calmar_ratio": np.nan,
    }
    if report_frame.empty:
        return empty_metrics

    frame = report_frame.copy()
    frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce")
    frame["net_return"] = pd.to_numeric(frame.get("net_return"), errors="coerce")
    frame["net_value"] = pd.to_numeric(frame.get("net_value"), errors="coerce")
    frame["relative_drawdown"] = pd.to_numeric(frame.get("relative_drawdown"), errors="coerce")
    frame = frame.dropna(subset=["datetime"]).sort_values("datetime")
    if frame.empty:
        return empty_metrics

    returns = frame["net_return"].dropna()
    net_values = frame["net_value"].dropna()
    drawdowns = frame["relative_drawdown"].dropna()
    period_start = pd.Timestamp(frame["datetime"].iloc[0])
    period_end = pd.Timestamp(frame["datetime"].iloc[-1])
    elapsed_days = max((period_end - period_start).days, 0)
    total_return = float(net_values.iloc[-1] / float(account) - 1.0) if not net_values.empty else np.nan
    annualized_return = (
        (1.0 + total_return) ** (365.25 / elapsed_days) - 1.0
        if elapsed_days > 0 and pd.notna(total_return) and total_return > -1.0
        else np.nan
    )
    weekly_volatility = returns.std()
    annualized_volatility = float(weekly_volatility * np.sqrt(52)) if pd.notna(weekly_volatility) else np.nan
    sharpe_ratio = (
        float(returns.mean() / weekly_volatility * np.sqrt(52))
        if pd.notna(weekly_volatility) and weekly_volatility > 0
        else np.nan
    )
    win_rate = float((returns > 0).mean()) if not returns.empty else np.nan
    max_drawdown = float(drawdowns.min()) if not drawdowns.empty else np.nan
    calmar_ratio = (
        float(annualized_return / abs(max_drawdown))
        if pd.notna(annualized_return) and pd.notna(max_drawdown) and max_drawdown < 0
        else np.nan
    )
    return {
        "period_start": str(period_start.date()),
        "period_end": str(period_end.date()),
        "period_count": int(len(frame)),
        "net_total_return": total_return,
        "annualized_return": float(annualized_return) if pd.notna(annualized_return) else np.nan,
        "annualized_volatility": annualized_volatility,
        "sharpe_ratio": sharpe_ratio,
        "win_rate": win_rate,
        "max_drawdown": max_drawdown,
        "calmar_ratio": calmar_ratio,
    }


def build_native_performance_metrics_frame(
    report_frame: pd.DataFrame,
    *,
    recipe_name: str,
    bundle_name: str,
    account: float,
) -> pd.DataFrame:
    metrics = _compute_native_performance_metrics(report_frame, account=account)
    return pd.DataFrame([{"recipe": recipe_name, "bundle": bundle_name, **metrics}])


def build_monthly_return_heatmap_frame(report_frame: pd.DataFrame, value_column: str = "net_value") -> pd.DataFrame:
    if report_frame.empty or "datetime" not in report_frame.columns or value_column not in report_frame.columns:
        return pd.DataFrame()
    frame = report_frame[["datetime", value_column]].copy()
    frame["datetime"] = pd.to_datetime(frame["datetime"])
    frame[value_column] = pd.to_numeric(frame[value_column], errors="coerce")
    frame = frame.dropna(subset=["datetime", value_column]).sort_values("datetime")
    if frame.empty:
        return pd.DataFrame()
    month_end_values = frame.groupby(frame["datetime"].dt.to_period("M"))[value_column].last()
    monthly_return = month_end_values.pct_change()
    monthly = monthly_return.rename("return").reset_index()
    monthly["year"] = monthly["datetime"].dt.year
    monthly["month"] = monthly["datetime"].dt.month
    heatmap = monthly.pivot(index="year", columns="month", values="return").sort_index()
    if heatmap.empty:
        return pd.DataFrame()
    heatmap = heatmap.reindex(columns=range(1, 13))
    heatmap.columns = MONTH_HEATMAP_COLUMNS
    return heatmap


def build_annual_return_heatmap_frame(report_frame: pd.DataFrame, value_column: str = "net_value") -> pd.DataFrame:
    if report_frame.empty or "datetime" not in report_frame.columns or value_column not in report_frame.columns:
        return pd.DataFrame()
    frame = report_frame[["datetime", value_column]].copy()
    frame["datetime"] = pd.to_datetime(frame["datetime"])
    frame[value_column] = pd.to_numeric(frame[value_column], errors="coerce")
    frame = frame.dropna(subset=["datetime", value_column]).sort_values("datetime")
    if frame.empty:
        return pd.DataFrame()
    frame["year"] = frame["datetime"].dt.year.astype(str)
    annual = (
        frame.groupby("year", as_index=False)[value_column]
        .agg(period_first="first", period_end="last")
    )
    annual["period_start"] = annual["period_end"].shift(1)
    annual["period_start"] = annual["period_start"].fillna(annual["period_first"])
    annual["return"] = (annual["period_end"] / annual["period_start"]) - 1.0
    if annual.empty:
        return pd.DataFrame()
    return pd.DataFrame([annual["return"].tolist()], index=["annual_return"], columns=annual["year"].tolist())


def run_validation_backtest(
    predictions: pd.DataFrame,
    panel: pd.DataFrame,
    config: NativeWorkflowConfig,
) -> dict[str, Any]:
    eval_dates = sorted(pd.to_datetime(predictions["feature_date"]).unique())
    frames = build_backtest_price_frames(panel=panel, eval_dates=eval_dates)
    signal_matrix = build_buffered_signal_matrix(
        predictions,
        execution_price_frame=frames.execution_price,
        topk=config.topk,
        hold_buffer_rank=config.hold_buffer_rank,
        rebalance_interval_weeks=config.rebalance_interval_weeks,
        min_liquidity_filter=config.min_liquidity_filter,
        min_score_spread=config.min_score_spread,
        industry_max_weight=config.industry_max_weight,
        execution_lag_steps=config.validation_execution_lag_steps,
    )
    equity_curve, metrics = run_strategy_backtest(
        frames.mark_price,
        signal_matrix,
        execution_price_frame=frames.execution_price,
        initial_capital=config.account,
        trading_config=default_weekly_net_backtest_config(),
    )
    equity_curve_frame = equity_curve.reset_index().rename(columns={"date": "datetime"}) if not equity_curve.empty else pd.DataFrame()
    if not equity_curve_frame.empty:
        equity_curve_frame["datetime"] = pd.to_datetime(equity_curve_frame["datetime"])
        equity_curve_frame["net_value"] = pd.to_numeric(equity_curve_frame["total_value"], errors="coerce")
        equity_curve_frame["net_return"] = equity_curve_frame["net_value"].pct_change().fillna(
            equity_curve_frame["net_value"].iloc[0] / config.account - 1.0
        )
    return {
        "signal_matrix": signal_matrix,
        "equity_curve": equity_curve,
        "equity_curve_frame": equity_curve_frame,
        "metrics": metrics,
    }


def build_execution_diff_summary(
    recipe_name: str,
    bundle_name: str,
    native_report_frame: pd.DataFrame,
    validation_result: dict[str, Any],
    account: float,
) -> pd.DataFrame:
    validation_frame = validation_result.get("equity_curve_frame", pd.DataFrame())
    if native_report_frame.empty or validation_frame.empty:
        return pd.DataFrame()
    native_final = float(native_report_frame["net_value"].iloc[-1])
    validation_final = float(validation_frame["net_value"].iloc[-1])
    native_drawdown = float(native_report_frame["relative_drawdown"].min())
    validation_drawdown = float((validation_frame["net_value"] / validation_frame["net_value"].cummax() - 1.0).min())
    return pd.DataFrame(
        [
            {
                "recipe": recipe_name,
                "bundle": bundle_name,
                "native_final_net_value": native_final,
                "validation_final_net_value": validation_final,
                "native_minus_validation_return": (native_final / float(account) - 1.0) - (validation_final / float(account) - 1.0),
                "native_max_drawdown": native_drawdown,
                "validation_max_drawdown": validation_drawdown,
            }
        ]
    )


def build_slice_regime_summary(predictions: pd.DataFrame, recipe_name: str) -> pd.DataFrame:
    if predictions.empty:
        return pd.DataFrame()
    frame = predictions.copy()
    frame["feature_date"] = pd.to_datetime(frame["feature_date"])
    frame["feature_year"] = frame["feature_date"].dt.year
    summaries: list[pd.DataFrame] = []
    for column in ("feature_year", "l1_name", "macro_phase"):
        if column not in frame.columns:
            continue
        grouped = frame.groupby(["bundle", column], dropna=False).agg(
            coverage=("instrument", "count"),
            score_dispersion=("score", "std"),
            mean_future_return_4w=("future_return_4w", "mean"),
            mean_excess_return_4w=("label_excess_return_4w", "mean"),
        ).reset_index()
        grouped["slice_type"] = column
        grouped["slice_value"] = grouped[column].astype(str)
        grouped["recipe"] = recipe_name
        summaries.append(grouped[["recipe", "bundle", "slice_type", "slice_value", "coverage", "score_dispersion", "mean_future_return_4w", "mean_excess_return_4w"]])
    return pd.concat(summaries, ignore_index=True) if summaries else pd.DataFrame()


def _prefer_non_empty_frame(primary: pd.DataFrame | None, fallback: pd.DataFrame) -> pd.DataFrame:
    if primary is None:
        return fallback.copy()
    if isinstance(primary, pd.DataFrame) and not primary.empty:
        return primary.copy()
    return fallback.copy()


def _prefilter_and_normalize_features(
    panel: pd.DataFrame,
    recipe: NativeResearchRecipe,
    *,
    calibration_end_date: pd.Timestamp | None = None,
) -> tuple[pd.DataFrame, list[str], pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    requested = compose_feature_columns(recipe.feature_groups)
    selected = resolve_feature_columns(
        requested,
        included_features=recipe.included_features,
        excluded_features=recipe.excluded_features,
        default_features=requested,
    )
    reference_panel = panel.copy()
    if calibration_end_date is not None:
        reference_panel = reference_panel.loc[
            pd.to_datetime(reference_panel["datetime"]) <= pd.Timestamp(calibration_end_date)
        ].copy()
    kept, feature_stats, corr_candidates = prefilter_feature_columns(reference_panel, selected)
    feature_redundancy = build_feature_redundancy_report(
        reference_panel,
        kept,
        end_date=calibration_end_date,
    )
    feature_outlier_audit = build_feature_outlier_audit(
        reference_panel,
        kept,
        end_date=calibration_end_date,
    )
    normalized_panel = apply_industry_normalization(
        panel,
        feature_columns=get_normalized_feature_candidates(kept),
        method=recipe.industry_normalization,
    )
    return normalized_panel, kept, feature_stats, corr_candidates, feature_redundancy, feature_outlier_audit


def _native_run_summary_row(
    report_frame: pd.DataFrame,
    *,
    recipe_name: str,
    bundle_name: str,
    config: NativeWorkflowConfig,
) -> dict[str, Any]:
    final_net_value = float(report_frame["net_value"].iloc[-1]) if not report_frame.empty else np.nan
    benchmark_value = float(report_frame["benchmark_value"].iloc[-1]) if not report_frame.empty else np.nan
    performance_metrics = _compute_native_performance_metrics(report_frame, account=config.account)
    return {
        "recipe": recipe_name,
        "bundle": bundle_name,
        "final_net_value": final_net_value,
        "net_total_return": performance_metrics["net_total_return"],
        "benchmark_total_return": benchmark_value / config.account - 1.0 if pd.notna(benchmark_value) else np.nan,
        "cost_drag": float(report_frame["cost"].sum()) if "cost" in report_frame.columns else 0.0,
        "strategy_max_drawdown": performance_metrics["max_drawdown"],
        "strategy_excess_drawdown": float(report_frame["benchmark_excess_drawdown"].min()) if "benchmark_excess_drawdown" in report_frame.columns else np.nan,
        "turnover_mean": float(pd.to_numeric(report_frame.get("turnover"), errors="coerce").mean()) if "turnover" in report_frame.columns else np.nan,
        "period_start": performance_metrics["period_start"],
        "period_end": performance_metrics["period_end"],
        "period_count": performance_metrics["period_count"],
        "annualized_return": performance_metrics["annualized_return"],
        "annualized_volatility": performance_metrics["annualized_volatility"],
        "sharpe_ratio": performance_metrics["sharpe_ratio"],
        "win_rate": performance_metrics["win_rate"],
        "calmar_ratio": performance_metrics["calmar_ratio"],
    }


def _normalize_summary_value(value: Any) -> Any:
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float):
        return value if np.isfinite(value) else None
    if pd.isna(value):
        return None
    return value


def _summary_frame_first_row(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {}
    return {str(key): _normalize_summary_value(value) for key, value in frame.iloc[0].to_dict().items()}


def _summary_frame_max_int(frame: pd.DataFrame, column: str) -> int | None:
    if frame.empty or column not in frame.columns:
        return None
    series = pd.to_numeric(frame[column], errors="coerce").dropna()
    if series.empty:
        return None
    return int(series.max())


def _native_summary_lookup(native_summary: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if native_summary.empty:
        return {}
    lookup: dict[str, dict[str, Any]] = {}
    for row in native_summary.to_dict(orient="records"):
        bundle_name = row.get("bundle")
        if bundle_name is None:
            continue
        lookup[str(bundle_name)] = {str(key): _normalize_summary_value(value) for key, value in row.items()}
    return lookup


def _build_recipe_overview_row_from_artifacts(recipe_name: str, artifacts: NativeRecipeArtifacts) -> dict[str, Any]:
    feature_prefilter = artifacts.feature_prefilter_stats
    rolling_summary_row = _summary_frame_first_row(
        artifacts.prediction_bundles.get("rolling", {}).get("summary", pd.DataFrame())
    )
    walk_forward_summary_row = _summary_frame_first_row(
        artifacts.prediction_bundles.get("walk_forward", {}).get("summary", pd.DataFrame())
    )
    native_lookup = _native_summary_lookup(artifacts.native_summary)
    rolling_native_row = native_lookup.get("rolling", {})
    walk_forward_native_row = native_lookup.get("walk_forward", {})
    used_feature_count = _summary_frame_max_int(feature_prefilter, "selected_feature_count")
    requested_feature_count = _summary_frame_max_int(feature_prefilter, "requested_feature_count")

    return {
        "recipe": recipe_name,
        "requested_feature_count": requested_feature_count,
        "used_feature_count": used_feature_count if used_feature_count is not None else len(artifacts.used_feature_columns),
        "rolling_rank_ic_ir": rolling_summary_row.get("rank_ic_ir"),
        "rolling_topk_mean_excess_return_4w": rolling_summary_row.get("topk_mean_excess_return_4w"),
        "rolling_net_total_return": rolling_native_row.get("net_total_return"),
        "rolling_benchmark_total_return": rolling_native_row.get("benchmark_total_return"),
        "rolling_excess_total_return": rolling_native_row.get("excess_total_return"),
        "rolling_max_drawdown": rolling_native_row.get("strategy_max_drawdown"),
        "rolling_excess_drawdown": rolling_native_row.get("strategy_excess_drawdown"),
        "rolling_cost_drag": rolling_native_row.get("cost_drag"),
        "rolling_turnover_mean": rolling_native_row.get("turnover_mean"),
        "rolling_annualized_return": rolling_native_row.get("annualized_return"),
        "rolling_annualized_volatility": rolling_native_row.get("annualized_volatility"),
        "rolling_sharpe_ratio": rolling_native_row.get("sharpe_ratio"),
        "rolling_win_rate": rolling_native_row.get("win_rate"),
        "rolling_calmar_ratio": rolling_native_row.get("calmar_ratio"),
        "walk_forward_rank_ic_ir": walk_forward_summary_row.get("rank_ic_ir"),
        "walk_forward_topk_mean_excess_return_4w": walk_forward_summary_row.get("topk_mean_excess_return_4w"),
        "walk_forward_net_total_return": walk_forward_native_row.get("net_total_return"),
        "walk_forward_benchmark_total_return": walk_forward_native_row.get("benchmark_total_return"),
        "walk_forward_excess_total_return": walk_forward_native_row.get("excess_total_return"),
        "walk_forward_max_drawdown": walk_forward_native_row.get("strategy_max_drawdown"),
        "walk_forward_excess_drawdown": walk_forward_native_row.get("strategy_excess_drawdown"),
        "walk_forward_cost_drag": walk_forward_native_row.get("cost_drag"),
        "walk_forward_turnover_mean": walk_forward_native_row.get("turnover_mean"),
        "walk_forward_annualized_return": walk_forward_native_row.get("annualized_return"),
        "walk_forward_annualized_volatility": walk_forward_native_row.get("annualized_volatility"),
        "walk_forward_sharpe_ratio": walk_forward_native_row.get("sharpe_ratio"),
        "walk_forward_win_rate": walk_forward_native_row.get("win_rate"),
        "walk_forward_calmar_ratio": walk_forward_native_row.get("calmar_ratio"),
    }


def _build_native_workflow_summary_payload(
    *,
    config: NativeWorkflowConfig,
    registry_payload: dict[str, Any],
    promotion_gate: dict[str, Any],
    output_dir: Path,
    artifacts: dict[str, NativeRecipeArtifacts],
) -> dict[str, Any]:
    overview_lookup = {
        recipe_name: _build_recipe_overview_row_from_artifacts(recipe_name, recipe_artifacts)
        for recipe_name, recipe_artifacts in artifacts.items()
    }
    return {
        "schema_version": NATIVE_WORKFLOW_SUMMARY_SCHEMA_VERSION,
        "config": asdict(config),
        "recipe_registry": registry_payload,
        "promotion_gate": promotion_gate,
        "promotion_gate_summary": promotion_gate,
        "overview_lookup": overview_lookup,
        "output_dir": str(output_dir),
    }


def run_native_recipe(
    config: NativeWorkflowConfig,
    recipe: NativeResearchRecipe,
) -> NativeRecipeArtifacts:
    apply_research_seed(config.seed)
    panel_path, execution_path, output_dir = _resolve_paths(config)
    research_panel = _ensure_panel(
        panel_path,
        universe_profile=config.universe_profile,
        start_date=config.start_date,
        end_date=config.end_date,
        batch_size=config.batch_size,
        run_export=config.run_export,
        filter_to_universe_membership=True,
        enrichment_scope="research_full",
        task_description=config.task_description,
    )
    research_panel = filter_panel_by_universe_profile(research_panel, config.universe_profile)
    modeling_panel = prepare_modeling_panel(research_panel, label_recipe=recipe.label_recipe, signal_objective=recipe.signal_objective)
    rolling_dates = select_evaluation_dates_for_label(
        modeling_panel,
        label_column="model_label_raw",
        train_weeks=config.train_weeks,
        valid_weeks=config.valid_weeks,
        eval_count=config.eval_count,
        recent_weeks=config.rolling_recent_weeks,
        step_weeks=config.step_weeks,
    )
    walk_forward_dates: list[pd.Timestamp] = []
    if config.walk_forward_enabled:
        walk_forward_dates = select_evaluation_dates_for_label(
            modeling_panel,
            label_column="model_label_raw",
            train_weeks=config.walk_forward_train_weeks,
            valid_weeks=config.walk_forward_valid_weeks,
            eval_count=config.walk_forward_eval_count,
            step_weeks=config.walk_forward_step_weeks,
            start_date=config.walk_forward_start_date,
            end_date=config.walk_forward_end_date,
        )
    calibration_dates = [date for date in [*rolling_dates[:1], *walk_forward_dates[:1]] if pd.notna(date)]
    calibration_end_date = min(calibration_dates) if calibration_dates else pd.to_datetime(modeling_panel["datetime"]).max()
    (
        normalized_panel,
        feature_columns,
        feature_prefilter_stats,
        corr_candidates,
        feature_redundancy,
        feature_outlier_audit,
    ) = _prefilter_and_normalize_features(
        modeling_panel,
        recipe,
        calibration_end_date=calibration_end_date,
    )

    execution_panel, execution_path = _prepare_execution_panel(config, execution_path)
    if execution_panel.empty:
        execution_panel = normalized_panel
    execution_panel["datetime"] = pd.to_datetime(execution_panel["datetime"])
    execution_panel["instrument"] = execution_panel["instrument"].astype(str)

    latest_feature_date = pd.to_datetime(normalized_panel["datetime"]).max()
    _booster, latest_score_frame, used_features, _latest_importance = train_lightgbm_model_for_date(
        normalized_panel,
        feature_date=latest_feature_date,
        feature_columns=feature_columns,
        label_column="model_label",
        signal_objective=recipe.signal_objective,
        model_params=build_model_params(
            config.seed,
            config.reproducibility_mode,
            recipe.signal_objective,
            recipe.model_params,
            num_threads=config.model_num_threads,
        ),
        train_weeks=config.train_weeks,
        valid_weeks=config.valid_weeks,
    )
    latest_score_frame = attach_prediction_metadata(latest_score_frame, normalized_panel, latest_feature_date).sort_values("score", ascending=False).reset_index(drop=True)

    rolling_bundle = collect_prediction_bundle(
        "rolling",
        normalized_panel,
        eval_dates=rolling_dates,
        feature_columns=feature_columns,
        recipe=recipe,
        config=config,
    )
    walk_forward_bundle = {
        "bundle": "walk_forward",
        "eval_dates": [],
        "predictions": pd.DataFrame(),
        "details": pd.DataFrame(),
        "summary": pd.DataFrame(),
        "feature_importance": pd.DataFrame(),
    }
    if config.walk_forward_enabled:
        walk_forward_bundle = collect_prediction_bundle(
            "walk_forward",
            normalized_panel,
            eval_dates=walk_forward_dates,
            feature_columns=feature_columns,
            recipe=recipe,
            config=config,
        )
    prediction_bundles = {
        bundle["bundle"]: bundle
        for bundle in (rolling_bundle, walk_forward_bundle)
        if not bundle["predictions"].empty
    }
    if not prediction_bundles:
        raise RuntimeError("No native workflow prediction bundles were produced")

    combined_prediction_frame = pd.concat(
        [bundle["predictions"] for bundle in prediction_bundles.values()],
        ignore_index=True,
    )
    native_signal_dates = sorted(pd.to_datetime(combined_prediction_frame["feature_date"]).unique())
    native_symbols = sorted(combined_prediction_frame["instrument"].astype(str).unique())
    native_quote_panel = execution_panel.loc[
        execution_panel["datetime"].isin(native_signal_dates)
        & execution_panel["instrument"].astype(str).isin(native_symbols)
    ].copy()
    native_quote_frame = build_native_quote_frame(native_quote_panel, symbols=native_symbols)
    native_provider_dir = ensure_minimal_qlib_provider(
        provider_dir=output_dir / recipe.name / "qlib_native_provider",
        calendar_dates=native_signal_dates,
        instruments=native_symbols,
        market_name=f"{config.universe_profile}_{recipe.name}_native",
        freq="week",
    )
    init_qlib_runtime(
        exp_name=f"qlib-native-{config.universe_profile}-{recipe.name}",
        artifacts_dir=output_dir / recipe.name,
        provider_uri=native_provider_dir,
        skip_if_registered=False,
    )

    benchmark_frames: dict[str, pd.DataFrame] = {}
    native_results: dict[str, Any] = {}
    validation_results: dict[str, dict[str, Any]] = {}
    execution_diff_frames: list[pd.DataFrame] = []
    signal_diag_frames: list[pd.DataFrame] = []
    portfolio_diag_frames: list[pd.DataFrame] = []
    native_summary_rows: list[dict[str, Any]] = []

    for bundle_name, bundle in prediction_bundles.items():
        native_signal = build_native_signal_frame(bundle["predictions"])
        benchmark_frame = build_universe_benchmark_frame(
            bundle["eval_dates"],
            universe_profile=config.universe_profile,
            benchmark_mode=config.benchmark_mode,
            period="W",
            initial_capital=config.account,
        )
        benchmark_frames[bundle_name] = benchmark_frame
        benchmark_series = build_universe_benchmark_series(
            bundle["eval_dates"],
            universe_profile=config.universe_profile,
            benchmark_mode=config.benchmark_mode,
            period="W",
        )
        native_result = run_native_backtest_analysis(
            signal_frame=native_signal,
            quote_frame=native_quote_frame,
            topk=config.topk,
            trading_config=default_weekly_net_backtest_config(),
            account=config.account,
            benchmark=benchmark_series if not benchmark_series.empty else None,
            market_name=f"{config.universe_profile}_{recipe.name}_native",
            time_per_step="week",
            risk_degree=config.native_risk_degree,
            only_tradable=config.native_only_tradable,
            hold_buffer_rank=config.hold_buffer_rank,
            rebalance_interval_steps=config.rebalance_interval_weeks,
            min_liquidity_filter=config.min_liquidity_filter,
            min_score_spread=config.min_score_spread,
            industry_max_weight=config.industry_max_weight,
        )
        native_results[bundle_name] = native_result
        native_report = _build_native_report_frame(native_result.artifacts.report_normal, config.account)
        native_summary_rows.append(
            _native_run_summary_row(
                native_report,
                recipe_name=recipe.name,
                bundle_name=bundle_name,
                config=config,
            )
        )
        signal_diag = _prefer_non_empty_frame(
            native_result.artifacts.signal_diagnostics,
            build_signal_diagnostics(native_signal, topk=config.topk),
        )
        signal_diag["recipe"] = recipe.name
        signal_diag["bundle"] = bundle_name
        signal_diag_frames.append(signal_diag)
        portfolio_diag = _prefer_non_empty_frame(
            native_result.artifacts.portfolio_diagnostics,
            build_native_portfolio_diagnostics(
                native_signal,
                native_quote_frame,
                native_result.artifacts.positions_normal,
                topk=config.topk,
                hold_buffer_rank=config.hold_buffer_rank,
                rebalance_interval_steps=config.rebalance_interval_weeks,
            ),
        )
        portfolio_diag["recipe"] = recipe.name
        portfolio_diag["bundle"] = bundle_name
        portfolio_diag_frames.append(portfolio_diag)
        if config.run_validation_comparison:
            validation_result = run_validation_backtest(bundle["predictions"], execution_panel, config)
            validation_results[bundle_name] = validation_result
            execution_diff_frames.append(
                build_execution_diff_summary(recipe.name, bundle_name, native_report, validation_result, config.account)
            )

    signal_diagnostics = pd.concat(signal_diag_frames, ignore_index=True) if signal_diag_frames else pd.DataFrame()
    portfolio_diagnostics = pd.concat(portfolio_diag_frames, ignore_index=True) if portfolio_diag_frames else pd.DataFrame()
    execution_diff_summary = pd.concat(execution_diff_frames, ignore_index=True) if execution_diff_frames else pd.DataFrame()
    slice_regime_summary = build_slice_regime_summary(combined_prediction_frame, recipe.name)
    native_summary = pd.DataFrame(native_summary_rows)

    recipe_dir = output_dir / recipe.name
    recipe_dir.mkdir(parents=True, exist_ok=True)
    latest_score_frame.to_csv(recipe_dir / "latest_score_frame.csv", index=False)
    latest_feature_date_str = str(pd.Timestamp(latest_feature_date).date())
    portfolio_targets = build_portfolio_targets(latest_score_frame, model_id=f"{config.universe_profile}-{recipe.name}-qlib-native", feature_date=latest_feature_date_str, topk=config.topk)
    publish_portfolio_targets(portfolio_targets, model_id=f"{config.universe_profile}-{recipe.name}-qlib-native")
    portfolio_targets.to_csv(recipe_dir / "portfolio_targets.csv", index=False)
    feature_prefilter_stats.to_csv(recipe_dir / "feature_prefilter.csv", index=False)
    corr_candidates.to_csv(recipe_dir / "feature_corr_candidates.csv", index=False)
    feature_redundancy.to_csv(recipe_dir / "feature_redundancy.csv", index=False)
    feature_outlier_audit.to_csv(recipe_dir / "feature_outlier_audit.csv", index=False)
    signal_diagnostics.to_csv(recipe_dir / "signal_diagnostics.csv", index=False)
    portfolio_diagnostics.to_csv(recipe_dir / "portfolio_diagnostics.csv", index=False)
    slice_regime_summary.to_csv(recipe_dir / "slice_regime_summary.csv", index=False)
    execution_diff_summary.to_csv(recipe_dir / "execution_diff_summary.csv", index=False)
    for bundle_name, bundle in prediction_bundles.items():
        bundle["predictions"].to_csv(recipe_dir / f"{bundle_name}_predictions.csv", index=False)
        bundle["details"].to_csv(recipe_dir / f"{bundle_name}_details.csv", index=False)
        bundle["summary"].to_csv(recipe_dir / f"{bundle_name}_summary.csv", index=False)
        bundle["feature_importance"].to_csv(recipe_dir / f"{bundle_name}_feature_importance.csv", index=False)
        native_report = _build_native_report_frame(native_results[bundle_name].artifacts.report_normal, config.account)
        native_report.to_csv(recipe_dir / f"{bundle_name}_native_report.csv", index=False)
        build_native_performance_metrics_frame(
            native_report,
            recipe_name=recipe.name,
            bundle_name=bundle_name,
            account=config.account,
        ).to_csv(recipe_dir / f"{bundle_name}_performance_metrics.csv", index=False)
        build_monthly_return_heatmap_frame(native_report).to_csv(
            recipe_dir / f"{bundle_name}_native_monthly_return_heatmap.csv"
        )
        build_annual_return_heatmap_frame(native_report).to_csv(
            recipe_dir / f"{bundle_name}_native_annual_return_heatmap.csv"
        )
        if not benchmark_frames[bundle_name].empty:
            benchmark_frames[bundle_name].to_csv(recipe_dir / f"{bundle_name}_native_benchmark.csv", index=False)

    manifest = {
        "recipe": asdict(recipe),
        "config": asdict(config),
        "benchmark_components": [asdict(component) for component in resolve_benchmark_components(config.universe_profile, config.benchmark_mode)],
        "used_feature_columns": feature_columns,
        "native_provider_dir": str(native_provider_dir),
    }
    (recipe_dir / "native_workflow_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    if config.publish_model:
        publish_score_snapshot(
            score_frame=latest_score_frame[["instrument", "score"]].copy(),
            model_id=f"{config.universe_profile}-{recipe.name}-qlib-native",
            feature_date=latest_feature_date_str,
            extra_manifest=manifest,
        )

    return NativeRecipeArtifacts(
        recipe=recipe,
        latest_score_frame=latest_score_frame,
        prediction_bundles=prediction_bundles,
        native_results=native_results,
        validation_results=validation_results,
        executor_comparison_summary=execution_diff_summary,
        signal_diagnostics=signal_diagnostics,
        portfolio_diagnostics=portfolio_diagnostics,
        slice_regime_summary=slice_regime_summary,
        feature_prefilter_stats=feature_prefilter_stats,
        feature_corr_candidates=corr_candidates,
        feature_redundancy=feature_redundancy,
        feature_outlier_audit=feature_outlier_audit,
        used_feature_columns=feature_columns,
        native_provider_dir=native_provider_dir,
        benchmark_frames=benchmark_frames,
        native_summary=native_summary,
    )


def passes_native_promotion_gate(
    baseline_artifacts: NativeRecipeArtifacts,
    candidate_artifacts: NativeRecipeArtifacts,
) -> dict[str, Any]:
    baseline_signal = baseline_artifacts.signal_diagnostics
    candidate_signal = candidate_artifacts.signal_diagnostics
    baseline_portfolio = baseline_artifacts.portfolio_diagnostics
    candidate_portfolio = candidate_artifacts.portfolio_diagnostics
    baseline_native_summary = baseline_artifacts.native_summary.copy()
    candidate_native_summary = candidate_artifacts.native_summary.copy()
    baseline_top10_unique = float(baseline_signal["topk_unique_score_ratio"].mean()) if not baseline_signal.empty else np.nan
    candidate_top10_unique = float(candidate_signal["topk_unique_score_ratio"].mean()) if not candidate_signal.empty else np.nan
    baseline_hit_rate = float(baseline_signal["topk_overlap_prev"].mean()) if "topk_overlap_prev" in baseline_signal.columns else np.nan
    candidate_post_cost = float(candidate_native_summary.loc[candidate_native_summary["bundle"] == "walk_forward", "net_total_return"].mean())
    baseline_post_cost = float(baseline_native_summary.loc[baseline_native_summary["bundle"] == "walk_forward", "net_total_return"].mean())
    candidate_drawdown = float(candidate_native_summary.loc[candidate_native_summary["bundle"] == "walk_forward", "strategy_max_drawdown"].mean())
    baseline_drawdown = float(baseline_native_summary.loc[baseline_native_summary["bundle"] == "walk_forward", "strategy_max_drawdown"].mean())
    passed = bool(
        candidate_top10_unique >= 0.6
        and candidate_post_cost >= baseline_post_cost
        and candidate_drawdown >= baseline_drawdown - 0.05
    )
    return {
        "promotion_gate_passed": passed,
        "baseline_topk_unique_score_ratio": baseline_top10_unique,
        "candidate_topk_unique_score_ratio": candidate_top10_unique,
        "baseline_signal_turnover_proxy": baseline_hit_rate,
        "candidate_walk_forward_net_total_return": candidate_post_cost,
        "baseline_walk_forward_net_total_return": baseline_post_cost,
        "candidate_walk_forward_drawdown": candidate_drawdown,
        "baseline_walk_forward_drawdown": baseline_drawdown,
    }


def run_native_research_workflow(
    config: NativeWorkflowConfig,
    *,
    recipe_names: Sequence[str] | None = None,
    progress_callback: NativeWorkflowProgressCallback | None = None,
) -> dict[str, Any]:
    registry = build_native_recipe_registry(config)
    selected_names = list(recipe_names) if recipe_names is not None else ["baseline"]
    missing = [name for name in selected_names if name not in registry]
    if missing:
        raise ValueError(f"Unknown native workflow recipes: {missing}")
    selected_recipes = [registry[name] for name in selected_names]
    resolved_parallel_workers = _resolve_recipe_parallel_workers(config.recipe_parallel_workers, len(selected_recipes))
    resolved_model_num_threads = _resolve_model_num_threads(
        config.model_num_threads,
        recipe_parallel_workers=resolved_parallel_workers,
        reproducibility_mode=config.reproducibility_mode,
    )
    effective_config = replace(
        config,
        recipe_parallel_workers=resolved_parallel_workers,
        model_num_threads=resolved_model_num_threads,
    )
    if resolved_parallel_workers > 1:
        effective_config = _prime_parallel_workflow_inputs(effective_config)
    artifacts: dict[str, NativeRecipeArtifacts] = {}
    if resolved_parallel_workers <= 1:
        total = len(selected_recipes)
        for index, recipe in enumerate(selected_recipes, start=1):
            _emit_native_workflow_progress(
                progress_callback,
                "recipe_start",
                recipe=recipe.name,
                index=index,
                total=total,
                execution_mode="serial",
                recipe_parallel_workers=resolved_parallel_workers,
                model_num_threads=resolved_model_num_threads,
            )
            started_at = time.perf_counter()
            recipe_name, artifact = _run_native_recipe_job(effective_config, recipe)
            artifacts[recipe_name] = artifact
            _emit_native_workflow_progress(
                progress_callback,
                "recipe_done",
                recipe=recipe_name,
                index=index,
                completed=index,
                total=total,
                elapsed=time.perf_counter() - started_at,
                execution_mode="serial",
                recipe_parallel_workers=resolved_parallel_workers,
                model_num_threads=resolved_model_num_threads,
            )
    else:
        total = len(selected_recipes)
        future_to_meta: dict[Any, dict[str, Any]] = {}
        with ProcessPoolExecutor(
            max_workers=resolved_parallel_workers,
            mp_context=get_context("spawn"),
        ) as executor:
            for index, recipe in enumerate(selected_recipes, start=1):
                future = executor.submit(_run_native_recipe_job, effective_config, recipe)
                future_to_meta[future] = {
                    "recipe": recipe.name,
                    "index": index,
                    "started_at": time.perf_counter(),
                }
                _emit_native_workflow_progress(
                    progress_callback,
                    "recipe_submitted",
                    recipe=recipe.name,
                    index=index,
                    total=total,
                    execution_mode="parallel",
                    recipe_parallel_workers=resolved_parallel_workers,
                    model_num_threads=resolved_model_num_threads,
                )
            completed = 0
            pending = set(future_to_meta)
            while pending:
                done, pending = wait(
                    pending,
                    timeout=PARALLEL_PROGRESS_HEARTBEAT_SECONDS,
                    return_when=FIRST_COMPLETED,
                )
                if not done:
                    _emit_native_workflow_progress(
                        progress_callback,
                        "recipe_heartbeat",
                        execution_mode="parallel",
                        recipe_parallel_workers=resolved_parallel_workers,
                        model_num_threads=resolved_model_num_threads,
                        **_build_parallel_recipe_heartbeat(
                            pending,
                            future_to_meta,
                            completed=completed,
                            total=total,
                            now=time.perf_counter(),
                        ),
                    )
                    continue
                for future in done:
                    meta = future_to_meta[future]
                    recipe_name, artifact = future.result()
                    artifacts[recipe_name] = artifact
                    completed += 1
                    _emit_native_workflow_progress(
                        progress_callback,
                        "recipe_done",
                        recipe=recipe_name,
                        index=meta["index"],
                        completed=completed,
                        total=total,
                        elapsed=time.perf_counter() - float(meta["started_at"]),
                        execution_mode="parallel",
                        recipe_parallel_workers=resolved_parallel_workers,
                        model_num_threads=resolved_model_num_threads,
                    )
    artifacts = {name: artifacts[name] for name in selected_names}
    output_dir = Path(config.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    registry_payload = {
        "baseline_recipe": asdict(registry["baseline"]),
        "candidate_recipes": {
            name: asdict(recipe)
            for name, recipe in registry.items()
            if name != "baseline"
        },
        "executed_recipes": selected_names,
    }
    (output_dir / "recipe_registry.json").write_text(
        json.dumps(registry_payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    promotion_gate = {}
    if "baseline" in artifacts and len(artifacts) > 1:
        for name, candidate in artifacts.items():
            if name == "baseline":
                continue
            promotion_gate[name] = passes_native_promotion_gate(artifacts["baseline"], candidate)
    summary_payload = _build_native_workflow_summary_payload(
        config=effective_config,
        registry_payload=registry_payload,
        promotion_gate=promotion_gate,
        output_dir=output_dir,
        artifacts=artifacts,
    )
    (output_dir / "native_workflow_summary.json").write_text(
        json.dumps(summary_payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return {
        **summary_payload,
        "artifacts": artifacts,
    }
