"""
Notebook-friendly qlib workflow helpers.

These helpers keep the research notebook thin while reusing the same
implementation paths as the CLI scripts.
"""

from __future__ import annotations

from dataclasses import fields
import importlib.metadata
import json
import math
import platform
from pathlib import Path
import shlex
import time
from typing import Any, Literal, Sequence

import pandas as pd

from qlib_research.config import get_project_root, get_qlib_artifacts_dir
from qlib_research.core.qlib_pipeline import (
    FEATURE_GROUP_COLUMNS,
    FULL_POSTFIX_BASELINE_FEATURE_COLUMNS,
    LABEL_COLUMN,
    apply_industry_normalization,
    build_training_frame,
    compute_feature_fill_values,
    init_qlib_runtime,
    load_panel_dataframe,
    normalize_feature_name_list,
    resolve_feature_columns,
    suppress_external_output,
)
from qlib_research.core.weekly_feature_panel import (
    ensure_panel_enrichment,
    export_weekly_feature_panel,
    resolve_panel_enrichment_scope,
)
from qlib_research.io.artifacts import build_portfolio_targets, publish_portfolio_targets, publish_score_snapshot

from qlib_research.core.weekly_model_eval import (
    EvaluationRuntimeCache,
    ModelRecipe,
    compare_recipe_evaluations,
    ensure_output_dir,
    evaluate_recipe,
    build_feature_outlier_audit,
    build_feature_redundancy_report,
    passes_promotion_gate,
    prefilter_feature_columns,
    prune_feature_groups,
    prune_features,
    select_evaluation_dates,
    tune_recipe,
)


ARTIFACT_CSV_FILES = {
    "summary": "summary.csv",
    "stage_summary": "stage_summary.csv",
    "slice_summary": "slice_summary.csv",
    "details": "details.csv",
    "predictions": "predictions.csv",
    "equity_curve": "equity_curve.csv",
    "feature_prefilter": "feature_prefilter.csv",
    "feature_corr_candidates": "feature_corr_candidates.csv",
    "feature_redundancy": "feature_redundancy.csv",
    "feature_outlier_audit": "feature_outlier_audit.csv",
    "feature_group_prune": "feature_group_prune.csv",
    "tuning_results": "tuning_results.csv",
}

ARTIFACT_JSON_FILES = {
    "comparison": "comparison.json",
    "selected_recipe": "selected_recipe.json",
    "feature_spec": "feature_spec.json",
    "stage_summary_json": "stage_summary.json",
    "feature_prune_log": "feature_prune_log.json",
    "progress": "progress.json",
}

NATIVE_RECIPE_CSV_FILES = {
    "latest_score_frame": "latest_score_frame.csv",
    "feature_prefilter": "feature_prefilter.csv",
    "feature_corr_candidates": "feature_corr_candidates.csv",
    "feature_redundancy": "feature_redundancy.csv",
    "feature_outlier_audit": "feature_outlier_audit.csv",
    "signal_diagnostics": "signal_diagnostics.csv",
    "portfolio_diagnostics": "portfolio_diagnostics.csv",
    "slice_regime_summary": "slice_regime_summary.csv",
    "execution_diff_summary": "execution_diff_summary.csv",
    "rolling_predictions": "rolling_predictions.csv",
    "rolling_details": "rolling_details.csv",
    "rolling_summary": "rolling_summary.csv",
    "rolling_feature_importance": "rolling_feature_importance.csv",
    "rolling_native_report": "rolling_native_report.csv",
    "rolling_native_benchmark": "rolling_native_benchmark.csv",
    "rolling_native_monthly_return_heatmap": "rolling_native_monthly_return_heatmap.csv",
    "rolling_native_annual_return_heatmap": "rolling_native_annual_return_heatmap.csv",
    "walk_forward_predictions": "walk_forward_predictions.csv",
    "walk_forward_details": "walk_forward_details.csv",
    "walk_forward_summary": "walk_forward_summary.csv",
    "walk_forward_feature_importance": "walk_forward_feature_importance.csv",
    "walk_forward_native_report": "walk_forward_native_report.csv",
    "walk_forward_native_benchmark": "walk_forward_native_benchmark.csv",
    "walk_forward_native_monthly_return_heatmap": "walk_forward_native_monthly_return_heatmap.csv",
    "walk_forward_native_annual_return_heatmap": "walk_forward_native_annual_return_heatmap.csv",
}


def _resolve_path(path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    return (get_project_root() / candidate).resolve()


def _safe_package_version(*package_names: str) -> str | None:
    for package_name in package_names:
        try:
            return importlib.metadata.version(package_name)
        except importlib.metadata.PackageNotFoundError:
            continue
    return None


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _safe_read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").dropna()


def sanitize_for_json(value: object) -> object:
    if isinstance(value, dict):
        return {str(key): sanitize_for_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize_for_json(item) for item in value]
    if isinstance(value, tuple):
        return [sanitize_for_json(item) for item in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def _summarize_date_range(frame: pd.DataFrame, *candidate_columns: str) -> dict[str, Any]:
    for column in candidate_columns:
        if column not in frame.columns:
            continue
        series = pd.to_datetime(frame[column], errors="coerce").dropna()
        if series.empty:
            continue
        return {
            "start": str(series.min().date()),
            "end": str(series.max().date()),
            "count": int(series.nunique()),
        }
    return {
        "start": None,
        "end": None,
        "count": 0,
    }


_NATIVE_WORKFLOW_CONFIG_ALIASES = {
    "panel": "panel_path",
    "execution_panel": "execution_panel_path",
    "feature_spec": "feature_spec_path",
    "include_feature": "included_features",
    "include_features": "included_features",
    "exclude_feature": "excluded_features",
    "exclude_features": "excluded_features",
}


def _normalize_native_workflow_overrides(
    config_overrides: dict[str, Any] | None,
) -> dict[str, Any]:
    from qlib_research.core.qlib_native_workflow import NativeWorkflowConfig

    normalized: dict[str, Any] = {}
    valid_fields = {field.name for field in fields(NativeWorkflowConfig)}

    for raw_key, raw_value in (config_overrides or {}).items():
        normalized_key = _NATIVE_WORKFLOW_CONFIG_ALIASES.get(str(raw_key).replace("-", "_"), str(raw_key).replace("-", "_"))
        normalized_value = raw_value
        if normalized_key in {"included_features", "excluded_features"}:
            normalized_value = tuple(normalize_feature_name_list(raw_value))

        if normalized_key in normalized and normalized[normalized_key] != normalized_value:
            raise ValueError(
                f"Conflicting native workflow overrides provided for '{normalized_key}': "
                f"{normalized[normalized_key]!r} vs {normalized_value!r}"
            )
        normalized[normalized_key] = normalized_value

    unknown_keys = sorted(key for key in normalized if key not in valid_fields)
    if unknown_keys:
        unknown_keys_display = ", ".join(unknown_keys)
        raise TypeError(f"Unexpected native workflow override(s): {unknown_keys_display}")

    return normalized


def build_native_workflow_cli_command(
    *,
    config_overrides: dict[str, Any] | None = None,
    recipe_names: Sequence[str] | None = None,
) -> str:
    from qlib_research.core.qlib_native_workflow import NativeWorkflowConfig

    resolved_overrides = _normalize_native_workflow_overrides(config_overrides)
    config = NativeWorkflowConfig(**resolved_overrides)
    command_lines = [
        "uv run python scripts/evaluate_native_weekly.py",
        f"  --panel {shlex.quote(str(config.panel_path))}",
        f"  --output-dir {shlex.quote(str(config.output_dir))}",
        f"  --universe-profile {shlex.quote(str(config.universe_profile))}",
        f"  --benchmark-mode {shlex.quote(str(config.benchmark_mode))}",
        f"  --signal-objective {shlex.quote(str(config.signal_objective))}",
        f"  --label-recipe {shlex.quote(str(config.label_recipe))}",
        f"  --topk {int(config.topk)}",
        f"  --rebalance-interval-weeks {int(config.rebalance_interval_weeks)}",
        f"  --min-liquidity-filter {float(config.min_liquidity_filter)}",
        f"  --min-score-spread {float(config.min_score_spread)}",
        f"  --eval-count {int(config.eval_count)}",
        f"  --train-weeks {int(config.train_weeks)}",
        f"  --valid-weeks {int(config.valid_weeks)}",
        f"  --rolling-recent-weeks {int(config.rolling_recent_weeks)}",
        f"  --step-weeks {int(config.step_weeks)}",
        f"  {'--walk-forward-enabled' if config.walk_forward_enabled else '--no-walk-forward-enabled'}",
        f"  --walk-forward-eval-count {int(config.walk_forward_eval_count)}",
        f"  --walk-forward-train-weeks {int(config.walk_forward_train_weeks)}",
        f"  --walk-forward-valid-weeks {int(config.walk_forward_valid_weeks)}",
        f"  --walk-forward-step-weeks {int(config.walk_forward_step_weeks)}",
        f"  {'--run-validation-comparison' if config.run_validation_comparison else '--no-run-validation-comparison'}",
        f"  {'--diagnostics-enabled' if config.diagnostics_enabled else '--no-diagnostics-enabled'}",
        f"  {'--native-only-tradable' if config.native_only_tradable else '--no-native-only-tradable'}",
        f"  --native-risk-degree {float(config.native_risk_degree)}",
        f"  --account {float(config.account)}",
        f"  --run-export {shlex.quote(str(config.run_export))}",
        f"  {'--publish-model' if config.publish_model else '--no-publish-model'}",
        f"  --seed {int(config.seed)}",
        f"  --reproducibility-mode {shlex.quote(str(config.reproducibility_mode))}",
    ]
    if config.execution_panel_path:
        command_lines.append(f"  --execution-panel {shlex.quote(str(config.execution_panel_path))}")
    if config.feature_spec_path:
        command_lines.append(f"  --feature-spec {shlex.quote(str(config.feature_spec_path))}")
    if config.start_date:
        command_lines.append(f"  --start-date {shlex.quote(str(config.start_date))}")
    if config.end_date:
        command_lines.append(f"  --end-date {shlex.quote(str(config.end_date))}")
    if config.hold_buffer_rank is not None:
        command_lines.append(f"  --hold-buffer-rank {int(config.hold_buffer_rank)}")
    if config.industry_max_weight is not None:
        command_lines.append(f"  --industry-max-weight {float(config.industry_max_weight)}")
    if config.walk_forward_start_date:
        command_lines.append(f"  --walk-forward-start-date {shlex.quote(str(config.walk_forward_start_date))}")
    if config.walk_forward_end_date:
        command_lines.append(f"  --walk-forward-end-date {shlex.quote(str(config.walk_forward_end_date))}")
    if config.validation_execution_lag_steps is not None:
        command_lines.append(f"  --validation-execution-lag-steps {int(config.validation_execution_lag_steps)}")
    command_lines.append(
        f"  {'--validation-only-tradable' if config.validation_only_tradable else '--no-validation-only-tradable'}"
    )
    command_lines.append(f"  --validation-risk-degree {float(config.validation_risk_degree)}")
    if config.universe_exit_policy:
        command_lines.append(f"  --universe-exit-policy {shlex.quote(str(config.universe_exit_policy))}")
    command_lines.append(f"  --batch-size {int(config.batch_size)}")
    command_lines.append(f"  --recipe-parallel-workers {int(config.recipe_parallel_workers)}")
    if config.model_num_threads is not None:
        command_lines.append(f"  --model-num-threads {int(config.model_num_threads)}")

    selected_recipe_names = list(recipe_names) if recipe_names is not None else ["baseline"]
    for recipe_name in selected_recipe_names:
        command_lines.append(f"  --recipe {shlex.quote(str(recipe_name))}")
    return " \\\n".join(command_lines)


def summarize_panel(panel: pd.DataFrame) -> dict[str, Any]:
    if panel.empty:
        return {
            "rows": 0,
            "instrument_count": 0,
            "start_date": None,
            "end_date": None,
            "csi300_rows": 0,
            "csi500_rows": 0,
            "csi300_instruments": 0,
            "csi500_instruments": 0,
        }

    time_column = "datetime" if "datetime" in panel.columns else "time"
    instrument_column = "instrument" if "instrument" in panel.columns else "symbol"
    frame = panel.copy()
    frame[time_column] = pd.to_datetime(frame[time_column])
    frame[instrument_column] = frame[instrument_column].astype(str)

    csi300_mask = frame["in_csi300"].fillna(False) if "in_csi300" in frame.columns else pd.Series(False, index=frame.index)
    csi500_mask = frame["in_csi500"].fillna(False) if "in_csi500" in frame.columns else pd.Series(False, index=frame.index)
    return {
        "rows": int(len(frame)),
        "instrument_count": int(frame[instrument_column].nunique()),
        "start_date": str(frame[time_column].min().date()),
        "end_date": str(frame[time_column].max().date()),
        "csi300_rows": int(csi300_mask.sum()),
        "csi500_rows": int(csi500_mask.sum()),
        "csi300_instruments": int(frame.loc[csi300_mask, instrument_column].nunique()),
        "csi500_instruments": int(frame.loc[csi500_mask, instrument_column].nunique()),
    }


def ensure_runtime(
    panel_path: str | Path | None = None,
    initialize_qlib: bool = False,
    experiment_name: str = "valueinvesting_weekly_qlib_notebook",
) -> dict[str, Any]:
    project_root = get_project_root()
    resolved_panel = _resolve_path(panel_path) if panel_path else None
    runtime = {
        "project_root": str(project_root),
        "cwd": str(Path.cwd().resolve()),
        "python_version": platform.python_version(),
        "artifacts_dir": str(get_qlib_artifacts_dir()),
        "ipykernel_version": _safe_package_version("ipykernel"),
        "plotly_version": _safe_package_version("plotly"),
        "finance_data_hub_version": _safe_package_version("finance-data-hub", "finance_data_hub"),
        "pyqlib_version": _safe_package_version("pyqlib"),
    }
    if resolved_panel is not None:
        runtime["panel_path"] = str(resolved_panel)
        runtime["panel_exists"] = resolved_panel.exists()
    if initialize_qlib:
        init_qlib_runtime(exp_name=experiment_name)
        runtime["qlib_runtime_initialized"] = True
    else:
        runtime["qlib_runtime_initialized"] = False
    return runtime


def export_or_load_panel(
    panel_path: str | Path,
    universe_profile: str | None = "merged_csi300_500",
    run_export: Literal["always", "auto_if_missing", "never"] | bool = "auto_if_missing",
    start_date: str | None = None,
    end_date: str | None = None,
    symbols: Sequence[str] | None = None,
    batch_size: int = 300,
    return_panel: bool = True,
    enrichment_scope: str | None = None,
) -> dict[str, Any]:
    resolved_path = _resolve_path(panel_path)
    export_performed = False

    if run_export is True or run_export == "always":
        export_weekly_feature_panel(
            output_path=resolved_path,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            batch_size=batch_size,
            universe_profile=universe_profile,
            enrichment_scope=enrichment_scope,
        )
        export_performed = True
    elif run_export == "auto_if_missing":
        if not resolved_path.exists():
            export_weekly_feature_panel(
                output_path=resolved_path,
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                batch_size=batch_size,
                universe_profile=universe_profile,
                enrichment_scope=enrichment_scope,
            )
            export_performed = True
    elif run_export is False or run_export == "never":
        if not resolved_path.exists():
            raise FileNotFoundError(f"Feature panel not found: {resolved_path}")
    else:
        raise ValueError(f"Unsupported run_export mode: {run_export}")

    panel = load_panel_dataframe(resolved_path) if return_panel else None
    if panel is not None and universe_profile:
        panel = filter_panel_by_universe_profile(panel, universe_profile)
    return {
        "path": resolved_path,
        "action": "exported" if export_performed else "loaded_existing",
        "export_performed": export_performed,
        "panel": panel,
        "summary": summarize_panel(panel) if panel is not None else {},
    }


def filter_panel_by_universe_profile(panel: pd.DataFrame, universe_profile: str) -> pd.DataFrame:
    if universe_profile == "merged_csi300_500":
        required = {"in_csi300", "in_csi500"}
        if not required.issubset(panel.columns):
            raise ValueError("merged_csi300_500 panel requires in_csi300 and in_csi500 flags")
        return panel.loc[panel["in_csi300"].fillna(False) | panel["in_csi500"].fillna(False)].copy()
    if universe_profile == "csi300":
        if "in_csi300" not in panel.columns:
            raise ValueError("csi300 panel requires in_csi300 flag")
        return panel.loc[panel["in_csi300"].fillna(False)].copy()
    if universe_profile == "csi500":
        if "in_csi500" not in panel.columns:
            raise ValueError("csi500 panel requires in_csi500 flag")
        return panel.loc[panel["in_csi500"].fillna(False)].copy()
    return panel.copy()


def groups_for_features(feature_columns: Sequence[str]) -> tuple[str, ...]:
    return tuple(
        group_name
        for group_name, columns in FEATURE_GROUP_COLUMNS.items()
        if any(feature in feature_columns for feature in columns)
    )


def combine_recipe_artifacts(
    evaluations: list[Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    summary = pd.concat([evaluation.summary for evaluation in evaluations if not evaluation.summary.empty], ignore_index=True) if evaluations else pd.DataFrame()
    slice_summary = pd.concat([evaluation.slice_summary for evaluation in evaluations if not evaluation.slice_summary.empty], ignore_index=True) if evaluations else pd.DataFrame()
    details = pd.concat([evaluation.details for evaluation in evaluations if not evaluation.details.empty], ignore_index=True) if evaluations else pd.DataFrame()
    predictions = pd.concat([evaluation.predictions for evaluation in evaluations if not evaluation.predictions.empty], ignore_index=True) if evaluations else pd.DataFrame()
    equity_curve = pd.concat([evaluation.equity_curve for evaluation in evaluations if not evaluation.equity_curve.empty], ignore_index=True) if evaluations else pd.DataFrame()
    return summary, slice_summary, details, predictions, equity_curve


def build_selected_recipe_payload(
    recipe: ModelRecipe,
    promotion_gate_passed: bool,
    evaluation_scope: str,
    excluded_features: Sequence[str] | str | None = None,
    calibration_window: dict[str, Any] | None = None,
    panel_enrichment_scope: str = "research_full",
    redundant_features: Sequence[str] | str | None = None,
) -> dict[str, object]:
    excluded = list(normalize_feature_name_list(excluded_features))
    redundant = list(normalize_feature_name_list(redundant_features))
    return {
        "version": "1.0",
        "model_id_suggestion": f"{evaluation_scope.replace('_', '')}-weekly-lgbm-small-stable",
        "selected_features": list(recipe.feature_columns),
        "selected_feature_groups": list(recipe.feature_groups),
        "industry_normalization": recipe.industry_normalization,
        "normalization_policy": {
            "default": recipe.industry_normalization,
            "selected_features": list(recipe.feature_columns),
        },
        "outlier_policy": {
            "default": "audit_only",
        },
        "tuned_params": recipe.model_params,
        "evaluation_scope": evaluation_scope,
        "promotion_gate_passed": promotion_gate_passed,
        "excluded_features": excluded,
        "redundant_features": redundant,
        "panel_requirements": {
            "enrichment_scope": panel_enrichment_scope,
        },
        "calibration_window": calibration_window or {},
    }


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(sanitize_for_json(payload), ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )


def _artifact_path(output_dir: Path, filename: str, partial: bool = False) -> Path:
    if not partial:
        return output_dir / filename
    artifact = Path(filename)
    return output_dir / f"{artifact.stem}.partial{artifact.suffix}"


def _format_duration(seconds: float | None) -> str | None:
    if seconds is None or not math.isfinite(seconds) or seconds < 0:
        return None
    total_seconds = int(round(seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes > 0:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def _estimate_tuning_fit_units(
    eval_date_count: int,
    coarse_limit: int | None,
    micro_limit: int | None,
) -> int:
    coarse_candidates = min(coarse_limit, 12) if coarse_limit is not None else 12
    micro_candidates = micro_limit if micro_limit is not None else 6
    return eval_date_count * (coarse_candidates + micro_candidates)


def _estimate_prune_fit_units(
    item_count: int,
    eval_date_count: int,
    max_rounds: int | None,
    candidate_limit: int | None,
) -> int:
    if item_count <= 1:
        return 0
    remaining = item_count
    total_candidates = 0
    round_index = 0
    while remaining > 1:
        round_index += 1
        if max_rounds is not None and round_index > max_rounds:
            break
        candidate_total = remaining if candidate_limit is None else min(remaining, candidate_limit)
        total_candidates += candidate_total
        remaining -= 1
    return total_candidates * eval_date_count


class WorkflowProgressReporter:
    def __init__(self, output_dir: Path, console_mode: Literal["bar", "verbose", "silent"] = "bar") -> None:
        self.output_dir = output_dir
        self.console_mode = console_mode
        self.started_at = time.monotonic()
        self.stage_started_at = self.started_at
        self.estimated_total_fit_units = 0
        self.completed_fit_units = 0
        self.active_budget_stage = "initializing"
        self.active_stage_estimated_fit_units = 0
        self.active_stage_start_completed_fit_units = 0
        self.status = "running"
        self.current_stage = "initializing"
        self.stage_message = "Preparing convergence workflow"
        self.last_event: dict[str, Any] = {}
        self.stage_summaries: list[dict[str, Any]] = []
        self.tuning_rows: list[dict[str, Any]] = []
        self.group_prune_rows: list[dict[str, Any]] = []
        self.feature_prune_rows: list[dict[str, Any]] = []
        self._bar_active = False
        self._bar_width = 0
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._write_progress_file()

    def add_estimated_fit_units(self, units: int, stage: str, message: str) -> None:
        if units > 0:
            self.estimated_total_fit_units += int(units)
        self.active_budget_stage = stage
        self.active_stage_estimated_fit_units = max(int(units), 0)
        self.active_stage_start_completed_fit_units = self.completed_fit_units
        self._set_stage(stage, message)
        self._emit_line(
            f"[workflow] stage={stage} message={message} "
            f"estimated_total_fit_units={self.estimated_total_fit_units}"
        )
        self._write_progress_file()

    def mark_stage(self, stage: str, message: str) -> None:
        self.active_budget_stage = stage
        self.active_stage_estimated_fit_units = 0
        self.active_stage_start_completed_fit_units = self.completed_fit_units
        self._set_stage(stage, message)
        self._emit_line(f"[workflow] stage={stage} message={message}")
        self._write_progress_file()

    def complete_stage(
        self,
        *,
        stage: str,
        stage_label: str,
        recipe_name: str,
        evaluation: Any,
        recipe_payload: dict[str, Any],
        eval_date_count: int,
        tuning_frame: pd.DataFrame | None = None,
        group_prune_log: pd.DataFrame | None = None,
        feature_prune_log: list[dict[str, Any]] | None = None,
        comparison: dict[str, Any] | None = None,
        promotion_gate: dict[str, Any] | None = None,
    ) -> None:
        stage_elapsed_seconds = time.monotonic() - self.stage_started_at
        main_row = _frame_first_row(evaluation.summary)
        slice_rows = _slice_row_map(evaluation.slice_summary)
        csi300_row = slice_rows.get("csi300", {})
        csi500_row = slice_rows.get("csi500", {})
        feature_count = len(recipe_payload.get("selected_features", []))
        feature_group_count = len(recipe_payload.get("selected_feature_groups", []))
        tuning_candidate_count = int(len(tuning_frame)) if tuning_frame is not None else 0
        accepted_group_prunes = 0
        if group_prune_log is not None and not group_prune_log.empty and "selected" in group_prune_log.columns:
            accepted_group_prunes = int(group_prune_log["selected"].fillna(False).sum())
        feature_prune_rows = feature_prune_log or []
        accepted_feature_prunes = int(sum(bool(row.get("selected")) for row in feature_prune_rows))
        stage_row = {
            "stage": stage,
            "stage_label": stage_label,
            "status": "completed",
            "recipe": recipe_name,
            "feature_count": feature_count,
            "feature_group_count": feature_group_count,
            "eval_date_count": eval_date_count,
            "tuning_candidate_count": tuning_candidate_count,
            "accepted_group_prunes": accepted_group_prunes,
            "accepted_feature_prunes": accepted_feature_prunes,
            "rank_ic_mean": main_row.get("rank_ic_mean"),
            "rank_ic_ir": main_row.get("rank_ic_ir"),
            "topk_mean_excess_return_4w": main_row.get("topk_mean_excess_return_4w"),
            "strategy_total_return": main_row.get("strategy_total_return"),
            "strategy_max_drawdown": main_row.get("strategy_max_drawdown"),
            "csi300_rank_ic_ir": csi300_row.get("rank_ic_ir"),
            "csi500_rank_ic_ir": csi500_row.get("rank_ic_ir"),
            "csi300_strategy_max_drawdown": csi300_row.get("strategy_max_drawdown"),
            "csi500_strategy_max_drawdown": csi500_row.get("strategy_max_drawdown"),
            "comparison_rank_ic_ir_delta": (comparison or {}).get("rank_ic_ir_delta"),
            "comparison_topk_mean_excess_return_4w_delta": (comparison or {}).get("topk_mean_excess_return_4w_delta"),
            "comparison_strategy_max_drawdown_delta": (comparison or {}).get("strategy_max_drawdown_delta"),
            "promotion_gate_passed": (promotion_gate or {}).get("promotion_gate_passed"),
            "stage_elapsed_seconds": stage_elapsed_seconds,
            "stage_elapsed_human": _format_duration(stage_elapsed_seconds),
        }
        self._upsert_stage_summary(stage_row)
        self._emit_line(
            "[summary] "
            f"stage={stage} "
            f"recipe={recipe_name} "
            f"features={feature_count} "
            f"rank_ic_ir={self._format_metric(stage_row['rank_ic_ir'])} "
            f"topk_excess={self._format_metric(stage_row['topk_mean_excess_return_4w'])} "
            f"drawdown={self._format_metric(stage_row['strategy_max_drawdown'])} "
            f"csi300_rank_ic_ir={self._format_metric(stage_row['csi300_rank_ic_ir'])} "
            f"csi500_rank_ic_ir={self._format_metric(stage_row['csi500_rank_ic_ir'])} "
            f"elapsed={stage_row['stage_elapsed_human']}"
        )
        if promotion_gate is not None:
            self._emit_line(
                "[summary] "
                f"stage={stage} "
                f"gate_passed={bool(promotion_gate.get('promotion_gate_passed', False))} "
                f"rank_ok={bool(promotion_gate.get('rank_ok', False))} "
                f"topk_ok={bool(promotion_gate.get('topk_ok', False))} "
                f"drawdown_ok={bool(promotion_gate.get('drawdown_ok', False))} "
                f"slice_rank_ok={bool(promotion_gate.get('slice_rank_ok', False))} "
                f"slice_drawdown_ok={bool(promotion_gate.get('slice_drawdown_ok', False))} "
                f"reduction_ok={bool(promotion_gate.get('reduction_ok', False))}"
            )
        self._write_stage_summary_files()
        self._write_progress_file()

    def handle_event(self, event: dict[str, Any]) -> None:
        event_name = str(event.get("event", "unknown"))
        if "stage" in event:
            self.current_stage = str(event["stage"])
        if event_name == "eval_date_complete":
            self.completed_fit_units += 1
        if event_name == "candidate_complete":
            row = event.get("row")
            if isinstance(row, dict):
                self.tuning_rows.append(dict(row))
                pd.DataFrame(self.tuning_rows).to_csv(
                    self.output_dir / "tuning_results.partial.csv",
                    index=False,
                )
        elif event_name == "prune_candidate_complete":
            row = event.get("row")
            prune_kind = str(event.get("prune_kind", ""))
            if isinstance(row, dict):
                if prune_kind == "group":
                    self.group_prune_rows.append(dict(row))
                    pd.DataFrame(self.group_prune_rows).to_csv(
                        self.output_dir / "feature_group_prune.partial.csv",
                        index=False,
                    )
                elif prune_kind == "feature":
                    self.feature_prune_rows.append(dict(row))
                    _write_json(
                        self.output_dir / "feature_prune_log.partial.json",
                        self.feature_prune_rows,
                    )

        self.last_event = self._event_snapshot(event)
        self._write_progress_file()
        self._handle_console_event(event)

    def write_partial_artifacts(
        self,
        *,
        feature_stats: pd.DataFrame,
        corr_marks: pd.DataFrame,
        feature_redundancy: pd.DataFrame | None,
        feature_outlier_audit: pd.DataFrame | None,
        evaluations: list[Any],
        group_prune_log: pd.DataFrame,
        feature_prune_log: list[dict[str, Any]],
        tuning_results: list[pd.DataFrame],
        comparison: dict[str, Any],
        promotion_gate: dict[str, Any],
        selected_recipe_payload: dict[str, Any],
        emit_slices: bool,
    ) -> None:
        summary, slice_summary, details, predictions, equity_curve = combine_recipe_artifacts(evaluations)
        if not emit_slices:
            slice_summary = pd.DataFrame()
        _write_artifact_bundle(
            output_dir=self.output_dir,
            feature_stats=feature_stats,
            corr_marks=corr_marks,
            feature_redundancy=feature_redundancy,
            feature_outlier_audit=feature_outlier_audit,
            summary=summary,
            slice_summary=slice_summary,
            details=details,
            predictions=predictions,
            equity_curve=equity_curve,
            group_prune_log=group_prune_log,
            feature_prune_log=feature_prune_log,
            tuning_results=tuning_results,
            comparison=comparison,
            promotion_gate=promotion_gate,
            selected_recipe_payload=selected_recipe_payload,
            partial=True,
        )

    def mark_completed(self) -> None:
        self.status = "completed"
        self.current_stage = "completed"
        self.stage_message = "Convergence workflow completed"
        self._emit_line("[workflow] status=completed")
        self._write_progress_file()

    def mark_failed(self, error: Exception) -> None:
        self.status = "failed"
        self.last_event = {
            "event": "workflow_failed",
            "error_type": error.__class__.__name__,
            "error": str(error),
        }
        self._emit_line(f"[workflow] status=failed error={error.__class__.__name__}: {error}")
        self._write_progress_file()

    def _event_snapshot(self, event: dict[str, Any]) -> dict[str, Any]:
        snapshot_keys = {
            "event",
            "stage",
            "phase",
            "candidate_index",
            "candidate_total",
            "eval_date_index",
            "eval_date_total",
            "feature_date",
            "recipe_name",
            "feature_count",
            "prune_kind",
            "round",
            "removed_group",
            "removed_feature",
            "accepted",
            "is_best_so_far",
            "elapsed_seconds",
        }
        snapshot = {key: sanitize_for_json(event[key]) for key in snapshot_keys if key in event}
        if "row" in event and isinstance(event["row"], dict):
            snapshot["row"] = sanitize_for_json(event["row"])
        return snapshot

    def _set_stage(self, stage: str, message: str) -> None:
        self.current_stage = stage
        self.stage_message = message
        self.stage_started_at = time.monotonic()

    def _upsert_stage_summary(self, row: dict[str, Any]) -> None:
        self.stage_summaries = [item for item in self.stage_summaries if item.get("stage") != row.get("stage")]
        self.stage_summaries.append(sanitize_for_json(row))

    def _write_stage_summary_files(self) -> None:
        stage_summary_frame = pd.DataFrame(self.stage_summaries)
        stage_summary_frame.to_csv(self.output_dir / "stage_summary.csv", index=False)
        _write_json(self.output_dir / "stage_summary.json", self.stage_summaries)

    @staticmethod
    def _format_metric(value: Any) -> str:
        if value is None or (isinstance(value, float) and not math.isfinite(value)) or pd.isna(value):
            return "NA"
        return f"{float(value):.4f}"

    def _progress_payload(self) -> dict[str, Any]:
        elapsed_seconds = time.monotonic() - self.started_at
        remaining_fit_units = max(self.estimated_total_fit_units - self.completed_fit_units, 0)
        stage_completed_fit_units = max(
            self.completed_fit_units - self.active_stage_start_completed_fit_units,
            0,
        )
        stage_remaining_fit_units = max(
            self.active_stage_estimated_fit_units - stage_completed_fit_units,
            0,
        )
        eta_seconds = None
        if self.completed_fit_units > 0 and self.estimated_total_fit_units >= self.completed_fit_units:
            eta_seconds = elapsed_seconds / self.completed_fit_units * remaining_fit_units
        progress_ratio = None
        if self.estimated_total_fit_units > 0:
            progress_ratio = min(self.completed_fit_units / self.estimated_total_fit_units, 1.0)
        stage_progress_ratio = None
        if self.active_stage_estimated_fit_units > 0:
            stage_progress_ratio = min(
                stage_completed_fit_units / self.active_stage_estimated_fit_units,
                1.0,
            )
        if self.status == "completed":
            remaining_fit_units = 0
            eta_seconds = 0.0
            progress_ratio = 1.0
            stage_remaining_fit_units = 0
            if self.active_stage_estimated_fit_units > 0:
                stage_progress_ratio = 1.0
        return {
            "status": self.status,
            "current_stage": self.current_stage,
            "active_budget_stage": self.active_budget_stage,
            "stage_message": self.stage_message,
            "completed_fit_units": self.completed_fit_units,
            "estimated_total_fit_units": self.estimated_total_fit_units,
            "estimated_remaining_fit_units": remaining_fit_units,
            "progress_ratio": progress_ratio,
            "stage_completed_fit_units": stage_completed_fit_units,
            "stage_estimated_fit_units": self.active_stage_estimated_fit_units,
            "stage_remaining_fit_units": stage_remaining_fit_units,
            "stage_progress_ratio": stage_progress_ratio,
            "elapsed_seconds": elapsed_seconds,
            "elapsed_human": _format_duration(elapsed_seconds),
            "eta_seconds": eta_seconds,
            "eta_human": _format_duration(eta_seconds),
            "last_event": self.last_event,
            "stage_summaries": self.stage_summaries,
            "updated_at": pd.Timestamp.now(tz="Asia/Shanghai").isoformat(),
        }

    def _write_progress_file(self) -> None:
        _write_json(self.output_dir / "progress.json", self._progress_payload())

    def _emit_line(self, line: str, *, console: bool = True) -> None:
        if console and self.console_mode != "silent":
            self._clear_progress_bar()
            print(line, flush=True)
        with (self.output_dir / "progress.log").open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")

    def _build_progress_line(self, event: dict[str, Any]) -> str:
        payload = self._progress_payload()
        candidate_text = ""
        if event.get("candidate_index") is not None and event.get("candidate_total") is not None:
            candidate_text = f" candidate={event['candidate_index']}/{event['candidate_total']}"
        eval_text = ""
        if event.get("eval_date_index") is not None and event.get("eval_date_total") is not None:
            eval_text = f" eval={event['eval_date_index']}/{event['eval_date_total']}"
        phase_text = f" phase={event['phase']}" if event.get("phase") else ""
        feature_date_text = f" feature_date={event['feature_date']}" if event.get("feature_date") else ""
        eta_text = f" eta={payload['eta_human']}" if payload.get("eta_human") else ""
        ratio = payload.get("progress_ratio")
        progress_text = (
            f" progress={payload['completed_fit_units']}/{payload['estimated_total_fit_units']} "
            f"({ratio * 100:.1f}%)"
            if ratio is not None
            else f" progress={payload['completed_fit_units']}"
        )
        return (
            f"[workflow] stage={self.current_stage}{phase_text}{candidate_text}{eval_text}"
            f"{feature_date_text}{progress_text}{eta_text} event={event.get('event')}"
        )

    def _handle_console_event(self, event: dict[str, Any]) -> None:
        progress_line = self._build_progress_line(event)
        if self.console_mode == "verbose":
            self._emit_line(progress_line, console=True)
            return

        self._emit_line(progress_line, console=False)
        if self.console_mode == "bar":
            self._render_progress_bar(event)

    def _render_progress_bar(self, event: dict[str, Any]) -> None:
        payload = self._progress_payload()
        total_ratio = payload.get("progress_ratio") or 0.0
        stage_ratio = payload.get("stage_progress_ratio") or 0.0
        bar_width = 20
        filled = min(max(int(bar_width * total_ratio), 0), bar_width)
        bar = "#" * filled + "-" * (bar_width - filled)
        details = [
            f"[{bar}]",
            f"total={total_ratio * 100:5.1f}% {payload['completed_fit_units']}/{payload['estimated_total_fit_units']}",
        ]
        stage_name = str(payload.get("active_budget_stage") or self.current_stage)
        stage_text = (
            f"stage={stage_name} {stage_ratio * 100:5.1f}% "
            f"{payload['stage_completed_fit_units']}/{payload['stage_estimated_fit_units']}"
            if payload.get("stage_estimated_fit_units")
            else f"stage={stage_name}"
        )
        details.append(stage_text)
        if stage_name != self.current_stage:
            details.append(f"scope={self.current_stage}")
        if event.get("phase"):
            details.append(f"phase={event['phase']}")
        if event.get("candidate_index") is not None and event.get("candidate_total") is not None:
            details.append(f"cand={event['candidate_index']}/{event['candidate_total']}")
        if event.get("eval_date_index") is not None and event.get("eval_date_total") is not None:
            details.append(f"eval={event['eval_date_index']}/{event['eval_date_total']}")
        if payload.get("eta_human"):
            details.append(f"eta={payload['eta_human']}")
        line = " ".join(details)
        padded = line.ljust(max(self._bar_width, len(line)))
        print(f"\r{padded}", end="", flush=True)
        self._bar_active = True
        self._bar_width = len(padded)

    def _clear_progress_bar(self) -> None:
        if self.console_mode != "bar" or not self._bar_active:
            return
        print("\r" + (" " * self._bar_width) + "\r", end="", flush=True)
        self._bar_active = False


def _write_artifact_bundle(
    *,
    output_dir: Path,
    feature_stats: pd.DataFrame,
    corr_marks: pd.DataFrame,
    feature_redundancy: pd.DataFrame | None = None,
    feature_outlier_audit: pd.DataFrame | None = None,
    summary: pd.DataFrame,
    slice_summary: pd.DataFrame,
    details: pd.DataFrame,
    predictions: pd.DataFrame,
    equity_curve: pd.DataFrame,
    group_prune_log: pd.DataFrame,
    feature_prune_log: list[dict[str, Any]],
    tuning_results: list[pd.DataFrame],
    comparison: dict[str, Any],
    promotion_gate: dict[str, Any],
    selected_recipe_payload: dict[str, Any],
    partial: bool,
) -> None:
    feature_stats.to_csv(_artifact_path(output_dir, "feature_prefilter.csv", partial=partial), index=False)
    corr_marks.to_csv(_artifact_path(output_dir, "feature_corr_candidates.csv", partial=partial), index=False)
    (feature_redundancy if feature_redundancy is not None else pd.DataFrame()).to_csv(
        _artifact_path(output_dir, "feature_redundancy.csv", partial=partial),
        index=False,
    )
    (feature_outlier_audit if feature_outlier_audit is not None else pd.DataFrame()).to_csv(
        _artifact_path(output_dir, "feature_outlier_audit.csv", partial=partial),
        index=False,
    )
    summary.to_csv(_artifact_path(output_dir, "summary.csv", partial=partial), index=False)
    slice_summary.to_csv(_artifact_path(output_dir, "slice_summary.csv", partial=partial), index=False)
    details.to_csv(_artifact_path(output_dir, "details.csv", partial=partial), index=False)
    predictions.to_csv(_artifact_path(output_dir, "predictions.csv", partial=partial), index=False)
    if not equity_curve.empty:
        equity_curve.to_csv(_artifact_path(output_dir, "equity_curve.csv", partial=partial), index=False)
    group_prune_log.to_csv(_artifact_path(output_dir, "feature_group_prune.csv", partial=partial), index=False)
    _write_json(_artifact_path(output_dir, "feature_prune_log.json", partial=partial), feature_prune_log)
    tuning_frame = pd.concat(tuning_results, ignore_index=True) if tuning_results else pd.DataFrame()
    tuning_frame.to_csv(_artifact_path(output_dir, "tuning_results.csv", partial=partial), index=False)
    _write_json(
        _artifact_path(output_dir, "comparison.json", partial=partial),
        {
            "comparison": comparison,
            "promotion_gate": promotion_gate,
            "selected_recipe": selected_recipe_payload,
        },
    )
    _write_json(
        _artifact_path(output_dir, "selected_recipe.json", partial=partial),
        selected_recipe_payload,
    )
    _write_json(
        _artifact_path(output_dir, "feature_spec.json", partial=partial),
        selected_recipe_payload,
    )


def _frame_first_row(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {}
    return frame.iloc[0].to_dict()


def _slice_row_map(slice_summary: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if slice_summary.empty:
        return {}
    return {
        str(row["slice"]): row.to_dict()
        for _, row in slice_summary.iterrows()
    }


def run_convergence_workflow(
    panel_or_path: pd.DataFrame | str | Path,
    output_dir: str | Path,
    universe_profile: str = "merged_csi300_500",
    selection_mode: Literal["full", "group_prune", "feature_prune", "final_compare"] = "final_compare",
    industry_normalization: Literal["none", "l1_weekly_robust"] = "l1_weekly_robust",
    emit_slices: bool = True,
    eval_count: int = 52,
    step_weeks: int = 1,
    train_weeks: int = 260,
    valid_weeks: int = 52,
    topk: int = 10,
    start_date: str | None = None,
    end_date: str | None = None,
    experiment_name: str = "valueinvesting_weekly_qlib_convergence",
    coarse_tune_limit: int | None = 12,
    micro_tune_limit: int | None = 6,
    max_group_prune_rounds: int | None = None,
    group_candidate_limit: int | None = None,
    max_feature_prune_rounds: int | None = None,
    feature_candidate_limit: int | None = None,
    excluded_features: Sequence[str] | str | None = None,
    initialize_qlib: bool = True,
    console_mode: Literal["bar", "verbose", "silent"] = "bar",
) -> dict[str, Any]:
    resolved_output_dir = ensure_output_dir(_resolve_path(output_dir))
    progress = WorkflowProgressReporter(resolved_output_dir, console_mode=console_mode)
    try:
        if initialize_qlib:
            progress.mark_stage("initialize_qlib", "Initializing qlib runtime")
            init_qlib_runtime(exp_name=experiment_name)

        panel = (
            panel_or_path.copy()
            if isinstance(panel_or_path, pd.DataFrame)
            else load_panel_dataframe(_resolve_path(panel_or_path))
        )
        panel["datetime"] = pd.to_datetime(panel["datetime"])
        panel["instrument"] = panel["instrument"].astype(str)
        panel = filter_panel_by_universe_profile(panel, universe_profile)
        panel = panel.sort_values(["datetime", "instrument"]).reset_index(drop=True)
        resolved_excluded_features = normalize_feature_name_list(excluded_features)

        eval_dates = select_evaluation_dates(
            panel=panel,
            train_weeks=train_weeks,
            valid_weeks=valid_weeks,
            eval_count=eval_count,
            step_weeks=step_weeks,
            start_date=start_date,
            end_date=end_date,
        )
        if not eval_dates:
            raise ValueError("No eligible evaluation dates selected")

        calibration_panel = panel.loc[pd.to_datetime(panel["datetime"]) <= pd.Timestamp(eval_dates[0])].copy()
        filtered_features, feature_stats, corr_marks = prefilter_feature_columns(
            panel=calibration_panel,
            feature_columns=resolve_feature_columns(
                FULL_POSTFIX_BASELINE_FEATURE_COLUMNS,
                excluded_features=resolved_excluded_features,
                default_features=FULL_POSTFIX_BASELINE_FEATURE_COLUMNS,
            ),
        )
        if not filtered_features:
            raise ValueError("No usable feature columns remain after prefilter/excluded_features")
        calibration_window = {
            "start": str(calibration_panel["datetime"].min().date()),
            "end": str(calibration_panel["datetime"].max().date()),
        }
        feature_redundancy = build_feature_redundancy_report(
            calibration_panel,
            filtered_features,
            end_date=calibration_window["end"],
        )
        redundant_feature_names = tuple(
            dict.fromkeys(
                feature_redundancy.get("right_feature", pd.Series(dtype=object)).astype(str).tolist()
            )
        )
        feature_outlier_audit = build_feature_outlier_audit(
            calibration_panel,
            filtered_features,
            end_date=calibration_window["end"],
        )
        feature_groups = groups_for_features(filtered_features)
        full_recipe = ModelRecipe(
            name="full_postfix_baseline",
            feature_columns=tuple(filtered_features),
            feature_groups=feature_groups,
            industry_normalization=industry_normalization,
        )

        evaluation_cache: dict[tuple[object, ...], Any] = {}
        runtime_cache = EvaluationRuntimeCache(
            evaluation_cache=evaluation_cache,
            normalization_feature_columns=tuple(filtered_features),
        )
        group_prune_log = pd.DataFrame()
        feature_prune_log: list[dict[str, Any]] = []
        tuning_results: list[pd.DataFrame] = []
        comparison: dict[str, Any] = {}
        promotion_gate = {
            "promotion_gate_passed": False,
            "feature_reduction_ratio": 0.0,
            "rank_ok": False,
            "topk_ok": False,
            "drawdown_ok": False,
            "slice_rank_ok": False,
            "slice_drawdown_ok": False,
            "reduction_ok": False,
        }

        progress.add_estimated_fit_units(
            _estimate_tuning_fit_units(len(eval_dates), coarse_tune_limit, micro_tune_limit) + len(eval_dates),
            stage="full_baseline",
            message="Tuning and evaluating full_postfix_baseline",
        )
        full_params, full_tuning_results, _ = tune_recipe(
            panel=panel,
            recipe=full_recipe,
            eval_dates=eval_dates,
            topk=topk,
            train_weeks=train_weeks,
            valid_weeks=valid_weeks,
            cache=evaluation_cache,
            runtime_cache=runtime_cache,
            coarse_limit=coarse_tune_limit,
            micro_limit=micro_tune_limit,
            progress_callback=progress.handle_event,
            progress_context={"stage": "full_tuning"},
        )
        tuning_results.append(full_tuning_results.assign(recipe="full_postfix_baseline"))

        full_recipe = ModelRecipe(
            name="full_postfix_baseline",
            feature_columns=full_recipe.feature_columns,
            feature_groups=full_recipe.feature_groups,
            industry_normalization=full_recipe.industry_normalization,
            model_params=full_params,
        )
        full_evaluation = evaluate_recipe(
            panel=panel,
            recipe=full_recipe,
            eval_dates=eval_dates,
            topk=topk,
            train_weeks=train_weeks,
            valid_weeks=valid_weeks,
            cache=evaluation_cache,
            runtime_cache=runtime_cache,
            progress_callback=progress.handle_event,
            progress_context={"stage": "full_evaluation"},
        )

        selected_recipe = full_recipe
        selected_evaluation = full_evaluation
        evaluations = [full_evaluation]
        selected_recipe_payload = build_selected_recipe_payload(
            recipe=selected_recipe,
            promotion_gate_passed=False,
            evaluation_scope=universe_profile,
            excluded_features=resolved_excluded_features,
            calibration_window=calibration_window,
            redundant_features=redundant_feature_names,
        )
        progress.write_partial_artifacts(
            feature_stats=feature_stats,
            corr_marks=corr_marks,
            feature_redundancy=feature_redundancy,
            feature_outlier_audit=feature_outlier_audit,
            evaluations=evaluations,
            group_prune_log=group_prune_log,
            feature_prune_log=feature_prune_log,
            tuning_results=tuning_results,
            comparison=comparison,
            promotion_gate=promotion_gate,
            selected_recipe_payload=selected_recipe_payload,
            emit_slices=emit_slices,
        )
        progress.complete_stage(
            stage="full_baseline",
            stage_label="full_postfix_baseline",
            recipe_name=full_recipe.name,
            evaluation=full_evaluation,
            recipe_payload=selected_recipe_payload,
            eval_date_count=len(eval_dates),
            tuning_frame=full_tuning_results,
            comparison=None,
            promotion_gate=None,
        )

        if selection_mode in {"group_prune", "feature_prune", "final_compare"}:
            progress.add_estimated_fit_units(
                _estimate_prune_fit_units(
                    len(selected_recipe.feature_groups),
                    len(eval_dates),
                    max_group_prune_rounds,
                    group_candidate_limit,
                ) + len(eval_dates),
                stage="group_prune",
                message="Pruning feature groups",
            )
            group_recipe, group_evaluation, group_prune_log = prune_feature_groups(
                panel=panel,
                base_recipe=full_recipe,
                base_evaluation=full_evaluation,
                eval_dates=eval_dates,
                topk=topk,
                train_weeks=train_weeks,
                valid_weeks=valid_weeks,
                cache=evaluation_cache,
                runtime_cache=runtime_cache,
                max_rounds=max_group_prune_rounds,
                candidate_limit=group_candidate_limit,
                progress_callback=progress.handle_event,
                progress_context={"stage": "group_prune"},
            )
            group_recipe = ModelRecipe(
                name="group_pruned_candidate",
                feature_columns=group_recipe.feature_columns,
                feature_groups=group_recipe.feature_groups,
                industry_normalization=group_recipe.industry_normalization,
                model_params=full_params,
            )
            group_evaluation = evaluate_recipe(
                panel=panel,
                recipe=group_recipe,
                eval_dates=eval_dates,
                topk=topk,
                train_weeks=train_weeks,
                valid_weeks=valid_weeks,
                cache=evaluation_cache,
                runtime_cache=runtime_cache,
                progress_callback=progress.handle_event,
                progress_context={"stage": "group_evaluation"},
            )
            selected_recipe = group_recipe
            selected_evaluation = group_evaluation
            evaluations.append(group_evaluation)
            selected_recipe_payload = build_selected_recipe_payload(
                recipe=selected_recipe,
                promotion_gate_passed=False,
                evaluation_scope=universe_profile,
                excluded_features=resolved_excluded_features,
                calibration_window=calibration_window,
                redundant_features=redundant_feature_names,
            )
            progress.write_partial_artifacts(
                feature_stats=feature_stats,
                corr_marks=corr_marks,
                feature_redundancy=feature_redundancy,
                feature_outlier_audit=feature_outlier_audit,
                evaluations=evaluations,
                group_prune_log=group_prune_log,
                feature_prune_log=feature_prune_log,
                tuning_results=tuning_results,
                comparison=comparison,
                promotion_gate=promotion_gate,
                selected_recipe_payload=selected_recipe_payload,
                emit_slices=emit_slices,
            )
            progress.complete_stage(
                stage="group_prune",
                stage_label="group_pruned_candidate",
                recipe_name=group_recipe.name,
                evaluation=group_evaluation,
                recipe_payload=selected_recipe_payload,
                eval_date_count=len(eval_dates),
                tuning_frame=None,
                group_prune_log=group_prune_log,
                feature_prune_log=feature_prune_log,
                comparison=None,
                promotion_gate=None,
            )

        if selection_mode in {"feature_prune", "final_compare"}:
            progress.add_estimated_fit_units(
                _estimate_prune_fit_units(
                    len(selected_recipe.feature_columns),
                    len(eval_dates),
                    max_feature_prune_rounds,
                    feature_candidate_limit,
                ) + len(eval_dates),
                stage="feature_prune",
                message="Pruning individual features",
            )
            feature_recipe, feature_evaluation, feature_prune_log = prune_features(
                panel=panel,
                base_recipe=selected_recipe,
                base_evaluation=selected_evaluation,
                eval_dates=eval_dates,
                topk=topk,
                train_weeks=train_weeks,
                valid_weeks=valid_weeks,
                cache=evaluation_cache,
                runtime_cache=runtime_cache,
                max_rounds=max_feature_prune_rounds,
                candidate_limit=feature_candidate_limit,
                progress_callback=progress.handle_event,
                progress_context={"stage": "feature_prune"},
            )
            feature_recipe = ModelRecipe(
                name="small_stable",
                feature_columns=feature_recipe.feature_columns,
                feature_groups=feature_recipe.feature_groups,
                industry_normalization=feature_recipe.industry_normalization,
                model_params=full_params,
            )
            feature_evaluation = evaluate_recipe(
                panel=panel,
                recipe=feature_recipe,
                eval_dates=eval_dates,
                topk=topk,
                train_weeks=train_weeks,
                valid_weeks=valid_weeks,
                cache=evaluation_cache,
                runtime_cache=runtime_cache,
                progress_callback=progress.handle_event,
                progress_context={"stage": "feature_evaluation"},
            )
            selected_recipe = feature_recipe
            selected_evaluation = feature_evaluation
            evaluations.append(feature_evaluation)
            selected_recipe_payload = build_selected_recipe_payload(
                recipe=selected_recipe,
                promotion_gate_passed=False,
                evaluation_scope=universe_profile,
                excluded_features=resolved_excluded_features,
                calibration_window=calibration_window,
                redundant_features=redundant_feature_names,
            )
            progress.write_partial_artifacts(
                feature_stats=feature_stats,
                corr_marks=corr_marks,
                feature_redundancy=feature_redundancy,
                feature_outlier_audit=feature_outlier_audit,
                evaluations=evaluations,
                group_prune_log=group_prune_log,
                feature_prune_log=feature_prune_log,
                tuning_results=tuning_results,
                comparison=comparison,
                promotion_gate=promotion_gate,
                selected_recipe_payload=selected_recipe_payload,
                emit_slices=emit_slices,
            )
            progress.complete_stage(
                stage="feature_prune",
                stage_label="small_stable_candidate",
                recipe_name=feature_recipe.name,
                evaluation=feature_evaluation,
                recipe_payload=selected_recipe_payload,
                eval_date_count=len(eval_dates),
                tuning_frame=None,
                group_prune_log=group_prune_log,
                feature_prune_log=feature_prune_log,
                comparison=None,
                promotion_gate=None,
            )

        if selection_mode == "final_compare":
            progress.add_estimated_fit_units(
                _estimate_tuning_fit_units(len(eval_dates), coarse_tune_limit, micro_tune_limit) + len(eval_dates),
                stage="final_compare",
                message="Retuning and comparing small_stable against full_postfix_baseline",
            )
            small_params, small_tuning_results, _ = tune_recipe(
                panel=panel,
                recipe=ModelRecipe(
                    name="small_stable",
                    feature_columns=selected_recipe.feature_columns,
                    feature_groups=selected_recipe.feature_groups,
                    industry_normalization=selected_recipe.industry_normalization,
                ),
                eval_dates=eval_dates,
                topk=topk,
                train_weeks=train_weeks,
                valid_weeks=valid_weeks,
                cache=evaluation_cache,
                runtime_cache=runtime_cache,
                coarse_limit=coarse_tune_limit,
                micro_limit=micro_tune_limit,
                progress_callback=progress.handle_event,
                progress_context={"stage": "final_tuning"},
            )
            tuning_results.append(small_tuning_results.assign(recipe="small_stable"))
            selected_recipe = ModelRecipe(
                name="small_stable",
                feature_columns=selected_recipe.feature_columns,
                feature_groups=selected_recipe.feature_groups,
                industry_normalization=selected_recipe.industry_normalization,
                model_params=small_params,
            )
            selected_evaluation = evaluate_recipe(
                panel=panel,
                recipe=selected_recipe,
                eval_dates=eval_dates,
                topk=topk,
                train_weeks=train_weeks,
                valid_weeks=valid_weeks,
                cache=evaluation_cache,
                runtime_cache=runtime_cache,
                progress_callback=progress.handle_event,
                progress_context={"stage": "final_evaluation"},
            )
            evaluations = [full_evaluation, selected_evaluation]
            comparison = compare_recipe_evaluations(selected_evaluation, full_evaluation)
            gate_passed, promotion_gate = passes_promotion_gate(selected_evaluation, full_evaluation)
            promotion_gate["promotion_gate_passed"] = gate_passed

        summary, slice_summary, details, predictions, equity_curve = combine_recipe_artifacts(evaluations)
        if not emit_slices:
            slice_summary = pd.DataFrame()

        selected_recipe_payload = build_selected_recipe_payload(
            recipe=selected_recipe,
            promotion_gate_passed=bool(promotion_gate["promotion_gate_passed"]),
            evaluation_scope=universe_profile,
            excluded_features=resolved_excluded_features,
            calibration_window=calibration_window,
            redundant_features=redundant_feature_names,
        )
        progress.write_partial_artifacts(
            feature_stats=feature_stats,
            corr_marks=corr_marks,
            feature_redundancy=feature_redundancy,
            feature_outlier_audit=feature_outlier_audit,
            evaluations=evaluations,
            group_prune_log=group_prune_log,
            feature_prune_log=feature_prune_log,
            tuning_results=tuning_results,
            comparison=comparison,
            promotion_gate=promotion_gate,
            selected_recipe_payload=selected_recipe_payload,
            emit_slices=emit_slices,
        )
        if selection_mode == "final_compare":
            small_tuning_frame = tuning_results[-1] if tuning_results else pd.DataFrame()
            progress.complete_stage(
                stage="final_compare",
                stage_label="small_stable_vs_full_postfix_baseline",
                recipe_name=selected_recipe.name,
                evaluation=selected_evaluation,
                recipe_payload=selected_recipe_payload,
                eval_date_count=len(eval_dates),
                tuning_frame=small_tuning_frame,
                group_prune_log=group_prune_log,
                feature_prune_log=feature_prune_log,
                comparison=comparison,
                promotion_gate=promotion_gate,
            )
        _write_artifact_bundle(
            output_dir=resolved_output_dir,
            feature_stats=feature_stats,
            corr_marks=corr_marks,
            feature_redundancy=feature_redundancy,
            feature_outlier_audit=feature_outlier_audit,
            summary=summary,
            slice_summary=slice_summary,
            details=details,
            predictions=predictions,
            equity_curve=equity_curve,
            group_prune_log=group_prune_log,
            feature_prune_log=feature_prune_log,
            tuning_results=tuning_results,
            comparison=comparison,
            promotion_gate=promotion_gate,
            selected_recipe_payload=selected_recipe_payload,
            partial=False,
        )
        progress.mark_completed()

        return {
            "panel": panel,
            "output_dir": resolved_output_dir,
            "eval_dates": eval_dates,
            "feature_stats": feature_stats,
            "corr_marks": corr_marks,
            "summary": summary,
            "slice_summary": slice_summary,
            "details": details,
            "predictions": predictions,
            "equity_curve": equity_curve,
            "group_prune_log": group_prune_log,
            "feature_prune_log": feature_prune_log,
            "tuning_results": pd.concat(tuning_results, ignore_index=True) if tuning_results else pd.DataFrame(),
            "selected_recipe": selected_recipe_payload,
            "comparison": comparison,
            "promotion_gate": promotion_gate,
        }
    except Exception as exc:
        progress.mark_failed(exc)
        raise


def load_evaluation_artifacts(output_dir: str | Path) -> dict[str, Any]:
    resolved_output_dir = _resolve_path(output_dir)
    artifacts: dict[str, Any] = {"output_dir": resolved_output_dir}

    for name, filename in ARTIFACT_CSV_FILES.items():
        artifacts[name] = _safe_read_csv(resolved_output_dir / filename)
    for name, filename in ARTIFACT_JSON_FILES.items():
        default = [] if name in {"feature_prune_log", "stage_summary_json"} else {}
        artifacts[name] = _safe_read_json(resolved_output_dir / filename, default)

    artifacts["gate_summary"] = summarize_gate_decision(artifacts)
    return artifacts


def summarize_gate_decision(payload: dict[str, Any]) -> dict[str, Any]:
    comparison_payload = payload.get("comparison", payload)
    promotion_gate = comparison_payload.get("promotion_gate", comparison_payload)
    selected_recipe = payload.get("selected_recipe") or comparison_payload.get("selected_recipe", {})

    if not promotion_gate:
        return {
            "status": "missing",
            "promotion_gate_passed": False,
            "recommendation": "评估产物不完整，无法判断是否可以发布。",
            "blocking_reasons": ["缺少 promotion_gate 信息"],
            "selected_feature_count": len(selected_recipe.get("selected_features", [])),
        }

    blocking_reasons = []
    labels = {
        "rank_ok": "主池 RankIC 与 RankIC_IR 未同时达到基线要求",
        "topk_ok": "主池 TopK 超额收益未达到基线要求",
        "drawdown_ok": "主池最大回撤相对基线恶化超过允许范围",
        "slice_rank_ok": "CSI300 或 CSI500 切片的 RankIC_IR 未达到基线要求",
        "slice_drawdown_ok": "CSI300 或 CSI500 切片的最大回撤恶化超过允许范围",
        "reduction_ok": "特征数未较基线减少至少 25%",
    }
    for key, message in labels.items():
        if not bool(promotion_gate.get(key, False)):
            blocking_reasons.append(message)

    passed = bool(promotion_gate.get("promotion_gate_passed", False))
    recommendation = (
        "满足晋级门槛，可以进入候选发布流程。"
        if passed
        else "保留研究结论，不发布线上候选模型。"
    )
    return {
        "status": "passed" if passed else "blocked",
        "promotion_gate_passed": passed,
        "recommendation": recommendation,
        "blocking_reasons": blocking_reasons,
        "selected_feature_count": len(selected_recipe.get("selected_features", [])),
        "feature_reduction_ratio": promotion_gate.get("feature_reduction_ratio"),
    }


def resolve_feature_date(panel: pd.DataFrame, requested: str | None) -> pd.Timestamp:
    available_dates = sorted(pd.to_datetime(panel["datetime"]).dropna().unique())
    if not available_dates:
        raise ValueError("Feature panel has no datetime rows")
    if requested is None:
        return pd.Timestamp(available_dates[-1])

    target = pd.to_datetime(requested)
    eligible = [dt for dt in available_dates if dt <= target]
    if not eligible:
        raise ValueError(f"No feature rows found at or before requested feature_date={target.date()}")
    return pd.Timestamp(eligible[-1])


def build_segments(panel: pd.DataFrame, feature_date: pd.Timestamp) -> dict[str, tuple[str, str]]:
    label_ready = panel[panel[LABEL_COLUMN].notna()].copy()
    timeline = sorted(pd.to_datetime(label_ready["datetime"]).dropna().unique())
    train_dates = [dt for dt in timeline if dt < feature_date]
    if len(train_dates) < 6:
        raise ValueError("At least 6 historical weekly dates are required before feature_date")

    valid_size = max(1, math.ceil(len(train_dates) * 0.2))
    valid_dates = train_dates[-valid_size:]
    train_only_dates = train_dates[:-valid_size]
    if not train_only_dates:
        raise ValueError("Training window is empty after splitting train/valid segments")

    return {
        "train": (
            str(pd.Timestamp(train_only_dates[0]).date()),
            str(pd.Timestamp(train_only_dates[-1]).date()),
        ),
        "valid": (
            str(pd.Timestamp(valid_dates[0]).date()),
            str(pd.Timestamp(valid_dates[-1]).date()),
        ),
        "test": (
            str(pd.Timestamp(feature_date).date()),
            str(pd.Timestamp(feature_date).date()),
        ),
    }


def build_score_frame(prediction: pd.Series) -> pd.DataFrame:
    score_frame = prediction.reset_index()
    score_frame = score_frame.rename(columns={"instrument": "code", 0: "qlib_score"})
    if "qlib_score" not in score_frame.columns:
        last_col = score_frame.columns[-1]
        score_frame = score_frame.rename(columns={last_col: "qlib_score"})

    score_frame = score_frame.sort_values("qlib_score", ascending=False).reset_index(drop=True)
    score_frame["qlib_rank"] = score_frame.index + 1
    score_frame["pred_return_4w"] = score_frame["qlib_score"]
    return score_frame[["code", "qlib_score", "qlib_rank", "pred_return_4w"]]


def load_feature_config(feature_config: str | Path | dict[str, Any] | None) -> dict[str, Any]:
    if feature_config is None:
        return {}
    if isinstance(feature_config, dict):
        payload = dict(feature_config)
        return dict(payload.get("feature_spec") or payload.get("selected_recipe") or payload)

    config_path = _resolve_path(feature_config)
    if not config_path.exists():
        raise FileNotFoundError(f"Feature config not found: {config_path}")
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    return dict(payload.get("feature_spec") or payload.get("selected_recipe") or payload)


def train_and_publish_weekly_snapshot(
    panel_path: str | Path,
    model_id: str,
    feature_date: str | None = None,
    feature_config: str | Path | dict[str, Any] | None = None,
    excluded_features: Sequence[str] | str | None = None,
    experiment_name: str = "valueinvesting_weekly_qlib",
    num_boost_round: int = 200,
    early_stopping_rounds: int = 50,
) -> dict[str, Any]:
    init_qlib_runtime(exp_name=experiment_name)
    with suppress_external_output():
        from qlib.contrib.model.gbdt import LGBModel
        from qlib.data.dataset import DatasetH
        from qlib.data.dataset.handler import DataHandlerLP
        from qlib.data.dataset.loader import StaticDataLoader
        from qlib.workflow import R

    panel = load_panel_dataframe(_resolve_path(panel_path))
    if panel.empty:
        raise ValueError("Feature panel is empty, cannot train/publish scores")

    config = load_feature_config(feature_config)
    required_enrichment_scope = resolve_panel_enrichment_scope(
        enrichment_scope=(config.get("panel_requirements") or {}).get("enrichment_scope", "research_full")
    )
    panel = ensure_panel_enrichment(panel, required_enrichment_scope)
    resolved_excluded_features = normalize_feature_name_list(
        (
            *normalize_feature_name_list(config.get("excluded_features")),
            *normalize_feature_name_list(excluded_features),
        )
    )
    selected_features = resolve_feature_columns(
        config.get("selected_features"),
        excluded_features=resolved_excluded_features,
        default_features=FULL_POSTFIX_BASELINE_FEATURE_COLUMNS,
    )
    if not selected_features:
        raise ValueError("No feature columns remain after applying excluded_features")
    selected_feature_groups = groups_for_features(selected_features)
    industry_normalization = str(config.get("industry_normalization") or "none")
    tuned_params = dict(config.get("tuned_params") or {})

    resolved_feature_date = resolve_feature_date(panel, feature_date)
    training_panel = panel[(panel["datetime"] <= resolved_feature_date)].copy()
    training_panel = apply_industry_normalization(
        training_panel,
        feature_columns=selected_features,
        method=industry_normalization,
    )
    segments = build_segments(training_panel, feature_date=resolved_feature_date)
    train_start, train_end = segments["train"]
    train_panel = training_panel.loc[
        pd.to_datetime(training_panel["datetime"]).between(pd.Timestamp(train_start), pd.Timestamp(train_end))
    ].copy()
    fill_values = compute_feature_fill_values(train_panel, feature_columns=selected_features)

    qlib_frame, feature_columns = build_training_frame(
        training_panel,
        feature_columns=selected_features,
        label_column=LABEL_COLUMN,
        fill_values=fill_values,
    )

    loader = StaticDataLoader(qlib_frame)
    handler = DataHandlerLP(data_loader=loader, infer_processors=[], learn_processors=[])
    dataset = DatasetH(handler=handler, segments=segments)
    model_params = {
        "loss": "mse",
        "num_boost_round": tuned_params.get("num_boost_round", num_boost_round),
        "early_stopping_rounds": tuned_params.get("early_stopping_rounds", early_stopping_rounds),
    }
    passthrough_params = {
        key: value
        for key, value in tuned_params.items()
        if key not in {"num_boost_round", "early_stopping_rounds"}
    }
    model = LGBModel(**model_params, **passthrough_params)

    with R.start(experiment_name=experiment_name):
        with suppress_external_output():
            model.fit(dataset, verbose_eval=False)
            prediction = model.predict(dataset, segment="test")

    score_frame = build_score_frame(prediction)
    artifacts_root = get_qlib_artifacts_dir()
    model_dir = artifacts_root / model_id
    model_dir.mkdir(parents=True, exist_ok=True)
    model_path = model_dir / "lgb_model.txt"
    model.model.save_model(str(model_path))

    resolved_feature_date_str = str(pd.to_datetime(resolved_feature_date).date())
    snapshot_path = publish_score_snapshot(
        score_frame=score_frame,
        model_id=model_id,
        feature_date=resolved_feature_date_str,
        extra_manifest={
            "label_column": LABEL_COLUMN,
            "feature_columns": feature_columns,
            "selected_feature_groups": list(selected_feature_groups),
            "industry_normalization": industry_normalization,
            "excluded_features": list(resolved_excluded_features),
            "tuned_params": {**model_params, **passthrough_params},
            "evaluation_scope": config.get("evaluation_scope"),
            "promotion_gate_passed": bool(config.get("promotion_gate_passed", False)),
            "segments": segments,
            "model_path": model_path.name,
        },
    )
    portfolio_targets = build_portfolio_targets(score_frame, model_id=model_id, feature_date=resolved_feature_date_str)
    portfolio_targets_path = publish_portfolio_targets(portfolio_targets, model_id=model_id)
    return {
        "published": True,
        "snapshot_path": snapshot_path,
        "model_dir": model_dir,
        "model_path": model_path,
        "model_id": model_id,
        "feature_date": resolved_feature_date_str,
        "portfolio_targets_path": portfolio_targets_path,
    }


def publish_from_selected_recipe(
    panel_path: str | Path,
    feature_config_path: str | Path,
    model_id: str | None = None,
    feature_date: str | None = None,
    excluded_features: Sequence[str] | str | None = None,
    experiment_name: str = "valueinvesting_weekly_qlib",
    num_boost_round: int = 200,
    early_stopping_rounds: int = 50,
    run_publish: bool = False,
) -> dict[str, Any]:
    config = load_feature_config(feature_config_path)
    resolved_model_id = model_id or str(config.get("model_id_suggestion") or "weekly-lgbm-selected-recipe")

    if not run_publish:
        return {
            "published": False,
            "model_id": resolved_model_id,
            "reason": "run_publish_disabled",
            "promotion_gate_passed": bool(config.get("promotion_gate_passed", False)),
        }

    if not bool(config.get("promotion_gate_passed", False)):
        raise RuntimeError("Selected recipe did not pass promotion gate; refusing to publish.")

    return train_and_publish_weekly_snapshot(
        panel_path=panel_path,
        model_id=resolved_model_id,
        feature_date=feature_date,
        feature_config=config,
        excluded_features=excluded_features,
        experiment_name=experiment_name,
        num_boost_round=num_boost_round,
        early_stopping_rounds=early_stopping_rounds,
    )


def _resolve_native_recipe_names(output_dir: Path, recipe_names: Sequence[str] | None) -> list[str]:
    if recipe_names is not None:
        return [str(name) for name in recipe_names]

    summary_payload = _safe_read_json(output_dir / "native_workflow_summary.json", {})
    executed = summary_payload.get("recipe_registry", {}).get("executed_recipes", [])
    if isinstance(executed, list) and executed:
        return [str(name) for name in executed]

    return sorted(path.name for path in output_dir.iterdir() if path.is_dir()) if output_dir.exists() else []


def load_native_workflow_artifacts(
    output_dir: str | Path,
    *,
    recipe_names: Sequence[str] | None = None,
) -> dict[str, Any]:
    from qlib_research.core.qlib_native_workflow import (
        build_annual_return_heatmap_frame,
        build_monthly_return_heatmap_frame,
    )

    resolved_output_dir = _resolve_path(output_dir)
    summary_payload = _safe_read_json(resolved_output_dir / "native_workflow_summary.json", {})
    selected_recipe_names = _resolve_native_recipe_names(resolved_output_dir, recipe_names)

    recipe_frames: dict[str, dict[str, Any]] = {}
    overview_rows: list[dict[str, Any]] = []

    def _safe_last_numeric(frame: pd.DataFrame, column: str) -> float | None:
        if frame.empty or column not in frame.columns:
            return None
        series = pd.to_numeric(frame[column], errors="coerce").dropna()
        if series.empty:
            return None
        return float(series.iloc[-1])

    def _safe_max_int(frame: pd.DataFrame, column: str) -> int | None:
        if frame.empty or column not in frame.columns:
            return None
        series = pd.to_numeric(frame[column], errors="coerce").dropna()
        if series.empty:
            return None
        return int(series.max())

    def _summarize_native_report(frame: pd.DataFrame, account: float = 1_000_000.0) -> dict[str, float | None]:
        if frame.empty:
            return {
                "net_total_return": None,
                "benchmark_total_return": None,
                "excess_total_return": None,
                "max_drawdown": None,
                "excess_drawdown": None,
                "cost_drag": None,
                "turnover_mean": None,
            }
        net_series = _numeric_series(frame, "net_value")
        benchmark_series = _numeric_series(frame, "benchmark_value")
        cost_series = _numeric_series(frame, "cost")
        turnover_series = _numeric_series(frame, "turnover")
        max_drawdown = _numeric_series(frame, "relative_drawdown")
        excess_drawdown = _numeric_series(frame, "benchmark_excess_drawdown")
        net_total_return = float(net_series.iloc[-1] / float(account) - 1.0) if not net_series.empty else None
        benchmark_total_return = float(benchmark_series.iloc[-1] / float(account) - 1.0) if not benchmark_series.empty else None
        return {
            "net_total_return": net_total_return,
            "benchmark_total_return": benchmark_total_return,
            "excess_total_return": (
                net_total_return - benchmark_total_return
                if net_total_return is not None and benchmark_total_return is not None
                else None
            ),
            "max_drawdown": float(max_drawdown.min()) if not max_drawdown.empty else None,
            "excess_drawdown": float(excess_drawdown.min()) if not excess_drawdown.empty else None,
            "cost_drag": float(cost_series.sum()) if not cost_series.empty else None,
            "turnover_mean": float(turnover_series.mean()) if not turnover_series.empty else None,
        }

    for recipe_name in selected_recipe_names:
        recipe_dir = resolved_output_dir / recipe_name
        frames = {
            name: _safe_read_csv(recipe_dir / filename)
            for name, filename in NATIVE_RECIPE_CSV_FILES.items()
        }
        for bundle_name in ("rolling", "walk_forward"):
            native_report = frames[f"{bundle_name}_native_report"]
            if native_report.empty:
                continue
            frames[f"{bundle_name}_native_monthly_return_heatmap"] = build_monthly_return_heatmap_frame(native_report)
            frames[f"{bundle_name}_native_annual_return_heatmap"] = build_annual_return_heatmap_frame(native_report)
        manifest = _safe_read_json(recipe_dir / "native_workflow_manifest.json", {})
        recipe_frames[recipe_name] = {
            **frames,
            "manifest": manifest,
            "path": recipe_dir,
        }

        feature_prefilter = frames["feature_prefilter"]
        signal_diagnostics = frames["signal_diagnostics"]
        portfolio_diagnostics = frames["portfolio_diagnostics"]
        rolling_summary = frames["rolling_summary"]
        walk_forward_summary = frames["walk_forward_summary"]
        rolling_details = frames["rolling_details"]
        walk_forward_details = frames["walk_forward_details"]
        rolling_native_report = frames["rolling_native_report"]
        walk_forward_native_report = frames["walk_forward_native_report"]
        latest_score_frame = frames["latest_score_frame"]
        rolling_summary_row = _frame_first_row(rolling_summary)
        walk_forward_summary_row = _frame_first_row(walk_forward_summary)
        rolling_eval_range = _summarize_date_range(rolling_details, "feature_date", "datetime")
        walk_forward_eval_range = _summarize_date_range(walk_forward_details, "feature_date", "datetime")
        rolling_report_range = _summarize_date_range(rolling_native_report, "datetime")
        walk_forward_report_range = _summarize_date_range(walk_forward_native_report, "datetime")
        rolling_native_row = _summarize_native_report(rolling_native_report)
        walk_forward_native_row = _summarize_native_report(walk_forward_native_report)
        overview_rows.append(
            {
                "recipe": recipe_name,
                "path": str(recipe_dir),
                "requested_feature_count": _safe_max_int(feature_prefilter, "requested_feature_count"),
                "used_feature_count": _safe_max_int(feature_prefilter, "selected_feature_count")
                if _safe_max_int(feature_prefilter, "selected_feature_count") is not None
                else len(manifest.get("used_feature_columns", [])),
                "rolling_rank_ic_ir": rolling_summary_row.get("rank_ic_ir"),
                "rolling_topk_mean_excess_return_4w": rolling_summary_row.get("topk_mean_excess_return_4w"),
                "rolling_eval_date_count": rolling_eval_range["count"],
                "rolling_eval_date_start": rolling_eval_range["start"],
                "rolling_eval_date_end": rolling_eval_range["end"],
                "rolling_report_date_start": rolling_report_range["start"],
                "rolling_report_date_end": rolling_report_range["end"],
                "walk_forward_rank_ic_ir": walk_forward_summary_row.get("rank_ic_ir"),
                "walk_forward_topk_mean_excess_return_4w": walk_forward_summary_row.get("topk_mean_excess_return_4w"),
                "walk_forward_eval_date_count": walk_forward_eval_range["count"],
                "walk_forward_eval_date_start": walk_forward_eval_range["start"],
                "walk_forward_eval_date_end": walk_forward_eval_range["end"],
                "walk_forward_report_date_start": walk_forward_report_range["start"],
                "walk_forward_report_date_end": walk_forward_report_range["end"],
                "rolling_net_total_return": rolling_native_row["net_total_return"],
                "rolling_benchmark_total_return": rolling_native_row["benchmark_total_return"],
                "rolling_excess_total_return": rolling_native_row["excess_total_return"],
                "rolling_max_drawdown": rolling_native_row["max_drawdown"],
                "rolling_excess_drawdown": rolling_native_row["excess_drawdown"],
                "rolling_cost_drag": rolling_native_row["cost_drag"],
                "rolling_turnover_mean": rolling_native_row["turnover_mean"],
                "walk_forward_net_total_return": walk_forward_native_row["net_total_return"],
                "walk_forward_benchmark_total_return": walk_forward_native_row["benchmark_total_return"],
                "walk_forward_excess_total_return": walk_forward_native_row["excess_total_return"],
                "walk_forward_max_drawdown": walk_forward_native_row["max_drawdown"],
                "walk_forward_excess_drawdown": walk_forward_native_row["excess_drawdown"],
                "walk_forward_cost_drag": walk_forward_native_row["cost_drag"],
                "walk_forward_turnover_mean": walk_forward_native_row["turnover_mean"],
                "latest_score_dispersion": _safe_last_numeric(signal_diagnostics, "score_dispersion"),
                "latest_top10_unique_score_count": _safe_last_numeric(signal_diagnostics, "topk_unique_score_count"),
                "latest_actual_hold_count": _safe_last_numeric(portfolio_diagnostics, "actual_hold_count"),
                "latest_blocked_sell_count": _safe_last_numeric(portfolio_diagnostics, "blocked_sell_count"),
                "latest_score_rows": int(len(latest_score_frame)),
            }
        )

    promotion_gate_rows = []
    promotion_gate_payload = summary_payload.get("promotion_gate", {})
    if isinstance(promotion_gate_payload, dict):
        for recipe_name, gate in promotion_gate_payload.items():
            if isinstance(gate, dict):
                promotion_gate_rows.append({"recipe": recipe_name, **gate})

    return {
        "output_dir": resolved_output_dir,
        "summary": summary_payload,
        "recipe_names": selected_recipe_names,
        "recipes": recipe_frames,
        "recipe_overview": pd.DataFrame(overview_rows),
        "promotion_gate": pd.DataFrame(promotion_gate_rows),
    }


def run_native_notebook_workflow(
    *,
    config_overrides: dict[str, Any] | None = None,
    recipe_names: Sequence[str] | None = None,
    run_workflow: bool = False,
) -> dict[str, Any]:
    from qlib_research.core.qlib_native_workflow import NativeWorkflowConfig, run_native_research_workflow

    resolved_overrides = _normalize_native_workflow_overrides(config_overrides)
    config = NativeWorkflowConfig(**resolved_overrides)
    resolved_output_dir = _resolve_path(config.output_dir)
    workflow_summary = _safe_read_json(
        resolved_output_dir / "native_workflow_summary.json",
        {
            "schema_version": None,
            "config": sanitize_for_json(resolved_overrides),
            "recipe_registry": {"executed_recipes": list(recipe_names or [])},
            "promotion_gate": {},
            "promotion_gate_summary": {},
            "overview_lookup": {},
            "output_dir": str(resolved_output_dir),
        },
    )
    if run_workflow:
        result = run_native_research_workflow(config, recipe_names=recipe_names)
        workflow_summary = {
            "schema_version": result.get("schema_version"),
            "config": result["config"],
            "recipe_registry": result["recipe_registry"],
            "promotion_gate": result["promotion_gate"],
            "promotion_gate_summary": result.get("promotion_gate_summary", result["promotion_gate"]),
            "overview_lookup": result.get("overview_lookup", {}),
            "output_dir": result["output_dir"],
        }

    artifact_view = load_native_workflow_artifacts(resolved_output_dir, recipe_names=recipe_names)
    return {
        "config": config,
        "cli_command": build_native_workflow_cli_command(config_overrides=resolved_overrides, recipe_names=recipe_names),
        "ran_workflow": run_workflow,
        "output_dir": resolved_output_dir,
        "workflow_summary": workflow_summary,
        "artifacts": artifact_view,
    }
