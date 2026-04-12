"""Artifact-backed services for the QlibResearch workbench API."""

from __future__ import annotations

from datetime import date, datetime
import json
import math
from pathlib import Path
import shlex
import subprocess
import sys
from typing import Any, Iterable
from uuid import uuid4

import numpy as np
import pandas as pd

from qlib_research.app.contracts import (
    ArtifactRef,
    CompareItemRef,
    CompareItemResult,
    CompareResponse,
    DataTablePayload,
    DiagnosticNode,
    EvidenceItem,
    ExportPanelTaskRequest,
    PanelDetail,
    PanelSummary,
    RecipeDetail,
    RecipeSummary,
    ResearchTaskSummary,
    RunDetail,
    RunListItem,
    RunNativeWorkflowTaskRequest,
    RunQuickSummary,
    TaskLogResponse,
)
from qlib_research.config import get_project_root, get_qlib_artifacts_dir
from qlib_research.core.notebook_workflow import (
    build_native_workflow_cli_command,
    load_native_workflow_artifacts,
    sanitize_for_json,
    summarize_panel,
)
from qlib_research.core.qlib_pipeline import load_panel_dataframe
from qlib_research.core.weekly_feature_panel import load_feature_panel_enrichment_scope

PROJECT_ROOT = get_project_root()
ARTIFACTS_ROOT = get_qlib_artifacts_dir()
NATIVE_WORKFLOW_ROOT = ARTIFACTS_ROOT / "native_workflow"
PANELS_ROOT = ARTIFACTS_ROOT / "panels"
TASKS_ROOT = ARTIFACTS_ROOT / "app_tasks"

REQUIRED_RECIPE_ARTIFACTS = {
    "native_workflow_manifest.json",
    "latest_score_frame.csv",
    "feature_prefilter.csv",
    "signal_diagnostics.csv",
    "portfolio_diagnostics.csv",
    "execution_diff_summary.csv",
    "slice_regime_summary.csv",
    "rolling_summary.csv",
    "rolling_details.csv",
    "rolling_feature_importance.csv",
    "rolling_native_report.csv",
    "rolling_native_benchmark.csv",
    "rolling_native_monthly_return_heatmap.csv",
    "walk_forward_summary.csv",
    "walk_forward_details.csv",
    "walk_forward_feature_importance.csv",
    "walk_forward_native_report.csv",
    "walk_forward_native_benchmark.csv",
    "walk_forward_native_monthly_return_heatmap.csv",
    "portfolio_targets.csv",
}

OPTIONAL_RECIPE_CSVS = {
    "feature_corr_candidates.csv",
    "rolling_predictions.csv",
    "walk_forward_predictions.csv",
    "rolling_native_annual_return_heatmap.csv",
    "walk_forward_native_annual_return_heatmap.csv",
}


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def _safe_read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _normalize_value(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if isinstance(value, np.ndarray):
        return [_normalize_value(item) for item in value.tolist()]
    if isinstance(value, pd.Series):
        return [_normalize_value(item) for item in value.tolist()]
    if isinstance(value, (list, tuple, set)):
        return [_normalize_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _normalize_value(item) for key, item in value.items()}
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if pd.isna(value):
        return None
    return value


def _frame_to_payload(frame: pd.DataFrame, *, sort_by: str | None = None, ascending: bool = False) -> DataTablePayload:
    if frame.empty:
        return DataTablePayload()
    result = frame.copy()
    if sort_by and sort_by in result.columns:
        result = result.sort_values(sort_by, ascending=ascending)
    rows = [{column: _normalize_value(value) for column, value in row.items()} for row in result.to_dict(orient="records")]
    return DataTablePayload(columns=[str(column) for column in result.columns], rows=rows)


def _format_metric(value: Any) -> str:
    value = _normalize_value(value)
    if value is None:
        return "缺失"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def _resolve_artifact_path(raw_path: str | None, *, base_dir: Path = PROJECT_ROOT) -> Path | None:
    if not raw_path:
        return None
    candidate = Path(raw_path).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    return (base_dir / candidate).resolve()


def _path_updated_at(path: Path) -> str | None:
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime).astimezone().isoformat()


def _artifact_ref(path: Path, *, name: str | None = None) -> ArtifactRef:
    exists = path.exists()
    return ArtifactRef(
        name=name or path.name,
        path=str(path),
        exists=exists,
        size_bytes=path.stat().st_size if exists else None,
        updated_at=_path_updated_at(path),
    )


def _list_run_dirs() -> list[Path]:
    if not NATIVE_WORKFLOW_ROOT.exists():
        return []
    return sorted(
        [path for path in NATIVE_WORKFLOW_ROOT.iterdir() if path.is_dir()],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )


def _list_panel_files() -> list[Path]:
    if not PANELS_ROOT.exists():
        return []
    return sorted(
        [path for path in PANELS_ROOT.iterdir() if path.is_file() and path.suffix.lower() in {".csv", ".parquet", ".pq"}],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )


def _list_task_dirs() -> list[Path]:
    if not TASKS_ROOT.exists():
        return []
    return sorted(
        [path for path in TASKS_ROOT.iterdir() if path.is_dir()],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )


def _recipe_inventory(recipe_dir: Path, *, prefix: str | None = None) -> list[ArtifactRef]:
    files = []
    for filename in sorted(REQUIRED_RECIPE_ARTIFACTS):
        display_name = f"{prefix}/{filename}" if prefix else filename
        files.append(_artifact_ref(recipe_dir / filename, name=display_name))
    return files


def _run_inventory(run_dir: Path, recipe_names: Iterable[str]) -> list[ArtifactRef]:
    inventory = [_artifact_ref(run_dir / "native_workflow_summary.json")]
    for recipe_name in recipe_names:
        inventory.extend(_recipe_inventory(run_dir / recipe_name, prefix=recipe_name))
    return inventory


def _artifact_status(inventory: list[ArtifactRef]) -> tuple[str, int, int, list[str]]:
    total = len(inventory)
    ready = sum(1 for item in inventory if item.exists)
    missing = [item.name if "/" not in item.name else item.path for item in inventory if not item.exists]
    if ready == 0:
        return "missing", ready, total, missing
    if ready == total:
        return "ready", ready, total, missing
    return "partial", ready, total, missing


def _resolve_run_recipe_names(run_dir: Path, summary_payload: dict[str, Any]) -> list[str]:
    executed = summary_payload.get("recipe_registry", {}).get("executed_recipes", [])
    if isinstance(executed, list) and executed:
        return [str(name) for name in executed]
    return sorted(path.name for path in run_dir.iterdir() if path.is_dir()) if run_dir.exists() else []


def _safe_native_report_return(frame: pd.DataFrame, account: float = 1_000_000.0) -> float | None:
    if frame.empty or "net_value" not in frame.columns:
        return None
    series = pd.to_numeric(frame["net_value"], errors="coerce").dropna()
    if series.empty:
        return None
    return float(series.iloc[-1] / float(account) - 1.0)


def _build_recipe_overview_row(recipe_name: str, recipe_frames: dict[str, Any]) -> dict[str, Any]:
    rolling_summary = recipe_frames.get("rolling_summary", pd.DataFrame())
    walk_summary = recipe_frames.get("walk_forward_summary", pd.DataFrame())
    rolling_row = _summary_row(rolling_summary)
    walk_row = _summary_row(walk_summary)
    return {
        "recipe": recipe_name,
        "used_feature_count": rolling_row.get("used_feature_count") or walk_row.get("used_feature_count"),
        "rolling_rank_ic_ir": rolling_row.get("rank_ic_ir"),
        "rolling_topk_mean_excess_return_4w": rolling_row.get("topk_mean_excess_return_4w"),
        "rolling_net_total_return": _safe_native_report_return(recipe_frames.get("rolling_native_report", pd.DataFrame())),
        "walk_forward_rank_ic_ir": walk_row.get("rank_ic_ir"),
        "walk_forward_topk_mean_excess_return_4w": walk_row.get("topk_mean_excess_return_4w"),
        "walk_forward_net_total_return": _safe_native_report_return(recipe_frames.get("walk_forward_native_report", pd.DataFrame())),
    }


def _scan_native_workflow_artifacts(output_dir: Path) -> dict[str, Any]:
    summary_payload = _safe_read_json(output_dir / "native_workflow_summary.json", {})
    recipe_names = _resolve_run_recipe_names(output_dir, summary_payload)
    recipes: dict[str, dict[str, Any]] = {}
    overview_rows: list[dict[str, Any]] = []

    for recipe_name in recipe_names:
        recipe_dir = output_dir / recipe_name
        if not recipe_dir.is_dir():
            continue
        recipe_frames: dict[str, Any] = {
            "manifest": _safe_read_json(recipe_dir / "native_workflow_manifest.json", {}),
        }
        csv_names = REQUIRED_RECIPE_ARTIFACTS.union(OPTIONAL_RECIPE_CSVS)
        for csv_name in csv_names:
            if not csv_name.endswith(".csv"):
                continue
            csv_path = recipe_dir / csv_name
            if csv_path.exists():
                recipe_frames[csv_path.stem] = _safe_read_csv(csv_path)
        recipes[recipe_name] = recipe_frames
        overview_rows.append(_build_recipe_overview_row(recipe_name, recipe_frames))

    return {
        "summary_payload": summary_payload,
        "recipe_names": recipe_names,
        "recipe_overview": pd.DataFrame(overview_rows),
        "recipes": recipes,
    }


def _load_native_workflow_artifacts_safe(output_dir: Path) -> dict[str, Any]:
    try:
        return load_native_workflow_artifacts(output_dir)
    except Exception as exc:
        fallback_messages = (
            "pyqlib is not installed",
            "cannot import name 'build_annual_return_heatmap_frame'",
            "cannot import name 'build_monthly_return_heatmap_frame'",
            "No module named 'qlib'",
        )
        if not isinstance(exc, (ImportError, ModuleNotFoundError, RuntimeError)):
            raise
        if not any(message in str(exc) for message in fallback_messages):
            raise
    return _scan_native_workflow_artifacts(output_dir)


def _resolve_recipe_registry(summary_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    registry = summary_payload.get("recipe_registry", {})
    result: dict[str, dict[str, Any]] = {}
    baseline_recipe = registry.get("baseline_recipe")
    if isinstance(baseline_recipe, dict) and baseline_recipe.get("name"):
        result[str(baseline_recipe["name"])] = baseline_recipe
    candidate_recipes = registry.get("candidate_recipes", {})
    if isinstance(candidate_recipes, dict):
        for recipe_name, recipe_config in candidate_recipes.items():
            if isinstance(recipe_config, dict):
                result[str(recipe_name)] = recipe_config
    return result


def _get_recipe_config(summary_payload: dict[str, Any], recipe_name: str) -> dict[str, Any]:
    return _resolve_recipe_registry(summary_payload).get(recipe_name, {})


def _summary_row(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {}
    return {str(key): _normalize_value(value) for key, value in frame.iloc[0].to_dict().items()}


def _last_row(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {}
    return {str(key): _normalize_value(value) for key, value in frame.iloc[-1].to_dict().items()}


def _select_baseline_recipe(recipe_names: list[str]) -> str | None:
    if not recipe_names:
        return None
    if "baseline" in recipe_names:
        return "baseline"
    return recipe_names[0]


def _health_status(ok: bool, warning: bool = False, missing: bool = False) -> str:
    if missing:
        return "missing"
    if ok:
        return "healthy"
    if warning:
        return "warning"
    return "danger"


def _build_recipe_nodes(
    recipe_name: str,
    recipe_frames: dict[str, Any],
    recipe_config: dict[str, Any],
    panel_summary: dict[str, Any] | None,
) -> list[DiagnosticNode]:
    signal_diag = recipe_frames.get("signal_diagnostics", pd.DataFrame())
    portfolio_diag = recipe_frames.get("portfolio_diagnostics", pd.DataFrame())
    rolling_summary = recipe_frames.get("rolling_summary", pd.DataFrame())
    walk_summary = recipe_frames.get("walk_forward_summary", pd.DataFrame())
    execution_gap = recipe_frames.get("execution_diff_summary", pd.DataFrame())
    slice_summary = recipe_frames.get("slice_regime_summary", pd.DataFrame())
    snapshot = recipe_frames.get("latest_score_frame", pd.DataFrame())
    feature_prefilter = recipe_frames.get("feature_prefilter", pd.DataFrame())

    latest_signal = _last_row(signal_diag)
    latest_portfolio = _last_row(portfolio_diag)
    rolling_row = _summary_row(rolling_summary)
    walk_row = _summary_row(walk_summary)
    exec_rows = execution_gap.to_dict(orient="records") if not execution_gap.empty else []

    keep_ratio = None
    if not feature_prefilter.empty and "keep" in feature_prefilter.columns:
        keep_ratio = float(pd.to_numeric(feature_prefilter["keep"], errors="coerce").fillna(0).mean())
    score_dispersion = latest_signal.get("score_dispersion")
    unique_ratio = latest_signal.get("topk_unique_score_ratio")
    overlap_prev = latest_signal.get("topk_overlap_prev")
    blocked_sell = latest_portfolio.get("blocked_sell_count")
    actual_hold = latest_portfolio.get("actual_hold_count")
    target_hold = latest_portfolio.get("target_hold_count")
    rolling_rank_ic_ir = rolling_row.get("rank_ic_ir")
    rolling_topk = rolling_row.get("topk_mean_excess_return_4w")
    walk_rank_ic_ir = walk_row.get("rank_ic_ir")
    walk_topk = walk_row.get("topk_mean_excess_return_4w")
    execution_delta = None
    if exec_rows:
        delta_values = [
            abs(float(row.get("native_minus_validation_return")))
            for row in exec_rows
            if row.get("native_minus_validation_return") is not None and not pd.isna(row.get("native_minus_validation_return"))
        ]
        execution_delta = max(delta_values) if delta_values else None

    slice_positive_ratio = None
    if not slice_summary.empty and "mean_excess_return_4w" in slice_summary.columns:
        valid = pd.to_numeric(slice_summary["mean_excess_return_4w"], errors="coerce").dropna()
        if not valid.empty:
            slice_positive_ratio = float((valid > 0).mean())

    nodes = [
        DiagnosticNode(
            key="panel_input",
            status="healthy" if panel_summary else "missing",
            headline="输入 panel 已解析" if panel_summary else "未找到 panel 输入信息",
            evidence=[
                EvidenceItem(label="panel", value=panel_summary.get("path") if panel_summary else None, tone="info"),
                EvidenceItem(label="样本行数", value=panel_summary.get("rows") if panel_summary else None, tone="neutral"),
                EvidenceItem(label="证券数", value=panel_summary.get("instrument_count") if panel_summary else None, tone="neutral"),
            ],
            interpretation=(
                "工作流能定位到 panel，后续诊断可结合样本规模与日期范围解释。"
                if panel_summary
                else "当前 run 没有可用的 panel 概要，建议先确认 config.panel_path 是否仍然可访问。"
            ),
            next_action="优先检查 panel 路径与导出时间，再解读后续模型指标。",
        ),
        DiagnosticNode(
            key="label_definition",
            status="healthy",
            headline=f"{recipe_name} 使用 {recipe_config.get('label_recipe') or '默认标签'} / {recipe_config.get('signal_objective') or '默认目标'}",
            evidence=[
                EvidenceItem(label="label_recipe", value=recipe_config.get("label_recipe"), tone="info"),
                EvidenceItem(label="signal_objective", value=recipe_config.get("signal_objective"), tone="info"),
                EvidenceItem(label="feature_groups", value=len(recipe_config.get("feature_groups", [])), tone="neutral"),
            ],
            interpretation="标签定义与优化目标决定了后面 rank IC、收益兑现和快照分布的解读口径。",
            next_action="对比 recipe 时，先确认 label 和 objective 是否同口径，再比较收益表现。",
        ),
        DiagnosticNode(
            key="feature_prefilter",
            status=_health_status(bool(keep_ratio is not None and keep_ratio >= 0.6), warning=bool(keep_ratio is not None and keep_ratio >= 0.35), missing=keep_ratio is None),
            headline="特征预过滤结果可用" if keep_ratio is not None else "缺少 feature prefilter 产物",
            evidence=[
                EvidenceItem(label="候选特征数", value=int(len(feature_prefilter)) if not feature_prefilter.empty else None, tone="neutral"),
                EvidenceItem(label="保留比例", value=_format_metric(keep_ratio), tone="info" if keep_ratio is not None and keep_ratio >= 0.6 else "warning"),
            ],
            interpretation=(
                "保留比例过低通常意味着原始特征缺失、近常数或横截面波动不足，后续指标会更不稳定。"
                if keep_ratio is not None
                else "当前无法判断特征筛选质量。"
            ),
            next_action="如果保留比例偏低，优先回看 feature_prefilter 和 panel 丰富度，而不是先调模型参数。",
        ),
        DiagnosticNode(
            key="normalization",
            status="healthy" if recipe_config.get("industry_normalization") else "info",
            headline=f"标准化策略：{recipe_config.get('industry_normalization') or 'raw'}",
            evidence=[
                EvidenceItem(label="industry_normalization", value=recipe_config.get("industry_normalization") or "none", tone="info"),
                EvidenceItem(label="显式 included_features", value=len(recipe_config.get("included_features", [])), tone="neutral"),
            ],
            interpretation="当前实现以规则化处理为主，标准化主要影响横截面可比性，不直接等同于自动特征选择。",
            next_action="比较 recipe 时，把标准化差异视为研究口径差异，而不是纯参数差异。",
        ),
        DiagnosticNode(
            key="score_quality",
            status=_health_status(
                bool(score_dispersion is not None and float(score_dispersion) > 0.05 and unique_ratio is not None and float(unique_ratio) >= 0.95),
                warning=bool(score_dispersion is not None and float(score_dispersion) > 0.03),
                missing=not latest_signal,
            ),
            headline="分数粒度健康" if latest_signal else "缺少 signal diagnostics",
            evidence=[
                EvidenceItem(label="score_dispersion", value=_format_metric(score_dispersion), tone="success" if score_dispersion is not None and float(score_dispersion) > 0.05 else "warning"),
                EvidenceItem(label="topk_unique_score_ratio", value=_format_metric(unique_ratio), tone="success" if unique_ratio is not None and float(unique_ratio) >= 0.95 else "warning"),
                EvidenceItem(label="topk_overlap_prev", value=_format_metric(overlap_prev), tone="neutral"),
            ],
            interpretation=(
                "分数离散度和 TopK 唯一性较好时，更容易把排序优势兑现成可交易选择。"
                if latest_signal
                else "当前无法判断分数是否足够区分。"
            ),
            next_action="如果粒度不足，先检查标签与特征，而不是先看净值曲线。",
        ),
        DiagnosticNode(
            key="rolling_backtest",
            status=_health_status(
                bool(rolling_rank_ic_ir is not None and float(rolling_rank_ic_ir) > 0.2 and rolling_topk is not None and float(rolling_topk) > 0),
                warning=bool(rolling_rank_ic_ir is not None and float(rolling_rank_ic_ir) > 0.1),
                missing=not rolling_row,
            ),
            headline="Rolling 回测表现可接受" if rolling_row else "缺少 rolling summary",
            evidence=[
                EvidenceItem(label="rank_ic_ir", value=_format_metric(rolling_rank_ic_ir), tone="success" if rolling_rank_ic_ir is not None and float(rolling_rank_ic_ir) > 0.2 else "warning"),
                EvidenceItem(label="topk_mean_excess_return_4w", value=_format_metric(rolling_topk), tone="success" if rolling_topk is not None and float(rolling_topk) > 0 else "danger"),
            ],
            interpretation=(
                "rank IC 衡量排序稳定性，TopK 超额更接近实际兑现；二者都好时 Rolling 才算真正站住。"
                if rolling_row
                else "当前无法完成 Rolling 维度判断。"
            ),
            next_action="如果 rank IC 还行但 TopK 超额为负，优先按“会排不会赚”路径排查。",
        ),
        DiagnosticNode(
            key="walk_forward_backtest",
            status=_health_status(
                bool(walk_rank_ic_ir is not None and float(walk_rank_ic_ir) > 0.2 and walk_topk is not None and float(walk_topk) > 0),
                warning=bool(walk_rank_ic_ir is not None and float(walk_rank_ic_ir) > 0.1),
                missing=not walk_row,
            ),
            headline="Walk-forward 稳定性可接受" if walk_row else "缺少 walk-forward summary",
            evidence=[
                EvidenceItem(label="rank_ic_ir", value=_format_metric(walk_rank_ic_ir), tone="success" if walk_rank_ic_ir is not None and float(walk_rank_ic_ir) > 0.2 else "warning"),
                EvidenceItem(label="topk_mean_excess_return_4w", value=_format_metric(walk_topk), tone="success" if walk_topk is not None and float(walk_topk) > 0 else "danger"),
            ],
            interpretation=(
                "Walk-forward 更接近真正研究外推能力，比 Rolling 更适合决定 recipe 是否继续推进。"
                if walk_row
                else "当前无法判断样本外稳定性。"
            ),
            next_action="如果 Walk-forward 明显弱于 Rolling，优先怀疑样本外退化或执行口径差异。",
        ),
        DiagnosticNode(
            key="execution_gap",
            status=_health_status(
                bool(execution_delta is not None and execution_delta < 0.05),
                warning=bool(execution_delta is not None and execution_delta < 0.1),
                missing=execution_delta is None,
            ),
            headline="执行口径差异可控" if execution_delta is not None else "缺少 execution diff",
            evidence=[
                EvidenceItem(label="max |native-validation|", value=_format_metric(execution_delta), tone="success" if execution_delta is not None and execution_delta < 0.05 else "warning"),
                EvidenceItem(label="blocked_sell_count", value=_format_metric(blocked_sell), tone="success" if blocked_sell in (0, 0.0, None) else "warning"),
                EvidenceItem(label="actual/target holds", value=f"{_format_metric(actual_hold)}/{_format_metric(target_hold)}", tone="neutral"),
            ],
            interpretation=(
                "strict native 与 validation-like 差异过大时，研究结论会被执行口径放大或扭曲。"
                if execution_delta is not None
                else "当前无法比较执行层偏差。"
            ),
            next_action="如果执行差异大，先看 execution diff 与 portfolio diagnostics，再讨论收益优劣。",
        ),
        DiagnosticNode(
            key="slice_stability",
            status=_health_status(
                bool(slice_positive_ratio is not None and slice_positive_ratio >= 0.6),
                warning=bool(slice_positive_ratio is not None and slice_positive_ratio >= 0.45),
                missing=slice_positive_ratio is None,
            ),
            headline="切片稳定性较好" if slice_positive_ratio is not None else "缺少切片稳定性产物",
            evidence=[
                EvidenceItem(label="正向切片占比", value=_format_metric(slice_positive_ratio), tone="success" if slice_positive_ratio is not None and slice_positive_ratio >= 0.6 else "warning"),
                EvidenceItem(label="切片记录数", value=int(len(slice_summary)) if not slice_summary.empty else None, tone="neutral"),
            ],
            interpretation=(
                "切片结果越分散，说明策略越依赖特定年份、行业或宏观环境。"
                if slice_positive_ratio is not None
                else "当前无法判断跨 regime 稳定性。"
            ),
            next_action="若正向切片占比偏低，优先回看年份/行业切片，而不是只盯总收益。",
        ),
        DiagnosticNode(
            key="signal_snapshot",
            status=_health_status(bool(not snapshot.empty), warning=bool(not snapshot.empty and len(snapshot) < 20), missing=snapshot.empty),
            headline="最新信号快照可用" if not snapshot.empty else "缺少最新信号快照",
            evidence=[
                EvidenceItem(label="snapshot rows", value=int(len(snapshot)) if not snapshot.empty else None, tone="neutral"),
                EvidenceItem(label="feature_date", value=_normalize_value(snapshot["feature_date"].iloc[0]) if not snapshot.empty and "feature_date" in snapshot.columns else None, tone="info"),
            ],
            interpretation=(
                "最新一期快照适合快速查看行业暴露、分数头部和待发布信号。"
                if not snapshot.empty
                else "当前没有可展示的最新一期信号。"
            ),
            next_action="发布前先确认头部持仓、行业分布和分数离散度是否符合预期。",
        ),
    ]
    return nodes


def _panel_summary_for_path(panel_path: Path | None) -> dict[str, Any] | None:
    if panel_path is None or not panel_path.exists():
        return None
    panel = load_panel_dataframe(panel_path)
    summary = summarize_panel(panel)
    summary["path"] = str(panel_path)
    summary["enrichment_scope"] = load_feature_panel_enrichment_scope(panel_path, panel=panel)
    return sanitize_for_json(summary)


def _build_recipe_summary(
    run_id: str,
    recipe_name: str,
    overview_row: dict[str, Any],
    recipe_config: dict[str, Any],
    promotion_gate: dict[str, Any],
) -> RecipeSummary:
    return RecipeSummary(
        run_id=run_id,
        recipe_name=recipe_name,
        signal_objective=recipe_config.get("signal_objective"),
        label_recipe=recipe_config.get("label_recipe"),
        used_feature_count=_normalize_value(overview_row.get("used_feature_count")),
        rolling_rank_ic_ir=_normalize_value(overview_row.get("rolling_rank_ic_ir")),
        walk_forward_rank_ic_ir=_normalize_value(overview_row.get("walk_forward_rank_ic_ir")),
        rolling_topk_mean_excess_return_4w=_normalize_value(overview_row.get("rolling_topk_mean_excess_return_4w")),
        walk_forward_topk_mean_excess_return_4w=_normalize_value(overview_row.get("walk_forward_topk_mean_excess_return_4w")),
        promotion_gate_passed=_normalize_value(promotion_gate.get("promotion_gate_passed")),
    )


def _collect_run_context(run_id: str) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    run_dir = NATIVE_WORKFLOW_ROOT / run_id
    if not run_dir.exists():
        raise FileNotFoundError(f"Unknown run: {run_id}")
    summary_payload = _safe_read_json(run_dir / "native_workflow_summary.json", {})
    artifact_view = _load_native_workflow_artifacts_safe(run_dir)
    return run_dir, summary_payload, artifact_view


def list_runs(limit: int = 50) -> list[RunListItem]:
    runs: list[RunListItem] = []
    for run_dir in _list_run_dirs()[:limit]:
        run_id = run_dir.name
        summary_payload = _safe_read_json(run_dir / "native_workflow_summary.json", {})
        artifact_view = _load_native_workflow_artifacts_safe(run_dir)
        recipe_names = list(artifact_view.get("recipe_names", []))
        inventory = _run_inventory(run_dir, recipe_names)
        artifact_status, ready_count, total_count, missing = _artifact_status(inventory)
        baseline_recipe = _select_baseline_recipe(recipe_names)
        recipe_overview = artifact_view.get("recipe_overview", pd.DataFrame())
        overview_lookup = {
            str(row["recipe"]): row for row in recipe_overview.to_dict(orient="records")
        } if not recipe_overview.empty else {}
        baseline_row = overview_lookup.get(baseline_recipe or "", {})
        baseline_recipe_frames = artifact_view.get("recipes", {}).get(baseline_recipe or "", {})
        execution_gap = baseline_recipe_frames.get("execution_diff_summary", pd.DataFrame())
        has_execution_gap_issue = False
        if not execution_gap.empty and "native_minus_validation_return" in execution_gap.columns:
            deltas = pd.to_numeric(execution_gap["native_minus_validation_return"], errors="coerce").dropna()
            has_execution_gap_issue = bool(not deltas.empty and deltas.abs().max() >= 0.05)
        quick_summary = RunQuickSummary(
            run_id=run_id,
            output_dir=str(run_dir),
            universe_profile=summary_payload.get("config", {}).get("universe_profile"),
            panel_path=summary_payload.get("config", {}).get("panel_path"),
            start_date=summary_payload.get("config", {}).get("start_date"),
            end_date=summary_payload.get("config", {}).get("end_date"),
            recipe_names=recipe_names,
            artifact_status=artifact_status,
            artifact_ready_count=ready_count,
            artifact_total_count=total_count,
            missing_artifacts=missing[:8],
            promotion_gate_summary=sanitize_for_json(summary_payload.get("promotion_gate", {})),
            baseline_recipe=baseline_recipe,
            baseline_metrics={
                "rolling_rank_ic_ir": _normalize_value(baseline_row.get("rolling_rank_ic_ir")),
                "rolling_topk_mean_excess_return_4w": _normalize_value(baseline_row.get("rolling_topk_mean_excess_return_4w")),
                "rolling_net_total_return": _normalize_value(baseline_row.get("rolling_net_total_return")),
                "walk_forward_rank_ic_ir": _normalize_value(baseline_row.get("walk_forward_rank_ic_ir")),
                "walk_forward_topk_mean_excess_return_4w": _normalize_value(baseline_row.get("walk_forward_topk_mean_excess_return_4w")),
                "walk_forward_net_total_return": _normalize_value(baseline_row.get("walk_forward_net_total_return")),
            },
            has_execution_gap_issue=has_execution_gap_issue,
            has_missing_artifacts=artifact_status != "ready",
            updated_at=_path_updated_at(run_dir),
        )
        runs.append(RunListItem(run_id=run_id, updated_at=quick_summary.updated_at, quick_summary=quick_summary))
    return runs


def get_run_detail(run_id: str) -> RunDetail:
    run_dir, summary_payload, artifact_view = _collect_run_context(run_id)
    recipe_names = list(artifact_view.get("recipe_names", []))
    quick_summary = next((item.quick_summary for item in list_runs(limit=200) if item.run_id == run_id), None)
    if quick_summary is None:
        inventory = _run_inventory(run_dir, recipe_names)
        artifact_status, ready_count, total_count, missing = _artifact_status(inventory)
        quick_summary = RunQuickSummary(
            run_id=run_id,
            output_dir=str(run_dir),
            recipe_names=recipe_names,
            artifact_status=artifact_status,
            artifact_ready_count=ready_count,
            artifact_total_count=total_count,
            missing_artifacts=missing,
        )
    recipe_configs = _resolve_recipe_registry(summary_payload)
    recipe_overview = artifact_view.get("recipe_overview", pd.DataFrame())
    overview_lookup = {
        str(row["recipe"]): row for row in recipe_overview.to_dict(orient="records")
    } if not recipe_overview.empty else {}
    baseline_recipe = quick_summary.baseline_recipe or _select_baseline_recipe(recipe_names)
    baseline_frames = artifact_view.get("recipes", {}).get(baseline_recipe or "", {})
    panel_path = _resolve_artifact_path(summary_payload.get("config", {}).get("panel_path"))
    panel_summary = _panel_summary_for_path(panel_path)
    nodes = _build_recipe_nodes(
        baseline_recipe or "baseline",
        baseline_frames,
        recipe_configs.get(baseline_recipe or "", {}),
        panel_summary,
    ) if baseline_recipe else []
    recipes = [
        _build_recipe_summary(
            run_id=run_id,
            recipe_name=recipe_name,
            overview_row=overview_lookup.get(recipe_name, {}),
            recipe_config=recipe_configs.get(recipe_name, {}),
            promotion_gate=(summary_payload.get("promotion_gate", {}) or {}).get(recipe_name, {}),
        )
        for recipe_name in recipe_names
    ]
    inventory = _run_inventory(run_dir, recipe_names)
    file_tree = sorted(
        [
            _artifact_ref(path, name=str(path.relative_to(run_dir)))
            for path in run_dir.rglob("*")
            if path.is_file()
        ],
        key=lambda item: item.name,
    )
    return RunDetail(
        run_id=run_id,
        output_dir=str(run_dir),
        quick_summary=quick_summary,
        config=sanitize_for_json(summary_payload.get("config", {})),
        recipe_registry=sanitize_for_json(summary_payload.get("recipe_registry", {})),
        promotion_gate=sanitize_for_json(summary_payload.get("promotion_gate", {})),
        nodes=nodes,
        recipes=recipes,
        artifact_inventory=inventory + file_tree,
    )


def list_run_recipes(run_id: str) -> list[RecipeSummary]:
    detail = get_run_detail(run_id)
    return detail.recipes


def get_recipe_detail(run_id: str, recipe_name: str) -> RecipeDetail:
    run_dir, summary_payload, artifact_view = _collect_run_context(run_id)
    recipe_frames = artifact_view.get("recipes", {}).get(recipe_name)
    if recipe_frames is None:
        raise FileNotFoundError(f"Unknown recipe '{recipe_name}' for run '{run_id}'")
    recipe_overview = artifact_view.get("recipe_overview", pd.DataFrame())
    overview_lookup = {
        str(row["recipe"]): row for row in recipe_overview.to_dict(orient="records")
    } if not recipe_overview.empty else {}
    recipe_config = _get_recipe_config(summary_payload, recipe_name)
    panel_path = _resolve_artifact_path(summary_payload.get("config", {}).get("panel_path"))
    panel_summary = _panel_summary_for_path(panel_path)
    recipe_dir = run_dir / recipe_name
    portfolio_targets = _safe_read_csv(recipe_dir / "portfolio_targets.csv")
    tables = {
        "feature_prefilter": _frame_to_payload(recipe_frames.get("feature_prefilter", pd.DataFrame())),
        "signal_diagnostics": _frame_to_payload(recipe_frames.get("signal_diagnostics", pd.DataFrame())),
        "portfolio_diagnostics": _frame_to_payload(recipe_frames.get("portfolio_diagnostics", pd.DataFrame())),
        "rolling_summary": _frame_to_payload(recipe_frames.get("rolling_summary", pd.DataFrame())),
        "walk_forward_summary": _frame_to_payload(recipe_frames.get("walk_forward_summary", pd.DataFrame())),
        "rolling_details": _frame_to_payload(recipe_frames.get("rolling_details", pd.DataFrame())),
        "walk_forward_details": _frame_to_payload(recipe_frames.get("walk_forward_details", pd.DataFrame())),
        "rolling_native_report": _frame_to_payload(recipe_frames.get("rolling_native_report", pd.DataFrame())),
        "walk_forward_native_report": _frame_to_payload(recipe_frames.get("walk_forward_native_report", pd.DataFrame())),
        "execution_diff_summary": _frame_to_payload(recipe_frames.get("execution_diff_summary", pd.DataFrame())),
        "slice_regime_summary": _frame_to_payload(recipe_frames.get("slice_regime_summary", pd.DataFrame())),
        "rolling_feature_importance": _frame_to_payload(recipe_frames.get("rolling_feature_importance", pd.DataFrame())),
        "walk_forward_feature_importance": _frame_to_payload(recipe_frames.get("walk_forward_feature_importance", pd.DataFrame())),
        "latest_score_frame": _frame_to_payload(recipe_frames.get("latest_score_frame", pd.DataFrame()), sort_by="score"),
        "portfolio_targets": _frame_to_payload(portfolio_targets, sort_by="rank", ascending=True),
        "rolling_native_monthly_return_heatmap": _frame_to_payload(recipe_frames.get("rolling_native_monthly_return_heatmap", pd.DataFrame())),
        "walk_forward_native_monthly_return_heatmap": _frame_to_payload(recipe_frames.get("walk_forward_native_monthly_return_heatmap", pd.DataFrame())),
        "rolling_native_annual_return_heatmap": _frame_to_payload(recipe_frames.get("rolling_native_annual_return_heatmap", pd.DataFrame())),
        "walk_forward_native_annual_return_heatmap": _frame_to_payload(recipe_frames.get("walk_forward_native_annual_return_heatmap", pd.DataFrame())),
    }
    nodes = _build_recipe_nodes(recipe_name, recipe_frames, recipe_config, panel_summary)
    return RecipeDetail(
        run_id=run_id,
        recipe_name=recipe_name,
        recipe_config=sanitize_for_json(recipe_config),
        manifest=sanitize_for_json(recipe_frames.get("manifest", {})),
        overview=sanitize_for_json(overview_lookup.get(recipe_name, {})),
        nodes=nodes,
        tables=tables,
        artifact_inventory=_recipe_inventory(recipe_dir, prefix=recipe_name),
    )


def compare_recipe_items(items: list[CompareItemRef]) -> CompareResponse:
    compare_items: list[CompareItemResult] = []
    metric_rows: list[dict[str, Any]] = []
    execution_rows: list[dict[str, Any]] = []
    slice_rows: list[dict[str, Any]] = []
    feature_importance: dict[str, DataTablePayload] = {}
    snapshots: dict[str, DataTablePayload] = {}

    for item in items:
        recipe_detail = get_recipe_detail(item.run_id, item.recipe_name)
        bundle_summary_key = f"{item.bundle}_summary"
        bundle_report_key = f"{item.bundle}_native_report"
        importance_key = f"{item.bundle}_feature_importance"
        overview = recipe_detail.overview
        summary_rows = recipe_detail.tables.get(bundle_summary_key, DataTablePayload()).rows
        summary_row = summary_rows[0] if summary_rows else {}
        report_rows = recipe_detail.tables.get(bundle_report_key, DataTablePayload()).rows
        report_tail = report_rows[-1] if report_rows else {}
        exec_table = recipe_detail.tables.get("execution_diff_summary", DataTablePayload())
        matching_exec = next((row for row in exec_table.rows if row.get("bundle") == item.bundle), {})
        label = f"{item.run_id} / {item.recipe_name} / {item.bundle}"
        metrics = {
            "rank_ic_ir": summary_row.get("rank_ic_ir"),
            "topk_mean_excess_return_4w": summary_row.get("topk_mean_excess_return_4w"),
            "coverage_mean": summary_row.get("coverage_mean"),
            "used_feature_count": summary_row.get("used_feature_count") or overview.get("used_feature_count"),
            "net_value": report_tail.get("net_value"),
            "turnover": report_tail.get("turnover"),
            "native_minus_validation_return": matching_exec.get("native_minus_validation_return"),
            "native_max_drawdown": matching_exec.get("native_max_drawdown"),
        }
        compare_items.append(
            CompareItemResult(
                ref=item,
                label=label,
                metrics=sanitize_for_json(metrics),
                nodes=recipe_detail.nodes,
            )
        )
        metric_rows.append({"item": label, **sanitize_for_json(metrics)})
        execution_rows.append({"item": label, **sanitize_for_json(matching_exec)})

        slice_table = recipe_detail.tables.get("slice_regime_summary", DataTablePayload())
        for row in slice_table.rows:
            if row.get("bundle") == item.bundle:
                slice_rows.append({"item": label, **row})

        importance_rows = recipe_detail.tables.get(importance_key, DataTablePayload()).rows
        importance_df = pd.DataFrame(importance_rows)
        if not importance_df.empty and "importance_gain" in importance_df.columns:
            importance_df = importance_df.sort_values("importance_gain", ascending=False).head(15)
        feature_importance[label] = _frame_to_payload(importance_df)

        snapshot_table = recipe_detail.tables.get("latest_score_frame", DataTablePayload())
        snapshot_df = pd.DataFrame(snapshot_table.rows)
        sort_column = "score" if "score" in snapshot_df.columns else ("qlib_score" if "qlib_score" in snapshot_df.columns else None)
        if sort_column and not snapshot_df.empty:
            snapshot_df = snapshot_df.sort_values(sort_column, ascending=False).head(20)
        snapshots[label] = _frame_to_payload(snapshot_df)

    return CompareResponse(
        items=compare_items,
        summary_metrics=_frame_to_payload(pd.DataFrame(metric_rows)),
        execution_gap=_frame_to_payload(pd.DataFrame(execution_rows)),
        slice_stability=_frame_to_payload(pd.DataFrame(slice_rows)),
        feature_importance=feature_importance,
        latest_signal_snapshot=snapshots,
    )


def _build_panel_run_links(limit: int = 200) -> dict[str, list[str]]:
    links: dict[str, list[str]] = {}
    for run_item in list_runs(limit=limit):
        raw_panel = run_item.quick_summary.panel_path
        resolved = _resolve_artifact_path(raw_panel)
        if resolved is None:
            continue
        for key in {str(resolved), resolved.name}:
            links.setdefault(key, []).append(run_item.run_id)
    return links


def list_panels() -> list[PanelSummary]:
    panels: list[PanelSummary] = []
    panel_links = _build_panel_run_links()
    for path in _list_panel_files():
        panel = load_panel_dataframe(path)
        summary = sanitize_for_json(summarize_panel(panel))
        panels.append(
            PanelSummary(
                panel_id=path.name,
                name=path.name,
                path=str(path),
                format=path.suffix.lower().lstrip("."),
                size_bytes=path.stat().st_size,
                updated_at=_path_updated_at(path),
                enrichment_scope=load_feature_panel_enrichment_scope(path, panel=panel),
                summary=summary,
                linked_runs=panel_links.get(str(path), panel_links.get(path.name, [])),
            )
        )
    return panels


def get_panel_detail(panel_id: str) -> PanelDetail:
    path = PANELS_ROOT / panel_id
    if not path.exists():
        raise FileNotFoundError(f"Unknown panel: {panel_id}")
    panel = load_panel_dataframe(path)
    panel_links = _build_panel_run_links()
    summary = sanitize_for_json(summarize_panel(panel))
    catalog_rows = []
    for column in panel.columns:
        series = panel[column]
        catalog_rows.append(
            {
                "column": column,
                "dtype": str(series.dtype),
                "non_null_ratio": float(series.notna().mean()) if len(series) else None,
                "sample": _normalize_value(series.dropna().iloc[0]) if series.notna().any() else None,
            }
        )
    sample_rows = panel.head(20).copy()
    for column in sample_rows.columns:
        if sample_rows[column].dtype.kind in {"M"}:
            sample_rows[column] = sample_rows[column].astype(str)
    return PanelDetail(
        panel_id=path.name,
        name=path.name,
        path=str(path),
        format=path.suffix.lower().lstrip("."),
        size_bytes=path.stat().st_size,
        updated_at=_path_updated_at(path),
        enrichment_scope=load_feature_panel_enrichment_scope(path, panel=panel),
        summary=summary,
        linked_runs=panel_links.get(str(path), panel_links.get(path.name, [])),
        columns=[str(column) for column in panel.columns],
        column_catalog=_frame_to_payload(pd.DataFrame(catalog_rows)),
        sample_rows=_frame_to_payload(sample_rows),
    )


def _task_paths(task_id: str) -> tuple[Path, Path, Path, Path]:
    task_dir = TASKS_ROOT / task_id
    return (
        task_dir,
        task_dir / "task.json",
        task_dir / "stdout.log",
        task_dir / "stderr.log",
    )


def _load_task(task_id: str) -> ResearchTaskSummary:
    _, task_file, _, _ = _task_paths(task_id)
    if not task_file.exists():
        raise FileNotFoundError(f"Unknown task: {task_id}")
    return ResearchTaskSummary(**_safe_read_json(task_file, {}))


def list_tasks() -> list[ResearchTaskSummary]:
    summaries = []
    for task_dir in _list_task_dirs():
        task_file = task_dir / "task.json"
        if task_file.exists():
            summaries.append(ResearchTaskSummary(**_safe_read_json(task_file, {})))
    return summaries


def get_task(task_id: str) -> ResearchTaskSummary:
    return _load_task(task_id)


def get_task_logs(task_id: str) -> TaskLogResponse:
    _, _, stdout_path, stderr_path = _task_paths(task_id)
    return TaskLogResponse(
        task_id=task_id,
        stdout=stdout_path.read_text(encoding="utf-8") if stdout_path.exists() else "",
        stderr=stderr_path.read_text(encoding="utf-8") if stderr_path.exists() else "",
    )


def _write_task(summary: ResearchTaskSummary) -> None:
    task_dir, task_file, _, _ = _task_paths(summary.task_id)
    task_dir.mkdir(parents=True, exist_ok=True)
    task_file.write_text(summary.model_dump_json(indent=2), encoding="utf-8")


def _spawn_task_worker(task_id: str) -> None:
    task_dir, _, _, _ = _task_paths(task_id)
    subprocess.Popen(
        [sys.executable, "-m", "qlib_research.app.task_worker", str(task_dir)],
        cwd=PROJECT_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )


def create_export_panel_task(request: ExportPanelTaskRequest) -> ResearchTaskSummary:
    TASKS_ROOT.mkdir(parents=True, exist_ok=True)
    task_id = f"task-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"
    command = [
        "uv",
        "run",
        "python",
        "scripts/export_weekly_panel.py",
        "--output",
        request.output,
        "--batch-size",
        str(request.batch_size),
    ]
    if request.start_date:
        command += ["--start-date", request.start_date]
    if request.end_date:
        command += ["--end-date", request.end_date]
    if request.universe_profile:
        command += ["--universe-profile", request.universe_profile]
    if request.enrichment_scope:
        command += ["--enrichment-scope", request.enrichment_scope]
    if request.symbols:
        command += ["--symbols", *request.symbols]
    summary = ResearchTaskSummary(
        task_id=task_id,
        task_kind="export_panel",
        status="queued",
        display_name=request.display_name or "Export Panel",
        requested_by=request.requested_by,
        created_at=_now_iso(),
        output_dir=str(_resolve_artifact_path(request.output) or request.output),
        message="Task queued",
        command=command,
        config_payload=request.model_dump(),
        logs={"stdout": str(_task_paths(task_id)[2]), "stderr": str(_task_paths(task_id)[3])},
        metadata={"cwd": str(PROJECT_ROOT)},
    )
    _write_task(summary)
    _spawn_task_worker(task_id)
    return summary


def create_native_workflow_task(request: RunNativeWorkflowTaskRequest) -> ResearchTaskSummary:
    TASKS_ROOT.mkdir(parents=True, exist_ok=True)
    task_id = f"task-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"
    command_string = build_native_workflow_cli_command(
        config_overrides=request.config_payload,
        recipe_names=request.recipe_names,
    ).replace("\\\n", " ")
    command = shlex.split(command_string)
    output_dir = request.config_payload.get("output_dir", "artifacts/native_workflow/csi300")
    summary = ResearchTaskSummary(
        task_id=task_id,
        task_kind="run_native_workflow",
        status="queued",
        display_name=request.display_name or "Run Native Workflow",
        requested_by=request.requested_by,
        created_at=_now_iso(),
        output_dir=str(_resolve_artifact_path(output_dir) or output_dir),
        message="Task queued",
        command=command,
        config_payload={
            "config_payload": request.config_payload,
            "recipe_names": request.recipe_names,
        },
        logs={"stdout": str(_task_paths(task_id)[2]), "stderr": str(_task_paths(task_id)[3])},
        metadata={"cwd": str(PROJECT_ROOT)},
    )
    _write_task(summary)
    _spawn_task_worker(task_id)
    return summary


def update_task_status(task_dir: Path, **updates: Any) -> ResearchTaskSummary:
    task_file = task_dir / "task.json"
    payload = _safe_read_json(task_file, {})
    payload.update(sanitize_for_json(updates))
    summary = ResearchTaskSummary(**payload)
    task_file.write_text(summary.model_dump_json(indent=2), encoding="utf-8")
    return summary


def task_worker_run(task_dir: Path) -> int:
    task_file = task_dir / "task.json"
    payload = _safe_read_json(task_file, {})
    summary = ResearchTaskSummary(**payload)
    stdout_path = task_dir / "stdout.log"
    stderr_path = task_dir / "stderr.log"
    summary = update_task_status(
        task_dir,
        status="running",
        started_at=_now_iso(),
        message="Task running",
        metadata={**summary.metadata, "worker_pid": os.getpid()},
    )
    try:
        with stdout_path.open("a", encoding="utf-8") as stdout_handle, stderr_path.open("a", encoding="utf-8") as stderr_handle:
            stdout_handle.write(f"[task-worker] started {summary.task_id} at {_now_iso()}\n")
            stdout_handle.write(f"[task-worker] command: {' '.join(summary.command)}\n")
            process = subprocess.Popen(
                summary.command,
                cwd=PROJECT_ROOT,
                stdout=stdout_handle,
                stderr=stderr_handle,
                text=True,
            )
            update_task_status(task_dir, metadata={**summary.metadata, "worker_pid": os.getpid(), "child_pid": process.pid})
            return_code = process.wait()
        final_status = "succeeded" if return_code == 0 else "failed"
        update_task_status(
            task_dir,
            status=final_status,
            finished_at=_now_iso(),
            message="Task completed" if return_code == 0 else f"Task failed with exit code {return_code}",
            metadata={**summary.metadata, "worker_pid": os.getpid(), "return_code": return_code},
        )
        result_path = task_dir / "result.json"
        result_path.write_text(
            json.dumps(
                {
                    "task_id": summary.task_id,
                    "status": final_status,
                    "return_code": return_code,
                    "finished_at": _now_iso(),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return return_code
    except Exception as exc:  # pragma: no cover - defensive filesystem/subprocess handling
        update_task_status(
            task_dir,
            status="failed",
            finished_at=_now_iso(),
            message=f"Task worker crashed: {exc}",
            metadata={**summary.metadata, "worker_pid": os.getpid(), "worker_error": str(exc)},
        )
        with stderr_path.open("a", encoding="utf-8") as stderr_handle:
            stderr_handle.write(f"[task-worker] crashed: {exc}\n")
        return 1
