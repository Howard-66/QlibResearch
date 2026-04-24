"""Artifact-backed services for the QlibResearch workbench API."""

from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime
import json
import math
import os
from pathlib import Path
import re
import shutil
import shlex
import signal
import subprocess
import sys
from typing import Any, Iterable
from uuid import uuid4

import numpy as np
import pandas as pd

from qlib_research.app.contracts import (
    AnalysisReportRef,
    ArtifactRef,
    ArtifactInventoryResponse,
    ChartAnnotationPayload,
    ChartPayload,
    ChartSeriesPayload,
    ChartThresholdPayload,
    CompareItemRef,
    CompareItemResult,
    CompareResponse,
    CompareTimeseriesPoint,
    CompareTimeseriesSeries,
    CompareWinnerSummary,
    DataTablePayload,
    DiagnosticNode,
    EvidenceItem,
    ExecutionAnomalySummary,
    ExportPanelTaskRequest,
    OverviewResponse,
    PanelDetail,
    PanelSummary,
    RecommendationAction,
    RecipeDetail,
    RecipeTablesResponse,
    RecipeSummary,
    ResearchSummary,
    RunResearchAnalysisTaskRequest,
    ResearchTaskDetail,
    ResearchTaskSummary,
    RunDetail,
    RunListItem,
    RunNativeWorkflowTaskRequest,
    RunQuickSummary,
    TaskBoardResponse,
    TaskLogResponse,
    TaskPresetResponse,
    TaskQueueState,
    TaskReorderRequest,
    TaskSourceRef,
)
from qlib_research.config import get_project_root, get_qlib_artifacts_dir
from qlib_research.core.notebook_workflow import (
    build_native_workflow_cli_command,
    load_native_workflow_artifacts,
    sanitize_for_json,
    summarize_panel,
)
from qlib_research.core.qlib_pipeline import FEATURE_GROUP_COLUMNS, load_panel_dataframe
from qlib_research.core.weekly_feature_panel import (
    load_feature_panel_enrichment_scope,
    load_feature_panel_metadata,
)

PROJECT_ROOT = get_project_root()
ARTIFACTS_ROOT = get_qlib_artifacts_dir()
NATIVE_WORKFLOW_ROOT = ARTIFACTS_ROOT / "native_workflow"
PANELS_ROOT = ARTIFACTS_ROOT / "panels"
TASKS_ROOT = ARTIFACTS_ROOT / "app_tasks"
TASK_QUEUE_FILENAME = "queue.json"
RESEARCH_VERDICT_VALUES = {
    "incumbent",
    "promoted",
    "promote",
    "rejected",
    "needs_explanation",
    "hold",
    "reject",
    "investigate",
}
NATIVE_WORKFLOW_SYSTEM_TEMPLATE = "native_workflow_system_report"
NATIVE_WORKFLOW_ANALYSIS_SKILL_NAME = "native-workflow-artifact-analysis"

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
    "holding_count_drift.csv",
    "rebalance_audit.csv",
    "regime_gate_diagnostics.csv",
    "rolling_predictions.csv",
    "rolling_performance_metrics.csv",
    "walk_forward_predictions.csv",
    "walk_forward_performance_metrics.csv",
    "rolling_native_annual_return_heatmap.csv",
    "sector_exposure_history.csv",
    "signal_realization_bridge.csv",
    "walk_forward_native_annual_return_heatmap.csv",
}

RUN_INDEX_FILENAME = "workbench_run_index.json"
RUN_INDEX_SCHEMA_VERSION = 4
PANEL_SUMMARY_CACHE_DIRNAME = ".workbench_cache"

RUN_INDEX_RECIPE_FILES = {
    "experiment_scorecard.json",
    "native_workflow_manifest.json",
    "rolling_summary.csv",
    "walk_forward_summary.csv",
    "execution_diff_summary.csv",
    "rolling_native_report.csv",
    "walk_forward_native_report.csv",
    "rolling_performance_metrics.csv",
    "walk_forward_performance_metrics.csv",
    "holding_count_drift.csv",
    "sector_exposure_history.csv",
}

RECIPE_TABLE_FILES = {
    "feature_prefilter": "feature_prefilter.csv",
    "signal_diagnostics": "signal_diagnostics.csv",
    "portfolio_diagnostics": "portfolio_diagnostics.csv",
    "rolling_summary": "rolling_summary.csv",
    "walk_forward_summary": "walk_forward_summary.csv",
    "rolling_details": "rolling_details.csv",
    "walk_forward_details": "walk_forward_details.csv",
    "rolling_native_report": "rolling_native_report.csv",
    "walk_forward_native_report": "walk_forward_native_report.csv",
    "execution_diff_summary": "execution_diff_summary.csv",
    "slice_regime_summary": "slice_regime_summary.csv",
    "signal_realization_bridge": "signal_realization_bridge.csv",
    "holding_count_drift": "holding_count_drift.csv",
    "rebalance_audit": "rebalance_audit.csv",
    "sector_exposure_history": "sector_exposure_history.csv",
    "regime_gate_diagnostics": "regime_gate_diagnostics.csv",
    "rolling_feature_importance": "rolling_feature_importance.csv",
    "walk_forward_feature_importance": "walk_forward_feature_importance.csv",
    "latest_score_frame": "latest_score_frame.csv",
    "portfolio_targets": "portfolio_targets.csv",
    "rolling_performance_metrics": "rolling_performance_metrics.csv",
    "walk_forward_performance_metrics": "walk_forward_performance_metrics.csv",
    "rolling_native_monthly_return_heatmap": "rolling_native_monthly_return_heatmap.csv",
    "walk_forward_native_monthly_return_heatmap": "walk_forward_native_monthly_return_heatmap.csv",
    "rolling_native_annual_return_heatmap": "rolling_native_annual_return_heatmap.csv",
    "walk_forward_native_annual_return_heatmap": "walk_forward_native_annual_return_heatmap.csv",
}

RUN_DETAIL_NODE_TABLES = {
    "feature_prefilter",
    "signal_diagnostics",
    "portfolio_diagnostics",
    "rolling_summary",
    "walk_forward_summary",
    "execution_diff_summary",
    "slice_regime_summary",
    "latest_score_frame",
    "signal_realization_bridge",
    "holding_count_drift",
    "sector_exposure_history",
    "regime_gate_diagnostics",
}

RECIPE_DETAIL_INITIAL_TABLES = {
    "feature_prefilter",
    "signal_diagnostics",
    "portfolio_diagnostics",
    "rolling_summary",
    "walk_forward_summary",
    "execution_diff_summary",
    "slice_regime_summary",
    "signal_realization_bridge",
    "holding_count_drift",
    "sector_exposure_history",
    "regime_gate_diagnostics",
}

COMPARE_REQUIRED_TABLES = {
    "latest_score_frame",
    "execution_diff_summary",
    "slice_regime_summary",
    "rolling_summary",
    "walk_forward_summary",
    "rolling_native_report",
    "walk_forward_native_report",
    "rolling_performance_metrics",
    "walk_forward_performance_metrics",
    "rolling_feature_importance",
    "walk_forward_feature_importance",
    "signal_realization_bridge",
    "holding_count_drift",
    "sector_exposure_history",
    "regime_gate_diagnostics",
}

_RUN_INDEX_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_PANEL_SUMMARY_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}


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


def _scorecard_path(base_dir: Path) -> Path:
    return base_dir / "experiment_scorecard.json"


def _analysis_dir(base_dir: Path) -> Path:
    return base_dir / "analysis"


def _latest_summary_markdown_path(base_dir: Path) -> Path:
    return _analysis_dir(base_dir) / "latest_summary.md"


def _normalize_markdown_heading(value: str) -> str:
    normalized = value.strip().lower()
    normalized = re.sub(r"^(?:第\s*)?\d+(?:\.\d+)*\s*[).、．-]?\s*", "", normalized)
    normalized = re.sub(r"^第[一二三四五六七八九十百千]+[章节部分]\s*", "", normalized)
    if normalized in {"主线建议", "recommended action", "main recommendation", "investment thesis", "核心建议"}:
        return "recommended_action"
    if normalized in {"current problem", "当前问题", "核心问题", "当前主要问题", "current issue"}:
        return "current_problem"
    if normalized in {
        "key findings",
        "关键发现",
        "核心发现",
        "主要结论",
        "结论摘要",
        "核心判断",
        "executive verdict",
        "system diagnosis",
    }:
        return "key_findings"
    if normalized in {
        "risks",
        "风险",
        "主要风险",
        "风险与约束",
        "当前研究的主要问题",
        "当前结果的主要问题",
        "方法学约束",
        "方法学问题",
        "live portfolio feasibility",
        "evidence gaps",
    }:
        return "risks"
    if normalized in {
        "recommended next actions",
        "next actions",
        "recommended next experiments",
        "next experiment roadmap",
        "后续动作",
        "下一步动作",
        "下一步实验",
        "建议动作",
        "优化方案",
        "实验方案",
        "实验优先级",
    }:
        return "recommended_next_actions"
    return normalized


def _parse_markdown_metadata_line(line: str) -> tuple[str, str] | None:
    raw = line.strip()
    if raw.startswith("- "):
        raw = raw[2:].strip()
    for separator in (":", "："):
        if separator not in raw:
            continue
        key, value = raw.split(separator, 1)
        key = key.strip().lower()
        value = value.strip()
        if not key or not value:
            return None
        return key, value
    return None


def _parse_markdown_list_item(line: str) -> str | None:
    raw = line.strip()
    for pattern in (r"^[-*]\s+", r"^\d+[.)]\s+", r"^[（(]\d+[）)]\s+"):
        if re.match(pattern, raw):
            return re.sub(pattern, "", raw, count=1).strip()
    return None


def _normalize_research_verdict(value: Any | None) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    normalized = raw.strip("`*_ \t\r\n\"'“”‘’").rstrip("。.,，；;:：").lower()
    normalized = normalized.replace("-", "_").replace(" ", "_")
    if normalized in RESEARCH_VERDICT_VALUES:
        return normalized
    searchable = re.sub(r"[`*_\"'“”‘’。.,，；;:：]+", " ", raw.lower().replace("-", "_"))
    searchable = re.sub(r"\s+", "_", searchable)
    for verdict in sorted(RESEARCH_VERDICT_VALUES, key=len, reverse=True):
        if re.search(rf"(?<![a-z_]){re.escape(verdict)}(?![a-z_])", searchable):
            return verdict
    return None


def _normalize_run_quick_summary_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    normalized["research_status"] = _normalize_research_verdict(normalized.get("research_status"))
    return normalized


def _parse_latest_summary_markdown(base_dir: Path) -> dict[str, Any]:
    path = _latest_summary_markdown_path(base_dir)
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8")
    lines = text.replace("\r\n", "\n").split("\n")
    title: str | None = None
    metadata: dict[str, str] = {}
    sections: dict[str, list[str]] = {}
    current_section: str | None = None
    in_code_block = False

    for raw_line in lines:
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("```"):
            in_code_block = not in_code_block
            continue
        if in_code_block:
            continue
        if stripped.startswith("# "):
            if title is None:
                title = stripped[2:].strip()
            current_section = None
            continue
        if stripped.startswith("## "):
            current_section = _normalize_markdown_heading(stripped[3:].strip())
            sections.setdefault(current_section, [])
            continue
        if stripped.startswith("### "):
            if current_section is not None:
                sections.setdefault(current_section, []).append(stripped[4:].strip())
            continue
        metadata_item = _parse_markdown_metadata_line(stripped)
        if current_section is None and metadata_item is not None:
            key, value = metadata_item
            metadata[key] = value
            continue
        list_item = _parse_markdown_list_item(stripped)
        if list_item is not None:
            if current_section is not None:
                sections.setdefault(current_section, []).append(list_item)
            continue
        if current_section is not None:
            sections.setdefault(current_section, []).append(stripped)

    return {
        "headline": title,
        "verdict": _normalize_research_verdict(metadata.get("verdict")),
        "current_problem": (
            (sections.get("current_problem") or [None])[0]
            or metadata.get("current problem")
            or metadata.get("当前问题")
        ),
        "recommended_action": (
            (sections.get("recommended_action") or [None])[0]
            or metadata.get("recommended action")
            or metadata.get("主线建议")
        ),
        "key_findings": sections.get("key_findings", []),
        "risks": sections.get("risks", []),
        "recommended_next_actions": sections.get("recommended_next_actions", []),
    }


def _merge_research_summaries(primary: ResearchSummary, markdown_payload: dict[str, Any]) -> ResearchSummary:
    if not markdown_payload:
        return primary
    return primary.model_copy(
        update={
            "headline": markdown_payload.get("headline") or primary.headline,
            "verdict": _normalize_research_verdict(markdown_payload.get("verdict")) or primary.verdict,
            "key_findings": markdown_payload.get("key_findings") or primary.key_findings,
            "risks": markdown_payload.get("risks") or primary.risks,
            "recommended_next_actions": markdown_payload.get("recommended_next_actions") or primary.recommended_next_actions,
            "current_problem": markdown_payload.get("current_problem") or primary.current_problem,
            "recommended_action": markdown_payload.get("recommended_action") or primary.recommended_action,
        }
    )


def _parse_latest_summary_json(base_dir: Path) -> dict[str, Any]:
    payload = _safe_read_json(_analysis_dir(base_dir) / "latest_summary.json", {})
    if not isinstance(payload, dict) or not payload:
        return {}
    live_feasibility = payload.get("live_feasibility") if isinstance(payload.get("live_feasibility"), dict) else {}
    next_experiments = payload.get("next_experiments") if isinstance(payload.get("next_experiments"), list) else []
    next_actions: list[str] = []
    for item in next_experiments:
        if isinstance(item, dict):
            name = str(item.get("name") or "").strip()
            rationale = str(item.get("rationale") or "").strip()
            priority = str(item.get("priority") or "").strip()
            label = "：".join(part for part in (priority, name) if part)
            if rationale:
                label = f"{label}。{rationale}" if label else rationale
            if label:
                next_actions.append(label)
        elif item is not None:
            next_actions.append(str(item))
    system_findings = payload.get("system_findings") if isinstance(payload.get("system_findings"), list) else []
    live_checks = live_feasibility.get("checks") if isinstance(live_feasibility.get("checks"), list) else []
    return {
        "headline": payload.get("headline"),
        "verdict": _normalize_research_verdict(payload.get("verdict")),
        "current_problem": payload.get("current_problem") or live_feasibility.get("summary"),
        "recommended_action": payload.get("recommended_action") or (next_actions[0] if next_actions else None),
        "key_findings": [str(item) for item in system_findings if item is not None],
        "risks": [str(item) for item in live_checks if item is not None],
        "recommended_next_actions": next_actions,
    }


def _read_research_summary(base_dir: Path) -> ResearchSummary:
    payload = _safe_read_json(_scorecard_path(base_dir), {})
    summary = ResearchSummary()
    if isinstance(payload, dict) and payload:
        summary = ResearchSummary(
            headline=payload.get("headline"),
            verdict=_normalize_research_verdict(payload.get("verdict")),
            key_findings=[str(item) for item in payload.get("key_findings", []) if item is not None],
            risks=[str(item) for item in payload.get("risks", []) if item is not None],
            recommended_next_actions=[
                str(item)
                for item in (
                    payload.get("recommended_next_actions")
                    or payload.get("recommended_next_experiments")
                    or []
                )
                if item is not None
            ],
            current_problem=payload.get("current_problem"),
            recommended_action=payload.get("recommended_action"),
            incumbent_recipe=payload.get("incumbent_recipe"),
            promoted_recipe=payload.get("promoted_recipe"),
            metrics=sanitize_for_json(payload.get("metrics", {})),
        )
    summary = _merge_research_summaries(summary, _parse_latest_summary_markdown(base_dir))
    return _merge_research_summaries(summary, _parse_latest_summary_json(base_dir))


def _fallback_run_research_summary(index_payload: dict[str, Any]) -> ResearchSummary:
    quick_summary = index_payload.get("quick_summary", {})
    baseline_metrics = quick_summary.get("baseline_metrics", {}) if isinstance(quick_summary, dict) else {}
    artifact_status = quick_summary.get("artifact_status") if isinstance(quick_summary, dict) else None
    return ResearchSummary(
        headline=f"当前基线：{quick_summary.get('baseline_recipe') or 'baseline'}",
        verdict=_normalize_research_verdict(quick_summary.get("research_status")),
        key_findings=[
            f"Walk-forward rank_ic_ir {baseline_metrics.get('walk_forward_rank_ic_ir')}",
            f"Walk-forward net return {baseline_metrics.get('walk_forward_net_total_return')}",
        ],
        risks=[str(quick_summary.get("current_problem"))] if quick_summary.get("current_problem") else [],
        recommended_next_actions=[str(quick_summary.get("recommended_action"))] if quick_summary.get("recommended_action") else [],
        current_problem=quick_summary.get("current_problem"),
        recommended_action=quick_summary.get("recommended_action"),
        incumbent_recipe=quick_summary.get("baseline_recipe"),
        promoted_recipe=quick_summary.get("incumbent_recipe") or quick_summary.get("baseline_recipe"),
        metrics={"artifact_status": artifact_status, **sanitize_for_json(baseline_metrics)},
    )


def _fallback_recipe_research_summary(recipe_name: str, overview: dict[str, Any]) -> ResearchSummary:
    return ResearchSummary(
        headline=f"{recipe_name} 暂无 scorecard，先按概览指标查看",
        key_findings=[
            f"Walk-forward 年化收益 {overview.get('walk_forward_annualized_return')}",
            f"Walk-forward 最大回撤 {overview.get('walk_forward_max_drawdown')}",
        ],
        metrics=sanitize_for_json(overview),
    )


def _scan_analysis_reports(base_dir: Path) -> list[AnalysisReportRef]:
    analysis_dir = _analysis_dir(base_dir)
    if not analysis_dir.exists():
        return []
    refs: list[AnalysisReportRef] = []
    latest_json_payload = _safe_read_json(analysis_dir / "latest_summary.json", {})
    for path in sorted(analysis_dir.iterdir(), key=lambda item: item.stat().st_mtime, reverse=True):
        if not path.is_file():
            continue
        engine = None
        template = None
        verdict = None
        content_type = "text"
        content_preview = None
        if path.suffix.lower() == ".json":
            payload = _safe_read_json(path, {})
            if isinstance(payload, dict):
                engine = payload.get("engine")
                template = payload.get("template")
                verdict = payload.get("verdict")
                content_preview = json.dumps(sanitize_for_json(payload), ensure_ascii=False, indent=2)
                content_type = "json"
        elif isinstance(latest_json_payload, dict) and latest_json_payload:
            engine = latest_json_payload.get("engine")
            template = latest_json_payload.get("template")
            verdict = latest_json_payload.get("verdict")
            try:
                content_preview = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                content_preview = None
            content_type = "markdown" if path.suffix.lower() == ".md" else "text"
        refs.append(
            AnalysisReportRef(
                name=path.name,
                path=str(path),
                exists=True,
                engine=engine,
                template=template,
                verdict=verdict,
                updated_at=_path_updated_at(path),
                content_type=content_type,
                content_preview=content_preview,
            )
        )
    return refs


def _extract_run_recipe_dossier_markdown(run_dir: Path, recipe_name: str) -> str | None:
    path = _latest_summary_markdown_path(run_dir)
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8")
    lines = text.replace("\r\n", "\n").split("\n")
    in_dossiers = False
    in_recipe = False
    collected: list[str] = []
    in_code_block = False

    for raw_line in lines:
        stripped = raw_line.strip()
        if stripped.startswith("```"):
            in_code_block = not in_code_block
        if not in_code_block and stripped.startswith("## "):
            section = _normalize_markdown_heading(stripped[3:].strip())
            if in_recipe:
                break
            in_dossiers = section == "recipe dossiers"
            continue
        if not in_dossiers:
            continue
        if not in_code_block and stripped.startswith("### "):
            heading = stripped[4:].strip().strip("`")
            if in_recipe:
                break
            if heading == recipe_name:
                in_recipe = True
                collected.append(f"### {recipe_name}")
            continue
        if in_recipe:
            collected.append(raw_line.rstrip())

    content = "\n".join(collected).strip()
    return content + "\n" if content else None


def _run_recipe_dossier_report(run_dir: Path, recipe_name: str) -> AnalysisReportRef | None:
    content = _extract_run_recipe_dossier_markdown(run_dir, recipe_name)
    if not content:
        return None
    path = _latest_summary_markdown_path(run_dir)
    latest_json_payload = _safe_read_json(_analysis_dir(run_dir) / "latest_summary.json", {})
    engine = latest_json_payload.get("engine") if isinstance(latest_json_payload, dict) else None
    template = latest_json_payload.get("template") if isinstance(latest_json_payload, dict) else None
    verdict = latest_json_payload.get("verdict") if isinstance(latest_json_payload, dict) else None
    return AnalysisReportRef(
        name="run_recipe_dossier.md",
        path=str(path),
        exists=True,
        engine=engine,
        template=template,
        verdict=verdict,
        updated_at=_path_updated_at(path),
        content_type="markdown",
        content_preview=content,
    )


def _scan_recipe_analysis_reports(run_dir: Path, recipe_dir: Path, recipe_name: str) -> list[AnalysisReportRef]:
    reports = _scan_analysis_reports(recipe_dir)
    run_dossier = _run_recipe_dossier_report(run_dir, recipe_name)
    return [run_dossier, *reports] if run_dossier else reports


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
    for filename in sorted(REQUIRED_RECIPE_ARTIFACTS.union(OPTIONAL_RECIPE_CSVS).union({"experiment_scorecard.json"})):
        display_name = f"{prefix}/{filename}" if prefix else filename
        files.append(_artifact_ref(recipe_dir / filename, name=display_name))
    analysis_dir = _analysis_dir(recipe_dir)
    if analysis_dir.exists():
        for path in sorted(analysis_dir.iterdir()):
            if path.is_file():
                display_name = f"{prefix}/analysis/{path.name}" if prefix else f"analysis/{path.name}"
                files.append(_artifact_ref(path, name=display_name))
    return files


def _run_inventory(run_dir: Path, recipe_names: Iterable[str]) -> list[ArtifactRef]:
    inventory = [
        _artifact_ref(run_dir / "native_workflow_summary.json"),
        _artifact_ref(run_dir / "experiment_scorecard.json"),
    ]
    analysis_dir = _analysis_dir(run_dir)
    if analysis_dir.exists():
        for path in sorted(analysis_dir.iterdir()):
            if path.is_file():
                inventory.append(_artifact_ref(path, name=f"analysis/{path.name}"))
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


def _safe_native_report_max_drawdown(frame: pd.DataFrame) -> float | None:
    if frame.empty:
        return None
    candidate_columns = ("relative_drawdown", "strategy_drawdown", "drawdown")
    for column in candidate_columns:
        if column not in frame.columns:
            continue
        series = pd.to_numeric(frame[column], errors="coerce").dropna()
        if not series.empty:
            return float(series.min())
    return None


def _performance_metrics_row(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {}
    return {str(key): _normalize_value(value) for key, value in frame.iloc[0].to_dict().items()}


def _summary_overview_lookup(summary_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw_lookup = summary_payload.get("overview_lookup", {})
    if not isinstance(raw_lookup, dict):
        return {}
    return {str(key): value if isinstance(value, dict) else {} for key, value in raw_lookup.items()}


def _promotion_gate_overview_fallback(
    recipe_name: str,
    *,
    baseline_recipe: str | None,
    promotion_gate_payload: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(promotion_gate_payload, dict) or not promotion_gate_payload:
        return {}

    def _row_from_gate(gate_payload: dict[str, Any], prefix: str) -> dict[str, Any]:
        if not isinstance(gate_payload, dict):
            return {}
        return {
            "walk_forward_net_total_return": _normalize_value(gate_payload.get(f"{prefix}_walk_forward_net_total_return")),
            "walk_forward_max_drawdown": _normalize_value(gate_payload.get(f"{prefix}_walk_forward_drawdown")),
        }

    if recipe_name == baseline_recipe:
        for gate_payload in promotion_gate_payload.values():
            fallback_row = _row_from_gate(gate_payload, "baseline")
            if any(value is not None for value in fallback_row.values()):
                return fallback_row
        return {}

    return _row_from_gate(promotion_gate_payload.get(recipe_name, {}), "candidate")


def _merge_overview_rows(primary: dict[str, Any], *fallbacks: dict[str, Any]) -> dict[str, Any]:
    merged = {str(key): _normalize_value(value) for key, value in primary.items()}
    for fallback in fallbacks:
        if not isinstance(fallback, dict):
            continue
        for key, value in fallback.items():
            normalized = _normalize_value(value)
            if merged.get(key) is None and normalized is not None:
                merged[str(key)] = normalized
    return merged


def _build_recipe_overview_row(
    recipe_name: str,
    recipe_frames: dict[str, Any],
    *,
    summary_overview: dict[str, Any] | None = None,
    existing_overview: dict[str, Any] | None = None,
    promotion_gate_overview: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rolling_summary = recipe_frames.get("rolling_summary", pd.DataFrame())
    walk_summary = recipe_frames.get("walk_forward_summary", pd.DataFrame())
    rolling_performance = recipe_frames.get("rolling_performance_metrics", pd.DataFrame())
    walk_forward_performance = recipe_frames.get("walk_forward_performance_metrics", pd.DataFrame())
    signal_diagnostics = recipe_frames.get("signal_diagnostics", pd.DataFrame())
    sector_exposure = recipe_frames.get("sector_exposure_history", pd.DataFrame())
    manifest = recipe_frames.get("manifest", {}) if isinstance(recipe_frames.get("manifest", {}), dict) else {}
    rolling_row = _summary_row(rolling_summary)
    walk_row = _summary_row(walk_summary)
    rolling_performance_row = _performance_metrics_row(rolling_performance)
    walk_forward_performance_row = _performance_metrics_row(walk_forward_performance)
    rolling_signal = _bundle_frame(signal_diagnostics, "rolling")
    walk_forward_signal = _bundle_frame(signal_diagnostics, "walk_forward")
    rolling_exposure = _bundle_frame(sector_exposure, "rolling")
    walk_forward_exposure = _bundle_frame(sector_exposure, "walk_forward")
    computed_row = {
        "recipe": recipe_name,
        "used_feature_count": rolling_row.get("used_feature_count") or walk_row.get("used_feature_count") or len(manifest.get("used_feature_columns", [])),
        "rolling_rank_ic_ir": rolling_row.get("rank_ic_ir"),
        "rolling_score_distinction": _mean_or_none(rolling_signal, "topk_unique_score_ratio"),
        "rolling_top1_sector_weight": _max_or_none(rolling_exposure, "top1_sector_weight"),
        "rolling_topk_mean_excess_return_4w": rolling_row.get("topk_mean_excess_return_4w"),
        "rolling_net_total_return": _safe_native_report_return(recipe_frames.get("rolling_native_report", pd.DataFrame())),
        "rolling_max_drawdown": _safe_native_report_max_drawdown(recipe_frames.get("rolling_native_report", pd.DataFrame())),
        "rolling_annualized_return": rolling_performance_row.get("annualized_return"),
        "rolling_annualized_volatility": rolling_performance_row.get("annualized_volatility"),
        "rolling_sharpe_ratio": rolling_performance_row.get("sharpe_ratio"),
        "rolling_win_rate": rolling_performance_row.get("win_rate"),
        "rolling_calmar_ratio": rolling_performance_row.get("calmar_ratio"),
        "walk_forward_rank_ic_ir": walk_row.get("rank_ic_ir"),
        "walk_forward_score_distinction": _mean_or_none(walk_forward_signal, "topk_unique_score_ratio"),
        "walk_forward_top1_sector_weight": _max_or_none(walk_forward_exposure, "top1_sector_weight"),
        "walk_forward_topk_mean_excess_return_4w": walk_row.get("topk_mean_excess_return_4w"),
        "walk_forward_net_total_return": _safe_native_report_return(recipe_frames.get("walk_forward_native_report", pd.DataFrame())),
        "walk_forward_max_drawdown": _safe_native_report_max_drawdown(recipe_frames.get("walk_forward_native_report", pd.DataFrame())),
        "walk_forward_annualized_return": walk_forward_performance_row.get("annualized_return"),
        "walk_forward_annualized_volatility": walk_forward_performance_row.get("annualized_volatility"),
        "walk_forward_sharpe_ratio": walk_forward_performance_row.get("sharpe_ratio"),
        "walk_forward_win_rate": walk_forward_performance_row.get("win_rate"),
        "walk_forward_calmar_ratio": walk_forward_performance_row.get("calmar_ratio"),
        "avg_actual_hold_count": _mean_or_none(recipe_frames.get("holding_count_drift", pd.DataFrame()), "actual_hold_count"),
        "max_actual_hold_count": _max_or_none(recipe_frames.get("holding_count_drift", pd.DataFrame()), "actual_hold_count"),
        "top1_sector_weight": _max_or_none(recipe_frames.get("sector_exposure_history", pd.DataFrame()), "top1_sector_weight"),
    }
    return _merge_overview_rows(
        computed_row,
        summary_overview or {},
        existing_overview or {},
        promotion_gate_overview or {},
    )


def _resolve_recipe_overview(
    *,
    recipe_name: str,
    recipe_frames: dict[str, Any],
    summary_payload: dict[str, Any],
    index_payload: dict[str, Any],
    baseline_recipe: str | None,
) -> dict[str, Any]:
    quick_summary_payload = index_payload.get("quick_summary", {})
    promotion_gate_payload = (
        summary_payload.get("promotion_gate", {})
        or summary_payload.get("promotion_gate_summary", {})
        or (quick_summary_payload.get("promotion_gate_summary", {}) if isinstance(quick_summary_payload, dict) else {})
        or {}
    )
    return sanitize_for_json(
        _build_recipe_overview_row(
            recipe_name,
            recipe_frames,
            summary_overview=_summary_overview_lookup(summary_payload).get(recipe_name, {}),
            existing_overview=_recipe_overview_lookup_from_index_payload(index_payload).get(recipe_name, {}),
            promotion_gate_overview=_promotion_gate_overview_fallback(
                recipe_name,
                baseline_recipe=baseline_recipe,
                promotion_gate_payload=promotion_gate_payload,
            ),
        )
    )


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
            "No module named 'lightgbm'",
        )
        if not isinstance(exc, (ImportError, ModuleNotFoundError, RuntimeError)):
            raise
        if not any(message in str(exc) for message in fallback_messages):
            raise
    return _scan_native_workflow_artifacts(output_dir)


def _run_index_path(run_dir: Path) -> Path:
    return run_dir / RUN_INDEX_FILENAME


def _panel_summary_cache_dir() -> Path:
    return PANELS_ROOT / PANEL_SUMMARY_CACHE_DIRNAME


def _panel_summary_cache_path(panel_path: Path) -> Path:
    return _panel_summary_cache_dir() / f"{panel_path.name}.json"


def _run_index_source_mtime(run_dir: Path, recipe_names: list[str]) -> float:
    candidates = [run_dir / "native_workflow_summary.json"]
    for recipe_name in recipe_names:
        recipe_dir = run_dir / recipe_name
        candidates.extend(recipe_dir / filename for filename in RUN_INDEX_RECIPE_FILES)
    existing = [path.stat().st_mtime for path in candidates if path.exists()]
    return max(existing, default=run_dir.stat().st_mtime)


def _safe_last_numeric_value(frame: pd.DataFrame, column: str) -> float | None:
    if frame.empty or column not in frame.columns:
        return None
    series = pd.to_numeric(frame[column], errors="coerce").dropna()
    if series.empty:
        return None
    return float(series.iloc[-1])


def _bundle_frame(frame: pd.DataFrame, bundle: str = "walk_forward") -> pd.DataFrame:
    if frame.empty or "bundle" not in frame.columns:
        return frame.copy()
    filtered = frame.loc[frame["bundle"].astype(str) == bundle].copy()
    if not filtered.empty:
        return filtered
    return frame.copy()


def _numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if frame.empty or column not in frame.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").dropna()


def _mean_or_none(frame: pd.DataFrame, column: str) -> float | None:
    series = _numeric_series(frame, column)
    if series.empty:
        return None
    return float(series.mean())


def _max_or_none(frame: pd.DataFrame, column: str) -> float | None:
    series = _numeric_series(frame, column)
    if series.empty:
        return None
    return float(series.max())


def _ratio_or_none(frame: pd.DataFrame, predicate: pd.Series) -> float | None:
    if frame.empty or predicate.empty:
        return None
    return float(predicate.fillna(False).mean())


def _first_present_numeric(row: dict[str, Any], keys: Iterable[str]) -> float | None:
    for key in keys:
        value = row.get(key)
        if value is None or pd.isna(value):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _execution_drift_columns(frame: pd.DataFrame) -> tuple[str | None, str | None]:
    actual_column = "post_trade_hold_count" if "post_trade_hold_count" in frame.columns else ("actual_hold_count" if "actual_hold_count" in frame.columns else None)
    target_column = "target_hold_count" if "target_hold_count" in frame.columns else None
    return actual_column, target_column


def _dominant_execution_cause(frame: pd.DataFrame) -> str | None:
    if frame.empty:
        return None
    limit_mean = _mean_or_none(frame, "sell_blocked_by_limit_count") or 0.0
    suspend_mean = _mean_or_none(frame, "sell_blocked_by_suspend_count") or 0.0
    volume_mean = _mean_or_none(frame, "sell_blocked_by_volume_count") or 0.0
    residual_mean = _mean_or_none(frame, "residual_hold_count") or 0.0
    leading = max(
        (
            ("limit sell blocked", limit_mean),
            ("suspend sell blocked", suspend_mean),
            ("volume sell blocked", volume_mean),
        ),
        key=lambda item: item[1],
    )
    if leading[1] >= 0.15:
        return leading[0]
    if residual_mean > 0:
        return "membership drift"
    return "stable"


def _execution_severity_from_frame(frame: pd.DataFrame) -> str | None:
    if frame.empty:
        return None
    actual_column, target_column = _execution_drift_columns(frame)
    if actual_column is None or target_column is None:
        return None
    actual = _numeric_series(frame, actual_column)
    target = _numeric_series(frame, target_column)
    if actual.empty or target.empty:
        return None
    drift = actual.reset_index(drop=True) - target.reset_index(drop=True)
    if drift.empty:
        return None
    max_drift = float(drift.max())
    affected_ratio = float((drift > 0).mean())
    if max_drift >= 4 or affected_ratio >= 0.6:
        return "critical"
    if max_drift >= 3 or affected_ratio >= 0.35:
        return "high"
    if max_drift >= 2 or affected_ratio >= 0.15:
        return "medium"
    return "low"


def _recommended_experiment_codes(frame: pd.DataFrame) -> list[str]:
    cause = _dominant_execution_cause(frame)
    severity = _execution_severity_from_frame(frame)
    if cause == "membership drift":
        return ["A1 strict_membership_only"]
    if cause and "sell blocked" in cause:
        if severity in {"high", "critical"}:
            return ["A2 actual_hold_cap=12", "A3 actual_hold_cap=14"]
        return ["A2 actual_hold_cap=12"]
    return []


def _build_execution_anomaly_summary(frame: pd.DataFrame) -> ExecutionAnomalySummary:
    bundle_frame = _bundle_frame(frame)
    if bundle_frame.empty:
        return ExecutionAnomalySummary()
    actual_column, target_column = _execution_drift_columns(bundle_frame)
    drift_ratio = None
    if actual_column and target_column:
        actual = _numeric_series(bundle_frame, actual_column)
        target = _numeric_series(bundle_frame, target_column)
        if not actual.empty and not target.empty:
            drift_ratio = float((actual.reset_index(drop=True) > target.reset_index(drop=True)).mean())
    return ExecutionAnomalySummary(
        dominant_cause=_dominant_execution_cause(bundle_frame),
        severity=_execution_severity_from_frame(bundle_frame),
        affected_period_ratio=drift_ratio,
        avg_actual_hold_count=_mean_or_none(bundle_frame, actual_column or ""),
        max_actual_hold_count=_max_or_none(bundle_frame, actual_column or ""),
        avg_locked_residual_count=_mean_or_none(bundle_frame, "locked_residual_count"),
        recommended_experiments=_recommended_experiment_codes(bundle_frame),
        summary_label=(
            f"{_dominant_execution_cause(bundle_frame) or 'unknown'} / "
            f"avg holds {_mean_or_none(bundle_frame, actual_column or ''):.2f}"
            if _mean_or_none(bundle_frame, actual_column or "") is not None
            else None
        ),
    )


def _build_exposure_summary(frame: pd.DataFrame) -> dict[str, Any]:
    bundle_frame = _bundle_frame(frame)
    if bundle_frame.empty:
        return {}
    return sanitize_for_json(
        {
            "top1_sector_weight": _max_or_none(bundle_frame, "top1_sector_weight"),
            "top3_sector_concentration": _max_or_none(bundle_frame, "top3_sector_concentration"),
            "finance_weight": _max_or_none(bundle_frame, "finance_weight"),
            "latest_sector_name": (
                str(bundle_frame.iloc[-1].get("top1_sector_name"))
                if not bundle_frame.empty and "top1_sector_name" in bundle_frame.columns
                else None
            ),
        }
    )


def _build_recommendation_actions(
    *,
    source_type: str,
    source_id: str,
    anomaly_summary: ExecutionAnomalySummary,
    include_analysis: bool = False,
) -> list[RecommendationAction]:
    actions: list[RecommendationAction] = []
    source_label = source_id
    if source_type == "recipe" and ":" in source_id:
        run_id, recipe_name = source_id.split(":", 1)
        source_label = f"{run_id}/{recipe_name}"
    elif source_type == "compare":
        source_label = "Compare Selection"
    if include_analysis:
        actions.append(
            RecommendationAction(
                label="Run Diagnosis Task",
                task_kind="run_research_analysis",
                source_type=source_type,
                source_id=source_id,
                prefill_config={
                    "display_name": f"Diagnose {source_label}",
                    "description": "Generate a run-level native workflow system diagnosis",
                    "analysis_template": NATIVE_WORKFLOW_SYSTEM_TEMPLATE if source_type == "run" else "anomaly_diagnosis",
                    "analysis_engine": "codex_cli",
                    "batch_mode": "run_only",
                    "include_all_recipes": False,
                    "skills": [NATIVE_WORKFLOW_ANALYSIS_SKILL_NAME] if source_type == "run" else [],
                },
                reason="生成 run 级系统诊断，优先比较 recipe、信号兑现、持仓漂移和实盘可行性。",
            )
        )
    return actions


def _chart_payload(
    key: str,
    title: str,
    x_values: list[str],
    series: list[ChartSeriesPayload],
    *,
    kind: str = "line",
    thresholds: list[ChartThresholdPayload] | None = None,
    annotations: list[ChartAnnotationPayload] | None = None,
) -> ChartPayload:
    return ChartPayload(
        key=key,
        title=title,
        kind=kind,
        x=x_values,
        series=series,
        thresholds=thresholds or [],
        annotations=annotations or [],
    )


def _build_holding_trend_chart(frame: pd.DataFrame, *, key: str, title: str) -> ChartPayload | None:
    bundle_frame = _bundle_frame(frame)
    if bundle_frame.empty:
        return None
    x_column = "trade_date" if "trade_date" in bundle_frame.columns else ("signal_date" if "signal_date" in bundle_frame.columns else None)
    actual_column, target_column = _execution_drift_columns(bundle_frame)
    if x_column is None or actual_column is None or target_column is None:
        return None
    x_values = [str(value) for value in bundle_frame[x_column].fillna("").tolist()]
    return _chart_payload(
        key,
        title,
        x_values,
        [
            ChartSeriesPayload(key="target", label="Target Holds", values=[_normalize_value(value) for value in bundle_frame[target_column].tolist()], color="#2563eb"),
            ChartSeriesPayload(key="actual", label="Actual Holds", values=[_normalize_value(value) for value in bundle_frame[actual_column].tolist()], color="#dc2626"),
            ChartSeriesPayload(key="locked", label="Locked Residual", values=[_normalize_value(value) for value in bundle_frame.get("locked_residual_count", pd.Series([None] * len(bundle_frame))).tolist()], color="#f59e0b"),
        ],
        thresholds=[ChartThresholdPayload(label="TopK", value=10.0, tone="info")] if target_column == "target_hold_count" else [],
        annotations=_chart_annotations_from_frame(bundle_frame, x_column),
    )


def _chart_annotations_from_frame(frame: pd.DataFrame, x_column: str) -> list[ChartAnnotationPayload]:
    if frame.empty or x_column not in frame.columns:
        return []
    frame = frame.reset_index(drop=True)
    residual = _numeric_series(frame, "residual_hold_count")
    if residual.empty:
        return []
    annotated: list[ChartAnnotationPayload] = []
    numeric = (
        pd.to_numeric(frame["residual_hold_count"], errors="coerce")
        if "residual_hold_count" in frame.columns
        else pd.Series(dtype=float)
    )
    for idx in numeric[numeric >= 2].nlargest(6).index.tolist():
        annotated.append(
            ChartAnnotationPayload(
                x=str(frame.iloc[idx].get(x_column)),
                label=f"Residual {int(numeric.iloc[idx])}",
                tone="warning" if numeric.iloc[idx] < 4 else "danger",
            )
        )
    return annotated


def _build_blocked_sell_chart(frame: pd.DataFrame, *, key: str, title: str) -> ChartPayload | None:
    bundle_frame = _bundle_frame(frame)
    if bundle_frame.empty:
        return None
    x_column = "trade_date" if "trade_date" in bundle_frame.columns else ("signal_date" if "signal_date" in bundle_frame.columns else None)
    if x_column is None:
        return None
    return _chart_payload(
        key,
        title,
        [str(value) for value in bundle_frame[x_column].fillna("").tolist()],
        [
            ChartSeriesPayload(key="limit", label="Limit", values=[_normalize_value(value) for value in bundle_frame.get("sell_blocked_by_limit_count", pd.Series([None] * len(bundle_frame))).tolist()], role="bar", stack="blocked", color="#dc2626"),
            ChartSeriesPayload(key="suspend", label="Suspend", values=[_normalize_value(value) for value in bundle_frame.get("sell_blocked_by_suspend_count", pd.Series([None] * len(bundle_frame))).tolist()], role="bar", stack="blocked", color="#7c3aed"),
            ChartSeriesPayload(key="volume", label="Volume", values=[_normalize_value(value) for value in bundle_frame.get("sell_blocked_by_volume_count", pd.Series([None] * len(bundle_frame))).tolist()], role="bar", stack="blocked", color="#f59e0b"),
        ],
        kind="stacked_bar",
        annotations=_chart_annotations_from_frame(bundle_frame, x_column),
    )


def _build_realization_bridge_chart(frame: pd.DataFrame, *, key: str, title: str) -> ChartPayload | None:
    bundle_frame = _bundle_frame(frame)
    if bundle_frame.empty:
        return None
    x_column = "trade_date" if "trade_date" in bundle_frame.columns else ("signal_date" if "signal_date" in bundle_frame.columns else None)
    if x_column is None:
        return None
    series: list[ChartSeriesPayload] = []
    for series_key, label, color in (
        ("topk_mean_return_4w", "TopK Mean Return 4W", "#2563eb"),
        ("realized_portfolio_return", "Realized Portfolio Return", "#059669"),
        ("execution_cost_drag", "Execution Cost Drag", "#dc2626"),
    ):
        if series_key in bundle_frame.columns:
            series.append(
                ChartSeriesPayload(
                    key=series_key,
                    label=label,
                    values=[_normalize_value(value) for value in bundle_frame[series_key].tolist()],
                    color=color,
                )
            )
    if not series:
        return None
    return _chart_payload(key, title, [str(value) for value in bundle_frame[x_column].fillna("").tolist()], series, annotations=_chart_annotations_from_frame(bundle_frame, x_column))


def _build_exposure_chart(frame: pd.DataFrame, *, key: str, title: str) -> ChartPayload | None:
    bundle_frame = _bundle_frame(frame)
    if bundle_frame.empty:
        return None
    x_column = "trade_date" if "trade_date" in bundle_frame.columns else ("signal_date" if "signal_date" in bundle_frame.columns else None)
    if x_column is None:
        return None
    series: list[ChartSeriesPayload] = []
    for series_key, label, color in (
        ("top1_sector_weight", "Top1 Sector Weight", "#2563eb"),
        ("top3_sector_concentration", "Top3 Sector Concentration", "#ea580c"),
        ("finance_weight", "Finance Weight", "#059669"),
    ):
        if series_key in bundle_frame.columns:
            role = "area" if series_key == "top1_sector_weight" else "line"
            series.append(
                ChartSeriesPayload(
                    key=series_key,
                    label=label,
                    values=[_normalize_value(value) for value in bundle_frame[series_key].tolist()],
                    role=role,
                    color=color,
                )
            )
    if not series:
        return None
    return _chart_payload(key, title, [str(value) for value in bundle_frame[x_column].fillna("").tolist()], series, kind="area", annotations=_chart_annotations_from_frame(bundle_frame, x_column))


def _top_anomaly_periods(frame: pd.DataFrame, limit: int = 10) -> list[dict[str, Any]]:
    bundle_frame = _bundle_frame(frame)
    if bundle_frame.empty:
        return []
    actual_column, target_column = _execution_drift_columns(bundle_frame)
    x_column = "trade_date" if "trade_date" in bundle_frame.columns else ("signal_date" if "signal_date" in bundle_frame.columns else None)
    if actual_column is None or target_column is None or x_column is None:
        return []
    scored = bundle_frame.copy()
    scored["_drift_score"] = pd.to_numeric(scored[actual_column], errors="coerce").fillna(0) - pd.to_numeric(scored[target_column], errors="coerce").fillna(0)
    scored["_blocked_score"] = (
        pd.to_numeric(scored["sell_blocked_total_count"], errors="coerce").fillna(0)
        if "sell_blocked_total_count" in scored.columns
        else 0
    )
    scored["_rank"] = scored["_drift_score"] * 10 + scored["_blocked_score"]
    scored = scored.sort_values(["_rank", "_drift_score"], ascending=False).head(limit)
    result: list[dict[str, Any]] = []
    for row in scored.to_dict(orient="records"):
        result.append(
            sanitize_for_json(
                {
                    "trade_date": row.get(x_column),
                    "actual_hold_count": row.get(actual_column),
                    "target_hold_count": row.get(target_column),
                    "locked_residual_count": row.get("locked_residual_count"),
                    "sell_blocked_total_count": row.get("sell_blocked_total_count"),
                    "dominant_cause": _dominant_execution_cause(pd.DataFrame([row])),
                }
            )
        )
    return result


def _group_artifact_name(ref: ArtifactRef) -> str:
    name = ref.name
    if name.startswith("analysis/") or "/analysis/" in name:
        return "Research"
    if any(token in name for token in ("signal_realization", "holding_count_drift", "sector_exposure", "rebalance_audit", "regime_gate", "execution_diff", "portfolio_diagnostics", "signal_diagnostics")):
        return "Diagnostics"
    if any(token in name for token in ("native_report", "summary", "benchmark", "heatmap", "performance_metrics")):
        return "Backtest"
    return "Exports"


def _load_recipe_summary_inputs(recipe_dir: Path) -> dict[str, Any]:
    return {
        "manifest": _safe_read_json(recipe_dir / "native_workflow_manifest.json", {}),
        "rolling_summary": _safe_read_csv(recipe_dir / "rolling_summary.csv"),
        "walk_forward_summary": _safe_read_csv(recipe_dir / "walk_forward_summary.csv"),
        "execution_diff_summary": _safe_read_csv(recipe_dir / "execution_diff_summary.csv"),
        "rolling_native_report": _safe_read_csv(recipe_dir / "rolling_native_report.csv"),
        "walk_forward_native_report": _safe_read_csv(recipe_dir / "walk_forward_native_report.csv"),
        "rolling_performance_metrics": _safe_read_csv(recipe_dir / "rolling_performance_metrics.csv"),
        "walk_forward_performance_metrics": _safe_read_csv(recipe_dir / "walk_forward_performance_metrics.csv"),
        "holding_count_drift": _safe_read_csv(recipe_dir / "holding_count_drift.csv"),
        "sector_exposure_history": _safe_read_csv(recipe_dir / "sector_exposure_history.csv"),
    }


def _build_run_index_payload(run_dir: Path, *, existing_payload: dict[str, Any] | None = None) -> dict[str, Any]:
    run_id = run_dir.name
    summary_payload = _safe_read_json(run_dir / "native_workflow_summary.json", {})
    recipe_names = _resolve_run_recipe_names(run_dir, summary_payload)
    inventory = _run_inventory(run_dir, recipe_names)
    artifact_status, ready_count, total_count, missing = _artifact_status(inventory)
    recipe_configs = _resolve_recipe_registry(summary_payload)
    existing_payload = existing_payload if isinstance(existing_payload, dict) else {}
    existing_quick_summary = existing_payload.get("quick_summary", {})
    existing_promotion_gate_payload = (
        existing_quick_summary.get("promotion_gate_summary", {})
        if isinstance(existing_quick_summary, dict)
        else {}
    )
    promotion_gate_payload = (
        summary_payload.get("promotion_gate", {})
        or summary_payload.get("promotion_gate_summary", {})
        or existing_promotion_gate_payload
        or {}
    )
    summary_overview_lookup = _summary_overview_lookup(summary_payload)
    existing_overview_lookup = _recipe_overview_lookup_from_index_payload(existing_payload)
    baseline_recipe = _select_baseline_recipe(recipe_names)
    run_research_summary = _read_research_summary(run_dir)

    overview_lookup: dict[str, dict[str, Any]] = {}
    recipes: list[RecipeSummary] = []

    for recipe_name in recipe_names:
        recipe_dir = run_dir / recipe_name
        summary_inputs = _load_recipe_summary_inputs(recipe_dir)
        overview_row = sanitize_for_json(
            _build_recipe_overview_row(
                recipe_name,
                summary_inputs,
                summary_overview=summary_overview_lookup.get(recipe_name, {}),
                existing_overview=existing_overview_lookup.get(recipe_name, {}),
                promotion_gate_overview=_promotion_gate_overview_fallback(
                    recipe_name,
                    baseline_recipe=baseline_recipe,
                    promotion_gate_payload=promotion_gate_payload,
                ),
            )
        )
        overview_lookup[recipe_name] = overview_row
        recipes.append(
            _build_recipe_summary(
                run_id=run_id,
                recipe_name=recipe_name,
                overview_row=overview_row,
                recipe_config=recipe_configs.get(recipe_name, {}),
                promotion_gate=promotion_gate_payload.get(recipe_name, {}),
            )
        )

    baseline_row = overview_lookup.get(baseline_recipe or "", {})
    baseline_exec = _safe_read_csv(run_dir / (baseline_recipe or "") / "execution_diff_summary.csv") if baseline_recipe else pd.DataFrame()
    baseline_holding_drift = _safe_read_csv(run_dir / (baseline_recipe or "") / "holding_count_drift.csv") if baseline_recipe else pd.DataFrame()
    has_execution_gap_issue = False
    if not baseline_exec.empty and "native_minus_validation_return" in baseline_exec.columns:
        deltas = pd.to_numeric(baseline_exec["native_minus_validation_return"], errors="coerce").dropna()
        has_execution_gap_issue = bool(not deltas.empty and deltas.abs().max() >= 0.05)
    anomaly_summary = _build_execution_anomaly_summary(baseline_holding_drift)

    quick_summary = RunQuickSummary(
        run_id=run_id,
        output_dir=str(run_dir),
        task_description=summary_payload.get("config", {}).get("task_description"),
        universe_profile=summary_payload.get("config", {}).get("universe_profile"),
        panel_path=summary_payload.get("config", {}).get("panel_path"),
        start_date=summary_payload.get("config", {}).get("start_date"),
        end_date=summary_payload.get("config", {}).get("end_date"),
        recipe_names=recipe_names,
        artifact_status=artifact_status,
        artifact_ready_count=ready_count,
        artifact_total_count=total_count,
        missing_artifacts=missing[:8],
        promotion_gate_summary=sanitize_for_json(promotion_gate_payload),
        baseline_recipe=baseline_recipe,
        baseline_metrics={
            "rolling_rank_ic_ir": _normalize_value(baseline_row.get("rolling_rank_ic_ir")),
            "rolling_topk_mean_excess_return_4w": _normalize_value(baseline_row.get("rolling_topk_mean_excess_return_4w")),
            "rolling_net_total_return": _normalize_value(baseline_row.get("rolling_net_total_return")),
            "rolling_max_drawdown": _normalize_value(baseline_row.get("rolling_max_drawdown")),
            "walk_forward_rank_ic_ir": _normalize_value(baseline_row.get("walk_forward_rank_ic_ir")),
            "walk_forward_topk_mean_excess_return_4w": _normalize_value(baseline_row.get("walk_forward_topk_mean_excess_return_4w")),
            "walk_forward_net_total_return": _normalize_value(baseline_row.get("walk_forward_net_total_return")),
            "walk_forward_max_drawdown": _normalize_value(baseline_row.get("walk_forward_max_drawdown")),
        },
        has_execution_gap_issue=has_execution_gap_issue,
        has_missing_artifacts=artifact_status != "ready",
        research_status=_normalize_research_verdict(run_research_summary.verdict),
        incumbent_recipe=run_research_summary.promoted_recipe or run_research_summary.incumbent_recipe or baseline_recipe,
        current_problem=run_research_summary.current_problem,
        recommended_action=run_research_summary.recommended_action,
        dominant_execution_cause=anomaly_summary.dominant_cause,
        portfolio_definition_status="healthy" if anomaly_summary.severity in {None, "low"} else ("warning" if anomaly_summary.severity == "medium" else "danger"),
        avg_actual_hold_count=_normalize_value(baseline_row.get("avg_actual_hold_count")),
        max_actual_hold_count=_normalize_value(baseline_row.get("max_actual_hold_count")),
        top1_sector_weight=_normalize_value(baseline_row.get("top1_sector_weight")),
        updated_at=_path_updated_at(run_dir),
    )
    return {
        "schema_version": RUN_INDEX_SCHEMA_VERSION,
        "run_id": run_id,
        "updated_at": quick_summary.updated_at,
        "quick_summary": quick_summary.model_dump(mode="json"),
        "research_summary": run_research_summary.model_dump(mode="json"),
        "recipes": [recipe.model_dump(mode="json") for recipe in recipes],
        "overview_lookup": sanitize_for_json(overview_lookup),
    }


def _load_run_index_payload(run_dir: Path) -> dict[str, Any]:
    recipe_names = _resolve_run_recipe_names(run_dir, _safe_read_json(run_dir / "native_workflow_summary.json", {}))
    source_mtime = _run_index_source_mtime(run_dir, recipe_names)
    cache_key = str(run_dir)
    cached = _RUN_INDEX_CACHE.get(cache_key)
    if cached and cached[0] >= source_mtime and cached[1].get("schema_version") == RUN_INDEX_SCHEMA_VERSION:
        return cached[1]

    index_path = _run_index_path(run_dir)
    existing_payload = cached[1] if cached else {}
    if index_path.exists() and index_path.stat().st_mtime >= source_mtime:
        payload = _safe_read_json(index_path, {})
        existing_payload = payload
        if payload.get("schema_version") == RUN_INDEX_SCHEMA_VERSION:
            _RUN_INDEX_CACHE[cache_key] = (index_path.stat().st_mtime, payload)
            return payload

    payload = _build_run_index_payload(run_dir, existing_payload=existing_payload)
    index_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    index_mtime = index_path.stat().st_mtime
    _RUN_INDEX_CACHE[cache_key] = (index_mtime, payload)
    return payload


def _run_list_item_from_index_payload(payload: dict[str, Any]) -> RunListItem:
    quick_summary = RunQuickSummary(**_normalize_run_quick_summary_payload(payload.get("quick_summary", {})))
    return RunListItem(
        run_id=str(payload.get("run_id") or quick_summary.run_id),
        updated_at=payload.get("updated_at") or quick_summary.updated_at,
        quick_summary=quick_summary,
    )


def _recipe_summaries_from_index_payload(payload: dict[str, Any]) -> list[RecipeSummary]:
    return [RecipeSummary(**item) for item in payload.get("recipes", [])]


def _recipe_overview_lookup_from_index_payload(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw_lookup = payload.get("overview_lookup", {})
    if not isinstance(raw_lookup, dict):
        return {}
    return {str(key): value if isinstance(value, dict) else {} for key, value in raw_lookup.items()}


def _load_recipe_frames(run_dir: Path, recipe_name: str, table_names: Iterable[str]) -> dict[str, Any]:
    recipe_dir = run_dir / recipe_name
    frames: dict[str, Any] = {
        "manifest": _safe_read_json(recipe_dir / "native_workflow_manifest.json", {}),
    }
    for table_name in table_names:
        filename = RECIPE_TABLE_FILES.get(table_name)
        if not filename:
            continue
        frames[table_name] = _safe_read_csv(recipe_dir / filename)
    return frames


def _build_table_payloads(recipe_frames: dict[str, Any], table_names: Iterable[str]) -> dict[str, DataTablePayload]:
    tables: dict[str, DataTablePayload] = {}
    for table_name in table_names:
        frame = recipe_frames.get(table_name, pd.DataFrame())
        if table_name == "latest_score_frame":
            tables[table_name] = _frame_to_payload(frame, sort_by="score")
        elif table_name == "portfolio_targets":
            tables[table_name] = _frame_to_payload(frame, sort_by="rank", ascending=True)
        else:
            tables[table_name] = _frame_to_payload(frame)
    return tables


def filter_table_by_bundle(frame: pd.DataFrame, bundle: str) -> pd.DataFrame:
    if frame.empty or "bundle" not in frame.columns:
        return frame
    return frame.loc[frame["bundle"].astype(str) == str(bundle)].copy()


def _build_compare_analysis_summary(
    items: list[CompareItemResult],
    execution_rows: list[dict[str, Any]],
) -> ResearchSummary:
    if not items:
        return ResearchSummary()
    ranked = sorted(
        items,
        key=lambda item: (
            float(item.metrics.get("annualized_return") or item.metrics.get("net_total_return") or -999),
            float(item.metrics.get("sharpe_ratio") or -999),
        ),
        reverse=True,
    )
    winner = ranked[0]
    worst_execution = None
    if execution_rows:
        worst_execution = max(
            execution_rows,
            key=lambda row: abs(float(row.get("native_minus_validation_return") or 0.0)),
        )
    verdict = "promote" if len(ranked) > 1 and winner.metrics.get("net_total_return") is not None else "investigate"
    risks = []
    if worst_execution and abs(float(worst_execution.get("native_minus_validation_return") or 0.0)) >= 0.05:
        risks.append(f"{worst_execution.get('item')} 执行偏差较大，需先解释 native 与 validation 差异")
        verdict = "investigate"
    return ResearchSummary(
        headline=f"当前对比领先项：{winner.label}",
        verdict=verdict,
        key_findings=[
            f"{winner.label} 当前净收益/年化最优",
            f"Sharpe {winner.metrics.get('sharpe_ratio')}, Max DD {winner.metrics.get('max_drawdown')}",
        ],
        risks=risks,
        recommended_next_actions=[
            "优先查看 signal realization bridge 与 holding count drift，确认收益兑现路径",
            "再结合行业暴露判断是否可以 promote",
        ],
        promoted_recipe=winner.ref.recipe_name,
        metrics={"winner": winner.metrics},
    )


def _load_panel_summary_payload(path: Path) -> dict[str, Any]:
    source_mtime = path.stat().st_mtime
    metadata_path = path.with_name(f"{path.name}.metadata.json")
    if metadata_path.exists():
        source_mtime = max(source_mtime, metadata_path.stat().st_mtime)
    cache_key = str(path)
    cached = _PANEL_SUMMARY_CACHE.get(cache_key)
    if cached and cached[0] >= source_mtime and "universe_mode" in cached[1]:
        return cached[1]

    cache_path = _panel_summary_cache_path(path)
    if cache_path.exists() and cache_path.stat().st_mtime >= source_mtime:
        payload = _safe_read_json(cache_path, {})
        if "universe_mode" in payload:
            _PANEL_SUMMARY_CACHE[cache_key] = (cache_path.stat().st_mtime, payload)
            return payload

    panel = load_panel_dataframe(path)
    metadata = load_feature_panel_metadata(path)
    payload = {
        "panel_id": path.name,
        "name": path.name,
        "path": str(path),
        "format": path.suffix.lower().lstrip("."),
        "task_description": metadata.get("task_description"),
        "size_bytes": path.stat().st_size,
        "updated_at": _path_updated_at(path),
        "enrichment_scope": load_feature_panel_enrichment_scope(path, panel=panel),
        "universe_mode": metadata.get("universe_mode"),
        "universe_profile": metadata.get("universe_profile"),
        "requested_start_date": metadata.get("requested_start_date"),
        "requested_end_date": metadata.get("requested_end_date"),
        "summary": sanitize_for_json(summarize_panel(panel)),
    }
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    cache_mtime = cache_path.stat().st_mtime
    _PANEL_SUMMARY_CACHE[cache_key] = (cache_mtime, payload)
    return payload


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
    derived_recipes = registry.get("derived_recipes", {})
    if isinstance(derived_recipes, dict):
        for recipe_name, recipe_config in derived_recipes.items():
            if isinstance(recipe_config, dict):
                result[str(recipe_name)] = recipe_config
    return result


def _resolve_base_recipe_names_from_registry(registry: dict[str, Any]) -> list[str]:
    names: list[str] = []
    baseline_recipe = registry.get("baseline_recipe")
    if isinstance(baseline_recipe, dict) and baseline_recipe.get("name"):
        names.append(str(baseline_recipe["name"]))
    candidate_recipes = registry.get("candidate_recipes", {})
    if isinstance(candidate_recipes, dict):
        for recipe_name, recipe_config in candidate_recipes.items():
            if isinstance(recipe_config, dict):
                names.append(str(recipe_config.get("name") or recipe_name))
    # Keep stable order while dropping duplicates.
    return list(dict.fromkeys(names))


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
    payload = _load_panel_summary_payload(panel_path)
    summary = dict(payload.get("summary", {}))
    summary["path"] = str(panel_path)
    summary["enrichment_scope"] = payload.get("enrichment_scope")
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
        rolling_score_distinction=_normalize_value(overview_row.get("rolling_score_distinction")),
        walk_forward_score_distinction=_normalize_value(overview_row.get("walk_forward_score_distinction")),
        rolling_top1_sector_weight=_normalize_value(overview_row.get("rolling_top1_sector_weight")),
        walk_forward_top1_sector_weight=_normalize_value(overview_row.get("walk_forward_top1_sector_weight")),
        rolling_topk_mean_excess_return_4w=_normalize_value(overview_row.get("rolling_topk_mean_excess_return_4w")),
        walk_forward_topk_mean_excess_return_4w=_normalize_value(overview_row.get("walk_forward_topk_mean_excess_return_4w")),
        rolling_net_total_return=_normalize_value(overview_row.get("rolling_net_total_return")),
        walk_forward_net_total_return=_normalize_value(overview_row.get("walk_forward_net_total_return")),
        rolling_max_drawdown=_normalize_value(overview_row.get("rolling_max_drawdown")),
        walk_forward_max_drawdown=_normalize_value(overview_row.get("walk_forward_max_drawdown")),
        promotion_gate_passed=_normalize_value(promotion_gate.get("promotion_gate_passed")),
    )


def _build_run_level_charts(recipe_frames: dict[str, Any]) -> dict[str, ChartPayload]:
    charts: dict[str, ChartPayload] = {}
    for bundle in ("walk_forward", "rolling"):
        holding = _build_holding_trend_chart(filter_table_by_bundle(recipe_frames.get("holding_count_drift", pd.DataFrame()), bundle), key=f"{bundle}_holding_trend", title=f"{bundle} Actual vs Target Holds")
        blocked = _build_blocked_sell_chart(filter_table_by_bundle(recipe_frames.get("holding_count_drift", pd.DataFrame()), bundle), key=f"{bundle}_blocked_sell", title=f"{bundle} Blocked Sell Breakdown")
        bridge = _build_realization_bridge_chart(filter_table_by_bundle(recipe_frames.get("signal_realization_bridge", pd.DataFrame()), bundle), key=f"{bundle}_realization_bridge", title=f"{bundle} Signal Realization Bridge")
        exposure = _build_exposure_chart(filter_table_by_bundle(recipe_frames.get("sector_exposure_history", pd.DataFrame()), bundle), key=f"{bundle}_exposure", title=f"{bundle} Sector Exposure")
        for chart in (holding, blocked, bridge, exposure):
            if chart is not None:
                charts[chart.key] = chart
    return charts


def _build_recipe_chart_payloads(recipe_frames: dict[str, Any]) -> dict[str, ChartPayload]:
    return _build_run_level_charts(recipe_frames)


def _build_compare_winner_summary(
    items: list[CompareItemResult],
    execution_rows: list[dict[str, Any]],
    analysis_summary: ResearchSummary,
) -> CompareWinnerSummary:
    if not items:
        return CompareWinnerSummary()
    ranked = sorted(
        items,
        key=lambda item: (
            float(item.metrics.get("annualized_return") or item.metrics.get("net_total_return") or -999),
            float(item.metrics.get("sharpe_ratio") or -999),
        ),
        reverse=True,
    )
    winner = ranked[0]
    rejection_reasons: list[str] = []
    for row in execution_rows:
        drift = _first_present_numeric(row, ("native_minus_validation_return",))
        if drift is not None and abs(drift) >= 0.05:
            rejection_reasons.append(f"{row.get('item')} 执行偏差仍偏大")
    return CompareWinnerSummary(
        recommended_winner=winner.label,
        recommended_next_experiment=(analysis_summary.recommended_next_actions[0] if analysis_summary.recommended_next_actions else None),
        rejection_reasons=rejection_reasons,
        summary_label=f"winner: {winner.label}",
    )


def _collect_run_context(run_id: str) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    run_dir = NATIVE_WORKFLOW_ROOT / run_id
    if not run_dir.exists():
        raise FileNotFoundError(f"Unknown run: {run_id}")
    summary_payload = _safe_read_json(run_dir / "native_workflow_summary.json", {})
    index_payload = _load_run_index_payload(run_dir)
    return run_dir, summary_payload, index_payload


def list_runs(limit: int = 50) -> list[RunListItem]:
    return [_run_list_item_from_index_payload(_load_run_index_payload(run_dir)) for run_dir in _list_run_dirs()[:limit]]


def get_overview(limit: int = 8) -> OverviewResponse:
    runs = list_runs(limit=200)
    ready_runs = sum(1 for item in runs if item.quick_summary.artifact_status == "ready")
    return OverviewResponse(
        total_runs=len(runs),
        ready_runs=ready_runs,
        total_panels=len(_list_panel_files()),
        total_tasks=len(_list_task_dirs()),
        recent_runs=runs[:limit],
    )


def get_run_detail(run_id: str) -> RunDetail:
    run_dir, summary_payload, index_payload = _collect_run_context(run_id)
    quick_summary = RunQuickSummary(**_normalize_run_quick_summary_payload(index_payload.get("quick_summary", {})))
    recipe_summaries = _recipe_summaries_from_index_payload(index_payload)
    baseline_recipe = quick_summary.baseline_recipe or _select_baseline_recipe(quick_summary.recipe_names)
    baseline_frames = (
        _load_recipe_frames(
            run_dir,
            baseline_recipe,
            RUN_DETAIL_NODE_TABLES.union({"rebalance_audit"}),
        )
        if baseline_recipe
        else {}
    )
    recipe_configs = _resolve_recipe_registry(summary_payload)
    panel_path = _resolve_artifact_path(summary_payload.get("config", {}).get("panel_path"))
    panel_summary = _panel_summary_for_path(panel_path)
    nodes = _build_recipe_nodes(
        baseline_recipe or "baseline",
        baseline_frames,
        recipe_configs.get(baseline_recipe or "", {}),
        panel_summary,
    ) if baseline_recipe else []
    research_summary = _read_research_summary(run_dir)
    if not research_summary.headline:
        research_summary = _fallback_run_research_summary(index_payload)
    anomaly_summary = _build_execution_anomaly_summary(baseline_frames.get("holding_count_drift", pd.DataFrame()))
    experiment_scorecard = sanitize_for_json(_safe_read_json(_scorecard_path(run_dir), {}))
    if anomaly_periods := _top_anomaly_periods(baseline_frames.get("holding_count_drift", pd.DataFrame())):
        experiment_scorecard = {**experiment_scorecard, "anomaly_periods": anomaly_periods}
    return RunDetail(
        run_id=run_id,
        output_dir=str(run_dir),
        quick_summary=quick_summary,
        config=sanitize_for_json(summary_payload.get("config", {})),
        recipe_registry=sanitize_for_json(summary_payload.get("recipe_registry", {})),
        promotion_gate=sanitize_for_json(summary_payload.get("promotion_gate", {})),
        research_summary=research_summary,
        nodes=nodes,
        recipes=recipe_summaries,
        analysis_reports=_scan_analysis_reports(run_dir),
        artifact_inventory=_run_inventory(run_dir, quick_summary.recipe_names),
        experiment_scorecard=experiment_scorecard,
        execution_anomaly_summary=anomaly_summary,
        recommendation_actions=_build_recommendation_actions(
            source_type="run",
            source_id=run_id,
            anomaly_summary=anomaly_summary,
            include_analysis=True,
        ),
        run_level_charts=_build_run_level_charts(baseline_frames),
    )


def list_run_recipes(run_id: str) -> list[RecipeSummary]:
    run_dir = NATIVE_WORKFLOW_ROOT / run_id
    if not run_dir.exists():
        raise FileNotFoundError(f"Unknown run: {run_id}")
    return _recipe_summaries_from_index_payload(_load_run_index_payload(run_dir))


def get_run_artifact_inventory(run_id: str) -> ArtifactInventoryResponse:
    run_dir = NATIVE_WORKFLOW_ROOT / run_id
    if not run_dir.exists():
        raise FileNotFoundError(f"Unknown run: {run_id}")
    index_payload = _load_run_index_payload(run_dir)
    quick_summary = RunQuickSummary(**_normalize_run_quick_summary_payload(index_payload.get("quick_summary", {})))
    inventory = _run_inventory(run_dir, quick_summary.recipe_names)
    file_tree = sorted(
        [
            _artifact_ref(path, name=str(path.relative_to(run_dir)))
            for path in run_dir.rglob("*")
            if path.is_file() and path.name != RUN_INDEX_FILENAME
        ],
        key=lambda item: item.name,
    )
    return ArtifactInventoryResponse(owner_id=run_id, artifact_inventory=inventory + file_tree)


def get_recipe_detail(run_id: str, recipe_name: str) -> RecipeDetail:
    run_dir, summary_payload, index_payload = _collect_run_context(run_id)
    quick_summary = RunQuickSummary(**_normalize_run_quick_summary_payload(index_payload.get("quick_summary", {})))
    if recipe_name not in quick_summary.recipe_names:
        raise FileNotFoundError(f"Unknown recipe '{recipe_name}' for run '{run_id}'")
    recipe_config = _get_recipe_config(summary_payload, recipe_name)
    panel_path = _resolve_artifact_path(summary_payload.get("config", {}).get("panel_path"))
    panel_summary = _panel_summary_for_path(panel_path)
    baseline_recipe = quick_summary.baseline_recipe or _select_baseline_recipe(quick_summary.recipe_names)
    recipe_frames = _load_recipe_frames(
        run_dir,
        recipe_name,
        RUN_DETAIL_NODE_TABLES.union(RECIPE_DETAIL_INITIAL_TABLES).union(
            {
                "rebalance_audit",
                "rolling_native_report",
                "walk_forward_native_report",
                "rolling_performance_metrics",
                "walk_forward_performance_metrics",
            }
        ),
    )
    overview = _resolve_recipe_overview(
        recipe_name=recipe_name,
        recipe_frames=recipe_frames,
        summary_payload=summary_payload,
        index_payload=index_payload,
        baseline_recipe=baseline_recipe,
    )
    tables = _build_table_payloads(recipe_frames, RECIPE_DETAIL_INITIAL_TABLES)
    nodes = _build_recipe_nodes(recipe_name, recipe_frames, recipe_config, panel_summary)
    recipe_dir = run_dir / recipe_name
    research_summary = _read_research_summary(recipe_dir)
    if not research_summary.headline:
        research_summary = _fallback_recipe_research_summary(recipe_name, overview)
    anomaly_summary = _build_execution_anomaly_summary(recipe_frames.get("holding_count_drift", pd.DataFrame()))
    return RecipeDetail(
        run_id=run_id,
        recipe_name=recipe_name,
        recipe_config=sanitize_for_json(recipe_config),
        manifest=sanitize_for_json(recipe_frames.get("manifest", {})),
        overview=overview,
        research_summary=research_summary,
        nodes=nodes,
        tables=tables,
        analysis_reports=_scan_recipe_analysis_reports(run_dir, recipe_dir, recipe_name),
        artifact_inventory=_recipe_inventory(recipe_dir, prefix=recipe_name),
        portfolio_realization_summary=anomaly_summary,
        exposure_summary=_build_exposure_summary(recipe_frames.get("sector_exposure_history", pd.DataFrame())),
        recommendation_actions=_build_recommendation_actions(
            source_type="recipe",
            source_id=f"{run_id}:{recipe_name}",
            anomaly_summary=anomaly_summary,
            include_analysis=True,
        ),
        chart_payloads=_build_recipe_chart_payloads(recipe_frames),
    )


def get_recipe_tables(run_id: str, recipe_name: str, table_names: Iterable[str]) -> RecipeTablesResponse:
    run_dir = NATIVE_WORKFLOW_ROOT / run_id
    if not run_dir.exists():
        raise FileNotFoundError(f"Unknown run: {run_id}")
    index_payload = _load_run_index_payload(run_dir)
    quick_summary = RunQuickSummary(**_normalize_run_quick_summary_payload(index_payload.get("quick_summary", {})))
    if recipe_name not in quick_summary.recipe_names:
        raise FileNotFoundError(f"Unknown recipe '{recipe_name}' for run '{run_id}'")
    selected_names = [name for name in dict.fromkeys(table_names) if name in RECIPE_TABLE_FILES]
    recipe_frames = _load_recipe_frames(run_dir, recipe_name, selected_names)
    return RecipeTablesResponse(
        run_id=run_id,
        recipe_name=recipe_name,
        tables=_build_table_payloads(recipe_frames, selected_names),
    )


def _compare_curve_points(report_frame: pd.DataFrame, value_column: str) -> list[CompareTimeseriesPoint]:
    if report_frame.empty or "datetime" not in report_frame.columns or value_column not in report_frame.columns:
        return []
    frame = report_frame[["datetime", value_column]].copy()
    frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce")
    frame[value_column] = pd.to_numeric(frame[value_column], errors="coerce")
    frame = frame.dropna(subset=["datetime"]).sort_values("datetime")
    points: list[CompareTimeseriesPoint] = []
    for row in frame.to_dict(orient="records"):
        points.append(
            CompareTimeseriesPoint(
                date=pd.Timestamp(row["datetime"]).isoformat(),
                value=_normalize_value(row.get(value_column)),
            )
        )
    return points


def _compare_curve_signature(points: list[CompareTimeseriesPoint]) -> tuple[tuple[str, float | None], ...]:
    signature: list[tuple[str, float | None]] = []
    for point in points:
        value = None if point.value is None else round(float(point.value), 10)
        signature.append((point.date, value))
    return tuple(signature)


def compare_recipe_items(items: list[CompareItemRef]) -> CompareResponse:
    compare_items: list[CompareItemResult] = []
    metric_rows: list[dict[str, Any]] = []
    execution_rows: list[dict[str, Any]] = []
    slice_rows: list[dict[str, Any]] = []
    feature_importance: dict[str, DataTablePayload] = {}
    snapshots: dict[str, DataTablePayload] = {}
    signal_realization: dict[str, DataTablePayload] = {}
    sector_exposure: dict[str, DataTablePayload] = {}
    holding_count_drift: dict[str, DataTablePayload] = {}
    net_value_curves: list[CompareTimeseriesSeries] = []
    benchmark_candidates: list[list[CompareTimeseriesPoint]] = []
    holding_chart_rows: list[dict[str, Any]] = []
    exposure_chart_rows: list[dict[str, Any]] = []
    realization_chart_rows: list[dict[str, Any]] = []
    anomaly_summaries: dict[str, ExecutionAnomalySummary] = {}

    run_contexts: dict[str, tuple[Path, dict[str, Any], dict[str, Any]]] = {}
    recipe_frame_cache: dict[tuple[str, str], dict[str, Any]] = {}
    label_counts: dict[str, int] = {}

    for index, item in enumerate(items):
        if item.run_id not in run_contexts:
            run_contexts[item.run_id] = _collect_run_context(item.run_id)
        run_dir, summary_payload, index_payload = run_contexts[item.run_id]
        quick_summary = RunQuickSummary(**_normalize_run_quick_summary_payload(index_payload.get("quick_summary", {})))
        if item.recipe_name not in quick_summary.recipe_names:
            raise FileNotFoundError(f"Unknown recipe '{item.recipe_name}' for run '{item.run_id}'")
        cache_key = (item.run_id, item.recipe_name)
        if cache_key not in recipe_frame_cache:
            recipe_frame_cache[cache_key] = _load_recipe_frames(run_dir, item.recipe_name, COMPARE_REQUIRED_TABLES)
        recipe_frames = recipe_frame_cache[cache_key]
        overview = _recipe_overview_lookup_from_index_payload(index_payload).get(item.recipe_name, {})
        summary_frame = recipe_frames.get(f"{item.bundle}_summary", pd.DataFrame())
        report_frame = recipe_frames.get(f"{item.bundle}_native_report", pd.DataFrame())
        performance_frame = recipe_frames.get(f"{item.bundle}_performance_metrics", pd.DataFrame())
        exec_frame = recipe_frames.get("execution_diff_summary", pd.DataFrame())
        slice_frame = recipe_frames.get("slice_regime_summary", pd.DataFrame())
        importance_frame = recipe_frames.get(f"{item.bundle}_feature_importance", pd.DataFrame())
        snapshot_frame = recipe_frames.get("latest_score_frame", pd.DataFrame())
        signal_realization_frame = filter_table_by_bundle(recipe_frames.get("signal_realization_bridge", pd.DataFrame()), item.bundle)
        sector_exposure_frame = filter_table_by_bundle(recipe_frames.get("sector_exposure_history", pd.DataFrame()), item.bundle)
        holding_drift_frame = filter_table_by_bundle(recipe_frames.get("holding_count_drift", pd.DataFrame()), item.bundle)
        summary_row = _summary_row(summary_frame)
        report_tail = _last_row(report_frame)
        performance_row = _performance_metrics_row(performance_frame)
        exec_rows = exec_frame.to_dict(orient="records") if not exec_frame.empty else []
        matching_exec = next((row for row in exec_rows if row.get("bundle") == item.bundle), {})
        base_label = f"{item.run_id} / {item.recipe_name} / {item.bundle}"
        label_counts[base_label] = label_counts.get(base_label, 0) + 1
        label = base_label if label_counts[base_label] == 1 else f"{base_label} ({label_counts[base_label]})"
        metrics = {
            "rank_ic_ir": summary_row.get("rank_ic_ir"),
            "topk_mean_excess_return_4w": summary_row.get("topk_mean_excess_return_4w"),
            "topk_hit_rate": summary_row.get("topk_hit_rate"),
            "coverage_mean": summary_row.get("coverage_mean"),
            "used_feature_count": summary_row.get("used_feature_count") or overview.get("used_feature_count"),
            "net_total_return": _safe_native_report_return(report_frame),
            "max_drawdown": _safe_native_report_max_drawdown(report_frame),
            "annualized_return": performance_row.get("annualized_return"),
            "sharpe_ratio": performance_row.get("sharpe_ratio"),
            "calmar_ratio": performance_row.get("calmar_ratio"),
            "turnover": report_tail.get("turnover"),
        }
        compare_items.append(
            CompareItemResult(
                ref=item,
                label=label,
                metrics=sanitize_for_json(metrics),
                nodes=[],
            )
        )
        metric_rows.append({"item": label, **sanitize_for_json(metrics)})
        execution_rows.append({"item": label, **sanitize_for_json(matching_exec)})

        net_value_points = _compare_curve_points(report_frame, "net_value")
        if net_value_points:
            net_value_curves.append(
                CompareTimeseriesSeries(
                    key=f"item-{index}",
                    label=label,
                    role="item",
                    points=net_value_points,
                )
            )

        benchmark_points = _compare_curve_points(report_frame, "benchmark_value")
        if benchmark_points:
            benchmark_candidates.append(benchmark_points)

        for row in slice_frame.to_dict(orient="records") if not slice_frame.empty else []:
            if row.get("bundle") == item.bundle:
                slice_rows.append({"item": label, **row})

        importance_df = importance_frame.copy()
        if not importance_df.empty and "importance_gain" in importance_df.columns:
            importance_df = importance_df.sort_values("importance_gain", ascending=False).head(15)
        feature_importance[label] = _frame_to_payload(importance_df)

        snapshot_df = snapshot_frame.copy()
        sort_column = "score" if "score" in snapshot_df.columns else ("qlib_score" if "qlib_score" in snapshot_df.columns else None)
        if sort_column and not snapshot_df.empty:
            snapshot_df = snapshot_df.sort_values(sort_column, ascending=False).head(20)
        snapshots[label] = _frame_to_payload(snapshot_df)
        signal_realization[label] = _frame_to_payload(signal_realization_frame)
        sector_exposure[label] = _frame_to_payload(sector_exposure_frame)
        holding_count_drift[label] = _frame_to_payload(holding_drift_frame)
        anomaly_summaries[label] = _build_execution_anomaly_summary(holding_drift_frame)
        holding_chart_rows.append(
            {
                "item": label,
                "target_hold_count": 10,
                "avg_actual_hold_count": anomaly_summaries[label].avg_actual_hold_count,
                "max_actual_hold_count": anomaly_summaries[label].max_actual_hold_count,
                "avg_locked_residual_count": anomaly_summaries[label].avg_locked_residual_count,
                "avg_excess_holds": max(float(anomaly_summaries[label].avg_actual_hold_count or 0.0) - 10.0, 0.0),
                "max_excess_holds": max(float(anomaly_summaries[label].max_actual_hold_count or 0.0) - 10.0, 0.0),
            }
        )
        exposure_chart_rows.append(
            {
                "item": label,
                **_build_exposure_summary(sector_exposure_frame),
            }
        )
        realization_chart_rows.append(
            {
                "item": label,
                "mean_topk_mean_return_4w": _mean_or_none(signal_realization_frame, "topk_mean_return_4w"),
                "mean_realized_portfolio_return": _mean_or_none(signal_realization_frame, "realized_portfolio_return"),
                "mean_execution_cost_drag": _mean_or_none(signal_realization_frame, "execution_cost_drag"),
            }
        )

    if benchmark_candidates and len(benchmark_candidates) == len(items):
        first_signature = _compare_curve_signature(benchmark_candidates[0])
        if first_signature and all(_compare_curve_signature(points) == first_signature for points in benchmark_candidates[1:]):
            net_value_curves.append(
                CompareTimeseriesSeries(
                    key="shared-benchmark",
                    label="Shared benchmark",
                    role="benchmark",
                    points=benchmark_candidates[0],
                )
            )

    analysis_summary = _build_compare_analysis_summary(compare_items, execution_rows)
    winner_summary = _build_compare_winner_summary(compare_items, execution_rows, analysis_summary)
    dominant_winner = next((item for item in compare_items if item.label == winner_summary.recommended_winner), compare_items[0] if compare_items else None)
    comparison_actions = (
        _build_recommendation_actions(
            source_type="compare",
            source_id=json.dumps([item.model_dump(mode="json") for item in items], ensure_ascii=False),
            anomaly_summary=anomaly_summaries.get(dominant_winner.label, ExecutionAnomalySummary()) if dominant_winner else ExecutionAnomalySummary(),
            include_analysis=True,
        )
        if dominant_winner
        else []
    )
    chart_payloads: dict[str, ChartPayload] = {}
    if holding_chart_rows:
        chart_payloads["holding_summary"] = _chart_payload(
            "holding_summary",
            "Holding Drift Above TopK",
            [str(row["item"]) for row in holding_chart_rows],
            [
                ChartSeriesPayload(key="avg_excess_holds", label="Avg Excess Holds", values=[_normalize_value(row.get("avg_excess_holds")) for row in holding_chart_rows], role="bar", color="#dc2626"),
                ChartSeriesPayload(key="max_excess_holds", label="Max Excess Holds", values=[_normalize_value(row.get("max_excess_holds")) for row in holding_chart_rows], role="bar", color="#7c3aed"),
                ChartSeriesPayload(key="avg_locked_residual_count", label="Avg Locked Residual", values=[_normalize_value(row.get("avg_locked_residual_count")) for row in holding_chart_rows], role="bar", color="#f59e0b"),
            ],
            thresholds=[ChartThresholdPayload(label="TopK drift target", value=0.0, tone="info")],
        )
    if exposure_chart_rows:
        chart_payloads["exposure_summary"] = _chart_payload(
            "exposure_summary",
            "Sector Concentration Risk",
            [str(row["item"]) for row in exposure_chart_rows],
            [
                ChartSeriesPayload(key="top1_sector_weight", label="Top1 Sector Weight", values=[_normalize_value(row.get("top1_sector_weight")) for row in exposure_chart_rows], role="bar", color="#2563eb"),
                ChartSeriesPayload(key="top3_sector_concentration", label="Top3 Sector Concentration", values=[_normalize_value(row.get("top3_sector_concentration")) for row in exposure_chart_rows], role="bar", color="#ea580c"),
            ],
        )
    if realization_chart_rows:
        chart_payloads["realization_summary"] = _chart_payload(
            "realization_summary",
            "Signal Realization Gap",
            [str(row["item"]) for row in realization_chart_rows],
            [
                ChartSeriesPayload(key="mean_topk_mean_return_4w", label="Paper TopK Return", values=[_normalize_value(row.get("mean_topk_mean_return_4w")) for row in realization_chart_rows], role="bar", color="#2563eb"),
                ChartSeriesPayload(key="mean_realized_portfolio_return", label="Realized Return", values=[_normalize_value(row.get("mean_realized_portfolio_return")) for row in realization_chart_rows], role="bar", color="#059669"),
                ChartSeriesPayload(key="mean_execution_cost_drag", label="Execution Drag", values=[_normalize_value(row.get("mean_execution_cost_drag")) for row in realization_chart_rows], role="bar", color="#dc2626"),
            ],
        )

    return CompareResponse(
        items=compare_items,
        summary_metrics=_frame_to_payload(pd.DataFrame(metric_rows)),
        net_value_curves=net_value_curves,
        execution_gap=_frame_to_payload(pd.DataFrame(execution_rows)),
        slice_stability=_frame_to_payload(pd.DataFrame(slice_rows)),
        feature_importance=feature_importance,
        latest_signal_snapshot=snapshots,
        signal_realization=signal_realization,
        sector_exposure=sector_exposure,
        holding_count_drift=holding_count_drift,
        analysis_summary=analysis_summary,
        winner_summary=winner_summary,
        comparison_recommendation_actions=comparison_actions,
        chart_payloads=chart_payloads,
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
        payload = _load_panel_summary_payload(path)
        panels.append(
            PanelSummary(
                panel_id=str(payload.get("panel_id") or path.name),
                name=str(payload.get("name") or path.name),
                path=str(payload.get("path") or path),
                format=str(payload.get("format") or path.suffix.lower().lstrip(".")),
                size_bytes=payload.get("size_bytes"),
                updated_at=payload.get("updated_at"),
                enrichment_scope=payload.get("enrichment_scope"),
                universe_mode=payload.get("universe_mode"),
                universe_profile=payload.get("universe_profile"),
                requested_start_date=payload.get("requested_start_date"),
                requested_end_date=payload.get("requested_end_date"),
                summary=payload.get("summary", {}),
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
    cached_summary = _load_panel_summary_payload(path)
    summary = cached_summary.get("summary", {})
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
        enrichment_scope=cached_summary.get("enrichment_scope"),
        universe_mode=cached_summary.get("universe_mode"),
        universe_profile=cached_summary.get("universe_profile"),
        requested_start_date=cached_summary.get("requested_start_date"),
        requested_end_date=cached_summary.get("requested_end_date"),
        summary=summary,
        linked_runs=panel_links.get(str(path), panel_links.get(path.name, [])),
        columns=[str(column) for column in panel.columns],
        column_catalog=_frame_to_payload(pd.DataFrame(catalog_rows)),
        sample_rows=_frame_to_payload(sample_rows),
    )


def _task_paths(task_id: str) -> tuple[Path, Path, Path, Path, Path]:
    task_dir = TASKS_ROOT / task_id
    return (
        task_dir,
        task_dir / "task.json",
        task_dir / "stdout.log",
        task_dir / "stderr.log",
        task_dir / "result.json",
    )


def _queue_state_path() -> Path:
    return TASKS_ROOT / TASK_QUEUE_FILENAME


def _write_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(sanitize_for_json(payload), ensure_ascii=False, indent=2), encoding="utf-8")


def _default_manual_source() -> TaskSourceRef:
    return TaskSourceRef(kind="manual", source_id="manual", label="Manual Task")


def _pid_exists(pid: int | None) -> bool:
    if pid is None:
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _load_task_raw(task_id: str) -> ResearchTaskSummary:
    _, task_file, _, _, _ = _task_paths(task_id)
    if not task_file.exists():
        raise FileNotFoundError(f"Unknown task: {task_id}")
    return ResearchTaskSummary(**_safe_read_json(task_file, {}))


def _task_sort_key(summary: ResearchTaskSummary) -> tuple[str, str]:
    return (
        str(summary.finished_at or summary.started_at or summary.created_at or ""),
        summary.task_id,
    )


def _available_task_actions(summary: ResearchTaskSummary, queue_state: TaskQueueState, queue_position: int | None) -> list[str]:
    if summary.status == "queued":
        actions = ["view"]
        if queue_position and queue_position > 1:
            actions.append("move_up")
        if queue_position and queue_position < len(queue_state.queued_task_ids):
            actions.append("move_down")
        actions.append("remove")
        return actions
    if summary.status == "running":
        return ["view", "stop"]
    if summary.status == "stopping":
        return ["view"]
    return ["view"]


def _hydrate_task_summary(summary: ResearchTaskSummary, queue_state: TaskQueueState) -> ResearchTaskSummary:
    queue_position = None
    if summary.task_id in queue_state.queued_task_ids:
        queue_position = queue_state.queued_task_ids.index(summary.task_id) + 1
    return summary.model_copy(
        update={
            "queue_position": queue_position,
            "source_ref": summary.source_ref or _default_manual_source(),
            "available_actions": _available_task_actions(summary, queue_state, queue_position),
            "result_path": str(_task_paths(summary.task_id)[4]),
        }
    )


def _write_queue_state(state: TaskQueueState) -> TaskQueueState:
    payload = state.model_copy(update={"updated_at": _now_iso()})
    _write_json_file(_queue_state_path(), payload.model_dump(mode="json"))
    return payload


def _load_queue_state() -> TaskQueueState:
    TASKS_ROOT.mkdir(parents=True, exist_ok=True)
    queue_path = _queue_state_path()
    raw_payload = _safe_read_json(queue_path, {})
    try:
        state = TaskQueueState(**raw_payload)
    except Exception:
        state = TaskQueueState()
    changed = not queue_path.exists()

    known_task_ids = set()
    queued_by_discovery: list[str] = []
    for task_dir in _list_task_dirs():
        task_file = task_dir / "task.json"
        if not task_file.exists():
            continue
        known_task_ids.add(task_dir.name)
        try:
            task_summary = ResearchTaskSummary(**_safe_read_json(task_file, {}))
        except Exception:
            continue
        if task_summary.status == "queued":
            queued_by_discovery.append(task_dir.name)

    normalized_queue: list[str] = []
    for task_id in state.queued_task_ids:
        if task_id not in known_task_ids:
            changed = True
            continue
        try:
            task_summary = _load_task_raw(task_id)
        except FileNotFoundError:
            changed = True
            continue
        if task_summary.status != "queued":
            changed = True
            continue
        if task_id not in normalized_queue:
            normalized_queue.append(task_id)

    for task_id in reversed(queued_by_discovery):
        if task_id not in normalized_queue and task_id != state.running_task_id:
            normalized_queue.append(task_id)
            changed = True
    state = state.model_copy(update={"queued_task_ids": normalized_queue})

    if state.running_task_id:
        try:
            running_summary = _load_task_raw(state.running_task_id)
        except FileNotFoundError:
            state = state.model_copy(update={"running_task_id": None, "dispatcher_status": "idle", "dispatcher_pid": None})
            changed = True
        else:
            worker_pid = running_summary.metadata.get("worker_pid")
            if running_summary.status in {"running", "stopping"} and _pid_exists(int(worker_pid) if worker_pid is not None else None):
                state = state.model_copy(
                    update={
                        "dispatcher_pid": int(worker_pid),
                        "dispatcher_status": "stopping" if running_summary.status == "stopping" else "running",
                    }
                )
            elif running_summary.status in {"running", "stopping"}:
                update_task_status(
                    _task_paths(state.running_task_id)[0],
                    status="cancelled" if running_summary.status == "stopping" else "failed",
                    finished_at=_now_iso(),
                    message="Task interrupted unexpectedly",
                    metadata={**running_summary.metadata, "worker_error": "worker process not found"},
                )
                state = state.model_copy(update={"running_task_id": None, "dispatcher_status": "idle", "dispatcher_pid": None})
                changed = True
            elif running_summary.status not in {"running", "stopping"}:
                state = state.model_copy(update={"running_task_id": None})
                changed = True

    if state.dispatcher_pid is not None and not _pid_exists(state.dispatcher_pid):
        if state.running_task_id is None:
            state = state.model_copy(update={"dispatcher_pid": None, "dispatcher_status": "idle"})
        changed = True

    if state.running_task_id is None and state.dispatcher_status == "stopping":
        state = state.model_copy(update={"dispatcher_status": "idle", "dispatcher_pid": None})
        changed = True

    if changed or state.updated_at is None:
        state = _write_queue_state(state)
    return state


def _write_task(summary: ResearchTaskSummary) -> None:
    task_dir, task_file, _, _, _ = _task_paths(summary.task_id)
    task_dir.mkdir(parents=True, exist_ok=True)
    task_file.write_text(summary.model_dump_json(indent=2), encoding="utf-8")


def _task_timeline(summary: ResearchTaskSummary) -> list[dict[str, Any]]:
    timeline = [
        {"label": "Created", "at": summary.created_at, "status": "queued"},
        {"label": "Started", "at": summary.started_at, "status": "running"},
        {"label": "Finished", "at": summary.finished_at, "status": summary.status},
    ]
    return [item for item in timeline if item["at"]]


def _task_result_payload(task_id: str) -> dict[str, Any]:
    result_path = _task_paths(task_id)[4]
    payload = _safe_read_json(result_path, {})
    return payload if isinstance(payload, dict) else {}


def _build_task_detail(summary: ResearchTaskSummary, queue_state: TaskQueueState) -> ResearchTaskDetail:
    hydrated = _hydrate_task_summary(summary, queue_state)
    return ResearchTaskDetail(
        **hydrated.model_dump(mode="json"),
        result=_task_result_payload(summary.task_id),
        timeline=_task_timeline(hydrated),
    )


def _task_logs_updated_at(stdout_path: Path, stderr_path: Path) -> str | None:
    candidates = [path.stat().st_mtime for path in (stdout_path, stderr_path) if path.exists()]
    if not candidates:
        return None
    return datetime.fromtimestamp(max(candidates)).astimezone().isoformat()


def _build_export_panel_command(request: ExportPanelTaskRequest) -> list[str]:
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
    if request.universe_mode:
        command += ["--universe-mode", request.universe_mode]
    if request.enrichment_scope:
        command += ["--enrichment-scope", request.enrichment_scope]
    if request.description:
        command += ["--task-description", request.description]
    for feature_group in request.feature_groups or []:
        command += ["--feature-group", feature_group]
    for feature_name in request.included_features or []:
        command += ["--include-feature", feature_name]
    for feature_name in request.excluded_features or []:
        command += ["--exclude-feature", feature_name]
    if request.symbols:
        command += ["--symbols", *request.symbols]
    return command


def _enqueue_task(summary: ResearchTaskSummary) -> ResearchTaskSummary:
    _write_task(summary)
    state = _load_queue_state()
    if summary.task_id not in state.queued_task_ids and summary.task_id != state.running_task_id:
        state = _write_queue_state(
            state.model_copy(update={"queued_task_ids": [*state.queued_task_ids, summary.task_id]})
        )
    return _hydrate_task_summary(summary, state)


def list_tasks() -> TaskBoardResponse:
    state = _load_queue_state()
    running_task = None
    if state.running_task_id:
        try:
            running_task = _hydrate_task_summary(_load_task_raw(state.running_task_id), state)
        except FileNotFoundError:
            running_task = None

    queued_tasks: list[ResearchTaskSummary] = []
    for task_id in state.queued_task_ids:
        try:
            queued_tasks.append(_hydrate_task_summary(_load_task_raw(task_id), state))
        except FileNotFoundError:
            continue

    queued_ids = set(state.queued_task_ids)
    history_summaries: list[ResearchTaskSummary] = []
    for task_dir in _list_task_dirs():
        task_file = task_dir / "task.json"
        if not task_file.exists():
            continue
        summary = ResearchTaskSummary(**_safe_read_json(task_file, {}))
        if summary.task_id == state.running_task_id or summary.task_id in queued_ids:
            continue
        history_summaries.append(_hydrate_task_summary(summary, state))
    history_summaries.sort(key=_task_sort_key, reverse=True)

    return TaskBoardResponse(
        running_task=running_task,
        queued_tasks=queued_tasks,
        history_tasks=history_summaries,
        queue_state=state,
        feature_group_options=sorted(FEATURE_GROUP_COLUMNS),
    )


def get_task(task_id: str) -> ResearchTaskDetail:
    state = _load_queue_state()
    return _build_task_detail(_load_task_raw(task_id), state)


def get_task_logs(task_id: str) -> TaskLogResponse:
    _, _, stdout_path, stderr_path, _ = _task_paths(task_id)
    return TaskLogResponse(
        task_id=task_id,
        stdout=stdout_path.read_text(encoding="utf-8") if stdout_path.exists() else "",
        stderr=stderr_path.read_text(encoding="utf-8") if stderr_path.exists() else "",
        updated_at=_task_logs_updated_at(stdout_path, stderr_path),
    )


def _default_task_source(source_ref: TaskSourceRef | None) -> TaskSourceRef:
    return source_ref or _default_manual_source()


def create_export_panel_task(request: ExportPanelTaskRequest) -> ResearchTaskSummary:
    TASKS_ROOT.mkdir(parents=True, exist_ok=True)
    task_id = f"task-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"
    task_paths = _task_paths(task_id)
    summary = ResearchTaskSummary(
        task_id=task_id,
        task_kind="export_panel",
        status="queued",
        display_name=request.display_name or "Export Panel",
        description=request.description,
        requested_by=request.requested_by,
        created_at=_now_iso(),
        output_dir=str(_resolve_artifact_path(request.output) or request.output),
        message="Task queued",
        command=_build_export_panel_command(request),
        config_payload=request.model_dump(mode="json"),
        logs={"stdout": str(task_paths[2]), "stderr": str(task_paths[3])},
        metadata={"cwd": str(PROJECT_ROOT)},
        source_ref=_default_task_source(request.source_ref),
        result_path=str(task_paths[4]),
    )
    return _enqueue_task(summary)


def create_native_workflow_task(request: RunNativeWorkflowTaskRequest) -> ResearchTaskSummary:
    TASKS_ROOT.mkdir(parents=True, exist_ok=True)
    task_id = f"task-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"
    config_payload = deepcopy(request.config_payload)
    if request.description:
        config_payload["task_description"] = request.description
    execution_panel_path = _resolve_existing_artifact_string(config_payload.get("execution_panel_path"))
    if execution_panel_path:
        config_payload["execution_panel_path"] = execution_panel_path
    else:
        fallback_execution_panel_path = _fallback_execution_panel_path_for_source(request.source_ref)
        if fallback_execution_panel_path:
            config_payload["execution_panel_path"] = fallback_execution_panel_path
        elif "execution_panel_path" in config_payload and config_payload.get("execution_panel_path"):
            # Let the workflow fall back to its own default generation path instead of
            # passing a guaranteed-missing artifact path through to the worker.
            config_payload.pop("execution_panel_path", None)
    command_string = build_native_workflow_cli_command(
        config_overrides=config_payload,
        recipe_names=request.recipe_names,
    ).replace("\\\n", " ")
    command = shlex.split(command_string)
    output_dir = config_payload.get("output_dir", "artifacts/native_workflow/csi300")
    task_paths = _task_paths(task_id)
    summary = ResearchTaskSummary(
        task_id=task_id,
        task_kind="run_native_workflow",
        status="queued",
        display_name=request.display_name or "Run Native Workflow",
        description=request.description,
        requested_by=request.requested_by,
        created_at=_now_iso(),
        output_dir=str(_resolve_artifact_path(output_dir) or output_dir),
        message="Task queued",
        command=command,
        config_payload={
            "config_payload": config_payload,
            "recipe_names": list(request.recipe_names or []),
        },
        logs={"stdout": str(task_paths[2]), "stderr": str(task_paths[3])},
        metadata={"cwd": str(PROJECT_ROOT)},
        source_ref=_default_task_source(request.source_ref),
        result_path=str(task_paths[4]),
    )
    return _enqueue_task(summary)


def _default_analysis_output_dir(request: RunResearchAnalysisTaskRequest) -> str:
    if request.output_dir:
        return request.output_dir
    if request.source_kind == "recipe" and request.run_id and request.recipe_name:
        return str(NATIVE_WORKFLOW_ROOT / request.run_id / request.recipe_name / "analysis")
    if request.source_kind == "run" and request.run_id:
        return str(NATIVE_WORKFLOW_ROOT / request.run_id / "analysis")
    if request.source_kind == "compare":
        compare_id = datetime.now().strftime("%Y%m%d%H%M%S")
        return str(ARTIFACTS_ROOT / "compare_analysis" / compare_id / "analysis")
    return str(ARTIFACTS_ROOT / "analysis")


def _normalized_analysis_batch_mode(request: RunResearchAnalysisTaskRequest) -> str:
    if request.batch_mode in {"run_only", "run_plus_lead_recipe", "run_plus_all_recipes"}:
        return request.batch_mode
    return "run_plus_all_recipes" if request.include_all_recipes else "run_only"


def _build_research_analysis_command(request: RunResearchAnalysisTaskRequest, output_dir: str) -> list[str]:
    batch_mode = _normalized_analysis_batch_mode(request)
    command = [
        "uv",
        "run",
        "python",
        "scripts/run_research_analysis.py",
        "--source-kind",
        request.source_kind,
        "--analysis-template",
        request.analysis_template,
        "--analysis-engine",
        request.analysis_engine,
        "--output-dir",
        output_dir,
    ]
    if request.source_kind == "run" and batch_mode != "run_only":
        command += ["--batch-mode", batch_mode]
    if request.run_id:
        command += ["--run-id", request.run_id]
    if request.recipe_name:
        command += ["--recipe-name", request.recipe_name]
    skills = list(request.skills or [])
    if request.analysis_template == NATIVE_WORKFLOW_SYSTEM_TEMPLATE and NATIVE_WORKFLOW_ANALYSIS_SKILL_NAME not in skills:
        skills.append(NATIVE_WORKFLOW_ANALYSIS_SKILL_NAME)
    for skill in skills:
        command += ["--skill", skill]
    if request.compare_items:
        command += ["--compare-items-json", json.dumps(sanitize_for_json(request.compare_items), ensure_ascii=False)]
    return command


def create_research_analysis_task(request: RunResearchAnalysisTaskRequest) -> ResearchTaskSummary:
    TASKS_ROOT.mkdir(parents=True, exist_ok=True)
    task_id = f"task-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"
    output_dir = _default_analysis_output_dir(request)
    task_paths = _task_paths(task_id)
    batch_mode = _normalized_analysis_batch_mode(request)
    config_payload = request.model_dump(mode="json")
    config_payload["batch_mode"] = batch_mode
    config_payload["include_all_recipes"] = batch_mode == "run_plus_all_recipes"
    if request.analysis_template == NATIVE_WORKFLOW_SYSTEM_TEMPLATE:
        skills = list(config_payload.get("skills") or [])
        if NATIVE_WORKFLOW_ANALYSIS_SKILL_NAME not in skills:
            skills.append(NATIVE_WORKFLOW_ANALYSIS_SKILL_NAME)
        config_payload["skills"] = skills
    summary = ResearchTaskSummary(
        task_id=task_id,
        task_kind="run_research_analysis",
        status="queued",
        display_name=request.display_name or "Run Research Analysis",
        description=request.description,
        requested_by=request.requested_by,
        created_at=_now_iso(),
        output_dir=str(_resolve_artifact_path(output_dir) or output_dir),
        message="Task queued",
        command=_build_research_analysis_command(request, output_dir),
        config_payload=config_payload,
        logs={"stdout": str(task_paths[2]), "stderr": str(task_paths[3])},
        metadata={"cwd": str(PROJECT_ROOT)},
        source_ref=_default_task_source(request.source_ref),
        result_path=str(task_paths[4]),
    )
    return _enqueue_task(summary)


def update_task_status(task_dir: Path, **updates: Any) -> ResearchTaskSummary:
    task_file = task_dir / "task.json"
    payload = _safe_read_json(task_file, {})
    payload.update(sanitize_for_json(updates))
    summary = ResearchTaskSummary(**payload)
    task_file.write_text(summary.model_dump_json(indent=2), encoding="utf-8")
    return summary


def _maybe_kill_process_group(pid: int | None) -> None:
    if pid is None:
        return
    try:
        os.killpg(int(pid), signal.SIGTERM)
    except (OSError, ProcessLookupError):
        try:
            os.kill(int(pid), signal.SIGTERM)
        except (OSError, ProcessLookupError):
            return


def run_task_queue() -> TaskBoardResponse:
    state = _load_queue_state()
    if state.running_task_id and _pid_exists(state.dispatcher_pid):
        return list_tasks()
    if not state.queued_task_ids:
        _write_queue_state(state.model_copy(update={"dispatcher_status": "idle", "dispatcher_pid": None}))
        return list_tasks()

    process = subprocess.Popen(
        [sys.executable, "-m", "qlib_research.app.task_dispatcher"],
        cwd=PROJECT_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )
    _write_queue_state(state.model_copy(update={"dispatcher_status": "running", "dispatcher_pid": process.pid}))
    return list_tasks()


def stop_current_task() -> TaskBoardResponse:
    state = _load_queue_state()
    if not state.running_task_id:
        return list_tasks()

    summary = _load_task_raw(state.running_task_id)
    update_task_status(
        _task_paths(summary.task_id)[0],
        status="stopping",
        message="Stop requested",
        metadata={**summary.metadata, "stop_requested_at": _now_iso()},
    )
    _write_queue_state(state.model_copy(update={"dispatcher_status": "stopping"}))

    pid_candidates = [
        state.dispatcher_pid,
        int(summary.metadata.get("worker_pid")) if summary.metadata.get("worker_pid") is not None else None,
        int(summary.metadata.get("child_pid")) if summary.metadata.get("child_pid") is not None else None,
    ]
    for pid in pid_candidates:
        if _pid_exists(pid):
            _maybe_kill_process_group(pid)
            break
    return list_tasks()


def reorder_tasks(request: TaskReorderRequest) -> TaskBoardResponse:
    state = _load_queue_state()
    requested_ids = list(request.queued_task_ids)
    if sorted(requested_ids) != sorted(state.queued_task_ids):
        raise ValueError("Reorder payload must contain exactly the queued task ids")
    _write_queue_state(state.model_copy(update={"queued_task_ids": requested_ids}))
    return list_tasks()


def remove_task(task_id: str) -> TaskBoardResponse:
    state = _load_queue_state()
    if task_id not in state.queued_task_ids:
        raise ValueError("Only queued tasks can be removed")
    summary = _load_task_raw(task_id)
    if summary.status != "queued":
        raise ValueError("Only queued tasks can be removed")
    remaining = [item for item in state.queued_task_ids if item != task_id]
    _write_queue_state(state.model_copy(update={"queued_task_ids": remaining}))
    shutil.rmtree(_task_paths(task_id)[0], ignore_errors=True)
    return list_tasks()


def _suggest_rerun_output_dir(output_dir: Any, run_id: str) -> str:
    base = str(output_dir or f"artifacts/native_workflow/{run_id}")
    base = base.rstrip("/") or f"artifacts/native_workflow/{run_id}"
    return f"{base}-rerun-{datetime.now().strftime('%Y%m%d%H%M%S')}"


def _resolve_existing_artifact_string(value: Any) -> str | None:
    if value in (None, ""):
        return None
    raw = str(value)
    resolved = _resolve_artifact_path(raw) or Path(raw).expanduser()
    return str(resolved) if resolved.exists() else None


def _fallback_execution_panel_path_for_source(source_ref: TaskSourceRef | None) -> str | None:
    if not source_ref or source_ref.kind != "run":
        return None
    try:
        detail = get_run_detail(source_ref.source_id)
    except Exception:
        return None
    return _resolve_existing_artifact_string(detail.config.get("execution_panel_path"))


def get_run_task_preset(run_id: str) -> TaskPresetResponse:
    detail = get_run_detail(run_id)
    payload = deepcopy(detail.config)
    payload["output_dir"] = _suggest_rerun_output_dir(payload.get("output_dir"), detail.run_id)
    base_recipe_names = _resolve_base_recipe_names_from_registry(detail.recipe_registry)
    return TaskPresetResponse(
        task_kind="run_native_workflow",
        display_name=f"Rerun {detail.run_id}",
        source_ref=TaskSourceRef(kind="run", source_id=detail.run_id, label=detail.run_id, path=detail.output_dir),
        payload={
            "display_name": f"Rerun {detail.run_id}",
            "description": detail.quick_summary.task_description,
            "requested_by": "webapp",
            "source_ref": {"kind": "run", "source_id": detail.run_id, "label": detail.run_id, "path": detail.output_dir},
            "config_payload": payload,
            "recipe_names": base_recipe_names or [recipe.recipe_name for recipe in detail.recipes],
        },
    )


def get_run_analysis_task_preset(run_id: str) -> TaskPresetResponse:
    detail = get_run_detail(run_id)
    return TaskPresetResponse(
        task_kind="run_research_analysis",
        display_name=f"Diagnose {detail.run_id}",
        source_ref=TaskSourceRef(kind="run", source_id=detail.run_id, label=detail.run_id, path=detail.output_dir),
        payload={
            "display_name": f"Diagnose {detail.run_id}",
            "description": detail.research_summary.headline,
            "requested_by": "webapp",
            "source_ref": {"kind": "run", "source_id": detail.run_id, "label": detail.run_id, "path": detail.output_dir},
            "source_kind": "run",
            "batch_mode": "run_only",
            "include_all_recipes": False,
            "run_id": detail.run_id,
            "analysis_template": NATIVE_WORKFLOW_SYSTEM_TEMPLATE,
            "analysis_engine": "codex_cli",
            "skills": [NATIVE_WORKFLOW_ANALYSIS_SKILL_NAME],
            "output_dir": str(Path(detail.output_dir) / "analysis"),
        },
    )


def get_recipe_analysis_task_preset(run_id: str, recipe_name: str) -> TaskPresetResponse:
    detail = get_recipe_detail(run_id, recipe_name)
    recipe_dir = NATIVE_WORKFLOW_ROOT / run_id / recipe_name
    return TaskPresetResponse(
        task_kind="run_research_analysis",
        display_name=f"Analyze {run_id}/{recipe_name}",
        source_ref=TaskSourceRef(kind="recipe", source_id=f"{run_id}:{recipe_name}", label=recipe_name, path=str(recipe_dir)),
        payload={
            "display_name": f"Analyze {run_id}/{recipe_name}",
            "description": detail.research_summary.headline,
            "requested_by": "webapp",
            "source_ref": {"kind": "recipe", "source_id": f"{run_id}:{recipe_name}", "label": recipe_name, "path": str(recipe_dir)},
            "source_kind": "recipe",
            "run_id": run_id,
            "recipe_name": recipe_name,
            "analysis_template": "experiment_review",
            "analysis_engine": "codex_cli",
            "skills": [],
            "output_dir": str(recipe_dir / "analysis"),
        },
    )


def get_panel_task_preset(panel_id: str) -> TaskPresetResponse:
    panel = get_panel_detail(panel_id)
    matched_task: ResearchTaskSummary | None = None
    resolved_panel_path = str(_resolve_artifact_path(panel.path) or panel.path)
    for task in list_tasks().history_tasks:
        if task.task_kind != "export_panel" or task.status != "succeeded":
            continue
        output_value = task.config_payload.get("output") or task.output_dir
        resolved_output = str(_resolve_artifact_path(str(output_value)) or output_value)
        if resolved_output == resolved_panel_path:
            matched_task = task
            break

    if matched_task is not None:
        payload = deepcopy(matched_task.config_payload)
    else:
        payload = {
            "display_name": f"Re-export {panel.name}",
            "description": panel.task_description,
            "requested_by": "webapp",
            "output": panel.path,
            "start_date": panel.requested_start_date or panel.summary.get("start_date"),
            "end_date": panel.requested_end_date or panel.summary.get("end_date"),
            "symbols": None,
            "universe_profile": panel.universe_profile,
            "universe_mode": panel.universe_mode or "historical_membership",
            "batch_size": 300,
            "enrichment_scope": panel.enrichment_scope,
            "feature_groups": None,
            "included_features": None,
            "excluded_features": None,
        }
    payload["display_name"] = payload.get("display_name") or f"Re-export {panel.name}"
    payload["description"] = payload.get("description") or panel.task_description
    payload["requested_by"] = "webapp"
    payload["source_ref"] = {"kind": "panel", "source_id": panel.panel_id, "label": panel.name, "path": panel.path}
    return TaskPresetResponse(
        task_kind="export_panel",
        display_name=str(payload["display_name"]),
        source_ref=TaskSourceRef(kind="panel", source_id=panel.panel_id, label=panel.name, path=panel.path),
        payload=payload,
    )


def task_worker_run(task_dir: Path) -> int:
    task_file = task_dir / "task.json"
    payload = _safe_read_json(task_file, {})
    summary = ResearchTaskSummary(**payload)
    stdout_path = task_dir / "stdout.log"
    stderr_path = task_dir / "stderr.log"
    result_path = task_dir / "result.json"
    cancellation = {"requested": False}
    process_holder: dict[str, subprocess.Popen[str]] = {}

    original_sigterm = signal.getsignal(signal.SIGTERM)
    original_sigint = signal.getsignal(signal.SIGINT)

    def _handle_cancel(_signum: int, _frame: Any) -> None:
        cancellation["requested"] = True
        update_task_status(
            task_dir,
            status="stopping",
            message="Stop requested",
            metadata={**summary.metadata, "worker_pid": os.getpid(), "stop_requested_at": _now_iso()},
        )
        process = process_holder.get("process")
        if process is not None and process.poll() is None:
            _maybe_kill_process_group(process.pid)

    signal.signal(signal.SIGTERM, _handle_cancel)
    signal.signal(signal.SIGINT, _handle_cancel)
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
                start_new_session=True,
            )
            process_holder["process"] = process
            summary = update_task_status(
                task_dir,
                metadata={**summary.metadata, "worker_pid": os.getpid(), "child_pid": process.pid},
            )
            return_code = process.wait()

        final_status = "cancelled" if cancellation["requested"] else ("succeeded" if return_code == 0 else "failed")
        final_message = "Task cancelled by user" if cancellation["requested"] else ("Task completed" if return_code == 0 else f"Task failed with exit code {return_code}")
        finished_at = _now_iso()
        update_task_status(
            task_dir,
            status=final_status,
            finished_at=finished_at,
            message=final_message,
            metadata={
                **summary.metadata,
                "worker_pid": os.getpid(),
                "return_code": return_code,
                "cancel_requested": cancellation["requested"],
            },
        )
        _write_json_file(
            result_path,
            {
                "task_id": summary.task_id,
                "status": final_status,
                "return_code": return_code,
                "finished_at": finished_at,
            },
        )
        return 0 if final_status in {"succeeded", "cancelled"} else (return_code or 1)
    except Exception as exc:  # pragma: no cover - defensive filesystem/subprocess handling
        final_status = "cancelled" if cancellation["requested"] else "failed"
        update_task_status(
            task_dir,
            status=final_status,
            finished_at=_now_iso(),
            message="Task cancelled by user" if cancellation["requested"] else f"Task worker crashed: {exc}",
            metadata={**summary.metadata, "worker_pid": os.getpid(), "worker_error": str(exc)},
        )
        with stderr_path.open("a", encoding="utf-8") as stderr_handle:
            stderr_handle.write(f"[task-worker] crashed: {exc}\n")
        return 0 if cancellation["requested"] else 1
    finally:
        signal.signal(signal.SIGTERM, original_sigterm)
        signal.signal(signal.SIGINT, original_sigint)


def task_dispatcher_run() -> int:
    TASKS_ROOT.mkdir(parents=True, exist_ok=True)
    original_sigterm = signal.getsignal(signal.SIGTERM)
    original_sigint = signal.getsignal(signal.SIGINT)
    stop_requested = {"value": False}

    def _handle_stop(_signum: int, _frame: Any) -> None:
        stop_requested["value"] = True
        state = _load_queue_state()
        _write_queue_state(state.model_copy(update={"dispatcher_status": "stopping", "dispatcher_pid": os.getpid()}))

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)
    _write_queue_state(_load_queue_state().model_copy(update={"dispatcher_status": "running", "dispatcher_pid": os.getpid()}))
    try:
        while True:
            state = _load_queue_state()
            if stop_requested["value"] or state.dispatcher_status == "stopping":
                _write_queue_state(state.model_copy(update={"dispatcher_status": "idle", "dispatcher_pid": None, "running_task_id": None}))
                return 0
            if not state.queued_task_ids:
                _write_queue_state(state.model_copy(update={"dispatcher_status": "idle", "dispatcher_pid": None, "running_task_id": None}))
                return 0

            task_id = state.queued_task_ids[0]
            state = _write_queue_state(
                state.model_copy(
                    update={
                        "queued_task_ids": state.queued_task_ids[1:],
                        "running_task_id": task_id,
                        "dispatcher_status": "running",
                        "dispatcher_pid": os.getpid(),
                    }
                )
            )
            task_worker_run(_task_paths(task_id)[0])

            state = _load_queue_state()
            if state.running_task_id == task_id:
                state = state.model_copy(update={"running_task_id": None})
            if stop_requested["value"] or state.dispatcher_status == "stopping":
                _write_queue_state(state.model_copy(update={"dispatcher_status": "idle", "dispatcher_pid": None, "running_task_id": None}))
                return 0
            if not state.queued_task_ids:
                _write_queue_state(state.model_copy(update={"dispatcher_status": "idle", "dispatcher_pid": None, "running_task_id": None}))
                return 0
            _write_queue_state(state.model_copy(update={"dispatcher_status": "running", "dispatcher_pid": os.getpid()}))
    finally:
        signal.signal(signal.SIGTERM, original_sigterm)
        signal.signal(signal.SIGINT, original_sigint)
