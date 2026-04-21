"""Typed contracts for the QlibResearch workbench API."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

TaskKind = Literal["export_panel", "run_native_workflow", "run_convergence", "publish_model", "sync_model", "run_research_analysis"]
TaskStatus = Literal["queued", "running", "stopping", "succeeded", "failed", "cancelled"]
DiagnosticStatus = Literal["healthy", "warning", "danger", "missing", "info"]
ResearchVerdict = Literal["incumbent", "promoted", "rejected", "needs_explanation", "hold", "reject", "investigate"]
NodeKey = Literal[
    "panel_input",
    "label_definition",
    "feature_prefilter",
    "normalization",
    "score_quality",
    "rolling_backtest",
    "walk_forward_backtest",
    "execution_gap",
    "slice_stability",
    "signal_snapshot",
]
Tone = Literal["default", "info", "success", "warning", "danger", "neutral"]
ExecutionSeverity = Literal["low", "medium", "high", "critical"]
ChartKind = Literal["line", "area", "stacked_bar"]


class DataTablePayload(BaseModel):
    columns: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)


class EvidenceItem(BaseModel):
    label: str
    value: Any | None = None
    tone: Tone = "neutral"


class ArtifactRef(BaseModel):
    name: str
    path: str
    exists: bool
    size_bytes: int | None = None
    updated_at: str | None = None


class AnalysisReportRef(BaseModel):
    name: str
    path: str
    exists: bool = True
    engine: str | None = None
    template: str | None = None
    verdict: str | None = None
    updated_at: str | None = None
    content_type: Literal["markdown", "json", "text"] | None = None
    content_preview: str | None = None


class ResearchSummary(BaseModel):
    headline: str | None = None
    verdict: str | None = None
    key_findings: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)
    recommended_next_actions: list[str] = Field(default_factory=list)
    current_problem: str | None = None
    recommended_action: str | None = None
    incumbent_recipe: str | None = None
    promoted_recipe: str | None = None
    metrics: dict[str, Any] = Field(default_factory=dict)


class ExecutionAnomalySummary(BaseModel):
    dominant_cause: str | None = None
    severity: ExecutionSeverity | None = None
    affected_period_ratio: float | None = None
    avg_actual_hold_count: float | None = None
    max_actual_hold_count: float | None = None
    avg_locked_residual_count: float | None = None
    recommended_experiments: list[str] = Field(default_factory=list)
    summary_label: str | None = None


class RecommendationAction(BaseModel):
    label: str
    task_kind: TaskKind
    source_type: Literal["manual", "run", "panel", "recipe", "compare"]
    source_id: str
    prefill_config: dict[str, Any] = Field(default_factory=dict)
    reason: str | None = None


class ChartSeriesPayload(BaseModel):
    key: str
    label: str
    values: list[float | None] = Field(default_factory=list)
    role: Literal["line", "bar", "area"] = "line"
    color: str | None = None
    stack: str | None = None


class ChartThresholdPayload(BaseModel):
    label: str
    value: float
    tone: Tone = "warning"


class ChartAnnotationPayload(BaseModel):
    x: str
    label: str
    tone: Tone = "warning"


class ChartPayload(BaseModel):
    key: str
    title: str
    kind: ChartKind = "line"
    x: list[str] = Field(default_factory=list)
    series: list[ChartSeriesPayload] = Field(default_factory=list)
    thresholds: list[ChartThresholdPayload] = Field(default_factory=list)
    annotations: list[ChartAnnotationPayload] = Field(default_factory=list)


class CompareWinnerSummary(BaseModel):
    recommended_winner: str | None = None
    recommended_next_experiment: str | None = None
    rejection_reasons: list[str] = Field(default_factory=list)
    summary_label: str | None = None


class DiagnosticNode(BaseModel):
    key: NodeKey
    status: DiagnosticStatus
    headline: str
    evidence: list[EvidenceItem] = Field(default_factory=list)
    interpretation: str
    next_action: str | None = None


class RunQuickSummary(BaseModel):
    run_id: str
    output_dir: str
    task_description: str | None = None
    universe_profile: str | None = None
    panel_path: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    recipe_names: list[str] = Field(default_factory=list)
    artifact_status: Literal["ready", "partial", "missing"] = "missing"
    artifact_ready_count: int = 0
    artifact_total_count: int = 0
    missing_artifacts: list[str] = Field(default_factory=list)
    promotion_gate_summary: dict[str, Any] = Field(default_factory=dict)
    baseline_recipe: str | None = None
    baseline_metrics: dict[str, Any] = Field(default_factory=dict)
    has_execution_gap_issue: bool = False
    has_missing_artifacts: bool = False
    research_status: ResearchVerdict | None = None
    incumbent_recipe: str | None = None
    current_problem: str | None = None
    recommended_action: str | None = None
    dominant_execution_cause: str | None = None
    portfolio_definition_status: DiagnosticStatus | None = None
    avg_actual_hold_count: float | None = None
    max_actual_hold_count: float | None = None
    top1_sector_weight: float | None = None
    updated_at: str | None = None


class RunListItem(BaseModel):
    run_id: str
    updated_at: str | None = None
    quick_summary: RunQuickSummary


class RecipeSummary(BaseModel):
    run_id: str
    recipe_name: str
    signal_objective: str | None = None
    label_recipe: str | None = None
    used_feature_count: int | None = None
    rolling_rank_ic_ir: float | None = None
    walk_forward_rank_ic_ir: float | None = None
    rolling_topk_mean_excess_return_4w: float | None = None
    walk_forward_topk_mean_excess_return_4w: float | None = None
    rolling_net_total_return: float | None = None
    walk_forward_net_total_return: float | None = None
    rolling_max_drawdown: float | None = None
    walk_forward_max_drawdown: float | None = None
    promotion_gate_passed: bool | None = None


class RunDetail(BaseModel):
    run_id: str
    output_dir: str
    quick_summary: RunQuickSummary
    config: dict[str, Any] = Field(default_factory=dict)
    recipe_registry: dict[str, Any] = Field(default_factory=dict)
    promotion_gate: dict[str, Any] = Field(default_factory=dict)
    research_summary: ResearchSummary = Field(default_factory=ResearchSummary)
    nodes: list[DiagnosticNode] = Field(default_factory=list)
    recipes: list[RecipeSummary] = Field(default_factory=list)
    analysis_reports: list[AnalysisReportRef] = Field(default_factory=list)
    artifact_inventory: list[ArtifactRef] = Field(default_factory=list)
    experiment_scorecard: dict[str, Any] = Field(default_factory=dict)
    execution_anomaly_summary: ExecutionAnomalySummary = Field(default_factory=ExecutionAnomalySummary)
    recommendation_actions: list[RecommendationAction] = Field(default_factory=list)
    run_level_charts: dict[str, ChartPayload] = Field(default_factory=dict)


class ArtifactInventoryResponse(BaseModel):
    owner_id: str
    artifact_inventory: list[ArtifactRef] = Field(default_factory=list)


class RecipeDetail(BaseModel):
    run_id: str
    recipe_name: str
    recipe_config: dict[str, Any] = Field(default_factory=dict)
    manifest: dict[str, Any] = Field(default_factory=dict)
    overview: dict[str, Any] = Field(default_factory=dict)
    research_summary: ResearchSummary = Field(default_factory=ResearchSummary)
    nodes: list[DiagnosticNode] = Field(default_factory=list)
    tables: dict[str, DataTablePayload] = Field(default_factory=dict)
    analysis_reports: list[AnalysisReportRef] = Field(default_factory=list)
    artifact_inventory: list[ArtifactRef] = Field(default_factory=list)
    portfolio_realization_summary: ExecutionAnomalySummary = Field(default_factory=ExecutionAnomalySummary)
    exposure_summary: dict[str, Any] = Field(default_factory=dict)
    recommendation_actions: list[RecommendationAction] = Field(default_factory=list)
    chart_payloads: dict[str, ChartPayload] = Field(default_factory=dict)


class RecipeTablesResponse(BaseModel):
    run_id: str
    recipe_name: str
    tables: dict[str, DataTablePayload] = Field(default_factory=dict)


class PanelSummary(BaseModel):
    panel_id: str
    name: str
    path: str
    format: str
    task_description: str | None = None
    size_bytes: int | None = None
    updated_at: str | None = None
    enrichment_scope: str | None = None
    universe_mode: Literal["historical_membership", "fixed_universe"] | None = None
    universe_profile: str | None = None
    requested_start_date: str | None = None
    requested_end_date: str | None = None
    summary: dict[str, Any] = Field(default_factory=dict)
    linked_runs: list[str] = Field(default_factory=list)


class PanelDetail(PanelSummary):
    columns: list[str] = Field(default_factory=list)
    column_catalog: DataTablePayload = Field(default_factory=DataTablePayload)
    sample_rows: DataTablePayload = Field(default_factory=DataTablePayload)


class CompareItemRef(BaseModel):
    run_id: str
    recipe_name: str
    bundle: Literal["rolling", "walk_forward"]


class CompareItemResult(BaseModel):
    ref: CompareItemRef
    label: str
    metrics: dict[str, Any] = Field(default_factory=dict)
    nodes: list[DiagnosticNode] = Field(default_factory=list)


class CompareTimeseriesPoint(BaseModel):
    date: str
    value: float | None = None


class CompareTimeseriesSeries(BaseModel):
    key: str
    label: str
    role: Literal["item", "benchmark"]
    points: list[CompareTimeseriesPoint] = Field(default_factory=list)


class CompareResponse(BaseModel):
    items: list[CompareItemResult] = Field(default_factory=list)
    summary_metrics: DataTablePayload = Field(default_factory=DataTablePayload)
    net_value_curves: list[CompareTimeseriesSeries] = Field(default_factory=list)
    execution_gap: DataTablePayload = Field(default_factory=DataTablePayload)
    slice_stability: DataTablePayload = Field(default_factory=DataTablePayload)
    feature_importance: dict[str, DataTablePayload] = Field(default_factory=dict)
    latest_signal_snapshot: dict[str, DataTablePayload] = Field(default_factory=dict)
    signal_realization: dict[str, DataTablePayload] = Field(default_factory=dict)
    sector_exposure: dict[str, DataTablePayload] = Field(default_factory=dict)
    holding_count_drift: dict[str, DataTablePayload] = Field(default_factory=dict)
    analysis_summary: ResearchSummary = Field(default_factory=ResearchSummary)
    winner_summary: CompareWinnerSummary = Field(default_factory=CompareWinnerSummary)
    comparison_recommendation_actions: list[RecommendationAction] = Field(default_factory=list)
    chart_payloads: dict[str, ChartPayload] = Field(default_factory=dict)


class OverviewResponse(BaseModel):
    total_runs: int = 0
    ready_runs: int = 0
    total_panels: int = 0
    total_tasks: int = 0
    recent_runs: list[RunListItem] = Field(default_factory=list)


class ResearchTaskRequest(BaseModel):
    task_kind: TaskKind
    display_name: str | None = None
    config_payload: dict[str, Any] = Field(default_factory=dict)
    requested_by: str | None = None


class ExportPanelTaskRequest(BaseModel):
    display_name: str | None = None
    description: str | None = None
    requested_by: str | None = None
    source_ref: TaskSourceRef | None = None
    output: str = "artifacts/panels/weekly_features.parquet"
    start_date: str | None = None
    end_date: str | None = None
    symbols: list[str] | None = None
    universe_profile: str | None = None
    universe_mode: Literal["historical_membership", "fixed_universe"] | None = "historical_membership"
    batch_size: int = 300
    enrichment_scope: str | None = None
    feature_groups: list[str] | None = None
    included_features: list[str] | None = None
    excluded_features: list[str] | None = None


class RunNativeWorkflowTaskRequest(BaseModel):
    display_name: str | None = None
    description: str | None = None
    requested_by: str | None = None
    source_ref: TaskSourceRef | None = None
    config_payload: dict[str, Any] = Field(default_factory=dict)
    recipe_names: list[str] | None = None


class TaskSourceRef(BaseModel):
    kind: Literal["manual", "run", "panel", "recipe", "compare"]
    source_id: str
    label: str | None = None
    path: str | None = None


class RunResearchAnalysisTaskRequest(BaseModel):
    display_name: str | None = None
    description: str | None = None
    requested_by: str | None = None
    source_ref: TaskSourceRef | None = None
    source_kind: Literal["run", "recipe", "compare"] = "run"
    batch_mode: Literal["run_only", "run_plus_lead_recipe", "run_plus_all_recipes"] = "run_only"
    include_all_recipes: bool = False
    run_id: str | None = None
    recipe_name: str | None = None
    compare_items: list[dict[str, Any]] | None = None
    analysis_template: Literal["investment_report", "experiment_review", "ui_insight", "anomaly_diagnosis"] = "investment_report"
    analysis_engine: Literal["auto", "codex_cli", "claude_cli"] = "codex_cli"
    skills: list[str] | None = None
    output_dir: str | None = None


class ResearchTaskSummary(BaseModel):
    task_id: str
    task_kind: TaskKind
    status: TaskStatus
    display_name: str | None = None
    description: str | None = None
    requested_by: str | None = None
    created_at: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    output_dir: str | None = None
    model_id: str | None = None
    message: str | None = None
    command: list[str] = Field(default_factory=list)
    config_payload: dict[str, Any] = Field(default_factory=dict)
    logs: dict[str, str] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    queue_position: int | None = None
    source_ref: TaskSourceRef | None = None
    available_actions: list[str] = Field(default_factory=list)
    result_path: str | None = None


class ResearchTaskDetail(ResearchTaskSummary):
    result: dict[str, Any] = Field(default_factory=dict)
    timeline: list[dict[str, Any]] = Field(default_factory=list)


class TaskQueueState(BaseModel):
    dispatcher_status: Literal["idle", "running", "stopping"] = "idle"
    dispatcher_pid: int | None = None
    running_task_id: str | None = None
    queued_task_ids: list[str] = Field(default_factory=list)
    updated_at: str | None = None


class TaskBoardResponse(BaseModel):
    running_task: ResearchTaskSummary | None = None
    queued_tasks: list[ResearchTaskSummary] = Field(default_factory=list)
    history_tasks: list[ResearchTaskSummary] = Field(default_factory=list)
    queue_state: TaskQueueState = Field(default_factory=TaskQueueState)
    feature_group_options: list[str] = Field(default_factory=list)


class TaskPresetResponse(BaseModel):
    task_kind: Literal["export_panel", "run_native_workflow", "run_research_analysis"]
    display_name: str | None = None
    source_ref: TaskSourceRef
    payload: dict[str, Any] = Field(default_factory=dict)


class TaskReorderRequest(BaseModel):
    queued_task_ids: list[str] = Field(default_factory=list)


class TaskLogResponse(BaseModel):
    task_id: str
    stdout: str = ""
    stderr: str = ""
    updated_at: str | None = None
