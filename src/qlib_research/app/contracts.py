"""Typed contracts reserved for the future QlibResearch API and UI layer."""

from __future__ import annotations

from typing import Literal, TypedDict

TaskKind = Literal["export_panel", "run_native_workflow", "run_convergence", "publish_model", "sync_model"]
TaskStatus = Literal["queued", "running", "succeeded", "failed", "cancelled"]


class ResearchTaskRequest(TypedDict, total=False):
    task_kind: TaskKind
    display_name: str
    config_payload: dict
    requested_by: str


class ResearchTaskSummary(TypedDict, total=False):
    task_id: str
    task_kind: TaskKind
    status: TaskStatus
    started_at: str | None
    finished_at: str | None
    output_dir: str | None
    model_id: str | None
    message: str | None


class ArtifactSummary(TypedDict, total=False):
    model_id: str
    feature_date: str | None
    artifact_type: Literal["score_snapshot", "portfolio_targets", "workflow_run"]
    path: str
    generated_at: str | None
