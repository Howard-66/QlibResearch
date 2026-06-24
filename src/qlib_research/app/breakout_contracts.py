"""Pydantic models for the breakout strategy API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------------
class BreakoutOverviewResponse(BaseModel):
    active_model: dict[str, Any] | None = None
    total_events: int = 0
    recent_events_7d: int = 0
    latest_score_date: str | None = None
    latest_train_date: str | None = None
    model_metrics: dict[str, float] | None = None


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------
class BreakoutEventItem(BaseModel):
    id: str
    symbol: str
    date: str
    frequency: str
    breakout_price: float
    prev_high_price: float
    prev_high_date: str
    volume_ratio: float
    score: float | None = None
    future_return: float | None = None
    label: int | None = None


class BreakoutEventsResponse(BaseModel):
    events: list[BreakoutEventItem] = Field(default_factory=list)
    total: int = 0
    page: int = 1
    page_size: int = 50


class BreakoutEventDetailResponse(BaseModel):
    event: BreakoutEventItem
    kline: list[dict[str, Any]] = Field(default_factory=list)
    features: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------
class TrainRequest(BaseModel):
    frequency: str = "daily"
    universe: str = "csi300"
    train_end: str = "2025-08-31"
    valid_end: str = "2026-01-31"
    lookback: int = 120
    horizon_days: int = 13
    num_leaves: int = 63
    learning_rate: float = 0.03
    early_stopping_rounds: int = 30


class TrainStatusResponse(BaseModel):
    status: str = "idle"
    task_id: str | None = None
    progress: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    result: dict[str, Any] | None = None


class TrainTriggerResponse(BaseModel):
    status: str
    task_id: str
    message: str | None = None


# ---------------------------------------------------------------------------
# Evaluations
# ---------------------------------------------------------------------------
class EvaluationItem(BaseModel):
    id: str
    model_name: str
    created_at: str
    metrics: dict[str, float] = Field(default_factory=dict)
    config: dict[str, Any] = Field(default_factory=dict)


class EvaluationDetailResponse(BaseModel):
    id: str
    model_name: str
    created_at: str
    metrics: dict[str, float] = Field(default_factory=dict)
    feature_importance: list[dict[str, Any]] = Field(default_factory=list)
    config: dict[str, Any] = Field(default_factory=dict)
    train_samples: int = 0
    valid_samples: int = 0
    test_samples: int | None = None


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------
class ModelItem(BaseModel):
    id: str
    filename: str
    created_at: str
    is_active: bool = False
    metrics: dict[str, float] | None = None
    config: dict[str, Any] | None = None


class ModelsListResponse(BaseModel):
    models: list[ModelItem] = Field(default_factory=list)
    active_model: str | None = None


class ModelActionResponse(BaseModel):
    status: str
    model_id: str
    message: str | None = None


# ---------------------------------------------------------------------------
# Scoring & Signals
# ---------------------------------------------------------------------------
class ScoreRequest(BaseModel):
    date: str | None = None
    frequency: str = "daily"
    top_k: int = 10


class SignalItem(BaseModel):
    symbol: str
    date: str
    score: float
    breakout_price: float
    prev_high_date: str
    frequency: str


class SignalsResponse(BaseModel):
    signals: list[SignalItem] = Field(default_factory=list)
    model_used: str | None = None
    scored_at: str | None = None


class ScoreResponse(BaseModel):
    status: str
    signals: list[SignalItem] = Field(default_factory=list)
    model_used: str | None = None
    scored_at: str | None = None
    message: str | None = None


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
class BreakoutConfigResponse(BaseModel):
    detector: dict[str, Any] = Field(default_factory=dict)
    labeler: dict[str, Any] = Field(default_factory=dict)
    trainer: dict[str, Any] = Field(default_factory=dict)


class BreakoutConfigUpdateRequest(BaseModel):
    detector: dict[str, Any] | None = None
    labeler: dict[str, Any] | None = None
    trainer: dict[str, Any] | None = None
