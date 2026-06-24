"""FastAPI router for breakout strategy endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from qlib_research.app import breakout_services as svc
from qlib_research.app.breakout_contracts import (
    BreakoutConfigResponse,
    BreakoutConfigUpdateRequest,
    BreakoutEventDetailResponse,
    BreakoutEventItem,
    BreakoutEventsResponse,
    BreakoutOverviewResponse,
    EvaluationDetailResponse,
    EvaluationItem,
    ModelActionResponse,
    ModelItem,
    ModelsListResponse,
    ScoreRequest,
    ScoreResponse,
    SignalItem,
    SignalsResponse,
    TrainRequest,
    TrainStatusResponse,
    TrainTriggerResponse,
)

router = APIRouter(prefix="/api/breakout", tags=["breakout"])


# ---------------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------------
@router.get("/overview", response_model=BreakoutOverviewResponse)
def api_breakout_overview() -> BreakoutOverviewResponse:
    payload = svc.get_overview()
    return BreakoutOverviewResponse(**payload)


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------
@router.get("/events", response_model=BreakoutEventsResponse)
def api_list_events(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    symbol: str | None = Query(default=None),
    start_date: str | None = Query(default=None),
    end_date: str | None = Query(default=None),
) -> BreakoutEventsResponse:
    payload = svc.list_events(
        page=page,
        page_size=page_size,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
    )
    return BreakoutEventsResponse(
        events=[BreakoutEventItem(**ev) for ev in payload.get("events", [])],
        total=int(payload.get("total", 0)),
        page=int(payload.get("page", page)),
        page_size=int(payload.get("page_size", page_size)),
    )


@router.get("/events/{event_id}", response_model=BreakoutEventDetailResponse)
def api_get_event(event_id: str) -> BreakoutEventDetailResponse:
    try:
        payload = svc.get_event_detail(event_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return BreakoutEventDetailResponse(
        event=BreakoutEventItem(**payload["event"]),
        kline=payload.get("kline", []),
        features=payload.get("features", {}),
    )


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------
@router.post("/train", response_model=TrainTriggerResponse)
def api_trigger_train(req: TrainRequest) -> TrainTriggerResponse:
    payload = svc.trigger_training(req.model_dump())
    return TrainTriggerResponse(
        status=str(payload.get("status", "")),
        task_id=str(payload.get("task_id", "")),
        message=payload.get("message"),
    )


@router.get("/train/status", response_model=TrainStatusResponse)
def api_train_status() -> TrainStatusResponse:
    payload = svc.get_train_status()
    return TrainStatusResponse(**payload)


# ---------------------------------------------------------------------------
# Evaluations
# ---------------------------------------------------------------------------
@router.get("/evaluations", response_model=list[EvaluationItem])
def api_list_evaluations() -> list[EvaluationItem]:
    return [EvaluationItem(**item) for item in svc.list_evaluations()]


@router.get("/evaluations/{eval_id}", response_model=EvaluationDetailResponse)
def api_get_evaluation(eval_id: str) -> EvaluationDetailResponse:
    try:
        payload = svc.get_evaluation_detail(eval_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return EvaluationDetailResponse(**payload)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------
@router.get("/models", response_model=ModelsListResponse)
def api_list_models() -> ModelsListResponse:
    payload = svc.list_models()
    return ModelsListResponse(
        models=[ModelItem(**item) for item in payload.get("models", [])],
        active_model=payload.get("active_model"),
    )


@router.post("/models/{model_id}/activate", response_model=ModelActionResponse)
def api_activate_model(model_id: str) -> ModelActionResponse:
    try:
        payload = svc.activate_model(model_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return ModelActionResponse(**payload)


@router.post("/models/{model_id}/sync", response_model=ModelActionResponse)
def api_sync_model(model_id: str) -> ModelActionResponse:
    try:
        payload = svc.sync_model(model_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return ModelActionResponse(**payload)


# ---------------------------------------------------------------------------
# Scoring & signals
# ---------------------------------------------------------------------------
@router.post("/score", response_model=ScoreResponse)
def api_score(req: ScoreRequest) -> ScoreResponse:
    payload = svc.trigger_scoring(req.date, req.frequency, req.top_k)
    return ScoreResponse(
        status=str(payload.get("status", "")),
        signals=[SignalItem(**s) for s in payload.get("signals", [])],
        model_used=payload.get("model_used"),
        scored_at=payload.get("scored_at"),
        message=payload.get("message"),
    )


@router.get("/signals/latest", response_model=SignalsResponse)
def api_latest_signals() -> SignalsResponse:
    payload = svc.get_latest_signals()
    return SignalsResponse(
        signals=[SignalItem(**s) for s in payload.get("signals", [])],
        model_used=payload.get("model_used"),
        scored_at=payload.get("scored_at"),
    )


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
@router.get("/config", response_model=BreakoutConfigResponse)
def api_get_config() -> BreakoutConfigResponse:
    payload = svc.get_config()
    return BreakoutConfigResponse(**payload)


@router.put("/config", response_model=BreakoutConfigResponse)
def api_update_config(req: BreakoutConfigUpdateRequest) -> BreakoutConfigResponse:
    updates = req.model_dump(exclude_none=True)
    payload = svc.update_config(updates)
    return BreakoutConfigResponse(**payload)
