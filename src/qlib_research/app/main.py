"""FastAPI entrypoint for the QlibResearch workbench."""

from __future__ import annotations

import json

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from qlib_research.app.contracts import (
    ArtifactInventoryResponse,
    CompareItemRef,
    CompareResponse,
    ExportPanelTaskRequest,
    OverviewResponse,
    PanelDetail,
    PanelSummary,
    RecipeDetail,
    RecipeTablesResponse,
    RecipeSummary,
    RunResearchAnalysisTaskRequest,
    ResearchTaskDetail,
    ResearchTaskSummary,
    RunDetail,
    RunListItem,
    RunNativeWorkflowTaskRequest,
    TaskBoardResponse,
    TaskLogResponse,
    TaskPresetResponse,
    TaskReorderRequest,
)
from qlib_research.app.services import (
    compare_recipe_items,
    create_export_panel_task,
    create_native_workflow_task,
    create_research_analysis_task,
    get_overview,
    get_panel_detail,
    get_recipe_detail,
    get_recipe_tables,
    get_recipe_analysis_task_preset,
    get_run_detail,
    get_run_artifact_inventory,
    get_run_analysis_task_preset,
    get_task,
    get_task_logs,
    get_panel_task_preset,
    get_run_task_preset,
    list_panels,
    list_run_recipes,
    list_runs,
    list_tasks,
    remove_task,
    reorder_tasks,
    run_task_queue,
    stop_current_task,
)

app = FastAPI(
    title="QlibResearch Workbench API",
    version="0.1.0",
    description="Read-only workflow review plus file-backed task execution for QlibResearch.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3010",
        "http://127.0.0.1:3010",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/overview", response_model=OverviewResponse)
def api_get_overview() -> OverviewResponse:
    return get_overview()


@app.get("/api/runs", response_model=list[RunListItem])
def api_list_runs(limit: int = Query(default=50, ge=1, le=200)) -> list[RunListItem]:
    return list_runs(limit=limit)


@app.get("/api/runs/{run_id}", response_model=RunDetail)
def api_get_run(run_id: str) -> RunDetail:
    try:
        return get_run_detail(run_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/runs/{run_id}/inventory", response_model=ArtifactInventoryResponse)
def api_get_run_inventory(run_id: str) -> ArtifactInventoryResponse:
    try:
        return get_run_artifact_inventory(run_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/runs/{run_id}/recipes", response_model=list[RecipeSummary])
def api_list_run_recipes(run_id: str) -> list[RecipeSummary]:
    try:
        return list_run_recipes(run_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/runs/{run_id}/recipes/{recipe_name}", response_model=RecipeDetail)
def api_get_recipe(run_id: str, recipe_name: str) -> RecipeDetail:
    try:
        return get_recipe_detail(run_id, recipe_name)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/runs/{run_id}/recipes/{recipe_name}/tables", response_model=RecipeTablesResponse)
def api_get_recipe_tables(
    run_id: str,
    recipe_name: str,
    names: str = Query(..., description="Comma-separated recipe table names"),
) -> RecipeTablesResponse:
    try:
        selected_names = [name.strip() for name in names.split(",") if name.strip()]
        return get_recipe_tables(run_id, recipe_name, selected_names)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/compare", response_model=CompareResponse)
def api_compare(items: str = Query(..., description="JSON encoded list of compare item refs")) -> CompareResponse:
    try:
        payload = json.loads(items)
        refs = [CompareItemRef(**item) for item in payload]
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid compare payload: {exc}") from exc
    return compare_recipe_items(refs)


@app.get("/api/panels", response_model=list[PanelSummary])
def api_list_panels() -> list[PanelSummary]:
    return list_panels()


@app.get("/api/panels/{panel_id}", response_model=PanelDetail)
def api_get_panel(panel_id: str) -> PanelDetail:
    try:
        return get_panel_detail(panel_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/tasks", response_model=TaskBoardResponse)
def api_list_tasks() -> TaskBoardResponse:
    return list_tasks()


@app.post("/api/tasks/export-panel", response_model=ResearchTaskSummary)
def api_create_export_panel_task(request: ExportPanelTaskRequest) -> ResearchTaskSummary:
    return create_export_panel_task(request)


@app.post("/api/tasks/run-native-workflow", response_model=ResearchTaskSummary)
def api_create_native_workflow_task(request: RunNativeWorkflowTaskRequest) -> ResearchTaskSummary:
    return create_native_workflow_task(request)


@app.post("/api/tasks/run-research-analysis", response_model=ResearchTaskSummary)
def api_create_research_analysis_task(request: RunResearchAnalysisTaskRequest) -> ResearchTaskSummary:
    return create_research_analysis_task(request)


@app.post("/api/tasks/run-queue", response_model=TaskBoardResponse)
def api_run_task_queue() -> TaskBoardResponse:
    return run_task_queue()


@app.post("/api/tasks/stop-current", response_model=TaskBoardResponse)
def api_stop_current_task() -> TaskBoardResponse:
    return stop_current_task()


@app.post("/api/tasks/reorder", response_model=TaskBoardResponse)
def api_reorder_tasks(request: TaskReorderRequest) -> TaskBoardResponse:
    try:
        return reorder_tasks(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/tasks/{task_id}/remove", response_model=TaskBoardResponse)
def api_remove_task(task_id: str) -> TaskBoardResponse:
    try:
        return remove_task(task_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/tasks/presets/panels/{panel_id}", response_model=TaskPresetResponse)
def api_get_panel_task_preset(panel_id: str) -> TaskPresetResponse:
    try:
        return get_panel_task_preset(panel_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/tasks/presets/runs/{run_id}", response_model=TaskPresetResponse)
def api_get_run_task_preset(run_id: str) -> TaskPresetResponse:
    try:
        return get_run_task_preset(run_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/tasks/presets/runs/{run_id}/analysis", response_model=TaskPresetResponse)
def api_get_run_analysis_task_preset(run_id: str) -> TaskPresetResponse:
    try:
        return get_run_analysis_task_preset(run_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/tasks/presets/runs/{run_id}/recipes/{recipe_name}/analysis", response_model=TaskPresetResponse)
def api_get_recipe_analysis_task_preset(run_id: str, recipe_name: str) -> TaskPresetResponse:
    try:
        return get_recipe_analysis_task_preset(run_id, recipe_name)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/tasks/{task_id}", response_model=ResearchTaskDetail)
def api_get_task(task_id: str) -> ResearchTaskDetail:
    try:
        return get_task(task_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/tasks/{task_id}/logs", response_model=TaskLogResponse)
def api_get_task_logs(task_id: str) -> TaskLogResponse:
    try:
        return get_task_logs(task_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
