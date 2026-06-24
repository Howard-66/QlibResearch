"""Business logic for breakout strategy API endpoints.

The functions in this module read from the ``artifacts/breakout/`` tree and
return JSON-friendly Python primitives. They are designed to degrade
gracefully when underlying artifacts have not yet been produced, returning
sensible empty defaults rather than raising.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import threading
import traceback
import uuid
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from qlib_research.breakout.config import (
    BreakoutResearchConfig,
    TrainConfig,
)
from qlib_research.config import get_project_root

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------
def get_breakout_artifacts_dir() -> Path:
    """Get breakout artifacts root directory."""
    return get_project_root() / "artifacts" / "breakout"


def _models_dir() -> Path:
    return get_breakout_artifacts_dir() / "models"


def _events_dir() -> Path:
    return get_breakout_artifacts_dir() / "events"


def _evaluations_dir() -> Path:
    return get_breakout_artifacts_dir() / "evaluations"


def _signals_dir() -> Path:
    return get_breakout_artifacts_dir() / "signals"


def _config_dir() -> Path:
    return get_breakout_artifacts_dir() / "config"


def _active_model_file() -> Path:
    return _config_dir() / "active_model.json"


def _research_config_file() -> Path:
    return _config_dir() / "research_config.json"


def _signals_csv() -> Path:
    return _signals_dir() / "scores_breakout.csv"


def _train_status_file() -> Path:
    return _config_dir() / "train_status.json"


# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------
def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe_read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to parse %s: %s", path, exc)
        return {}

def _safe_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _format_date(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    try:
        return pd.Timestamp(value).date().isoformat()
    except Exception:
        return str(value)


def _coerce_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        f = float(value)
        if f != f:  # NaN
            return None
        return f
    except Exception:
        return None


def _coerce_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Model discovery
# ---------------------------------------------------------------------------
def _list_model_files() -> list[Path]:
    models_dir = _models_dir()
    if not models_dir.exists():
        return []
    files = sorted(models_dir.glob("*.pkl"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files


def _model_id_from_path(path: Path) -> str:
    return path.stem


def _model_metadata(path: Path) -> dict[str, Any]:
    return _safe_read_json(path.with_suffix(".json"))


def _read_active_model_id() -> str | None:
    payload = _safe_read_json(_active_model_file())
    value = payload.get("model_id")
    return str(value) if value else None


def _parse_created_at(metadata: dict[str, Any], path: Path) -> str:
    created = metadata.get("created_at")
    if created:
        # trainer writes "%Y%m%d_%H%M%S"
        try:
            return datetime.strptime(str(created), "%Y%m%d_%H%M%S").isoformat(timespec="seconds")
        except Exception:
            return str(created)
    try:
        return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")
    except Exception:
        return ""


def _model_to_item(path: Path, active_id: str | None) -> dict[str, Any]:
    metadata = _model_metadata(path)
    model_id = _model_id_from_path(path)
    return {
        "id": model_id,
        "filename": path.name,
        "created_at": _parse_created_at(metadata, path),
        "is_active": active_id == model_id,
        "metrics": metadata.get("metrics") or None,
        "config": metadata.get("config") or None,
    }


# ---------------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------------
def get_overview() -> dict[str, Any]:
    """Gather overview data for the breakout research dashboard."""
    files = _list_model_files()
    active_id = _read_active_model_id()
    active_model: dict[str, Any] | None = None
    model_metrics: dict[str, float] | None = None
    latest_train_date: str | None = None

    if files:
        # Prefer active, fall back to most recent
        chosen: Path | None = None
        if active_id:
            for path in files:
                if _model_id_from_path(path) == active_id:
                    chosen = path
                    break
        if chosen is None:
            chosen = files[0]
        active_model = _model_to_item(chosen, active_id)
        if active_model.get("metrics"):
            model_metrics = {
                k: float(v)
                for k, v in active_model["metrics"].items()
                if isinstance(v, (int, float)) and v == v
            }
        latest_train_date = active_model.get("created_at")

    events_df = _load_all_events()
    total_events = int(len(events_df))
    recent_events_7d = 0
    if not events_df.empty and "date" in events_df.columns:
        cutoff = pd.Timestamp(datetime.now()) - pd.Timedelta(days=7)
        recent_events_7d = int((pd.to_datetime(events_df["date"]) >= cutoff).sum())

    latest_score_date: str | None = None
    signals = _load_signals_df()
    if not signals.empty and "date" in signals.columns:
        try:
            latest_score_date = _format_date(pd.to_datetime(signals["date"]).max())
        except Exception:
            latest_score_date = None

    return {
        "active_model": active_model,
        "total_events": total_events,
        "recent_events_7d": recent_events_7d,
        "latest_score_date": latest_score_date,
        "latest_train_date": latest_train_date,
        "model_metrics": model_metrics,
    }


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------
def _load_all_events() -> pd.DataFrame:
    """Concatenate every event file (parquet or CSV) under ``artifacts/breakout/events/``."""
    events_dir = _events_dir()
    if not events_dir.exists():
        return pd.DataFrame()
    frames: list[pd.DataFrame] = []
    # Load *_events.parquet files
    for path in sorted(events_dir.glob("*_events.parquet")):
        try:
            frames.append(pd.read_parquet(path))
        except Exception as exc:
            logger.warning("Failed to read events parquet %s: %s", path, exc)
    # Load *_events.csv files
    for path in sorted(events_dir.glob("*_events.csv")):
        try:
            frames.append(pd.read_csv(path))
        except Exception as exc:
            logger.warning("Failed to read events csv %s: %s", path, exc)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if "prev_high_date" in df.columns:
        df["prev_high_date"] = pd.to_datetime(df["prev_high_date"], errors="coerce")
    return df


def _event_id(row: pd.Series) -> str:
    symbol = str(row.get("symbol", ""))
    date = _format_date(row.get("date"))
    return f"{symbol}_{date}"


def _event_to_item(row: pd.Series) -> dict[str, Any]:
    return {
        "id": _event_id(row),
        "symbol": str(row.get("symbol", "")),
        "date": _format_date(row.get("date")),
        "frequency": str(row.get("frequency", "daily")),
        "breakout_price": _coerce_float(row.get("breakout_price")) or 0.0,
        "prev_high_price": _coerce_float(row.get("prev_high_price")) or 0.0,
        "prev_high_date": _format_date(row.get("prev_high_date")),
        "volume_ratio": _coerce_float(row.get("volume_ratio")) or 0.0,
        "score": _coerce_float(row.get("score")),
        "future_return": _coerce_float(row.get("future_return")),
        "label": _coerce_int(row.get("label")),
    }


def list_events(
    page: int = 1,
    page_size: int = 50,
    symbol: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    """List historical breakout events with pagination and filters."""
    df = _load_all_events()
    if df.empty:
        return {"events": [], "total": 0, "page": page, "page_size": page_size}

    if symbol:
        df = df[df.get("symbol").astype(str).str.contains(symbol, case=False, na=False)]
    if start_date and "date" in df.columns:
        start_ts = pd.Timestamp(start_date)
        if hasattr(df["date"].dt, "tz") and df["date"].dt.tz is not None:
            start_ts = start_ts.tz_localize(df["date"].dt.tz)
        df = df[df["date"] >= start_ts]
    if end_date and "date" in df.columns:
        end_ts = pd.Timestamp(end_date)
        if hasattr(df["date"].dt, "tz") and df["date"].dt.tz is not None:
            end_ts = end_ts.tz_localize(df["date"].dt.tz)
        df = df[df["date"] <= end_ts]

    if "date" in df.columns:
        df = df.sort_values("date", ascending=False)
    total = int(len(df))
    start = (page - 1) * page_size
    page_df = df.iloc[start : start + page_size]
    events = [_event_to_item(row) for _, row in page_df.iterrows()]
    return {"events": events, "total": total, "page": page, "page_size": page_size}


def get_event_detail(event_id: str) -> dict[str, Any]:
    """Get detailed info for a single event.

    The returned ``kline`` list is best-effort; if K-line data is not
    co-located with events we simply return an empty list.
    """
    df = _load_all_events()
    if df.empty:
        raise FileNotFoundError(f"Event not found: {event_id}")
    df["__id"] = df.apply(_event_id, axis=1)
    row = df[df["__id"] == event_id]
    if row.empty:
        raise FileNotFoundError(f"Event not found: {event_id}")
    event = _event_to_item(row.iloc[0])
    # Surface every additional column as feature/context metadata
    extras: dict[str, Any] = {}
    skip = {
        "symbol", "date", "frequency", "breakout_price", "prev_high_price",
        "prev_high_date", "volume_ratio", "score", "future_return", "label", "__id",
    }
    for col in row.columns:
        if col in skip:
            continue
        value = row.iloc[0][col]
        if isinstance(value, pd.Timestamp):
            extras[col] = _format_date(value)
        elif isinstance(value, (int, float)):
            extras[col] = _coerce_float(value)
        else:
            extras[col] = None if pd.isna(value) else value
    return {"event": event, "kline": [], "features": extras}


# ---------------------------------------------------------------------------
# Training task
# ---------------------------------------------------------------------------
_TRAIN_LOCK = threading.Lock()


def _read_train_status() -> dict[str, Any]:
    payload = _safe_read_json(_train_status_file())
    if not payload:
        return {"status": "idle"}
    return payload


def _write_train_status(payload: dict[str, Any]) -> None:
    _safe_write_json(_train_status_file(), payload)


def _datasets_dir() -> Path:
    return get_breakout_artifacts_dir() / "datasets"


def _build_train_config(params: dict[str, Any]) -> TrainConfig:
    base = TrainConfig()
    return TrainConfig(
        train_end=str(params.get("train_end", base.train_end)),
        valid_end=str(params.get("valid_end", base.valid_end)),
        num_leaves=int(params.get("num_leaves", base.num_leaves)),
        learning_rate=float(params.get("learning_rate", base.learning_rate)),
        early_stopping_rounds=int(params.get("early_stopping_rounds", base.early_stopping_rounds)),
        num_boost_round=int(params.get("num_boost_round", base.num_boost_round)),
        feature_fraction=float(params.get("feature_fraction", base.feature_fraction)),
        lambda_l1=float(params.get("lambda_l1", base.lambda_l1)),
        lambda_l2=float(params.get("lambda_l2", base.lambda_l2)),
        min_data_in_leaf=int(params.get("min_data_in_leaf", base.min_data_in_leaf)),
        objective=str(params.get("objective", base.objective)),
    )


def _run_training_job(task_id: str, params: dict[str, Any]) -> None:
    """Background training worker.

    Runs the full breakout research pipeline (data fetch → detect → label →
    feature → train) by invoking ``scripts/run_breakout_research.py`` as a
    subprocess. The CLI script already implements the async FinanceDataHub
    data loading and the downstream sync stages, so we re-use it here rather
    than duplicating the orchestration logic in-process.

    Status is persisted to ``artifacts/breakout/config/train_status.json``
    so the API workers can poll progress.
    """
    project_root = get_project_root()
    script_path = project_root / "scripts" / "run_breakout_research.py"

    status = _read_train_status()

    if not script_path.exists():
        status.update({
            "status": "failed",
            "completed_at": _now_iso(),
            "progress": f"Pipeline script not found: {script_path}",
        })
        _write_train_status(status)
        return

    # Build CLI command from training params. Fall back to the CLI script's
    # own defaults when a key is missing.
    cmd: list[str] = [sys.executable, str(script_path)]
    cmd += ["--frequency", str(params.get("frequency", "daily"))]
    cmd += ["--universe", str(params.get("universe", "csi300"))]
    cmd += ["--train-end", str(params.get("train_end", "2025-08-31"))]
    cmd += ["--valid-end", str(params.get("valid_end", "2026-01-31"))]
    cmd += ["--lookback", str(int(params.get("lookback", 120)))]
    horizon = params.get("horizon_days", params.get("horizon", 13))
    cmd += ["--horizon", str(int(horizon))]
    if params.get("min_consolidation") is not None:
        cmd += ["--min-consolidation", str(int(params["min_consolidation"]))]
    if params.get("volume_threshold") is not None:
        cmd += ["--volume-threshold", str(float(params["volume_threshold"]))]
    if params.get("success_threshold") is not None:
        cmd += ["--success-threshold", str(float(params["success_threshold"]))]
    if params.get("max_symbols") is not None:
        cmd += ["--max-symbols", str(int(params["max_symbols"]))]
    if params.get("batch_size") is not None:
        cmd += ["--batch-size", str(int(params["batch_size"]))]

    status.update({
        "status": "running",
        "progress": "Running research pipeline (detect \u2192 label \u2192 features \u2192 train)...",
        "command": " ".join(cmd),
    })
    _write_train_status(status)

    timeout_seconds = int(params.get("timeout", 3600))

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(project_root),
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env={**os.environ, "PYTHONPATH": str(project_root / "src")
                 + os.pathsep + os.environ.get("PYTHONPATH", "")},
        )
        stdout_tail = (proc.stdout or "")[-2000:]
        stderr_tail = (proc.stderr or "")[-2000:]

        if proc.returncode == 0:
            status.update({
                "status": "completed",
                "completed_at": _now_iso(),
                "progress": "Pipeline completed successfully",
                "result": {
                    "returncode": proc.returncode,
                    "stdout_tail": stdout_tail,
                    "stderr_tail": stderr_tail,
                },
            })
        else:
            status.update({
                "status": "failed",
                "completed_at": _now_iso(),
                "progress": f"Pipeline failed (exit {proc.returncode})",
                "result": {
                    "returncode": proc.returncode,
                    "stdout_tail": stdout_tail,
                    "stderr_tail": stderr_tail,
                },
            })
    except subprocess.TimeoutExpired as exc:
        logger.exception("Breakout pipeline timed out")
        status.update({
            "status": "failed",
            "completed_at": _now_iso(),
            "progress": f"Pipeline timed out after {timeout_seconds}s",
            "result": {
                "stdout_tail": (exc.stdout or "")[-2000:] if isinstance(exc.stdout, str) else None,
                "stderr_tail": (exc.stderr or "")[-2000:] if isinstance(exc.stderr, str) else None,
            },
        })
    except Exception as exc:
        logger.exception("Breakout training failed")
        status.update({
            "status": "failed",
            "completed_at": _now_iso(),
            "progress": f"Training failed: {exc}",
            "result": {"traceback": traceback.format_exc()[-2000:]},
        })
    finally:
        _write_train_status(status)


def trigger_training(params: dict[str, Any]) -> dict[str, Any]:
    """Create a training task and dispatch it in a background thread.

    The training runs in-process via ``train_breakout_model``. Status is
    persisted to ``artifacts/breakout/config/train_status.json`` so multiple
    API workers can observe progress.
    """
    with _TRAIN_LOCK:
        status = _read_train_status()
        if status.get("status") == "running":
            return {
                "status": "running",
                "task_id": status.get("task_id") or "",
                "message": "A training task is already running",
            }

        task_id = f"breakout-train-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"
        payload: dict[str, Any] = {
            "task_id": task_id,
            "status": "running",
            "started_at": _now_iso(),
            "completed_at": None,
            "progress": "Training started",
            "params": params,
            "result": None,
        }
        _write_train_status(payload)

        worker = threading.Thread(
            target=_run_training_job,
            args=(task_id, params),
            name=f"breakout-train-{task_id}",
            daemon=True,
        )
        worker.start()
        return {
            "status": "running",
            "task_id": task_id,
            "message": "Training task started",
        }


def get_train_status() -> dict[str, Any]:
    """Get current training status as recorded on disk."""
    status = _read_train_status()
    return {
        "status": status.get("status", "idle"),
        "task_id": status.get("task_id"),
        "progress": status.get("progress"),
        "started_at": status.get("started_at"),
        "completed_at": status.get("completed_at"),
        "result": status.get("result"),
    }


# ---------------------------------------------------------------------------
# Evaluations
# ---------------------------------------------------------------------------
def _evaluation_to_item(path: Path) -> dict[str, Any] | None:
    payload = _safe_read_json(path)
    if not payload:
        return None
    return {
        "id": path.stem,
        "model_name": payload.get("model_name") or payload.get("model_path") or path.stem,
        "created_at": payload.get("created_at") or _parse_created_at({}, path),
        "metrics": payload.get("metrics") or {},
        "config": payload.get("config") or {},
    }


def list_evaluations() -> list[dict[str, Any]]:
    """List all evaluation results stored as JSON under ``evaluations/``."""
    evals_dir = _evaluations_dir()
    if not evals_dir.exists():
        return []
    items: list[dict[str, Any]] = []
    for path in sorted(evals_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        item = _evaluation_to_item(path)
        if item is not None:
            items.append(item)
    # If no explicit evaluation files exist, fall back to model metadata
    if not items:
        for path in _list_model_files():
            metadata = _model_metadata(path)
            if not metadata:
                continue
            items.append({
                "id": _model_id_from_path(path),
                "model_name": _model_id_from_path(path),
                "created_at": _parse_created_at(metadata, path),
                "metrics": metadata.get("metrics") or {},
                "config": metadata.get("config") or {},
            })
    return items


def get_evaluation_detail(eval_id: str) -> dict[str, Any]:
    """Get detailed evaluation with feature importance."""
    # Try evaluations dir first
    direct = _evaluations_dir() / f"{eval_id}.json"
    payload: dict[str, Any] = {}
    source_path: Path | None = None
    if direct.exists():
        payload = _safe_read_json(direct)
        source_path = direct
    else:
        # Fall back to model metadata of the same id
        candidate = _models_dir() / f"{eval_id}.json"
        if candidate.exists():
            payload = _safe_read_json(candidate)
            source_path = candidate
    if not payload:
        raise FileNotFoundError(f"Evaluation not found: {eval_id}")

    # If the evaluation file has no feature_importance, try the associated model metadata
    feature_importance = payload.get("feature_importance") or []
    if not feature_importance:
        model_path_str = payload.get("model_path", "")
        if model_path_str:
            model_meta_path = Path(model_path_str).with_suffix(".json")
            if model_meta_path.exists():
                model_meta = _safe_read_json(model_meta_path)
                feature_importance = model_meta.get("feature_importance") or []

    return {
        "id": eval_id,
        "model_name": payload.get("model_name") or payload.get("model_path") or eval_id,
        "created_at": payload.get("created_at") or (_parse_created_at({}, source_path) if source_path else ""),
        "metrics": payload.get("metrics") or {},
        "feature_importance": feature_importance,
        "config": payload.get("config") or {},
        "train_samples": int(payload.get("train_samples") or 0),
        "valid_samples": int(payload.get("valid_samples") or 0),
        "test_samples": _coerce_int(payload.get("test_samples")),
    }


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------
def list_models() -> dict[str, Any]:
    """List all trained models discovered under ``models/``."""
    active_id = _read_active_model_id()
    items = [_model_to_item(path, active_id) for path in _list_model_files()]
    return {"models": items, "active_model": active_id}


def activate_model(model_id: str) -> dict[str, Any]:
    """Set ``model_id`` as the active scoring model."""
    candidate = _models_dir() / f"{model_id}.pkl"
    if not candidate.exists():
        raise FileNotFoundError(f"Model not found: {model_id}")
    _safe_write_json(_active_model_file(), {"model_id": model_id, "activated_at": _now_iso()})
    return {"status": "ok", "model_id": model_id, "message": "Model activated"}


def sync_model(model_id: str) -> dict[str, Any]:
    """Sync model signals to ValueInvesting.

    Reuses the existing ``scripts/sync_to_valueinvesting.py`` script if it
    exists; otherwise records a pending status. Best-effort.
    """
    candidate = _models_dir() / f"{model_id}.pkl"
    if not candidate.exists():
        raise FileNotFoundError(f"Model not found: {model_id}")

    sync_script = get_project_root() / "scripts" / "sync_to_valueinvesting.py"
    if not sync_script.exists():
        return {
            "status": "skipped",
            "model_id": model_id,
            "message": "sync_to_valueinvesting.py not found",
        }

    try:
        proc = subprocess.run(
            [sys.executable, str(sync_script), "--model-id", model_id],
            cwd=str(get_project_root()),
            capture_output=True,
            text=True,
            timeout=300,
        )
        if proc.returncode != 0:
            return {
                "status": "failed",
                "model_id": model_id,
                "message": proc.stderr.strip()[:500] or "sync failed",
            }
        return {"status": "ok", "model_id": model_id, "message": "Synced"}
    except subprocess.TimeoutExpired:
        return {"status": "failed", "model_id": model_id, "message": "sync timed out"}
    except Exception as exc:  # pragma: no cover - defensive
        return {"status": "failed", "model_id": model_id, "message": str(exc)}


# ---------------------------------------------------------------------------
# Scoring & signals
# ---------------------------------------------------------------------------
_SIGNAL_COLUMNS = (
    "symbol", "date", "score", "breakout_price", "prev_high_date", "frequency",
)


def _load_signals_df() -> pd.DataFrame:
    path = _signals_csv()
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
    except Exception as exc:
        logger.warning("Failed to read signals csv %s: %s", path, exc)
        return pd.DataFrame()
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if "prev_high_date" in df.columns:
        df["prev_high_date"] = pd.to_datetime(df["prev_high_date"], errors="coerce")
    return df


def _signals_to_items(df: pd.DataFrame, limit: int | None = None) -> list[dict[str, Any]]:
    if df.empty:
        return []
    if "score" in df.columns:
        df = df.sort_values("score", ascending=False)
    if limit is not None:
        df = df.head(limit)
    items: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        items.append({
            "symbol": str(row.get("symbol", "")),
            "date": _format_date(row.get("date")),
            "score": _coerce_float(row.get("score")) or 0.0,
            "breakout_price": _coerce_float(row.get("breakout_price")) or 0.0,
            "prev_high_date": _format_date(row.get("prev_high_date")),
            "frequency": str(row.get("frequency", "daily")),
        })
    return items


def trigger_scoring(date: str | None, frequency: str, top_k: int) -> dict[str, Any]:
    """Score candidates for a given date using the currently active model.

    If no candidates can be assembled (no events file for the requested date),
    we return an empty signals list with a ``no_data`` status rather than
    raising.
    """
    active_id = _read_active_model_id()
    files = _list_model_files()
    if not files:
        return {
            "status": "no_model",
            "signals": [],
            "model_used": None,
            "scored_at": _now_iso(),
            "message": "No trained model available",
        }

    chosen: Path | None = None
    if active_id:
        for path in files:
            if _model_id_from_path(path) == active_id:
                chosen = path
                break
    if chosen is None:
        chosen = files[0]

    events_df = _load_all_events()
    if events_df.empty:
        return {
            "status": "no_data",
            "signals": [],
            "model_used": _model_id_from_path(chosen),
            "scored_at": _now_iso(),
            "message": "No breakout events available to score",
        }

    target_date: pd.Timestamp | None = None
    if date:
        try:
            target_date = pd.Timestamp(date)
        except Exception:
            target_date = None
    if target_date is None and "date" in events_df.columns:
        target_date = pd.to_datetime(events_df["date"]).max()

    if target_date is not None and "date" in events_df.columns:
        events_df = events_df[events_df["date"] == target_date]
    if "frequency" in events_df.columns:
        events_df = events_df[events_df["frequency"].astype(str) == frequency]

    if events_df.empty:
        return {
            "status": "no_data",
            "signals": [],
            "model_used": _model_id_from_path(chosen),
            "scored_at": _now_iso(),
            "message": "No events match the requested date/frequency",
        }

    try:
        from qlib_research.breakout.scorer import BreakoutScorer
        scorer = BreakoutScorer(chosen)
        scored = scorer.score(events_df)
    except Exception as exc:
        logger.exception("Scoring failed")
        return {
            "status": "failed",
            "signals": [],
            "model_used": _model_id_from_path(chosen),
            "scored_at": _now_iso(),
            "message": f"Scoring failed: {exc}",
        }

    if "score" in scored.columns:
        scored = scored.sort_values("score", ascending=False)
    top = scored.head(max(int(top_k), 0))
    columns = [c for c in _SIGNAL_COLUMNS if c in top.columns or c == "score"]
    out = top.copy()
    if "date" not in out.columns and target_date is not None:
        out["date"] = target_date
    if "frequency" not in out.columns:
        out["frequency"] = frequency
    items = _signals_to_items(out)

    # Persist scoring snapshot for /signals/latest
    try:
        snapshot = out[[c for c in _SIGNAL_COLUMNS if c in out.columns]].copy()
        _signals_dir().mkdir(parents=True, exist_ok=True)
        snapshot.to_csv(_signals_csv(), index=False)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to persist signals csv: %s", exc)

    return {
        "status": "ok",
        "signals": items,
        "model_used": _model_id_from_path(chosen),
        "scored_at": _now_iso(),
        "message": None,
    }


def get_latest_signals() -> dict[str, Any]:
    """Read latest signals from ``artifacts/breakout/signals/scores_breakout.csv``."""
    df = _load_signals_df()
    if df.empty:
        return {"signals": [], "model_used": _read_active_model_id(), "scored_at": None}
    items = _signals_to_items(df)
    scored_at = None
    try:
        scored_at = datetime.fromtimestamp(_signals_csv().stat().st_mtime).isoformat(timespec="seconds")
    except Exception:
        scored_at = None
    return {
        "signals": items,
        "model_used": _read_active_model_id(),
        "scored_at": scored_at,
    }


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
def _default_config_dict() -> dict[str, Any]:
    cfg = BreakoutResearchConfig()
    return {
        "detector": asdict(cfg.detector),
        "labeler": asdict(cfg.labeler),
        "trainer": asdict(cfg.trainer),
    }


def get_config() -> dict[str, Any]:
    """Read current breakout research config, falling back to defaults."""
    persisted = _safe_read_json(_research_config_file())
    base = _default_config_dict()
    if not persisted:
        return base
    for key in ("detector", "labeler", "trainer"):
        if isinstance(persisted.get(key), dict):
            base[key] = {**base[key], **persisted[key]}
    return base


def update_config(updates: dict[str, Any]) -> dict[str, Any]:
    """Merge ``updates`` into the persisted config and return the new value."""
    current = get_config()
    for key in ("detector", "labeler", "trainer"):
        section = updates.get(key)
        if isinstance(section, dict):
            current[key] = {**current[key], **section}
    _safe_write_json(_research_config_file(), current)
    return current
