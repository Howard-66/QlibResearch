"""Artifact contracts and publishing helpers for QlibResearch."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

from qlib_research.config import get_qlib_artifacts_dir
from qlib_research.core.stock_utils import safe_float

LATEST_MANIFEST = "latest_model.json"
MODEL_MANIFEST = "manifest.json"
SCORE_SNAPSHOT = "scores.csv"
PORTFOLIO_TARGETS = "portfolio_targets.csv"


@dataclass(frozen=True)
class ScoreRecord:
    code: str
    qlib_score: Optional[float]
    pred_return_4w: Optional[float]
    qlib_rank: Optional[int]
    feature_date: Optional[str]
    model_id: Optional[str]


@dataclass(frozen=True)
class ScoreSnapshot:
    model_id: Optional[str]
    feature_date: Optional[str]
    generated_at: Optional[str]
    snapshot_path: Path
    records: Dict[str, ScoreRecord]


class QlibScoreStore:
    """Load published qlib scores from local artifacts."""

    def __init__(self, artifacts_dir: Optional[Path | str] = None):
        self.artifacts_dir = Path(artifacts_dir or get_qlib_artifacts_dir()).resolve()

    def resolve_manifest_path(self, model_id: Optional[str] = None) -> Path:
        if model_id:
            return self.artifacts_dir / model_id / MODEL_MANIFEST
        return self.artifacts_dir / LATEST_MANIFEST

    def load_snapshot(self, model_id: Optional[str] = None) -> ScoreSnapshot:
        manifest_path = self.resolve_manifest_path(model_id)
        if not manifest_path.exists():
            raise FileNotFoundError(f"Qlib manifest not found: {manifest_path}")

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        snapshot_path = Path(manifest.get("snapshot_path", "")).expanduser()
        if not snapshot_path.is_absolute():
            snapshot_path = (manifest_path.parent / snapshot_path).resolve()
        if not snapshot_path.exists():
            raise FileNotFoundError(f"Qlib score snapshot not found: {snapshot_path}")
        score_frame = self._load_score_frame(snapshot_path)
        records = self._build_records(score_frame, manifest)
        return ScoreSnapshot(
            model_id=manifest.get("model_id"),
            feature_date=manifest.get("feature_date"),
            generated_at=manifest.get("generated_at"),
            snapshot_path=snapshot_path,
            records=records,
        )

    def _load_score_frame(self, snapshot_path: Path) -> pd.DataFrame:
        suffix = snapshot_path.suffix.lower()
        if suffix == ".json":
            payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict) and "scores" in payload:
                payload = payload["scores"]
            if isinstance(payload, dict):
                rows = []
                for code, values in payload.items():
                    row = {"code": code}
                    if isinstance(values, dict):
                        row.update(values)
                    else:
                        row["qlib_score"] = values
                    rows.append(row)
                payload = rows
            return pd.DataFrame(payload)
        if suffix == ".csv":
            return pd.read_csv(snapshot_path)
        if suffix in {".parquet", ".pq"}:
            return pd.read_parquet(snapshot_path)
        raise ValueError(f"Unsupported qlib score snapshot format: {snapshot_path.suffix}")

    def _build_records(self, score_frame: pd.DataFrame, manifest: dict) -> Dict[str, ScoreRecord]:
        if score_frame.empty:
            return {}
        code_col = self._pick_column(score_frame, ("code", "symbol", "instrument"))
        score_col = self._pick_column(score_frame, ("qlib_score", "score", "pred", "prediction"))
        pred_col = self._pick_column(score_frame, ("pred_return_4w", "pred_return", "prediction_return"))
        rank_col = self._pick_column(score_frame, ("qlib_rank", "rank"))
        feature_date_col = self._pick_column(score_frame, ("feature_date", "time", "datetime"))
        if not code_col or not score_col:
            raise ValueError("Qlib score snapshot must contain code/symbol and score columns")
        records: Dict[str, ScoreRecord] = {}
        for _, row in score_frame.iterrows():
            code = str(row.get(code_col, "")).strip()
            if not code:
                continue
            feature_date = None
            if feature_date_col and pd.notna(row.get(feature_date_col)):
                feature_date = str(pd.to_datetime(row.get(feature_date_col)).date())
            elif manifest.get("feature_date"):
                feature_date = str(manifest["feature_date"])
            qlib_rank = None
            if rank_col and pd.notna(row.get(rank_col)):
                qlib_rank = int(row.get(rank_col))
            records[code] = ScoreRecord(
                code=code,
                qlib_score=safe_float(row.get(score_col)),
                pred_return_4w=safe_float(row.get(pred_col)) if pred_col else None,
                qlib_rank=qlib_rank,
                feature_date=feature_date,
                model_id=manifest.get("model_id"),
            )
        if any(record.qlib_rank is None for record in records.values()):
            records = self._assign_ranks(records)
        return records

    @staticmethod
    def _pick_column(frame: pd.DataFrame, candidates: Tuple[str, ...]) -> Optional[str]:
        for candidate in candidates:
            if candidate in frame.columns:
                return candidate
        return None

    @staticmethod
    def _assign_ranks(records: Dict[str, ScoreRecord]) -> Dict[str, ScoreRecord]:
        ranked = sorted(
            records.values(),
            key=lambda item: (item.qlib_score is not None, item.qlib_score or float("-inf")),
            reverse=True,
        )
        updated: Dict[str, ScoreRecord] = {}
        for idx, record in enumerate(ranked, start=1):
            updated[record.code] = ScoreRecord(
                code=record.code,
                qlib_score=record.qlib_score,
                pred_return_4w=record.pred_return_4w,
                qlib_rank=idx,
                feature_date=record.feature_date,
                model_id=record.model_id,
            )
        return updated


def publish_score_snapshot(
    score_frame: pd.DataFrame,
    model_id: str,
    feature_date: str,
    artifacts_dir: Optional[str | Path] = None,
    extra_manifest: Optional[dict] = None,
    update_latest: bool = True,
) -> Path:
    root = Path(artifacts_dir or get_qlib_artifacts_dir()).expanduser().resolve()
    model_dir = root / model_id
    model_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = model_dir / SCORE_SNAPSHOT
    manifest_path = model_dir / MODEL_MANIFEST
    latest_path = root / LATEST_MANIFEST
    score_frame.to_csv(snapshot_path, index=False)
    manifest = {
        "model_id": model_id,
        "feature_date": feature_date,
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "snapshot_path": snapshot_path.name,
    }
    if extra_manifest:
        manifest.update(extra_manifest)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    if update_latest:
        latest_path.write_text(
            json.dumps({**manifest, "snapshot_path": str(snapshot_path.relative_to(root))}, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
    return snapshot_path


def build_portfolio_targets(
    score_frame: pd.DataFrame,
    model_id: str,
    feature_date: str,
    topk: int = 10,
    selected_codes: list[str] | None = None,
) -> pd.DataFrame:
    empty_targets = pd.DataFrame(columns=["trade_date", "model_id", "feature_date", "code", "target_weight", "score", "rank"])
    if score_frame.empty:
        return empty_targets
    frame = score_frame.copy()
    code_col = "code" if "code" in frame.columns else "instrument"
    score_col = "qlib_score" if "qlib_score" in frame.columns else ("score" if "score" in frame.columns else frame.columns[-1])
    frame = frame.rename(columns={code_col: "code", score_col: "score"})
    frame = frame[["code", "score"]].copy()
    if selected_codes:
        order = {str(code): idx for idx, code in enumerate(selected_codes)}
        frame["selection_order"] = frame["code"].astype(str).map(order)
        frame = (
            frame.loc[frame["selection_order"].notna()]
            .sort_values(["selection_order", "code"], ascending=[True, True])
            .drop(columns=["selection_order"])
            .reset_index(drop=True)
        )
    else:
        frame = frame.sort_values("score", ascending=False).head(max(int(topk), 1)).reset_index(drop=True)
    if frame.empty:
        return empty_targets
    frame["rank"] = frame.index + 1
    frame["trade_date"] = feature_date
    frame["model_id"] = model_id
    frame["feature_date"] = feature_date
    frame["target_weight"] = 1.0 / len(frame)
    return frame[["trade_date", "model_id", "feature_date", "code", "target_weight", "score", "rank"]]


def publish_portfolio_targets(
    target_frame: pd.DataFrame,
    model_id: str,
    artifacts_dir: Optional[str | Path] = None,
) -> Path:
    root = Path(artifacts_dir or get_qlib_artifacts_dir()).expanduser().resolve()
    model_dir = root / model_id
    model_dir.mkdir(parents=True, exist_ok=True)
    target_path = model_dir / PORTFOLIO_TARGETS
    target_frame.to_csv(target_path, index=False)
    return target_path
