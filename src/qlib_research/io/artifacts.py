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
STRATEGY_SIGNALS = "signals.csv"


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


@dataclass(frozen=True)
class StrategySignalRecord:
    event_id: str
    code: str
    signal_date: str
    signal_type: str
    side: str
    signal_score: Optional[float]
    pred_return: Optional[float]
    entry_price: Optional[float]
    model_id: Optional[str]


@dataclass(frozen=True)
class StrategySignalSnapshot:
    model_id: Optional[str]
    feature_date: Optional[str]
    generated_at: Optional[str]
    signal_path: Path
    records: list[StrategySignalRecord]


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


class StrategySignalStore:
    """Load published strategy signals from local qlib artifacts."""

    def __init__(self, artifacts_dir: Optional[Path | str] = None):
        self.artifacts_dir = Path(artifacts_dir or get_qlib_artifacts_dir()).resolve()

    def resolve_manifest_path(self, model_id: Optional[str] = None) -> Path:
        if model_id:
            return self.artifacts_dir / model_id / MODEL_MANIFEST
        return self.artifacts_dir / LATEST_MANIFEST

    def load_snapshot(self, model_id: Optional[str] = None) -> StrategySignalSnapshot:
        manifest_path = self.resolve_manifest_path(model_id)
        if not manifest_path.exists():
            raise FileNotFoundError(f"Qlib manifest not found: {manifest_path}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        default_signal_path = f"{manifest.get('model_id')}/{STRATEGY_SIGNALS}" if manifest_path.name == LATEST_MANIFEST and manifest.get("model_id") else STRATEGY_SIGNALS
        signal_path = Path(manifest.get("signal_path") or manifest.get("signals_path") or default_signal_path).expanduser()
        if not signal_path.is_absolute():
            signal_path = (manifest_path.parent / signal_path).resolve()
        if not signal_path.exists():
            raise FileNotFoundError(f"Qlib strategy signals not found: {signal_path}")
        frame = pd.read_csv(signal_path)
        return StrategySignalSnapshot(
            model_id=manifest.get("model_id"),
            feature_date=manifest.get("feature_date"),
            generated_at=manifest.get("generated_at"),
            signal_path=signal_path,
            records=self._build_records(frame, manifest),
        )

    def load_signals_for_code(self, code: str, model_id: Optional[str] = None) -> list[StrategySignalRecord]:
        snapshot = self.load_snapshot(model_id=model_id)
        normalized = str(code or "").strip().upper()
        return [record for record in snapshot.records if record.code == normalized]

    def _build_records(self, signal_frame: pd.DataFrame, manifest: dict) -> list[StrategySignalRecord]:
        if signal_frame.empty:
            return []
        code_col = QlibScoreStore._pick_column(signal_frame, ("code", "symbol", "instrument"))
        date_col = QlibScoreStore._pick_column(signal_frame, ("signal_date", "event_date", "feature_date", "date", "time"))
        score_col = QlibScoreStore._pick_column(signal_frame, ("signal_score", "qlib_score", "score", "pred", "prediction"))
        pred_col = QlibScoreStore._pick_column(signal_frame, ("pred_return_13d", "pred_return", "prediction_return", "pred_return_4w"))
        if not pred_col:
            pred_col = next((column for column in signal_frame.columns if str(column).startswith("pred_return_")), None)
        if not code_col or not date_col:
            raise ValueError("Qlib strategy signals must contain code/symbol and signal_date/event_date columns")
        records: list[StrategySignalRecord] = []
        for index, row in signal_frame.iterrows():
            code = str(row.get(code_col, "")).strip().upper()
            if not code:
                continue
            signal_date = str(pd.to_datetime(row.get(date_col)).date()) if pd.notna(row.get(date_col)) else ""
            records.append(
                StrategySignalRecord(
                    event_id=str(row.get("event_id") or f"{code}:{signal_date}:{index}"),
                    code=code,
                    signal_date=signal_date,
                    signal_type=str(row.get("signal_type") or "breakout"),
                    side=str(row.get("side") or "long"),
                    signal_score=safe_float(row.get(score_col)) if score_col else None,
                    pred_return=safe_float(row.get(pred_col)) if pred_col else None,
                    entry_price=safe_float(row.get("entry_price")),
                    model_id=str(row.get("model_id") or manifest.get("model_id") or ""),
                )
            )
        return records


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


def publish_strategy_signals(
    signal_frame: pd.DataFrame,
    model_id: str,
    artifacts_dir: Optional[str | Path] = None,
) -> Path:
    root = Path(artifacts_dir or get_qlib_artifacts_dir()).expanduser().resolve()
    model_dir = root / model_id
    model_dir.mkdir(parents=True, exist_ok=True)
    signal_path = model_dir / STRATEGY_SIGNALS
    signal_frame.to_csv(signal_path, index=False)

    manifest_path = model_dir / MODEL_MANIFEST
    latest_path = root / LATEST_MANIFEST
    manifest = {}
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.update(
        {
            "model_id": model_id,
            "generated_at": manifest.get("generated_at") or pd.Timestamp.utcnow().isoformat(),
            "signal_path": signal_path.name,
        }
    )
    if "feature_date" not in manifest and "feature_date" in signal_frame.columns and not signal_frame.empty:
        manifest["feature_date"] = str(signal_frame["feature_date"].iloc[-1])
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    if latest_path.exists():
        latest = json.loads(latest_path.read_text(encoding="utf-8"))
        if latest.get("model_id") == model_id:
            latest.update({**manifest, "signal_path": str(signal_path.relative_to(root))})
            latest_path.write_text(json.dumps(latest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return signal_path
