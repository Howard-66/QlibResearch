"""Sync helpers for publishing QlibResearch artifacts into ValueInvesting."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from qlib_research.config import get_qlib_artifacts_dir, get_valueinvesting_artifacts_dir
from qlib_research.io.artifacts import LATEST_MANIFEST, MODEL_MANIFEST


def sync_model_artifacts(
    model_id: str,
    source_root: str | Path | None = None,
    target_root: str | Path | None = None,
    *,
    set_latest: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    source_root = Path(source_root or get_qlib_artifacts_dir()).resolve()
    target_root = Path(target_root or get_valueinvesting_artifacts_dir()).resolve()
    source_model_dir = source_root / model_id
    target_model_dir = target_root / model_id
    manifest_path = source_model_dir / MODEL_MANIFEST
    if not manifest_path.exists():
        raise FileNotFoundError(f"Model manifest not found: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    files_to_copy = sorted([p for p in source_model_dir.iterdir() if p.is_file()], key=lambda p: p.name)
    planned = {
        "model_id": model_id,
        "source_model_dir": str(source_model_dir),
        "target_model_dir": str(target_model_dir),
        "files": [str(p.name) for p in files_to_copy],
        "set_latest": bool(set_latest),
        "dry_run": bool(dry_run),
    }
    if dry_run:
        return planned
    target_model_dir.mkdir(parents=True, exist_ok=True)
    for src_path in files_to_copy:
        shutil.copy2(src_path, target_model_dir / src_path.name)
    if set_latest:
        latest_payload = {**manifest, "snapshot_path": f"{model_id}/{Path(manifest.get('snapshot_path', 'scores.csv')).name}"}
        (target_root / LATEST_MANIFEST).write_text(json.dumps(latest_payload, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    planned["synced"] = True
    return planned
