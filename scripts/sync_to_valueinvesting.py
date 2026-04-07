"""Sync a published QlibResearch model into the sibling ValueInvesting repository."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from qlib_research.io.sync import sync_model_artifacts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync a published QlibResearch model into ValueInvesting")
    parser.add_argument("--model-id", required=True)
    parser.add_argument("--source-root", default=None)
    parser.add_argument("--target-root", default=None)
    parser.add_argument("--set-latest", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--dry-run", action=argparse.BooleanOptionalAction, default=False)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = sync_model_artifacts(
        model_id=args.model_id,
        source_root=args.source_root,
        target_root=args.target_root,
        set_latest=args.set_latest,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
