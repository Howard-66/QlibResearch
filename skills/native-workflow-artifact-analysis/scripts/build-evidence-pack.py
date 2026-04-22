#!/usr/bin/env python3
"""Invoke the QlibResearch repository evidence-pack builder from the current repo."""

from __future__ import annotations

import runpy
import sys
from pathlib import Path


def main() -> int:
    repo_script = Path.cwd() / "scripts" / "build_native_workflow_evidence_pack.py"
    if not repo_script.exists():
        raise SystemExit(
            "Cannot find scripts/build_native_workflow_evidence_pack.py. "
            "Run this wrapper from the QlibResearch repository root."
        )
    sys.argv = [str(repo_script), *sys.argv[1:]]
    runpy.run_path(str(repo_script), run_name="__main__")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
