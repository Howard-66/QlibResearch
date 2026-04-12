"""Detached task worker for file-backed QlibResearch tasks."""

from __future__ import annotations

from pathlib import Path
import sys

from qlib_research.app.services import task_worker_run


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if len(args) != 1:
        print("usage: python -m qlib_research.app.task_worker <task_dir>", file=sys.stderr)
        return 2
    return task_worker_run(Path(args[0]).expanduser().resolve())


if __name__ == "__main__":  # pragma: no cover - exercised through subprocess
    raise SystemExit(main())
