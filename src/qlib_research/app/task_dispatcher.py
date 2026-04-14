"""Detached queue dispatcher for file-backed QlibResearch tasks."""

from __future__ import annotations

import sys

from qlib_research.app.services import task_dispatcher_run


def main(argv: list[str] | None = None) -> int:
    _ = argv if argv is not None else sys.argv[1:]
    return task_dispatcher_run()


if __name__ == "__main__":  # pragma: no cover - exercised through subprocess
    raise SystemExit(main())
