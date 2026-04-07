"""Compatibility wrapper around the canonical artifact loader module."""

from qlib_research.io.artifacts import LATEST_MANIFEST, MODEL_MANIFEST, QlibScoreStore, ScoreRecord, ScoreSnapshot

__all__ = [
    "LATEST_MANIFEST",
    "MODEL_MANIFEST",
    "QlibScoreStore",
    "ScoreRecord",
    "ScoreSnapshot",
]
