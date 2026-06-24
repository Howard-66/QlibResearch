"""Model inference and candidate scoring."""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from qlib_research.breakout.features import ALL_FEATURE_COLUMNS


class BreakoutScorer:
    """Load a trained model and score breakout candidates."""

    def __init__(self, model_path: Path | str) -> None:
        """Load a pickled model from ``model_path``.

        The model may be a raw ``lightgbm.Booster`` or any object exposing a
        ``predict`` method (e.g. sklearn estimator).
        """
        model_path = Path(model_path)
        if not model_path.exists():
            raise FileNotFoundError(f"Model not found: {model_path}")
        with open(model_path, "rb") as f:
            self.model: Any = pickle.load(f)
        self.model_path = model_path

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def score(self, features_df: pd.DataFrame) -> pd.DataFrame:
        """Score candidates and return the input frame with a ``score`` column.

        Missing feature columns are added as NaN; LightGBM handles missing
        values natively. Non-feature columns (``symbol``, ``date``, ...) are
        preserved in the output. The result is sorted by ``score`` descending.
        """
        if features_df is None or len(features_df) == 0:
            empty = features_df.copy() if features_df is not None else pd.DataFrame()
            empty["score"] = pd.Series(dtype=float)
            return empty

        feature_matrix = self._build_feature_matrix(features_df)
        scores = self._predict(feature_matrix)

        result = features_df.copy()
        result["score"] = scores
        result = result.sort_values("score", ascending=False, kind="mergesort")
        result = result.reset_index(drop=True)
        return result

    def score_top_k(self, features_df: pd.DataFrame, k: int = 10) -> pd.DataFrame:
        """Score and return only the top ``k`` candidates."""
        if k <= 0:
            return self.score(features_df).iloc[0:0]
        scored = self.score(features_df)
        return scored.head(k).reset_index(drop=True)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _build_feature_matrix(self, features_df: pd.DataFrame) -> pd.DataFrame:
        """Return a frame restricted to ``ALL_FEATURE_COLUMNS`` (in order)."""
        present = [c for c in ALL_FEATURE_COLUMNS if c in features_df.columns]
        missing = [c for c in ALL_FEATURE_COLUMNS if c not in features_df.columns]

        if present:
            matrix = features_df[present].copy()
        else:
            matrix = pd.DataFrame(index=features_df.index)

        for col in missing:
            matrix[col] = np.nan

        # Re-order to canonical feature order expected by the model.
        matrix = matrix[ALL_FEATURE_COLUMNS]
        # Ensure numeric dtype so LightGBM can consume it.
        matrix = matrix.apply(pd.to_numeric, errors="coerce")
        return matrix

    def _predict(self, matrix: pd.DataFrame) -> np.ndarray:
        """Invoke the underlying model's predict API."""
        predict_fn = getattr(self.model, "predict", None)
        if predict_fn is None:
            raise TypeError(
                f"Loaded model {type(self.model).__name__!r} has no predict() method"
            )
        return np.asarray(predict_fn(matrix.values), dtype=float).ravel()
