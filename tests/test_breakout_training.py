"""Tests for trainer, evaluator, and scorer."""

from __future__ import annotations

import pickle
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


class TestEvaluator:
    def test_perfect_predictions(self):
        """Perfect ranking should give spearman ~1.0."""
        from qlib_research.breakout.evaluator import evaluate_predictions

        df = pd.DataFrame(
            {
                "date": pd.Timestamp("2025-01-01"),
                "symbol": [f"S{i}" for i in range(100)],
                "future_return": np.linspace(-0.1, 0.2, 100),
                "pred": np.linspace(-0.1, 0.2, 100),
            }
        )
        metrics = evaluate_predictions(df)
        assert metrics["spearman_corr"] > 0.99
        assert metrics["top10_winrate"] > 0.8

    def test_random_predictions(self):
        """Random predictions should give spearman close to 0."""
        from qlib_research.breakout.evaluator import evaluate_predictions

        rng = np.random.default_rng(42)
        df = pd.DataFrame(
            {
                "date": pd.Timestamp("2025-01-01"),
                "symbol": [f"S{i}" for i in range(200)],
                "future_return": rng.normal(0, 0.05, 200),
                "pred": rng.normal(0, 1, 200),
            }
        )
        metrics = evaluate_predictions(df)
        assert abs(metrics["spearman_corr"]) < 0.2

    def test_topk_metrics_present(self):
        """Default percentiles should populate the standard metric keys."""
        from qlib_research.breakout.evaluator import evaluate_predictions

        rng = np.random.default_rng(7)
        df = pd.DataFrame(
            {
                "date": pd.Timestamp("2025-02-01"),
                "symbol": [f"S{i}" for i in range(60)],
                "future_return": rng.normal(0, 0.05, 60),
                "pred": rng.normal(0, 1, 60),
            }
        )
        metrics = evaluate_predictions(df)
        for key in (
            "rmse",
            "mae",
            "top5_return",
            "top10_return",
            "top20_return",
            "top5_winrate",
            "top10_winrate",
            "top20_winrate",
            "bottom20_return",
            "long_short_spread",
            "ic_mean",
            "ic_std",
            "icir",
        ):
            assert key in metrics, f"missing metric: {key}"

    def test_overfit_ratio(self):
        from qlib_research.breakout.evaluator import compute_overfit_ratio

        train = {"spearman_corr": 0.6}
        test = {"spearman_corr": 0.2}
        ratio = compute_overfit_ratio(train, test)
        assert ratio == pytest.approx(3.0)

        # Missing / zero metric handled gracefully.
        assert np.isnan(compute_overfit_ratio({}, test))
        assert np.isnan(compute_overfit_ratio(train, {"spearman_corr": 0.0}))


class TestScorer:
    def test_score_output(self, tmp_path):
        """Scorer should produce sorted scores."""
        from qlib_research.breakout.features import ALL_FEATURE_COLUMNS
        from qlib_research.breakout.scorer import BreakoutScorer
        import lightgbm as lgb

        rng = np.random.default_rng(42)
        X = rng.normal(size=(100, len(ALL_FEATURE_COLUMNS)))
        y = rng.normal(size=100)
        ds = lgb.Dataset(X, label=y, feature_name=ALL_FEATURE_COLUMNS)
        model = lgb.train(
            {"objective": "regression", "num_leaves": 4, "verbose": -1},
            ds,
            num_boost_round=5,
        )

        model_path = tmp_path / "test_model.pkl"
        with open(model_path, "wb") as f:
            pickle.dump(model, f)

        scorer = BreakoutScorer(model_path)
        features_df = pd.DataFrame(
            rng.normal(size=(10, len(ALL_FEATURE_COLUMNS))),
            columns=ALL_FEATURE_COLUMNS,
        )
        features_df["symbol"] = [f"S{i}" for i in range(10)]
        features_df["date"] = pd.Timestamp("2025-06-01")

        result = scorer.score(features_df)
        assert "score" in result.columns
        assert result["score"].is_monotonic_decreasing
        assert len(result) == 10
        # Non-feature columns must be preserved.
        assert "symbol" in result.columns
        assert "date" in result.columns

    def test_score_top_k(self, tmp_path):
        """score_top_k returns the requested number of rows."""
        from qlib_research.breakout.features import ALL_FEATURE_COLUMNS
        from qlib_research.breakout.scorer import BreakoutScorer
        import lightgbm as lgb

        rng = np.random.default_rng(0)
        X = rng.normal(size=(50, len(ALL_FEATURE_COLUMNS)))
        y = rng.normal(size=50)
        ds = lgb.Dataset(X, label=y, feature_name=ALL_FEATURE_COLUMNS)
        model = lgb.train(
            {"objective": "regression", "num_leaves": 4, "verbose": -1},
            ds,
            num_boost_round=3,
        )
        model_path = tmp_path / "topk_model.pkl"
        with open(model_path, "wb") as f:
            pickle.dump(model, f)

        scorer = BreakoutScorer(model_path)
        features_df = pd.DataFrame(
            rng.normal(size=(20, len(ALL_FEATURE_COLUMNS))),
            columns=ALL_FEATURE_COLUMNS,
        )
        top = scorer.score_top_k(features_df, k=5)
        assert len(top) == 5
        assert top["score"].is_monotonic_decreasing

    def test_missing_features_filled(self, tmp_path):
        """Missing feature columns should be filled with NaN, not raise."""
        from qlib_research.breakout.features import ALL_FEATURE_COLUMNS
        from qlib_research.breakout.scorer import BreakoutScorer
        import lightgbm as lgb

        rng = np.random.default_rng(1)
        X = rng.normal(size=(60, len(ALL_FEATURE_COLUMNS)))
        y = rng.normal(size=60)
        ds = lgb.Dataset(X, label=y, feature_name=ALL_FEATURE_COLUMNS)
        model = lgb.train(
            {"objective": "regression", "num_leaves": 4, "verbose": -1},
            ds,
            num_boost_round=3,
        )
        model_path = tmp_path / "missing_model.pkl"
        with open(model_path, "wb") as f:
            pickle.dump(model, f)

        scorer = BreakoutScorer(model_path)
        # Provide only the first 10 feature columns; rest should be NaN-filled.
        cols = ALL_FEATURE_COLUMNS[:10]
        features_df = pd.DataFrame(rng.normal(size=(5, len(cols))), columns=cols)
        result = scorer.score(features_df)
        assert "score" in result.columns
        assert len(result) == 5

    def test_missing_model_raises(self, tmp_path):
        from qlib_research.breakout.scorer import BreakoutScorer

        with pytest.raises(FileNotFoundError):
            BreakoutScorer(tmp_path / "does_not_exist.pkl")


class TestTrainer:
    def test_train_basic(self, tmp_path):
        """Basic training pipeline should produce model and metrics."""
        from qlib_research.breakout.config import TrainConfig
        from qlib_research.breakout.features import ALL_FEATURE_COLUMNS
        from qlib_research.breakout.trainer import train_breakout_model

        rng = np.random.default_rng(42)
        n = 300
        dates = pd.bdate_range("2024-01-01", periods=n)

        features_df = pd.DataFrame(
            rng.normal(size=(n, len(ALL_FEATURE_COLUMNS))),
            columns=ALL_FEATURE_COLUMNS,
        )
        labels_df = pd.DataFrame(
            {
                "date": dates,
                "symbol": [f"S{i % 50}" for i in range(n)],
                "future_return": rng.normal(0.02, 0.05, n),
            }
        )

        config = TrainConfig(
            train_end="2024-06-30",
            valid_end="2024-09-30",
            num_boost_round=20,
            early_stopping_rounds=5,
        )

        result = train_breakout_model(features_df, labels_df, config, output_dir=tmp_path)
        assert result.model_path.exists()
        assert result.train_samples > 0
        assert result.valid_samples > 0
        assert "spearman_corr" in result.metrics
        # Feature importance shape sanity.
        assert "feature" in result.feature_importance.columns
        assert "gain" in result.feature_importance.columns
        assert len(result.feature_importance) == len(ALL_FEATURE_COLUMNS)
        # Metadata sidecar is written next to the model.
        meta_path = result.model_path.with_suffix(".json")
        assert meta_path.exists()

    def test_train_drops_nan_targets(self, tmp_path):
        """Rows with NaN future_return should be excluded from training."""
        from qlib_research.breakout.config import TrainConfig
        from qlib_research.breakout.features import ALL_FEATURE_COLUMNS
        from qlib_research.breakout.trainer import train_breakout_model

        rng = np.random.default_rng(0)
        n = 200
        dates = pd.bdate_range("2024-01-01", periods=n)

        features_df = pd.DataFrame(
            rng.normal(size=(n, len(ALL_FEATURE_COLUMNS))),
            columns=ALL_FEATURE_COLUMNS,
        )
        targets = rng.normal(0.0, 0.05, n)
        targets[::5] = np.nan  # 20% NaN
        labels_df = pd.DataFrame(
            {
                "date": dates,
                "symbol": [f"S{i % 30}" for i in range(n)],
                "future_return": targets,
            }
        )
        config = TrainConfig(
            train_end="2024-05-31",
            valid_end="2024-08-31",
            num_boost_round=10,
            early_stopping_rounds=3,
        )
        result = train_breakout_model(features_df, labels_df, config, output_dir=tmp_path)
        assert result.train_samples + result.valid_samples <= n - n // 5

    def test_train_produces_loadable_model(self, tmp_path):
        """The persisted pickle must be re-loadable by BreakoutScorer."""
        from qlib_research.breakout.config import TrainConfig
        from qlib_research.breakout.features import ALL_FEATURE_COLUMNS
        from qlib_research.breakout.scorer import BreakoutScorer
        from qlib_research.breakout.trainer import train_breakout_model

        rng = np.random.default_rng(1)
        n = 250
        dates = pd.bdate_range("2024-01-01", periods=n)
        features_df = pd.DataFrame(
            rng.normal(size=(n, len(ALL_FEATURE_COLUMNS))),
            columns=ALL_FEATURE_COLUMNS,
        )
        labels_df = pd.DataFrame(
            {
                "date": dates,
                "symbol": [f"S{i % 40}" for i in range(n)],
                "future_return": rng.normal(0.02, 0.05, n),
            }
        )
        config = TrainConfig(
            train_end="2024-06-30",
            valid_end="2024-09-30",
            num_boost_round=10,
            early_stopping_rounds=3,
        )
        result = train_breakout_model(features_df, labels_df, config, output_dir=tmp_path)

        scorer = BreakoutScorer(result.model_path)
        sample = features_df.head(5).copy()
        scored = scorer.score(sample)
        assert "score" in scored.columns
        assert len(scored) == 5
