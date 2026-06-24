"""Configuration dataclasses for the breakout research engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass
class BreakoutEvent:
    """A single detected breakout event."""

    symbol: str
    date: pd.Timestamp
    frequency: str           # "daily" | "weekly"
    breakout_price: float    # 突破日收盘价或最高价
    prev_high_price: float   # 被突破的前高价格
    prev_high_date: pd.Timestamp
    volume_ratio: float      # 突破日量比（相对近期均量）


@dataclass
class DetectorConfig:
    """Configuration for breakout detection."""

    lookback: int = 120          # 前高回看窗口（交易日数）
    min_consolidation: int = 10  # 最短盘整期（交易日数）
    frequency: str = "daily"     # "daily" | "weekly"
    volume_threshold: float = 1.5  # 量比阈值


@dataclass
class LabelConfig:
    """Configuration for auto-labeling breakout events."""

    horizon_days: int = 13          # 标注窗口期（含3天回踩宽限）
    success_threshold: float = 0.03  # 涨幅阈值（相对突破日最高价）
    target_type: str = "return"     # "return" | "binary"


@dataclass
class TrainConfig:
    """Configuration for LightGBM model training."""

    train_end: str = "2025-08-31"
    valid_end: str = "2026-01-31"
    # LightGBM hyperparameters
    num_leaves: int = 63
    learning_rate: float = 0.03
    early_stopping_rounds: int = 30
    num_boost_round: int = 500
    feature_fraction: float = 0.8
    lambda_l1: float = 0.0
    lambda_l2: float = 1.0
    min_data_in_leaf: int = 50
    objective: str = "regression"


@dataclass
class TrainResult:
    """Result of a model training run."""

    model_path: Path
    feature_importance: pd.DataFrame
    metrics: dict[str, float]   # spearman, rmse, mae, topk_return, topk_winrate
    train_samples: int
    valid_samples: int
    config: TrainConfig


@dataclass
class BreakoutResearchConfig:
    """Top-level research configuration combining all sub-configs."""

    detector: DetectorConfig = field(default_factory=DetectorConfig)
    labeler: LabelConfig = field(default_factory=LabelConfig)
    trainer: TrainConfig = field(default_factory=TrainConfig)
    # Paths
    artifacts_dir: Path = field(default_factory=lambda: Path("artifacts/breakout"))

    def models_dir(self) -> Path:
        return self.artifacts_dir / "models"

    def events_dir(self) -> Path:
        return self.artifacts_dir / "events"

    def evaluations_dir(self) -> Path:
        return self.artifacts_dir / "evaluations"

    def signals_dir(self) -> Path:
        return self.artifacts_dir / "signals"

    def config_dir(self) -> Path:
        return self.artifacts_dir / "config"
