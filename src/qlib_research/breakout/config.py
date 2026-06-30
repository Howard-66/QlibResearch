"""Config helpers for breakout event research."""

from __future__ import annotations

import hashlib
import json
from typing import Any


DEFAULT_BREAKOUT_CONFIG_PROFILE: dict[str, Any] = {
    "detector": {
        "lookback_window": 60,
        "consolidation_window": 20,
        "breakout_pct": 0.0,
        "max_breakout_pct": None,
        "min_volume_ratio": 1.0,
        "max_consolidation_range": None,
        "min_close_position": 0.0,
        "min_history_days": 80,
        "frequency": "D",
    },
    "labeler": {
        "label_horizon_days": 13,
        "success_return_pct": 0.03,
        "target": "future_return_13d",
    },
    "features": {
        "feature_set": "stock_breakout_80_v1",
        "enabled_groups": [
            "breakout_strength",
            "prev_high_pattern",
            "consolidation",
            "volume_price",
            "trend_momentum",
            "market_env",
            "price_momentum",
            "fundamentals",
            "interaction",
        ],
    },
    "trainer": {
        "objective": "regression",
        "learning_rate": 0.03,
        "num_leaves": 63,
        "num_boost_round": 500,
        "early_stopping_rounds": 30,
        "train_end_date": None,
        "valid_end_date": None,
        "random_state": 42,
    },
    "cache": {
        "enabled": True,
        "overlap_days": 80,
    },
}


def stable_config_hash(payload: dict[str, Any]) -> str:
    """Return a short deterministic hash for config/dataset identity."""

    text = json.dumps(payload, sort_keys=True, ensure_ascii=True, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
