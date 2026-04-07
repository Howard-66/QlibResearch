"""
Train and publish a weekly qlib score snapshot.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from qlib_research.core.notebook_workflow import train_and_publish_weekly_snapshot


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train/publish a weekly qlib score snapshot")
    parser.add_argument("--panel", default="artifacts/panels/weekly_features.csv")
    parser.add_argument("--model-id", required=True)
    parser.add_argument("--feature-date", required=False, default=None)
    parser.add_argument("--feature-config", default=None)
    parser.add_argument("--exclude-features", nargs="*", default=None)
    parser.add_argument("--experiment-name", default="valueinvesting_weekly_qlib")
    parser.add_argument("--num-boost-round", type=int, default=200)
    parser.add_argument("--early-stopping-rounds", type=int, default=50)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = train_and_publish_weekly_snapshot(
        panel_path=args.panel,
        model_id=args.model_id,
        feature_date=args.feature_date,
        feature_config=args.feature_config,
        excluded_features=args.exclude_features,
        experiment_name=args.experiment_name,
        num_boost_round=args.num_boost_round,
        early_stopping_rounds=args.early_stopping_rounds,
    )
    print(f"Published score snapshot to {result['snapshot_path']}")


if __name__ == "__main__":
    main()
