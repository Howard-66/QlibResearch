"""
Run the CSI300+CSI500 full-feature convergence workflow.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from qlib_research.core.notebook_workflow import run_convergence_workflow


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run QlibResearch convergence workflow")
    parser.add_argument("--panel", default="artifacts/panels/csi300500_weekly.csv")
    parser.add_argument("--output-dir", default="artifacts/evaluations/csi300500_convergence")
    parser.add_argument("--universe-profile", default="merged_csi300_500")
    parser.add_argument("--selection-mode", choices=["full", "group_prune", "feature_prune", "final_compare"], default="final_compare")
    parser.add_argument("--industry-normalization", choices=["none", "l1_weekly_robust"], default="l1_weekly_robust")
    parser.add_argument("--emit-slices", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--eval-count", type=int, default=52)
    parser.add_argument("--step-weeks", type=int, default=1)
    parser.add_argument("--train-weeks", type=int, default=260)
    parser.add_argument("--valid-weeks", type=int, default=52)
    parser.add_argument("--topk", type=int, default=10)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--experiment-name", default="valueinvesting_weekly_qlib_convergence")
    parser.add_argument("--coarse-tune-limit", type=int, default=12)
    parser.add_argument("--micro-tune-limit", type=int, default=6)
    parser.add_argument("--max-group-prune-rounds", type=int, default=None)
    parser.add_argument("--group-candidate-limit", type=int, default=None)
    parser.add_argument("--max-feature-prune-rounds", type=int, default=None)
    parser.add_argument("--feature-candidate-limit", type=int, default=None)
    parser.add_argument("--exclude-features", nargs="*", default=None)
    parser.add_argument("--console-mode", choices=["bar", "verbose", "silent"], default="bar")
    parser.add_argument("--summary-mode", choices=["compact", "json"], default="compact")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_convergence_workflow(
        panel_or_path=args.panel,
        output_dir=args.output_dir,
        universe_profile=args.universe_profile,
        selection_mode=args.selection_mode,
        industry_normalization=args.industry_normalization,
        emit_slices=args.emit_slices,
        eval_count=args.eval_count,
        step_weeks=args.step_weeks,
        train_weeks=args.train_weeks,
        valid_weeks=args.valid_weeks,
        topk=args.topk,
        start_date=args.start_date,
        end_date=args.end_date,
        experiment_name=args.experiment_name,
        coarse_tune_limit=args.coarse_tune_limit,
        micro_tune_limit=args.micro_tune_limit,
        max_group_prune_rounds=args.max_group_prune_rounds,
        group_candidate_limit=args.group_candidate_limit,
        max_feature_prune_rounds=args.max_feature_prune_rounds,
        feature_candidate_limit=args.feature_candidate_limit,
        excluded_features=args.exclude_features,
        console_mode=args.console_mode,
    )
    if args.summary_mode == "json":
        print("Convergence workflow completed.")
        print("Selected recipe:")
        print(json.dumps(result["selected_recipe"], ensure_ascii=False, indent=2))
        if result["comparison"]:
            print("\nComparison deltas:")
            print(json.dumps(result["comparison"], ensure_ascii=False, indent=2))
        print("\nPromotion gate:")
        print(json.dumps(result["promotion_gate"], ensure_ascii=False, indent=2))
        print(f"\nSaved evaluation artifacts to {result['output_dir']}")
        return

    selected_recipe = result["selected_recipe"]
    promotion_gate = result["promotion_gate"]
    print("Convergence workflow completed.")
    print(f"output_dir={result['output_dir']}")
    print(
        "selected_recipe="
        f"{selected_recipe.get('model_id_suggestion', 'unknown')} "
        f"features={len(selected_recipe.get('selected_features', []))} "
        f"groups={len(selected_recipe.get('selected_feature_groups', []))}"
    )
    print(
        "promotion_gate_passed="
        f"{bool(promotion_gate.get('promotion_gate_passed', False))} "
        f"feature_reduction_ratio={promotion_gate.get('feature_reduction_ratio')}"
    )


if __name__ == "__main__":
    main()
