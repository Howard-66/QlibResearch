"""
Run the scriptable qlib-native weekly workflow.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from qlib_research.core.notebook_workflow import load_native_workflow_artifacts
from qlib_research.core.qlib_native_workflow import NativeWorkflowConfig, run_native_research_workflow


def _emit_stage(stage: str, detail: str | None = None) -> None:
    message = f"[native-workflow] {stage}"
    if detail:
        message = f"{message} | {detail}"
    print(message, file=sys.stderr, flush=True)


def _build_workflow_progress_callback():
    def _progress(payload: dict[str, object]) -> None:
        event = str(payload.get("event", ""))
        recipe = payload.get("recipe")
        if event == "recipe_submitted":
            _emit_stage(
                "2/4 Recipe Submitted",
                (
                    f"recipe={recipe}, index={payload.get('index')}/{payload.get('total')}, "
                    f"workers={payload.get('recipe_parallel_workers')}, model_threads={payload.get('model_num_threads')}"
                ),
            )
        elif event == "recipe_start":
            _emit_stage(
                "2/4 Recipe Start",
                (
                    f"recipe={recipe}, index={payload.get('index')}/{payload.get('total')}, "
                    f"mode={payload.get('execution_mode')}, model_threads={payload.get('model_num_threads')}"
                ),
            )
        elif event == "recipe_done":
            _emit_stage(
                "2/4 Recipe Done",
                (
                    f"recipe={recipe}, completed={payload.get('completed')}/{payload.get('total')}, "
                    f"elapsed={float(payload.get('elapsed', 0.0)):.1f}s"
                ),
            )
        elif event == "recipe_heartbeat":
            active_recipes = list(payload.get("active_recipes") or [])
            active_elapsed = list(payload.get("active_recipe_elapsed_seconds") or [])
            active_pairs = [
                f"{name}:{float(elapsed):.0f}s"
                for name, elapsed in zip(active_recipes, active_elapsed, strict=False)
            ]
            detail = (
                f"completed={payload.get('completed')}/{payload.get('total')}, "
                f"active={payload.get('active_recipe_count')}, "
                f"oldest={payload.get('oldest_recipe')}:{float(payload.get('oldest_recipe_elapsed', 0.0)):.0f}s"
            )
            if active_pairs:
                detail = f"{detail}, running={active_pairs}"
            _emit_stage("2/4 Recipes Running", detail)

    return _progress


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run QlibResearch native weekly research workflow")
    parser.add_argument("--panel", default="artifacts/panels/csi300_weekly.parquet")
    parser.add_argument("--execution-panel", default=None)
    parser.add_argument("--output-dir", default="artifacts/native_workflow/csi300")
    parser.add_argument("--universe-profile", default="csi300")
    parser.add_argument("--start-date", default="2016-01-01")
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument(
        "--benchmark-mode",
        default="auto",
        help="Benchmark mode: auto, flat_zero, or a manual ts_code such as 000001.SH (optional @label, e.g. 000001.SH@上证指数).",
    )
    parser.add_argument("--signal-objective", choices=["mse_regression", "mae_regression", "huber_regression", "binary_top_quintile", "grouped_rank"], default="huber_regression")
    parser.add_argument("--label-recipe", choices=["excess_4w", "excess_8w", "blended_excess_4w_8w"], default="blended_excess_4w_8w")
    parser.add_argument("--topk", type=int, default=10)
    parser.add_argument("--hold-buffer-rank", type=int, default=None)
    parser.add_argument("--rebalance-interval-weeks", type=int, default=1)
    parser.add_argument("--min-liquidity-filter", type=float, default=0.0)
    parser.add_argument("--min-score-spread", type=float, default=0.0)
    parser.add_argument("--industry-max-weight", type=float, default=None)
    parser.add_argument("--eval-count", type=int, default=52)
    parser.add_argument("--train-weeks", type=int, default=260)
    parser.add_argument("--valid-weeks", type=int, default=52)
    parser.add_argument("--step-weeks", type=int, default=1)
    parser.add_argument("--walk-forward-enabled", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--walk-forward-start-date", default="2016-01-01")
    parser.add_argument("--walk-forward-end-date", default=None)
    parser.add_argument("--walk-forward-train-weeks", type=int, default=260)
    parser.add_argument("--walk-forward-valid-weeks", type=int, default=52)
    parser.add_argument("--walk-forward-step-weeks", type=int, default=1)
    parser.add_argument(
        "--walk-forward-eval-count",
        type=int,
        default=0,
        help="Number of walk-forward evaluation weeks to keep. Use 0 to keep all eligible dates after the history window; positive values keep only the most recent N weeks.",
    )
    parser.add_argument("--diagnostics-enabled", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--run-validation-comparison", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--validation-execution-lag-steps", type=int, default=1)
    parser.add_argument("--validation-only-tradable", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--validation-risk-degree", type=float, default=1.0)
    parser.add_argument("--native-risk-degree", type=float, default=0.95)
    parser.add_argument("--native-only-tradable", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--account", type=float, default=1_000_000.0)
    parser.add_argument("--universe-exit-policy", default="retain_quotes_for_existing_positions")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--reproducibility-mode", choices=["balanced", "strict"], default="balanced")
    parser.add_argument(
        "--recipe-parallel-workers",
        type=int,
        default=1,
        help="Parallel workers across recipes. Use 1 for serial, or 0 for auto based on CPU count and recipe count.",
    )
    parser.add_argument(
        "--model-num-threads",
        type=int,
        default=None,
        help="LightGBM threads per recipe. Leave unset to auto-derive in parallel mode and preserve current behavior in serial mode.",
    )
    parser.add_argument(
        "--include-feature",
        "--include-features",
        action="append",
        default=None,
        help="Only keep matching features. Repeat the flag or pass comma-separated values. Supports exact names and glob patterns such as macro*.",
    )
    parser.add_argument(
        "--exclude-feature",
        "--exclude-features",
        action="append",
        default=None,
        help="Drop matching features. Repeat the flag or pass comma-separated values. Supports exact names and glob patterns such as macro*.",
    )
    parser.add_argument("--publish-model", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--run-export", choices=["always", "auto_if_missing", "never"], default="auto_if_missing")
    parser.add_argument("--recipe", action="append", default=None, help="Recipe name from the registry; repeat to run multiple recipes.")
    return parser.parse_args()


def main() -> None:
    overall_start = time.perf_counter()
    _emit_stage("1/4 Parse Args", "Parsing CLI arguments and building workflow config")
    args = parse_args()
    config = NativeWorkflowConfig(
        universe_profile=args.universe_profile,
        panel_path=args.panel,
        execution_panel_path=args.execution_panel,
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        batch_size=args.batch_size,
        benchmark_mode=args.benchmark_mode,
        signal_objective=args.signal_objective,
        label_recipe=args.label_recipe,
        topk=args.topk,
        hold_buffer_rank=args.hold_buffer_rank,
        rebalance_interval_weeks=args.rebalance_interval_weeks,
        min_liquidity_filter=args.min_liquidity_filter,
        min_score_spread=args.min_score_spread,
        industry_max_weight=args.industry_max_weight,
        eval_count=args.eval_count,
        train_weeks=args.train_weeks,
        valid_weeks=args.valid_weeks,
        step_weeks=args.step_weeks,
        walk_forward_enabled=args.walk_forward_enabled,
        walk_forward_start_date=args.walk_forward_start_date,
        walk_forward_end_date=args.walk_forward_end_date,
        walk_forward_train_weeks=args.walk_forward_train_weeks,
        walk_forward_valid_weeks=args.walk_forward_valid_weeks,
        walk_forward_step_weeks=args.walk_forward_step_weeks,
        walk_forward_eval_count=args.walk_forward_eval_count,
        diagnostics_enabled=args.diagnostics_enabled,
        run_validation_comparison=args.run_validation_comparison,
        validation_execution_lag_steps=args.validation_execution_lag_steps,
        validation_only_tradable=args.validation_only_tradable,
        validation_risk_degree=args.validation_risk_degree,
        native_risk_degree=args.native_risk_degree,
        native_only_tradable=args.native_only_tradable,
        account=args.account,
        universe_exit_policy=args.universe_exit_policy,
        seed=args.seed,
        reproducibility_mode=args.reproducibility_mode,
        recipe_parallel_workers=args.recipe_parallel_workers,
        model_num_threads=args.model_num_threads,
        included_features=tuple(args.include_feature or ()),
        excluded_features=tuple(args.exclude_feature or ()),
        publish_model=args.publish_model,
        run_export=args.run_export,
    )
    selected_recipes = list(args.recipe) if args.recipe else ["baseline"]
    _emit_stage(
        "2/4 Run Workflow",
        (
            f"universe={config.universe_profile}, benchmark={config.benchmark_mode}, "
            f"recipes={selected_recipes}, output_dir={config.output_dir}, "
            f"recipe_parallel_workers={config.recipe_parallel_workers}, model_num_threads={config.model_num_threads}"
        ),
    )
    workflow_start = time.perf_counter()
    result = run_native_research_workflow(
        config,
        recipe_names=args.recipe,
        progress_callback=_build_workflow_progress_callback(),
    )
    workflow_elapsed = time.perf_counter() - workflow_start
    _emit_stage(
        "2/4 Done",
        (
            f"executed_recipes={result['recipe_registry']['executed_recipes']}, "
            f"elapsed={workflow_elapsed:.1f}s"
        ),
    )
    _emit_stage("3/4 Load Artifacts", "Reading workflow artifacts for key metrics summary")
    artifact_start = time.perf_counter()
    artifact_view = load_native_workflow_artifacts(result["output_dir"], recipe_names=result["recipe_registry"]["executed_recipes"])
    artifact_elapsed = time.perf_counter() - artifact_start
    _emit_stage("3/4 Done", f"loaded_recipe_count={len(artifact_view['recipe_names'])}, elapsed={artifact_elapsed:.1f}s")
    _emit_stage("4/4 Emit Summary", "Printing JSON summary to stdout")
    print(json.dumps(
        {
            "output_dir": result["output_dir"],
            "executed_recipes": result["recipe_registry"]["executed_recipes"],
            "promotion_gate": result["promotion_gate"],
            "key_metrics": artifact_view["recipe_overview"].to_dict(orient="records"),
        },
        ensure_ascii=False,
        indent=2,
        default=str,
    ))
    total_elapsed = time.perf_counter() - overall_start
    _emit_stage("Completed", f"total_elapsed={total_elapsed:.1f}s")


if __name__ == "__main__":
    main()
