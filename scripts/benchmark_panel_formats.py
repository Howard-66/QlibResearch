"""
Benchmark CSV vs Parquet panel export and native workflow runtime.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from qlib_research.core.notebook_workflow import load_native_workflow_artifacts
from qlib_research.core.qlib_native_workflow import NativeWorkflowConfig, run_native_research_workflow
from qlib_research.core.weekly_feature_panel import export_weekly_feature_panel


def _run_case(name: str, callback) -> dict[str, object]:
    started = time.perf_counter()
    outcome = callback()
    elapsed = time.perf_counter() - started
    return {
        "elapsed_sec": elapsed,
        **outcome,
    }


def _ensure_parquet_copy(csv_path: Path, parquet_path: Path) -> None:
    if parquet_path.exists():
        return
    frame = pd.read_csv(csv_path, low_memory=False)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(parquet_path, index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark CSV vs Parquet panel formats")
    parser.add_argument("--export-start-date", default="2023-01-01")
    parser.add_argument("--export-end-date", default="2024-12-31")
    parser.add_argument("--workflow-start-date", default="2021-01-01")
    parser.add_argument("--train-weeks", type=int, default=52)
    parser.add_argument("--valid-weeks", type=int, default=12)
    parser.add_argument("--eval-count", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--recipe", default="baseline")
    parser.add_argument("--recipe-parallel-workers", type=int, default=1)
    parser.add_argument("--model-num-threads", type=int, default=1)
    parser.add_argument("--csv-panel", default=str(PROJECT_ROOT / "artifacts/panels/csi300_weekly.csv"))
    parser.add_argument("--parquet-panel", default=str(PROJECT_ROOT / "artifacts/panels/csi300_weekly.parquet"))
    parser.add_argument(
        "--csv-execution-panel",
        default=str(PROJECT_ROOT / "artifacts/native_workflow/csi300b/csi300_execution_panel.csv"),
    )
    parser.add_argument(
        "--parquet-execution-panel",
        default="/tmp/csi300_execution_panel_benchmark.parquet",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    csv_panel = Path(args.csv_panel).expanduser().resolve()
    parquet_panel = Path(args.parquet_panel).expanduser().resolve()
    csv_execution_panel = Path(args.csv_execution_panel).expanduser().resolve()
    parquet_execution_panel = Path(args.parquet_execution_panel).expanduser().resolve()

    _ensure_parquet_copy(csv_panel, parquet_panel)
    _ensure_parquet_copy(csv_execution_panel, parquet_execution_panel)

    run_id = str(int(time.time()))
    results: dict[str, object] = {
        "inputs": {
            "python": sys.executable,
            "csv_panel": str(csv_panel),
            "parquet_panel": str(parquet_panel),
            "csv_execution_panel": str(csv_execution_panel),
            "parquet_execution_panel": str(parquet_execution_panel),
        }
    }

    export_csv = Path("/tmp/qlib_export_bench_csi300.csv")
    export_parquet = Path("/tmp/qlib_export_bench_csi300.parquet")
    for output in (export_csv, export_parquet):
        if output.exists():
            output.unlink()

    results["export_csv"] = _run_case(
        "export_csv",
        lambda: {
            "path": str(
                export_weekly_feature_panel(
                    output_path=export_csv,
                    universe_profile="csi300",
                    start_date=args.export_start_date,
                    end_date=args.export_end_date,
                    batch_size=args.batch_size,
                )
            ),
            "returncode": 0,
        },
    )
    results["export_parquet"] = _run_case(
        "export_parquet",
        lambda: {
            "path": str(
                export_weekly_feature_panel(
                    output_path=export_parquet,
                    universe_profile="csi300",
                    start_date=args.export_start_date,
                    end_date=args.export_end_date,
                    batch_size=args.batch_size,
                )
            ),
            "returncode": 0,
        },
    )
    results["export_csv"]["size_mb"] = export_csv.stat().st_size / 1024 / 1024
    results["export_parquet"]["size_mb"] = export_parquet.stat().st_size / 1024 / 1024

    def _run_workflow(panel_path: Path, execution_panel_path: Path, output_dir: Path) -> dict[str, object]:
        config = NativeWorkflowConfig(
            panel_path=str(panel_path),
            execution_panel_path=str(execution_panel_path),
            output_dir=str(output_dir),
            run_export="never",
            start_date=args.workflow_start_date,
            walk_forward_start_date=args.workflow_start_date,
            train_weeks=args.train_weeks,
            valid_weeks=args.valid_weeks,
            eval_count=args.eval_count,
            walk_forward_train_weeks=args.train_weeks,
            walk_forward_valid_weeks=args.valid_weeks,
            walk_forward_eval_count=args.eval_count,
            recipe_parallel_workers=args.recipe_parallel_workers,
            model_num_threads=args.model_num_threads,
        )
        workflow_result = run_native_research_workflow(config, recipe_names=[args.recipe])
        artifacts = load_native_workflow_artifacts(
            workflow_result["output_dir"],
            recipe_names=workflow_result["recipe_registry"]["executed_recipes"],
        )
        return {
            "returncode": 0,
            "output_dir": workflow_result["output_dir"],
            "executed_recipes": workflow_result["recipe_registry"]["executed_recipes"],
            "artifact_recipe_names": artifacts["recipe_names"],
        }

    results["workflow_csv"] = _run_case(
        "workflow_csv",
        lambda: _run_workflow(
            panel_path=csv_panel,
            execution_panel_path=csv_execution_panel,
            output_dir=Path(f"/tmp/qlib_native_bench_csv_{run_id}"),
        ),
    )
    results["workflow_parquet"] = _run_case(
        "workflow_parquet",
        lambda: _run_workflow(
            panel_path=parquet_panel,
            execution_panel_path=parquet_execution_panel,
            output_dir=Path(f"/tmp/qlib_native_bench_parquet_{run_id}"),
        ),
    )

    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
