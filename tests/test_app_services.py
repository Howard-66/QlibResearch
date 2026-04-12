from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from qlib_research.app.contracts import (
    DataTablePayload,
    RunListItem,
    RunQuickSummary,
)
from qlib_research.app import services


def test_list_runs_returns_quick_summary(monkeypatch, tmp_path):
    workflow_root = tmp_path / "artifacts" / "native_workflow"
    run_dir = workflow_root / "demo_run"
    recipe_dir = run_dir / "baseline"
    recipe_dir.mkdir(parents=True)
    (run_dir / "native_workflow_summary.json").write_text(
        json.dumps(
            {
                "config": {
                    "universe_profile": "csi300",
                    "panel_path": "artifacts/panels/demo_panel.csv",
                    "start_date": "2020-01-01",
                    "end_date": "2020-12-31",
                },
                "promotion_gate": {"baseline": {"promotion_gate_passed": True}},
                "recipe_registry": {"executed_recipes": ["baseline"]},
            }
        ),
        encoding="utf-8",
    )
    (recipe_dir / "native_workflow_manifest.json").write_text(
        json.dumps({"used_feature_columns": ["ma20", "ma50"]}),
        encoding="utf-8",
    )
    pd.DataFrame(
        [{"rank_ic_ir": 0.11, "topk_mean_excess_return_4w": 0.01, "used_feature_count": 2}]
    ).to_csv(recipe_dir / "rolling_summary.csv", index=False)
    pd.DataFrame(
        [{"rank_ic_ir": 0.22, "topk_mean_excess_return_4w": 0.02, "used_feature_count": 2}]
    ).to_csv(recipe_dir / "walk_forward_summary.csv", index=False)
    pd.DataFrame([{"bundle": "walk_forward", "native_minus_validation_return": 0.02}]).to_csv(
        recipe_dir / "execution_diff_summary.csv",
        index=False,
    )
    pd.DataFrame([{"net_value": 1_180_000}]).to_csv(recipe_dir / "walk_forward_native_report.csv", index=False)

    monkeypatch.setattr(services, "NATIVE_WORKFLOW_ROOT", workflow_root)

    runs = services.list_runs()

    assert len(runs) == 1
    assert runs[0].run_id == "demo_run"
    assert runs[0].quick_summary.universe_profile == "csi300"
    assert runs[0].quick_summary.baseline_metrics["walk_forward_rank_ic_ir"] == 0.22
    assert runs[0].quick_summary.artifact_status == "partial"
    assert (run_dir / services.RUN_INDEX_FILENAME).exists()


def test_get_run_detail_does_not_depend_on_listing_all_runs(monkeypatch, tmp_path):
    workflow_root = tmp_path / "artifacts" / "native_workflow"
    run_dir = workflow_root / "demo_run"
    recipe_dir = run_dir / "baseline"
    recipe_dir.mkdir(parents=True)
    (run_dir / "native_workflow_summary.json").write_text(
        json.dumps(
            {
                "config": {
                    "universe_profile": "csi300",
                    "panel_path": "artifacts/panels/demo_panel.csv",
                    "start_date": "2020-01-01",
                    "end_date": "2020-12-31",
                },
                "recipe_registry": {"executed_recipes": ["baseline"]},
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame([{"rank_ic_ir": 0.11, "topk_mean_excess_return_4w": 0.01, "used_feature_count": 12}]).to_csv(
        recipe_dir / "rolling_summary.csv",
        index=False,
    )
    pd.DataFrame([{"rank_ic_ir": 0.22, "topk_mean_excess_return_4w": 0.02, "used_feature_count": 12}]).to_csv(
        recipe_dir / "walk_forward_summary.csv",
        index=False,
    )
    pd.DataFrame([{"net_value": 1_180_000}]).to_csv(recipe_dir / "walk_forward_native_report.csv", index=False)
    pd.DataFrame([{"bundle": "walk_forward", "native_minus_validation_return": 0.02}]).to_csv(
        recipe_dir / "execution_diff_summary.csv", index=False
    )
    pd.DataFrame([{"score_dispersion": 0.1, "topk_unique_score_ratio": 1.0}]).to_csv(
        recipe_dir / "signal_diagnostics.csv",
        index=False,
    )
    pd.DataFrame([{"blocked_sell_count": 0, "actual_hold_count": 10, "target_hold_count": 10}]).to_csv(
        recipe_dir / "portfolio_diagnostics.csv",
        index=False,
    )
    pd.DataFrame([{"mean_excess_return_4w": 0.02}]).to_csv(recipe_dir / "slice_regime_summary.csv", index=False)
    pd.DataFrame([{"feature_date": "2026-01-03", "score": 0.5}]).to_csv(recipe_dir / "latest_score_frame.csv", index=False)
    pd.DataFrame([{"keep": 1}]).to_csv(recipe_dir / "feature_prefilter.csv", index=False)
    (recipe_dir / "native_workflow_manifest.json").write_text(
        json.dumps({"used_feature_columns": ["ma20"]}),
        encoding="utf-8",
    )

    monkeypatch.setattr(services, "NATIVE_WORKFLOW_ROOT", workflow_root)
    monkeypatch.setattr(services, "list_runs", lambda limit=50: (_ for _ in ()).throw(AssertionError("should not list all runs")))

    detail = services.get_run_detail("demo_run")

    assert detail.quick_summary.universe_profile == "csi300"
    assert detail.quick_summary.baseline_metrics["walk_forward_rank_ic_ir"] == 0.22
    assert detail.quick_summary.baseline_metrics["walk_forward_net_total_return"] == pytest.approx(0.18)
    assert detail.artifact_inventory == []


def test_get_panel_detail_reads_panel_and_links_runs(monkeypatch, tmp_path):
    panels_root = tmp_path / "artifacts" / "panels"
    panels_root.mkdir(parents=True)
    panel_path = panels_root / "demo_panel.csv"
    pd.DataFrame(
        {
            "datetime": ["2026-01-03", "2026-01-10"],
            "instrument": ["AAA.SH", "BBB.SZ"],
            "in_csi300": [True, True],
            "close": [10.0, 11.2],
        }
    ).to_csv(panel_path, index=False)

    monkeypatch.setattr(services, "PANELS_ROOT", panels_root)
    monkeypatch.setattr(
        services,
        "list_runs",
        lambda limit=200: [
            RunListItem(
                run_id="demo_run",
                quick_summary=RunQuickSummary(
                    run_id="demo_run",
                    output_dir="/tmp/demo_run",
                    panel_path=str(panel_path),
                    recipe_names=["baseline"],
                    artifact_status="ready",
                ),
            )
        ],
    )

    detail = services.get_panel_detail("demo_panel.csv")

    assert detail.panel_id == "demo_panel.csv"
    assert detail.summary["rows"] == 2
    assert "demo_run" in detail.linked_runs
    assert "close" in detail.columns


def test_get_panel_detail_normalizes_array_like_samples(monkeypatch, tmp_path):
    panels_root = tmp_path / "artifacts" / "panels"
    panels_root.mkdir(parents=True)
    panel_path = panels_root / "demo_panel.parquet"
    panel_path.write_text("placeholder", encoding="utf-8")

    monkeypatch.setattr(services, "PANELS_ROOT", panels_root)
    monkeypatch.setattr(services, "_build_panel_run_links", lambda limit=200: {})
    monkeypatch.setattr(
        services,
        "load_panel_dataframe",
        lambda path: pd.DataFrame(
            {
                "datetime": ["2026-01-03"],
                "instrument": ["AAA.SH"],
                "embedding": [[1.0, 2.0, 3.0]],
            }
        ),
    )

    detail = services.get_panel_detail("demo_panel.parquet")

    embedding_row = next(row for row in detail.column_catalog.rows if row["column"] == "embedding")
    assert embedding_row["sample"] == [1.0, 2.0, 3.0]


def test_create_export_panel_task_persists_command(monkeypatch, tmp_path):
    tasks_root = tmp_path / "artifacts" / "app_tasks"
    monkeypatch.setattr(services, "TASKS_ROOT", tasks_root)
    monkeypatch.setattr(services, "_spawn_task_worker", lambda task_id: None)

    summary = services.create_export_panel_task(
        services.ExportPanelTaskRequest(
            output="artifacts/panels/from_test.parquet",
            universe_profile="csi300",
        )
    )

    task_file = tasks_root / summary.task_id / "task.json"
    assert task_file.exists()
    payload = json.loads(task_file.read_text(encoding="utf-8"))
    assert payload["task_kind"] == "export_panel"
    assert "scripts/export_weekly_panel.py" in " ".join(payload["command"])


def test_compare_recipe_items_returns_recipe_level_payload(monkeypatch):
    run_dir = Path("/tmp/demo_run")
    index_payload = {
        "run_id": "demo_run",
        "quick_summary": RunQuickSummary(
            run_id="demo_run",
            output_dir=str(run_dir),
            recipe_names=["baseline"],
            artifact_status="ready",
        ).model_dump(mode="json"),
        "overview_lookup": {"baseline": {"used_feature_count": 12}},
    }
    frames = {
        "manifest": {},
        "rolling_summary": pd.DataFrame([{"rank_ic_ir": 0.21, "topk_mean_excess_return_4w": 0.03, "coverage_mean": 298}]),
        "rolling_native_report": pd.DataFrame([{"net_value": 1_120_000, "turnover": 0.6}]),
        "execution_diff_summary": pd.DataFrame(
            [{"bundle": "rolling", "native_minus_validation_return": 0.02, "native_max_drawdown": -0.08}]
        ),
        "slice_regime_summary": pd.DataFrame(
            [{"bundle": "rolling", "slice_type": "feature_year", "mean_excess_return_4w": 0.01}]
        ),
        "rolling_feature_importance": pd.DataFrame([{"feature": "ma20", "importance_gain": 10.0}]),
        "latest_score_frame": pd.DataFrame([{"instrument": "AAA.SH", "score": 0.5}]),
        "walk_forward_summary": pd.DataFrame(),
        "walk_forward_native_report": pd.DataFrame(),
        "walk_forward_feature_importance": pd.DataFrame(),
    }

    monkeypatch.setattr(services, "_collect_run_context", lambda run_id: (run_dir, {"config": {}}, index_payload))
    monkeypatch.setattr(services, "_load_recipe_frames", lambda run_dir, recipe_name, table_names: frames)

    response = services.compare_recipe_items(
        [
            services.CompareItemRef(run_id="demo_run", recipe_name="baseline", bundle="rolling"),
            services.CompareItemRef(run_id="demo_run", recipe_name="baseline", bundle="rolling"),
        ]
    )

    assert len(response.items) == 2
    assert response.summary_metrics.rows[0]["rank_ic_ir"] == 0.21
    assert response.feature_importance
