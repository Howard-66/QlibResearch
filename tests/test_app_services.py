from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from qlib_research.app.contracts import (
    DataTablePayload,
    RecipeDetail,
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
    (recipe_dir / "native_workflow_manifest.json").write_text("{}", encoding="utf-8")

    def fake_load_native_workflow_artifacts(output_dir, recipe_names=None):
        return {
            "recipe_names": ["baseline"],
            "recipe_overview": pd.DataFrame(
                [
                    {
                        "recipe": "baseline",
                        "rolling_rank_ic_ir": 0.11,
                        "rolling_topk_mean_excess_return_4w": 0.01,
                        "walk_forward_rank_ic_ir": 0.22,
                        "walk_forward_topk_mean_excess_return_4w": 0.02,
                        "walk_forward_net_total_return": 0.18,
                    }
                ]
            ),
            "recipes": {
                "baseline": {
                    "execution_diff_summary": pd.DataFrame(
                        [{"bundle": "walk_forward", "native_minus_validation_return": 0.02}]
                    )
                }
            },
        }

    monkeypatch.setattr(services, "NATIVE_WORKFLOW_ROOT", workflow_root)
    monkeypatch.setattr(services, "load_native_workflow_artifacts", fake_load_native_workflow_artifacts)

    runs = services.list_runs()

    assert len(runs) == 1
    assert runs[0].run_id == "demo_run"
    assert runs[0].quick_summary.universe_profile == "csi300"
    assert runs[0].quick_summary.baseline_metrics["walk_forward_rank_ic_ir"] == 0.22
    assert runs[0].quick_summary.artifact_status == "partial"


def test_list_runs_falls_back_to_artifact_scan_when_pyqlib_missing(monkeypatch, tmp_path):
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
    pd.DataFrame(
        [
            {
                "bundle": "rolling",
                "rank_ic_ir": 0.11,
                "topk_mean_excess_return_4w": 0.01,
                "used_feature_count": 12,
            }
        ]
    ).to_csv(recipe_dir / "rolling_summary.csv", index=False)
    pd.DataFrame(
        [
            {
                "bundle": "walk_forward",
                "rank_ic_ir": 0.22,
                "topk_mean_excess_return_4w": 0.02,
                "used_feature_count": 12,
            }
        ]
    ).to_csv(recipe_dir / "walk_forward_summary.csv", index=False)
    pd.DataFrame([{"net_value": 1_180_000}]).to_csv(recipe_dir / "walk_forward_native_report.csv", index=False)
    pd.DataFrame([{"bundle": "walk_forward", "native_minus_validation_return": 0.02}]).to_csv(
        recipe_dir / "execution_diff_summary.csv", index=False
    )
    (recipe_dir / "native_workflow_manifest.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(services, "NATIVE_WORKFLOW_ROOT", workflow_root)
    monkeypatch.setattr(
        services,
        "load_native_workflow_artifacts",
        lambda output_dir: (_ for _ in ()).throw(RuntimeError("pyqlib is not installed in the project environment.")),
    )

    runs = services.list_runs()

    assert len(runs) == 1
    assert runs[0].quick_summary.universe_profile == "csi300"
    assert runs[0].quick_summary.baseline_metrics["walk_forward_rank_ic_ir"] == 0.22
    assert runs[0].quick_summary.baseline_metrics["walk_forward_net_total_return"] == pytest.approx(0.18)


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
    detail = RecipeDetail(
        run_id="demo_run",
        recipe_name="baseline",
        recipe_config={"signal_objective": "huber_regression"},
        manifest={},
        overview={"used_feature_count": 12},
        nodes=[],
        tables={
            "rolling_summary": DataTablePayload(
                columns=["rank_ic_ir", "topk_mean_excess_return_4w", "coverage_mean"],
                rows=[{"rank_ic_ir": 0.21, "topk_mean_excess_return_4w": 0.03, "coverage_mean": 298}],
            ),
            "rolling_native_report": DataTablePayload(columns=["net_value", "turnover"], rows=[{"net_value": 1_120_000, "turnover": 0.6}]),
            "execution_diff_summary": DataTablePayload(
                columns=["bundle", "native_minus_validation_return", "native_max_drawdown"],
                rows=[{"bundle": "rolling", "native_minus_validation_return": 0.02, "native_max_drawdown": -0.08}],
            ),
            "slice_regime_summary": DataTablePayload(
                columns=["bundle", "slice_type", "mean_excess_return_4w"],
                rows=[{"bundle": "rolling", "slice_type": "feature_year", "mean_excess_return_4w": 0.01}],
            ),
            "rolling_feature_importance": DataTablePayload(
                columns=["feature", "importance_gain"],
                rows=[{"feature": "ma20", "importance_gain": 10.0}],
            ),
            "latest_score_frame": DataTablePayload(
                columns=["instrument", "score"],
                rows=[{"instrument": "AAA.SH", "score": 0.5}],
            ),
        },
        artifact_inventory=[],
    )

    monkeypatch.setattr(services, "get_recipe_detail", lambda run_id, recipe_name: detail)

    response = services.compare_recipe_items(
        [
            services.CompareItemRef(run_id="demo_run", recipe_name="baseline", bundle="rolling"),
            services.CompareItemRef(run_id="demo_run", recipe_name="baseline", bundle="rolling"),
        ]
    )

    assert len(response.items) == 2
    assert response.summary_metrics.rows[0]["rank_ic_ir"] == 0.21
    assert response.feature_importance
