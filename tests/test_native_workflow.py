from concurrent.futures import Future

import pandas as pd
import pytest

from scripts.evaluate_native_weekly import parse_args
from qlib_research.core.notebook_workflow import load_native_workflow_artifacts, run_native_notebook_workflow
from qlib_research.core.qlib_native_workflow import (
    NativeWorkflowConfig,
    _build_parallel_recipe_heartbeat,
    _prime_parallel_workflow_inputs,
    build_annual_return_heatmap_frame,
    build_monthly_return_heatmap_frame,
    build_native_recipe_registry,
    select_evaluation_dates_for_label,
)


def test_build_native_recipe_registry_propagates_feature_matchers():
    config = NativeWorkflowConfig(
        included_features=("macro*", "pb"),
        excluded_features=("macro_industry_match",),
    )

    registry = build_native_recipe_registry(config)

    baseline = registry["baseline"]
    rank_blended = registry["rank_blended"]

    assert baseline.included_features == ("macro*", "pb")
    assert baseline.excluded_features == ("macro_industry_match",)
    assert rank_blended.included_features == baseline.included_features
    assert rank_blended.excluded_features == baseline.excluded_features


def test_native_workflow_config_defaults_to_parquet_panel():
    config = NativeWorkflowConfig()

    assert str(config.panel_path).endswith(".parquet")


def test_evaluate_native_weekly_parse_args_accepts_feature_matchers(monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        [
            "evaluate_native_weekly.py",
            "--include-feature",
            "macro*",
            "--include-feature",
            "pb,pe_ttm",
            "--exclude-feature",
            "macro_industry_match",
        ],
    )

    args = parse_args()

    assert args.include_feature == ["macro*", "pb,pe_ttm"]
    assert args.exclude_feature == ["macro_industry_match"]
    assert args.panel.endswith(".parquet")
    assert args.rolling_recent_weeks == 52


def test_run_native_notebook_workflow_accepts_cli_style_overrides(tmp_path):
    workflow_run = run_native_notebook_workflow(
        config_overrides={
            "output-dir": str(tmp_path / "native_workflow"),
            "panel": "artifacts/panels/csi300_weekly.parquet",
            "execution-panel": "artifacts/panels/csi300_execution_panel.parquet",
            "validation-execution-lag-steps": 3,
            "rolling-recent-weeks": 26,
            "include-feature": ["macro*", "pb,pe_ttm"],
            "exclude-feature": "macro_industry_match",
        },
        recipe_names=["baseline"],
        run_workflow=False,
    )

    config = workflow_run["config"]

    assert config.validation_execution_lag_steps == 3
    assert config.rolling_recent_weeks == 26
    assert str(config.panel_path).endswith("csi300_weekly.parquet")
    assert str(config.execution_panel_path).endswith("csi300_execution_panel.parquet")
    assert config.included_features == ("macro*", "pb", "pe_ttm")
    assert config.excluded_features == ("macro_industry_match",)
    assert "--rolling-recent-weeks 26" in workflow_run["cli_command"]
    assert "--validation-execution-lag-steps 3" in workflow_run["cli_command"]


def test_prime_parallel_workflow_inputs_preloads_shared_panels(monkeypatch, tmp_path):
    calls = []

    def fake_materialize(path, **kwargs):
        calls.append((path, kwargs["filter_to_universe_membership"], kwargs["run_export"]))
        return path

    monkeypatch.setattr(
        "qlib_research.core.qlib_native_workflow._materialize_panel_artifact",
        fake_materialize,
    )

    config = NativeWorkflowConfig(
        panel_path=tmp_path / "panels" / "csi300_weekly.parquet",
        output_dir=tmp_path / "native_workflow" / "csi300",
        execution_panel_path=None,
        run_export="auto_if_missing",
    )

    primed = _prime_parallel_workflow_inputs(config)

    assert len(calls) == 2
    assert calls[0][1] is True
    assert calls[1][1] is False
    assert all(call[2] == "auto_if_missing" for call in calls)
    assert primed.run_export == "never"
    assert str(primed.panel_path).endswith(".parquet")
    assert str(primed.execution_panel_path).endswith("csi300_execution_panel.parquet")


def test_build_parallel_recipe_heartbeat_summarizes_long_running_recipes():
    future_a = Future()
    future_b = Future()
    future_c = Future()
    pending = {future_a, future_b, future_c}
    meta = {
        future_a: {"recipe": "baseline", "index": 1, "started_at": 10.0},
        future_b: {"recipe": "mae_4w", "index": 2, "started_at": 40.0},
        future_c: {"recipe": "rank_blended", "index": 3, "started_at": 55.0},
    }

    heartbeat = _build_parallel_recipe_heartbeat(
        pending,
        meta,
        completed=1,
        total=5,
        now=70.0,
        max_listed_recipes=2,
    )

    assert heartbeat["active_recipe_count"] == 3
    assert heartbeat["active_recipes"] == ["baseline", "mae_4w"]
    assert heartbeat["active_recipe_elapsed_seconds"] == [60.0, 30.0]
    assert heartbeat["oldest_recipe"] == "baseline"
    assert heartbeat["oldest_recipe_elapsed"] == 60.0


def test_select_evaluation_dates_for_label_keeps_rolling_in_recent_calendar_window():
    dates = pd.date_range("2024-01-05", periods=80, freq="W-FRI")
    panel = pd.DataFrame(
        {
            "datetime": dates,
            "model_label_raw": range(len(dates)),
        }
    )

    selected = select_evaluation_dates_for_label(
        panel,
        label_column="model_label_raw",
        train_weeks=10,
        valid_weeks=5,
        eval_count=52,
        recent_weeks=52,
        step_weeks=4,
    )

    assert selected
    assert selected[-1] == dates[-1]
    assert selected[0] >= dates[-1] - pd.Timedelta(weeks=51)
    assert len(selected) <= 13


def test_build_monthly_return_heatmap_uses_previous_month_end_for_sparse_reports():
    report = pd.DataFrame(
        {
            "datetime": [
                "2022-04-08",
                "2022-05-06",
                "2022-06-03",
                "2022-07-01",
                "2022-07-29",
            ],
            "net_value": [100.0, 97.0, 101.0, 111.0, 104.0],
        }
    )

    heatmap = build_monthly_return_heatmap_frame(report)

    assert pd.isna(heatmap.loc[2022, "04"])
    assert heatmap.loc[2022, "05"] == pytest.approx(97.0 / 100.0 - 1.0)
    assert heatmap.loc[2022, "06"] == pytest.approx(101.0 / 97.0 - 1.0)
    assert heatmap.loc[2022, "07"] == pytest.approx(104.0 / 101.0 - 1.0)


def test_build_annual_return_heatmap_uses_previous_year_end_values():
    report = pd.DataFrame(
        {
            "datetime": [
                "2022-03-18",
                "2022-12-30",
                "2023-01-06",
                "2023-12-29",
            ],
            "net_value": [100.0, 110.0, 120.0, 132.0],
        }
    )

    heatmap = build_annual_return_heatmap_frame(report)

    assert heatmap.loc["annual_return", "2022"] == pytest.approx(110.0 / 100.0 - 1.0)
    assert heatmap.loc["annual_return", "2023"] == pytest.approx(132.0 / 110.0 - 1.0)


def test_load_native_workflow_artifacts_recomputes_stale_sparse_heatmaps(tmp_path):
    recipe_dir = tmp_path / "baseline"
    recipe_dir.mkdir(parents=True)
    (tmp_path / "native_workflow_summary.json").write_text('{"recipe_registry":{"executed_recipes":["baseline"]}}')
    report = pd.DataFrame(
        {
            "datetime": ["2022-04-08", "2022-05-06", "2022-06-03"],
            "net_value": [100.0, 97.0, 101.0],
            "benchmark_value": [100.0, 99.0, 102.0],
            "cost": [0.0, 0.0, 0.0],
            "turnover": [0.0, 0.0, 0.0],
            "relative_drawdown": [0.0, -0.03, 0.0],
            "benchmark_excess_drawdown": [0.0, 0.0, 0.0],
        }
    )
    report.to_csv(recipe_dir / "rolling_native_report.csv", index=False)
    report.to_csv(recipe_dir / "walk_forward_native_report.csv", index=False)
    pd.DataFrame({"year": [2022], "04": [0.0], "05": [0.0], "06": [0.0]}).to_csv(
        recipe_dir / "rolling_native_monthly_return_heatmap.csv",
        index=False,
    )
    pd.DataFrame({"year": [2022], "04": [0.0], "05": [0.0], "06": [0.0]}).to_csv(
        recipe_dir / "walk_forward_native_monthly_return_heatmap.csv",
        index=False,
    )

    artifacts = load_native_workflow_artifacts(tmp_path, recipe_names=["baseline"])
    rolling_heatmap = artifacts["recipes"]["baseline"]["rolling_native_monthly_return_heatmap"]
    walk_forward_heatmap = artifacts["recipes"]["baseline"]["walk_forward_native_monthly_return_heatmap"]

    assert rolling_heatmap.loc[2022, "05"] == pytest.approx(97.0 / 100.0 - 1.0)
    assert rolling_heatmap.loc[2022, "06"] == pytest.approx(101.0 / 97.0 - 1.0)
    assert walk_forward_heatmap.loc[2022, "06"] == pytest.approx(101.0 / 97.0 - 1.0)
