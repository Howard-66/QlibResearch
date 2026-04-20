from concurrent.futures import Future
from pathlib import Path

import pandas as pd
import pytest

from scripts.evaluate_native_weekly import parse_args
from qlib_research.core.notebook_workflow import load_native_workflow_artifacts, run_native_notebook_workflow
from qlib_research.core.qlib_native_workflow import (
    NativeRecipeArtifacts,
    NativeResearchRecipe,
    NativeWorkflowConfig,
    _build_native_workflow_summary_payload,
    _build_recipe_experiment_scorecard,
    _build_parallel_recipe_heartbeat,
    _prime_parallel_workflow_inputs,
    build_native_performance_metrics_frame,
    build_annual_return_heatmap_frame,
    build_holding_count_drift,
    build_monthly_return_heatmap_frame,
    build_native_recipe_registry,
    build_regime_gate_diagnostics,
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
            "--feature-spec",
            "artifacts/native_workflow/csi300/feature_spec.json",
        ],
    )

    args = parse_args()

    assert args.include_feature == ["macro*", "pb,pe_ttm"]
    assert args.exclude_feature == ["macro_industry_match"]
    assert args.feature_spec.endswith("feature_spec.json")
    assert args.panel.endswith(".parquet")
    assert args.rolling_recent_weeks == 52


def test_build_native_recipe_registry_uses_feature_spec(tmp_path):
    feature_spec = tmp_path / "feature_spec.json"
    feature_spec.write_text(
        """{
  "selected_features": ["pb", "pe_ttm"],
  "selected_feature_groups": ["valuation_absolute"],
  "industry_normalization": "none",
  "excluded_features": ["macro_industry_match"],
  "tuned_params": {"num_boost_round": 123}
}""",
        encoding="utf-8",
    )

    registry = build_native_recipe_registry(
        NativeWorkflowConfig(
            feature_spec_path=feature_spec,
            included_features=("macro*",),
        )
    )

    baseline = registry["baseline"]
    assert baseline.feature_groups == ("valuation_absolute",)
    assert baseline.included_features == ("pb", "pe_ttm")
    assert baseline.excluded_features == ("macro_industry_match",)
    assert baseline.industry_normalization == "none"
    assert baseline.model_params["num_boost_round"] == 123


def test_build_native_workflow_summary_payload_collects_recipe_overview(tmp_path):
    artifacts = {
        "baseline": NativeRecipeArtifacts(
            recipe=NativeResearchRecipe(name="baseline"),
            latest_score_frame=pd.DataFrame(),
            prediction_bundles={
                "rolling": {"summary": pd.DataFrame([{"rank_ic_ir": 0.11, "topk_mean_excess_return_4w": 0.02}])},
                "walk_forward": {"summary": pd.DataFrame([{"rank_ic_ir": 0.07, "topk_mean_excess_return_4w": 0.01}])},
            },
            native_results={},
            validation_results={},
            executor_comparison_summary=pd.DataFrame(),
            signal_diagnostics=pd.DataFrame(),
            portfolio_diagnostics=pd.DataFrame(),
            slice_regime_summary=pd.DataFrame(),
            feature_prefilter_stats=pd.DataFrame([{"requested_feature_count": 5, "selected_feature_count": 3}]),
            feature_corr_candidates=pd.DataFrame(),
            feature_redundancy=pd.DataFrame(),
            feature_outlier_audit=pd.DataFrame(),
            used_feature_columns=["ma20", "ma50", "pb"],
            native_provider_dir=tmp_path,
            benchmark_frames={},
            native_summary=pd.DataFrame(
                [
                    {
                        "bundle": "rolling",
                        "net_total_return": 0.12,
                        "benchmark_total_return": 0.04,
                        "excess_total_return": 0.08,
                        "strategy_max_drawdown": -0.07,
                        "strategy_excess_drawdown": -0.03,
                        "cost_drag": 0.01,
                        "turnover_mean": 0.25,
                        "annualized_return": 0.15,
                        "annualized_volatility": 0.22,
                        "sharpe_ratio": 0.68,
                        "win_rate": 0.57,
                        "calmar_ratio": 2.1,
                    },
                    {
                        "bundle": "walk_forward",
                        "net_total_return": 0.08,
                        "benchmark_total_return": 0.03,
                        "excess_total_return": 0.05,
                        "strategy_max_drawdown": -0.05,
                        "strategy_excess_drawdown": -0.02,
                        "cost_drag": 0.02,
                        "turnover_mean": 0.3,
                        "annualized_return": 0.11,
                        "annualized_volatility": 0.18,
                        "sharpe_ratio": 0.61,
                        "win_rate": 0.54,
                        "calmar_ratio": 2.2,
                    },
                ]
            ),
        )
    }

    payload = _build_native_workflow_summary_payload(
        config=NativeWorkflowConfig(output_dir=tmp_path),
        registry_payload={"executed_recipes": ["baseline"]},
        promotion_gate={},
        output_dir=tmp_path,
        artifacts=artifacts,
    )

    overview = payload["overview_lookup"]["baseline"]

    assert payload["schema_version"] == 2
    assert payload["promotion_gate_summary"] == {}
    assert overview["requested_feature_count"] == 5
    assert overview["used_feature_count"] == 3
    assert overview["rolling_rank_ic_ir"] == pytest.approx(0.11)
    assert overview["rolling_net_total_return"] == pytest.approx(0.12)
    assert overview["rolling_max_drawdown"] == pytest.approx(-0.07)
    assert overview["rolling_sharpe_ratio"] == pytest.approx(0.68)
    assert overview["rolling_win_rate"] == pytest.approx(0.57)
    assert overview["walk_forward_rank_ic_ir"] == pytest.approx(0.07)
    assert overview["walk_forward_net_total_return"] == pytest.approx(0.08)
    assert overview["walk_forward_max_drawdown"] == pytest.approx(-0.05)
    assert overview["walk_forward_annualized_return"] == pytest.approx(0.11)
    assert overview["walk_forward_calmar_ratio"] == pytest.approx(2.2)


def test_build_native_performance_metrics_frame_computes_weekly_metrics():
    report = pd.DataFrame(
        {
            "datetime": pd.to_datetime(["2026-01-02", "2026-01-09", "2026-01-16"]),
            "net_value": [105.0, 102.9, 105.987],
            "net_return": [0.05, -0.02, 0.03],
            "relative_drawdown": [0.0, -0.02, 0.0],
        }
    )

    metrics = build_native_performance_metrics_frame(
        report,
        recipe_name="baseline",
        bundle_name="rolling",
        account=100.0,
    ).iloc[0]

    assert metrics["period_start"] == "2026-01-02"
    assert metrics["period_end"] == "2026-01-16"
    assert metrics["period_count"] == 3
    assert metrics["net_total_return"] == pytest.approx(0.05987)
    assert metrics["annualized_volatility"] == pytest.approx(0.2601, rel=1e-3)
    assert metrics["sharpe_ratio"] == pytest.approx(4.0, rel=1e-2)
    assert metrics["win_rate"] == pytest.approx(2 / 3)
    assert metrics["max_drawdown"] == pytest.approx(-0.02)
    assert metrics["calmar_ratio"] > 0


def test_build_holding_count_drift_keeps_portfolio_columns():
    frame = pd.DataFrame(
        [
            {
                "signal_date": "2026-01-02",
                "target_hold_count": 10,
                "actual_hold_count": 12,
                "residual_hold_count": 2,
                "blocked_sell_count": 1,
                "topk_overlap_prev": 0.7,
            }
        ]
    )

    result = build_holding_count_drift(frame)

    assert list(result.columns) == [
        "signal_date",
        "target_hold_count",
        "actual_hold_count",
        "residual_hold_count",
        "blocked_sell_count",
        "topk_overlap_prev",
    ]
    assert result.iloc[0]["actual_hold_count"] == 12


def test_build_regime_gate_diagnostics_marks_low_dispersion_and_stagflation():
    signal_diagnostics = pd.DataFrame(
            [
                {
                    "signal_date": "2026-01-02",
                    "macro_phase": "STAGFLATION",
                    "score_dispersion": 0.01,
                    "topk_unique_score_ratio": 0.5,
                }
            ]
    )
    predictions = pd.DataFrame(
        [
            {"feature_date": "2026-01-02", "score": 1.0, "macro_phase": "STAGFLATION"},
            {"feature_date": "2026-01-02", "score": 1.0, "macro_phase": "STAGFLATION"},
            {"feature_date": "2026-01-02", "score": 0.8, "macro_phase": "GOLDILOCKS"},
        ]
    )

    result = build_regime_gate_diagnostics(signal_diagnostics, predictions).iloc[0]

    assert bool(result["trigger_stagflation_reduce_50"]) is True
    assert bool(result["trigger_low_dispersion_reduce_50"]) is True
    assert bool(result["trigger_low_uniqueness_filter"]) is True


def test_recipe_experiment_scorecard_rejects_low_uniqueness():
    provider_dir = Path("/tmp")
    artifacts = NativeRecipeArtifacts(
        recipe=NativeResearchRecipe(name="binary_4w"),
        latest_score_frame=pd.DataFrame(),
        prediction_bundles={
            "rolling": {"summary": pd.DataFrame([{"rank_ic_ir": 0.1, "topk_mean_excess_return_4w": 0.01}])},
            "walk_forward": {"summary": pd.DataFrame([{"rank_ic_ir": 0.05, "topk_mean_excess_return_4w": -0.01}])},
        },
        native_results={},
        validation_results={},
        executor_comparison_summary=pd.DataFrame(),
        signal_diagnostics=pd.DataFrame(
            [
                {"bundle": "walk_forward", "topk_unique_score_ratio": 0.4},
            ]
        ),
        portfolio_diagnostics=pd.DataFrame(
            [
                {"bundle": "walk_forward", "actual_hold_count": 10},
            ]
        ),
        slice_regime_summary=pd.DataFrame(),
        feature_prefilter_stats=pd.DataFrame(),
        feature_corr_candidates=pd.DataFrame(),
        feature_redundancy=pd.DataFrame(),
        feature_outlier_audit=pd.DataFrame(),
        used_feature_columns=["ma20"],
        native_provider_dir=provider_dir,
        benchmark_frames={},
        native_summary=pd.DataFrame(
            [
                {
                    "bundle": "walk_forward",
                    "annualized_return": 0.12,
                    "sharpe_ratio": 0.8,
                    "strategy_max_drawdown": -0.1,
                }
            ]
        ),
        signal_realization_bridge=pd.DataFrame([{"bundle": "walk_forward"}]),
        sector_exposure_history=pd.DataFrame([{"bundle": "walk_forward", "top1_sector_weight": 0.2}]),
    )

    scorecard = _build_recipe_experiment_scorecard(
        run_id="demo_run",
        recipe_name="binary_4w",
        artifacts=artifacts,
        baseline_artifacts=None,
        promotion_gate=None,
        topk=10,
    )

    assert scorecard["verdict"] == "rejected"


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


def test_prime_parallel_workflow_inputs_auto_materializes_default_execution_panel_when_run_export_never(monkeypatch, tmp_path):
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
        universe_exit_policy="retain_quotes_for_existing_positions",
        run_export="never",
    )

    primed = _prime_parallel_workflow_inputs(config)

    assert len(calls) == 2
    assert calls[0][1] is True
    assert calls[0][2] == "never"
    assert calls[1][1] is False
    assert calls[1][2] == "auto_if_missing"
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
