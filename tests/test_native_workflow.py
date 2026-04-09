from concurrent.futures import Future

from scripts.evaluate_native_weekly import parse_args
from qlib_research.core.qlib_native_workflow import (
    NativeWorkflowConfig,
    _build_parallel_recipe_heartbeat,
    _prime_parallel_workflow_inputs,
    build_native_recipe_registry,
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
