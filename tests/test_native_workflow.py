from concurrent.futures import Future
from pathlib import Path

import pandas as pd
import pytest

import qlib_research.core.qlib_native_workflow as native_workflow
from scripts.evaluate_native_weekly import parse_args
from qlib_research.core.notebook_workflow import build_native_workflow_cli_command, load_native_workflow_artifacts, run_native_notebook_workflow
from qlib_research.core.qlib_native_backtest import (
    DirectionalOrderGenWInteract,
    _build_target_weight_dict,
    build_native_quote_frame,
    build_rebalance_audit,
    select_topk_with_buffer,
)
from qlib_research.core.qlib_native_workflow import (
    ConsensusRecipeSpec,
    NativeRecipeArtifacts,
    NativeResearchRecipe,
    NativeWorkflowConfig,
    _apply_consensus_filter,
    _build_native_workflow_summary_payload,
    _decorate_latest_snapshot_frame,
    _build_recipe_experiment_scorecard,
    _build_parallel_recipe_heartbeat,
    _mask_untradable_execution_prices,
    _prediction_bundle_rebalance_steps,
    _normalize_consensus_recipe_specs,
    _prime_parallel_workflow_inputs,
    build_sector_exposure_history,
    build_execution_diff_summary,
    build_native_performance_metrics_frame,
    build_annual_return_heatmap_frame,
    build_holding_count_drift,
    build_monthly_return_heatmap_frame,
    build_native_recipe_registry,
    build_regime_gate_diagnostics,
    build_validation_signal_matrix,
    run_native_research_workflow,
    select_evaluation_dates_for_label,
)


def _stub_native_recipe_artifacts(recipe_name: str, recipe_dir: Path) -> NativeRecipeArtifacts:
    recipe_dir.mkdir(parents=True, exist_ok=True)
    return NativeRecipeArtifacts(
        recipe=NativeResearchRecipe(name=recipe_name),
        latest_score_frame=pd.DataFrame(),
        prediction_bundles={
            "rolling": {"summary": pd.DataFrame()},
            "walk_forward": {"summary": pd.DataFrame()},
        },
        native_results={},
        validation_results={},
        executor_comparison_summary=pd.DataFrame(),
        signal_diagnostics=pd.DataFrame(),
        portfolio_diagnostics=pd.DataFrame(),
        slice_regime_summary=pd.DataFrame(),
        feature_prefilter_stats=pd.DataFrame(),
        feature_corr_candidates=pd.DataFrame(),
        feature_redundancy=pd.DataFrame(),
        feature_outlier_audit=pd.DataFrame(),
        used_feature_columns=[],
        native_provider_dir=recipe_dir,
        benchmark_frames={},
        native_summary=pd.DataFrame(),
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


def test_native_workflow_config_normalizes_consensus_recipe_specs():
    config = NativeWorkflowConfig(
        consensus_recipe_specs=(
            {
                "primary_recipe": "rank_blended",
                "filter_recipe": "mae_4w",
                "filter_topn": 15,
                "name": "rank_blended__consensus__mae_4w_top15",
            },
        ),
    )

    assert config.consensus_recipe_specs == (
        ConsensusRecipeSpec(
            primary_recipe="rank_blended",
            filter_recipe="mae_4w",
            filter_topn=15,
            name="rank_blended__consensus__mae_4w_top15",
        ),
    )


def test_normalize_consensus_recipe_specs_applies_default_topn_and_name():
    config = NativeWorkflowConfig(
        topk=10,
        hold_buffer_rank=15,
        consensus_recipe_specs=(
            {
                "primary_recipe": "rank_blended",
                "filter_recipe": "mae_4w",
            },
        ),
    )

    specs = _normalize_consensus_recipe_specs(config, build_native_recipe_registry(config))

    assert specs == (
        ConsensusRecipeSpec(
            primary_recipe="rank_blended",
            filter_recipe="mae_4w",
            filter_topn=15,
            name="rank_blended__consensus__mae_4w_top15",
        ),
    )


def test_normalize_consensus_recipe_specs_rejects_invalid_pairs():
    config = NativeWorkflowConfig(
        consensus_recipe_specs=(
            {
                "primary_recipe": "rank_blended",
                "filter_recipe": "rank_blended",
            },
        ),
    )

    with pytest.raises(ValueError, match="cannot reuse the same recipe twice"):
        _normalize_consensus_recipe_specs(config, build_native_recipe_registry(config))


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


def test_evaluate_native_weekly_parse_args_accepts_consensus_specs(monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        [
            "evaluate_native_weekly.py",
            "--consensus-spec-json",
            '{"primary_recipe":"rank_blended","filter_recipe":"mae_4w","filter_topn":15}',
        ],
    )

    args = parse_args()

    assert args.consensus_spec_json == ['{"primary_recipe":"rank_blended","filter_recipe":"mae_4w","filter_topn":15}']


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


def test_build_sector_exposure_history_falls_back_to_global_sector_map():
    predictions = pd.DataFrame(
        [
            {"feature_date": "2026-01-03", "instrument": "AAA.SH", "l1_name": "银行"},
            {"feature_date": "2026-01-03", "instrument": "BBB.SH", "l1_name": "电子"},
            {"feature_date": "2026-01-10", "instrument": "BBB.SH", "l1_name": "电子"},
        ]
    )
    audit = pd.DataFrame(
        [
            {
                "signal_date": "2026-01-10",
                "trade_date": "2026-01-17",
                "post_trade_holdings": "AAA.SH,BBB.SH",
            }
        ]
    )

    history = build_sector_exposure_history(predictions, audit)

    assert len(history) == 1
    assert history.loc[0, "sector_weight__银行"] == pytest.approx(0.5)
    assert history.loc[0, "sector_weight__电子"] == pytest.approx(0.5)
    assert history.loc[0, "top1_sector_weight"] == pytest.approx(0.5)


def test_build_rebalance_audit_does_not_lock_sellable_positions():
    class _Position:
        def __init__(self, codes):
            self._codes = list(codes)

        def get_stock_list(self):
            return list(self._codes)

    dates = pd.to_datetime(["2026-01-02", "2026-01-09", "2026-01-16"])
    signal = pd.DataFrame(
        [
            {"feature_date": dates[0], "instrument": "AAA.SH", "score": 0.9},
            {"feature_date": dates[0], "instrument": "BBB.SH", "score": 0.1},
            {"feature_date": dates[1], "instrument": "AAA.SH", "score": 0.1},
            {"feature_date": dates[1], "instrument": "BBB.SH", "score": 0.9},
            {"feature_date": dates[2], "instrument": "AAA.SH", "score": 0.1},
            {"feature_date": dates[2], "instrument": "BBB.SH", "score": 0.9},
        ]
    )
    quote_panel = pd.DataFrame(
        [
            {"datetime": date, "instrument": code, "open": 10.0, "close": 10.0, "volume": 1000.0}
            for date in dates
            for code in ("AAA.SH", "BBB.SH")
        ]
    )
    quote_frame = build_native_quote_frame(quote_panel)
    positions = {
        dates[1]: _Position(["AAA.SH"]),
        dates[2]: _Position(["BBB.SH"]),
    }

    audit = build_rebalance_audit(signal, quote_frame, positions, topk=1)

    assert len(audit) == 2
    assert int(audit.loc[1, "locked_residual_count"]) == 0
    assert int(audit.loc[1, "sell_blocked_total_count"]) == 0
    assert int(audit.loc[1, "target_hold_count"]) == 1
    assert audit.loc[1, "target_holdings"] == "BBB.SH"


def test_select_topk_with_buffer_applies_industry_cap_to_retained_holdings():
    score = pd.DataFrame(
        [
            {"instrument": "AAA.SH", "score": 0.90, "l1_name": "银行", "amount": 90.0},
            {"instrument": "BBB.SH", "score": 0.89, "l1_name": "银行", "amount": 80.0},
            {"instrument": "CCC.SH", "score": 0.88, "l1_name": "电子", "amount": 70.0},
        ]
    )

    selected = select_topk_with_buffer(
        score,
        current_holdings=["AAA.SH", "BBB.SH"],
        topk=2,
        industry_max_weight=0.5,
        score_column="score",
    )

    assert selected == ["AAA.SH", "CCC.SH"]


def test_select_topk_with_buffer_uses_liquidity_tie_break_for_equal_scores():
    score = pd.DataFrame(
        [
            {"instrument": "AAA.SH", "score": 0.50, "amount": 100.0, "volume": 1000.0},
            {"instrument": "BBB.SH", "score": 0.50, "amount": 300.0, "volume": 900.0},
            {"instrument": "CCC.SH", "score": 0.50, "amount": 200.0, "volume": 800.0},
        ]
    )

    selected = select_topk_with_buffer(score, topk=2, score_column="score")

    assert selected == ["BBB.SH", "CCC.SH"]


def test_decorate_latest_snapshot_frame_prioritizes_selected_codes():
    frame = pd.DataFrame(
        [
            {"instrument": "AAA.SH", "score": 0.50, "amount": 100.0, "volume": 1000.0},
            {"instrument": "BBB.SH", "score": 0.50, "amount": 300.0, "volume": 900.0},
            {"instrument": "CCC.SH", "score": 0.49, "amount": 200.0, "volume": 800.0},
        ]
    )

    decorated = _decorate_latest_snapshot_frame(frame, selected_codes=["CCC.SH", "BBB.SH"])

    assert list(decorated["instrument"])[:2] == ["CCC.SH", "BBB.SH"]
    assert list(decorated["target_selection_rank"])[:2] == [1, 2]
    assert decorated.loc[0, "selected_for_target"]
    assert "selection_priority" in decorated.columns


def test_build_target_weight_dict_reserves_budget_for_locked_residuals():
    weights = _build_target_weight_dict(
        selected_codes=["BBB.SH", "CCC.SH"],
        current_weights={"AAA.SH": 0.4, "DDD.SH": 0.6},
        locked_residual_codes=["AAA.SH"],
    )

    assert weights["AAA.SH"] == pytest.approx(0.4)
    assert weights["BBB.SH"] == pytest.approx(0.3)
    assert weights["CCC.SH"] == pytest.approx(0.3)
    assert weights["DDD.SH"] == pytest.approx(0.0)


def test_prediction_bundle_rebalance_steps_does_not_double_throttle_sparse_predictions():
    predictions = pd.DataFrame(
        [
            {"feature_date": "2026-01-02", "instrument": "AAA.SH", "score": 0.9},
            {"feature_date": "2026-01-30", "instrument": "AAA.SH", "score": 0.8},
            {"feature_date": "2026-02-27", "instrument": "AAA.SH", "score": 0.7},
        ]
    )

    assert _prediction_bundle_rebalance_steps(predictions) == 1


def test_mask_untradable_execution_prices_accepts_dataframe_inputs():
    dates = pd.to_datetime(["2026-01-02", "2026-01-09"])
    execution_price = pd.DataFrame(
        {
            "A": ["10.0", "0"],
            "B": [12.0, 13.0],
        },
        index=dates,
    )
    volume = pd.DataFrame(
        {
            "A": [1000.0, 1000.0],
            "B": [0.0, 2000.0],
        },
        index=dates,
    )

    masked = _mask_untradable_execution_prices(execution_price, volume)

    assert masked.loc[dates[0], "A"] == "10.0"
    assert pd.isna(masked.loc[dates[1], "A"])
    assert pd.isna(masked.loc[dates[0], "B"])
    assert masked.loc[dates[1], "B"] == 13.0


def test_directional_order_generator_allows_sell_when_buy_is_limited():
    from qlib.backtest.decision import Order

    class _Position:
        def get_stock_amount_dict(self):
            return {"LIMITUP.SH": 100.0, "NORMAL.SH": 100.0}

        def get_cash(self):
            return 0.0

    class _Exchange:
        open_cost = 0.0
        close_cost = 0.0

        def calculate_amount_position_value(self, amount_dict, start_time, end_time, only_tradable=False):
            if not only_tradable:
                return float(sum(amount_dict.values()))
            return float(sum(value for code, value in amount_dict.items() if code != "LIMITUP.SH"))

        def generate_amount_position_from_weight_position(self, weight_position, cash, start_time, end_time):
            return {}

        def get_factor(self, stock_id, start_time, end_time):
            return 1.0

        def get_real_deal_amount(self, current_amount, target_amount, factor=None):
            if current_amount == target_amount:
                return 0.0
            if current_amount < target_amount:
                return target_amount - current_amount
            return -(current_amount - target_amount)

        def is_stock_tradable(self, stock_id, start_time, end_time, direction=None):
            if stock_id != "LIMITUP.SH":
                return True
            if direction == Order.SELL:
                return True
            return False

    generator = DirectionalOrderGenWInteract()
    orders = generator.generate_order_list_from_target_weight_position(
        current=_Position(),
        trade_exchange=_Exchange(),
        target_weight_position={},
        risk_degree=1.0,
        pred_start_time=pd.Timestamp("2026-01-02"),
        pred_end_time=pd.Timestamp("2026-01-02"),
        trade_start_time=pd.Timestamp("2026-01-09"),
        trade_end_time=pd.Timestamp("2026-01-09"),
    )

    assert {(order.stock_id, order.direction, order.amount) for order in orders} == {
        ("LIMITUP.SH", Order.SELL, 100.0),
        ("NORMAL.SH", Order.SELL, 100.0),
    }


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

    assert payload["schema_version"] == 3
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


def test_apply_consensus_filter_preserves_primary_ranking_without_backfill():
    primary = pd.DataFrame(
        [
            {"feature_date": "2026-01-02", "instrument": "CCC", "score": 0.9},
            {"feature_date": "2026-01-02", "instrument": "BBB", "score": 0.8},
            {"feature_date": "2026-01-02", "instrument": "AAA", "score": 0.7},
        ]
    )
    filter_frame = pd.DataFrame(
        [
            {"feature_date": "2026-01-02", "instrument": "BBB", "score": 1.0},
            {"feature_date": "2026-01-02", "instrument": "CCC", "score": 0.5},
            {"feature_date": "2026-01-02", "instrument": "AAA", "score": 0.1},
        ]
    )

    filtered = _apply_consensus_filter(primary, filter_frame, filter_topn=2)

    assert filtered["instrument"].tolist() == ["CCC", "BBB"]
    assert filtered["score"].tolist() == [pytest.approx(0.9), pytest.approx(0.8)]
    assert filtered["filter_rank"].tolist() == [2, 1]
    assert filtered["consensus_passed"].tolist() == [True, True]


def test_run_native_research_workflow_auto_adds_consensus_dependencies(monkeypatch, tmp_path):
    executed_recipe_names: list[str] = []
    execution_panel = pd.DataFrame(
        [
            {"datetime": "2026-01-02", "instrument": "AAA"},
            {"datetime": "2026-01-02", "instrument": "BBB"},
        ]
    )

    def fake_run_native_recipe_job(config: NativeWorkflowConfig, recipe: NativeResearchRecipe) -> tuple[str, NativeRecipeArtifacts]:
        executed_recipe_names.append(recipe.name)
        return recipe.name, _stub_native_recipe_artifacts(recipe.name, Path(config.output_dir) / recipe.name)

    def fake_build_consensus_recipe_artifacts(**kwargs: object) -> tuple[str, NativeRecipeArtifacts]:
        spec = kwargs["spec"]
        assert isinstance(spec, ConsensusRecipeSpec)
        return str(spec.name), _stub_native_recipe_artifacts(str(spec.name), tmp_path / str(spec.name))

    monkeypatch.setattr(native_workflow, "_run_native_recipe_job", fake_run_native_recipe_job)
    monkeypatch.setattr(native_workflow, "_build_consensus_recipe_artifacts", fake_build_consensus_recipe_artifacts)
    monkeypatch.setattr(native_workflow, "_prepare_execution_panel", lambda config, execution_path: (execution_panel.copy(), Path(config.output_dir) / "execution.parquet"))
    monkeypatch.setattr(
        native_workflow,
        "_build_recipe_experiment_scorecard",
        lambda **kwargs: {"recipe_name": kwargs["recipe_name"], "verdict": "monitor"},
    )
    monkeypatch.setattr(native_workflow, "_build_run_experiment_scorecard", lambda **kwargs: {"headline": "ok"})
    monkeypatch.setattr(native_workflow, "passes_native_promotion_gate", lambda baseline, candidate: {"promotion_gate_passed": True})
    monkeypatch.setattr(
        native_workflow,
        "_build_native_workflow_summary_payload",
        lambda **kwargs: {
            "schema_version": 3,
            "recipe_registry": kwargs["registry_payload"],
            "promotion_gate_summary": kwargs["promotion_gate"],
            "overview_lookup": {},
        },
    )

    result = run_native_research_workflow(
        NativeWorkflowConfig(
            output_dir=tmp_path,
            consensus_recipe_specs=(
                {
                    "primary_recipe": "rank_blended",
                    "filter_recipe": "mae_4w",
                    "filter_topn": 15,
                },
            ),
        ),
        recipe_names=["baseline"],
    )

    derived_recipe_name = "rank_blended__consensus__mae_4w_top15"
    assert executed_recipe_names == ["baseline", "rank_blended", "mae_4w"]
    assert result["recipe_registry"]["requested_recipes"] == ["baseline"]
    assert result["recipe_registry"]["executed_recipes"] == ["baseline", "rank_blended", "mae_4w", derived_recipe_name]
    assert result["recipe_registry"]["derived_recipes"][derived_recipe_name]["recipe_kind"] == "consensus_filter"
    assert result["recipe_registry"]["derived_recipes"][derived_recipe_name]["primary_recipe"] == "rank_blended"
    assert result["recipe_registry"]["derived_recipes"][derived_recipe_name]["filter_recipe"] == "mae_4w"


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


def test_validation_signal_matrix_uses_risk_degree_and_locked_residual_budget():
    dates = pd.to_datetime(["2026-01-02", "2026-01-09", "2026-01-16"])
    predictions = pd.DataFrame(
        [
            {"feature_date": dates[0], "instrument": "A", "score": 0.9},
            {"feature_date": dates[0], "instrument": "B", "score": 0.2},
            {"feature_date": dates[1], "instrument": "A", "score": 0.1},
            {"feature_date": dates[1], "instrument": "B", "score": 0.8},
            {"feature_date": dates[2], "instrument": "A", "score": 0.1},
            {"feature_date": dates[2], "instrument": "B", "score": 0.9},
        ]
    )
    execution_price = pd.DataFrame(
        {
            "A": [10.0, 10.0, 10.0],
            "B": [10.0, 10.0, 10.0],
        },
        index=dates,
    )
    volume = pd.DataFrame(
        {
            "A": [1000.0, 0.0, 1000.0],
            "B": [1000.0, 1000.0, 1000.0],
        },
        index=dates,
    )

    result = build_validation_signal_matrix(
        predictions,
        execution_price,
        topk=1,
        risk_degree=0.5,
        only_tradable=True,
        enforce_locked_residual_slot_budget=True,
        volume_frame=volume,
    )

    assert result.signal_matrix.loc[dates[0], "A"] == pytest.approx(0.5)
    assert result.signal_matrix.loc[dates[1], "A"] == pytest.approx(0.5)
    assert result.signal_matrix.loc[dates[1], "B"] == pytest.approx(0.0)
    assert result.signal_matrix.loc[dates[2], "B"] == pytest.approx(0.5)
    assert result.diagnostics.loc[1, "locked_residual_count"] == 1


def test_validation_signal_matrix_can_disable_tradability_locking():
    dates = pd.to_datetime(["2026-01-02", "2026-01-09"])
    predictions = pd.DataFrame(
        [
            {"feature_date": dates[0], "instrument": "A", "score": 0.9},
            {"feature_date": dates[0], "instrument": "B", "score": 0.2},
            {"feature_date": dates[1], "instrument": "A", "score": 0.1},
            {"feature_date": dates[1], "instrument": "B", "score": 0.8},
        ]
    )
    execution_price = pd.DataFrame({"A": [10.0, 10.0], "B": [10.0, 10.0]}, index=dates)
    volume = pd.DataFrame({"A": [1000.0, 0.0], "B": [1000.0, 1000.0]}, index=dates)

    result = build_validation_signal_matrix(
        predictions,
        execution_price,
        topk=1,
        only_tradable=False,
        enforce_locked_residual_slot_budget=True,
        volume_frame=volume,
    )

    assert result.signal_matrix.loc[dates[1], "A"] == pytest.approx(0.0)
    assert result.signal_matrix.loc[dates[1], "B"] == pytest.approx(1.0)
    assert result.diagnostics.loc[1, "locked_residual_count"] == 0


def test_build_execution_diff_summary_includes_validation_alignment_fields():
    native_report = pd.DataFrame(
        {
            "datetime": pd.to_datetime(["2026-01-02", "2026-01-09"]),
            "net_value": [100.0, 110.0],
            "relative_drawdown": [0.0, -0.01],
        }
    )
    validation_frame = pd.DataFrame(
        {
            "datetime": pd.to_datetime(["2026-01-02", "2026-01-09"]),
            "net_value": [100.0, 104.0],
            "holding_count": [1, 2],
        }
    )
    validation_result = {
        "equity_curve_frame": validation_frame,
        "signal_diagnostics": pd.DataFrame(
            [
                {"target_hold_count": 1, "locked_residual_count": 0},
                {"target_hold_count": 2, "locked_residual_count": 1},
            ]
        ),
    }
    config = NativeWorkflowConfig(
        account=100.0,
        native_risk_degree=0.95,
        validation_risk_degree=0.9,
        native_only_tradable=True,
        validation_only_tradable=True,
        validation_execution_lag_steps=2,
    )

    summary = build_execution_diff_summary(
        "baseline",
        "rolling",
        native_report,
        validation_result,
        account=100.0,
        config=config,
    ).iloc[0]

    assert summary["native_minus_validation_return"] == pytest.approx(0.06)
    assert summary["validation_target_hold_max"] == 2
    assert summary["validation_locked_residual_max"] == 1
    assert summary["validation_actual_hold_latest"] == 2
    assert summary["native_risk_degree"] == pytest.approx(0.95)
    assert summary["validation_risk_degree"] == pytest.approx(0.9)
    assert summary["validation_execution_lag_steps"] == 2


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
