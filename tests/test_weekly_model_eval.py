import pandas as pd

from qlib_research.core.weekly_model_eval import (
    BacktestPriceFrames,
    EvaluationRuntimeCache,
    ModelRecipe,
    RecipeEvaluation,
    build_backtest_price_frames,
    build_feature_outlier_audit,
    build_feature_redundancy_report,
    build_signal_matrix,
    default_weekly_net_backtest_config,
    get_or_prepare_model_input,
    get_or_prepare_normalized_panel,
    passes_promotion_gate,
    passes_prune_gate,
    prefilter_feature_columns,
    prepare_backtest_price_frame,
    run_strategy_backtest,
)


def test_prepare_backtest_price_frame_forward_fills_and_drops_all_nan_columns():
    price_frame = pd.DataFrame(
        {
            "AAA.SH": [10.0, None, 12.0],
            "BBB.SZ": [None, None, None],
            "CCC.SH": [None, 5.0, None],
        },
        index=pd.to_datetime(["2026-01-02", "2026-01-09", "2026-01-16"]),
    )

    prepared = prepare_backtest_price_frame(price_frame)

    assert "BBB.SZ" not in prepared.columns
    assert prepared.loc[pd.Timestamp("2026-01-09"), "AAA.SH"] == 10.0
    assert prepared.loc[pd.Timestamp("2026-01-16"), "CCC.SH"] == 5.0


def test_build_signal_matrix_skips_symbols_without_price_on_rebalance_date():
    price_frame = pd.DataFrame(
        {
            "AAA.SH": [10.0, 10.5],
            "BBB.SZ": [None, 8.2],
            "CCC.SH": [7.5, 7.8],
        },
        index=pd.to_datetime(["2026-01-02", "2026-01-09"]),
    )

    predictions = pd.DataFrame(
        {
            "feature_date": pd.to_datetime(["2026-01-02", "2026-01-02", "2026-01-02"]),
            "instrument": ["BBB.SZ", "AAA.SH", "CCC.SH"],
            "score": [0.99, 0.70, 0.65],
        }
    )

    signal = build_signal_matrix(predictions, price_frame, topk=2)

    first_date = pd.Timestamp("2026-01-02")
    assert signal.loc[first_date, "BBB.SZ"] == 0.0
    assert signal.loc[first_date, "AAA.SH"] == 0.5
    assert signal.loc[first_date, "CCC.SH"] == 0.5


def test_build_signal_matrix_applies_execution_lag_steps():
    price_frame = pd.DataFrame(
        {
            "AAA.SH": [10.0, 10.5, 10.8],
            "BBB.SZ": [8.0, 8.2, 8.3],
        },
        index=pd.to_datetime(["2026-01-02", "2026-01-09", "2026-01-16"]),
    )
    predictions = pd.DataFrame(
        {
            "feature_date": pd.to_datetime(["2026-01-02", "2026-01-02"]),
            "instrument": ["AAA.SH", "BBB.SZ"],
            "score": [0.9, 0.8],
        }
    )

    signal = build_signal_matrix(predictions, price_frame, topk=1, execution_lag_steps=1)

    assert signal.loc[pd.Timestamp("2026-01-02")].sum() == 0.0
    assert signal.loc[pd.Timestamp("2026-01-09"), "AAA.SH"] == 1.0


def test_run_strategy_backtest_tolerates_leading_nan_prices():
    price_frame = pd.DataFrame(
        {
            "AAA.SH": [None, 10.0, 10.5],
            "BBB.SZ": [8.0, 8.1, 8.2],
        },
        index=pd.to_datetime(["2026-01-02", "2026-01-09", "2026-01-16"]),
    )

    signal_data = pd.DataFrame(
        {
            "AAA.SH": [0.0, 0.5, 0.0],
            "BBB.SZ": [1.0, 0.5, 1.0],
        },
        index=price_frame.index,
    )

    equity_curve, metrics = run_strategy_backtest(price_frame, signal_data)

    assert not equity_curve.empty
    assert "strategy_total_return" in metrics


def test_run_strategy_backtest_collects_net_cost_metrics():
    execution_price_frame = pd.DataFrame(
        {"AAA.SH": [10.0, 11.0], "BBB.SZ": [8.0, 7.5]},
        index=pd.to_datetime(["2026-01-02", "2026-01-09"]),
    )
    mark_price_frame = pd.DataFrame(
        {"AAA.SH": [10.2, 11.5], "BBB.SZ": [8.1, 7.8]},
        index=execution_price_frame.index,
    )
    signal_data = pd.DataFrame(
        {"AAA.SH": [0.5, 0.5], "BBB.SZ": [0.5, 0.5]},
        index=execution_price_frame.index,
    )

    equity_curve, metrics = run_strategy_backtest(
        mark_price_frame,
        signal_data,
        execution_price_frame=execution_price_frame,
        trading_config=default_weekly_net_backtest_config(),
    )

    assert not equity_curve.empty
    assert metrics["strategy_total_cost"] > 0
    assert metrics["strategy_trade_count"] > 0
    assert metrics["strategy_total_return"] <= ((equity_curve["total_value"].iloc[-1] / 1_000_000.0) - 1.0) + 1e-9


def test_build_backtest_price_frames_uses_open_close_low_columns():
    panel = pd.DataFrame(
        {
            "datetime": pd.to_datetime(["2026-01-02", "2026-01-02", "2026-01-09", "2026-01-09"]),
            "instrument": ["AAA.SH", "BBB.SZ", "AAA.SH", "BBB.SZ"],
            "open": [10.0, 8.0, 10.5, 8.1],
            "close": [10.2, 8.1, 10.8, 8.3],
            "low": [9.8, 7.9, 10.1, 8.0],
        }
    )

    frames = build_backtest_price_frames(
        panel=panel,
        eval_dates=pd.to_datetime(["2026-01-02", "2026-01-09"]),
    )

    assert isinstance(frames, BacktestPriceFrames)
    assert frames.execution_price.loc[pd.Timestamp("2026-01-09"), "AAA.SH"] == 10.5
    assert frames.mark_price.loc[pd.Timestamp("2026-01-09"), "BBB.SZ"] == 8.3
    assert frames.stop_price.loc[pd.Timestamp("2026-01-02"), "AAA.SH"] == 9.8


def test_prefilter_feature_columns_drops_sparse_and_constant_features():
    panel = pd.DataFrame(
        {
            "datetime": pd.to_datetime(["2026-01-02", "2026-01-02", "2026-01-09", "2026-01-09"]),
            "good_feature": [1.0, 2.0, 3.0, 4.0],
            "sparse_feature": [1.0, None, None, None],
            "constant_feature": [5.0, 5.0, 5.0, 5.0],
        }
    )

    kept, stats, corr_marks = prefilter_feature_columns(
        panel,
        feature_columns=["good_feature", "sparse_feature", "constant_feature"],
        missing_threshold=0.35,
    )

    assert kept == ["good_feature"]
    assert set(stats["feature"]) == {"good_feature", "sparse_feature", "constant_feature"}
    assert corr_marks.empty


def test_get_or_prepare_normalized_panel_reuses_cached_superset(monkeypatch):
    panel = pd.DataFrame(
        {
            "datetime": pd.to_datetime(["2026-01-02", "2026-01-02"]),
            "instrument": ["AAA.SH", "BBB.SZ"],
            "l1_name": ["Tech", "Tech"],
            "feature_a": [1.0, 2.0],
            "feature_b": [3.0, 4.0],
        }
    )
    calls: list[tuple[tuple[str, ...], str]] = []

    def fake_apply_industry_normalization(frame, feature_columns, method):
        calls.append((tuple(feature_columns), method))
        normalized = frame.copy()
        normalized["_normalization_call"] = len(calls)
        return normalized

    monkeypatch.setattr("qlib_research.core.weekly_model_eval.apply_industry_normalization", fake_apply_industry_normalization)

    runtime_cache = EvaluationRuntimeCache(normalization_feature_columns=("feature_a", "feature_b"))
    recipe_a = ModelRecipe(name="recipe_a", feature_columns=("feature_a",), industry_normalization="l1_weekly_robust")
    recipe_b = ModelRecipe(name="recipe_b", feature_columns=("feature_b",), industry_normalization="l1_weekly_robust")

    normalized_a, key_a = get_or_prepare_normalized_panel(panel, recipe_a, runtime_cache=runtime_cache)
    normalized_b, key_b = get_or_prepare_normalized_panel(panel, recipe_b, runtime_cache=runtime_cache)

    assert calls == [(("feature_a", "feature_b"), "l1_weekly_robust")]
    assert key_a == key_b
    assert normalized_a is normalized_b
    assert int(normalized_b["_normalization_call"].iloc[0]) == 1


def test_get_or_prepare_model_input_reuses_cached_training_frame(monkeypatch):
    panel = pd.DataFrame(
        {
            "datetime": pd.to_datetime(
                ["2026-01-02", "2026-01-09", "2026-01-16", "2026-01-23"]
            ),
            "instrument": ["AAA.SH"] * 4,
            "feature_a": [1.0, 2.0, 3.0, 4.0],
            "label_excess_return_4w": [0.01, 0.02, 0.03, 0.04],
        }
    )
    build_calls: list[tuple[pd.Timestamp, ...]] = []
    segment_calls: list[pd.Timestamp] = []

    def fake_build_training_frame(frame, feature_columns, label_column, fill_values=None):
        build_calls.append(tuple(pd.to_datetime(frame["datetime"])))
        assert fill_values == {"feature_a": 1.5}
        qlib_frame = pd.DataFrame(
            {
                "feature_a": [1.0],
                "label_excess_return_4w": [0.01],
            },
            index=pd.MultiIndex.from_tuples(
                [(pd.Timestamp("2026-01-23"), "AAA.SH")],
                names=["datetime", "instrument"],
            ),
        )
        return qlib_frame, list(feature_columns)

    def fake_build_rolling_segments(frame, feature_date, train_weeks, valid_weeks, label_ready_dates=None):
        segment_calls.append(pd.Timestamp(feature_date))
        return {
            "train": ("2026-01-02", "2026-01-09"),
            "valid": ("2026-01-16", "2026-01-16"),
            "test": ("2026-01-23", "2026-01-23"),
        }

    monkeypatch.setattr(
        "qlib_research.core.weekly_model_eval.compute_feature_fill_values",
        lambda frame, feature_columns=None: {"feature_a": 1.5},
    )
    monkeypatch.setattr("qlib_research.core.weekly_model_eval.build_training_frame", fake_build_training_frame)
    monkeypatch.setattr("qlib_research.core.weekly_model_eval.build_rolling_segments", fake_build_rolling_segments)

    runtime_cache = EvaluationRuntimeCache()
    feature_date = pd.Timestamp("2026-01-23")
    normalization_cache_key = ("l1_weekly_robust", ("feature_a",))

    prepared_a = get_or_prepare_model_input(
        panel=panel,
        feature_date=feature_date,
        feature_columns=("feature_a",),
        train_weeks=2,
        valid_weeks=1,
        normalization_cache_key=normalization_cache_key,
        runtime_cache=runtime_cache,
    )
    prepared_b = get_or_prepare_model_input(
        panel=panel,
        feature_date=feature_date,
        feature_columns=("feature_a",),
        train_weeks=2,
        valid_weeks=1,
        normalization_cache_key=normalization_cache_key,
        runtime_cache=runtime_cache,
    )

    assert len(build_calls) == 1
    assert len(segment_calls) == 1
    assert prepared_a is prepared_b
    assert prepared_b.used_features == ["feature_a"]


def test_feature_redundancy_report_uses_requested_window():
    panel = pd.DataFrame(
        {
            "datetime": pd.to_datetime(["2026-01-02", "2026-01-09", "2026-01-16", "2026-01-23"]),
            "feature_a": [1.0, 2.0, 3.0, 4.0],
            "feature_b": [2.0, 4.0, 6.0, 8.0],
            "feature_c": [1.0, 1.0, 2.0, 3.0],
        }
    )

    redundancy = build_feature_redundancy_report(
        panel,
        ["feature_a", "feature_b", "feature_c"],
        end_date="2026-01-16",
        corr_threshold=0.95,
    )

    assert not redundancy.empty
    assert redundancy.iloc[0]["left_feature"] == "feature_a"
    assert redundancy.iloc[0]["right_feature"] == "feature_b"
    assert redundancy.iloc[0]["window_end"] == "2026-01-16"


def test_feature_outlier_audit_skips_flag_and_rank_columns():
    panel = pd.DataFrame(
        {
            "datetime": pd.to_datetime(["2026-01-02", "2026-01-09", "2026-01-16", "2026-01-23"]),
            "pe_ttm": [10.0, 11.0, 12.0, 100.0],
            "macro_phase_reflation": [1, 0, 1, 0],
            "industry_mom_4w_rank_pct": [0.1, 0.2, 0.3, 0.4],
        }
    )

    audit = build_feature_outlier_audit(
        panel,
        ["pe_ttm", "macro_phase_reflation", "industry_mom_4w_rank_pct"],
    )

    assert audit["feature"].tolist() == ["pe_ttm"]


def make_evaluation(
    *,
    used_feature_count: int,
    rank_ic_mean: float,
    rank_ic_ir: float,
    topk_excess: float,
    drawdown: float,
    csi300_rank_ic_ir: float,
    csi500_rank_ic_ir: float,
    csi300_drawdown: float,
    csi500_drawdown: float,
) -> RecipeEvaluation:
    recipe = ModelRecipe(name="test", feature_columns=("a",), feature_groups=("technical_core",))
    summary = pd.DataFrame(
        [
            {
                "recipe": "test",
                "used_feature_count": used_feature_count,
                "rank_ic_mean": rank_ic_mean,
                "rank_ic_ir": rank_ic_ir,
                "topk_mean_excess_return_4w": topk_excess,
                "strategy_max_drawdown": drawdown,
                "strategy_total_return": 0.20,
            }
        ]
    )
    slice_summary = pd.DataFrame(
        [
            {"recipe": "test", "slice": "csi300", "rank_ic_ir": csi300_rank_ic_ir, "strategy_max_drawdown": csi300_drawdown},
            {"recipe": "test", "slice": "csi500", "rank_ic_ir": csi500_rank_ic_ir, "strategy_max_drawdown": csi500_drawdown},
        ]
    )
    return RecipeEvaluation(
        recipe=recipe,
        summary=summary,
        slice_summary=slice_summary,
        details=pd.DataFrame(),
        slice_details=pd.DataFrame(),
        predictions=pd.DataFrame(),
        equity_curve=pd.DataFrame(),
    )


def test_passes_prune_gate_accepts_small_rank_drop_with_better_drawdown():
    baseline = make_evaluation(
        used_feature_count=40,
        rank_ic_mean=0.03,
        rank_ic_ir=0.12,
        topk_excess=0.01,
        drawdown=-0.10,
        csi300_rank_ic_ir=0.05,
        csi500_rank_ic_ir=0.04,
        csi300_drawdown=-0.11,
        csi500_drawdown=-0.12,
    )
    candidate = make_evaluation(
        used_feature_count=35,
        rank_ic_mean=0.03,
        rank_ic_ir=0.115,
        topk_excess=0.0095,
        drawdown=-0.08,
        csi300_rank_ic_ir=0.05,
        csi500_rank_ic_ir=0.04,
        csi300_drawdown=-0.10,
        csi500_drawdown=-0.10,
    )

    accepted, deltas = passes_prune_gate(candidate, baseline)

    assert accepted
    assert round(float(deltas["strategy_max_drawdown_delta"]), 6) == 0.02


def test_passes_promotion_gate_requires_feature_reduction_and_slice_strength():
    baseline = make_evaluation(
        used_feature_count=40,
        rank_ic_mean=0.02,
        rank_ic_ir=0.10,
        topk_excess=0.01,
        drawdown=-0.10,
        csi300_rank_ic_ir=0.08,
        csi500_rank_ic_ir=0.07,
        csi300_drawdown=-0.10,
        csi500_drawdown=-0.11,
    )
    candidate = make_evaluation(
        used_feature_count=30,
        rank_ic_mean=0.025,
        rank_ic_ir=0.11,
        topk_excess=0.012,
        drawdown=-0.095,
        csi300_rank_ic_ir=0.08,
        csi500_rank_ic_ir=0.08,
        csi300_drawdown=-0.11,
        csi500_drawdown=-0.12,
    )

    passed, details = passes_promotion_gate(candidate, baseline)

    assert passed
    assert round(float(details["feature_reduction_ratio"]), 6) == 0.25
