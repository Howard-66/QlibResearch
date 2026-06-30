import pandas as pd
import pytest

from qlib_research.core.breakout_stock import (
    StockBreakoutConfig,
    build_stock_breakout_feature_panel,
    build_stock_breakout_research_frame,
    build_stock_breakout_signal_frame,
    detect_stock_breakout_events,
    evaluate_stock_breakout_scores,
    label_stock_breakout_events,
    score_stock_breakout_events,
    stock_breakout_feature_columns,
    train_lightgbm_stock_breakout_model,
)
from scripts import run_stock_breakout_research as breakout_script


def _price_frame() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-01", periods=110)
    rows = []
    for idx, date in enumerate(dates):
        if idx < 90:
            close = 10.0 + min(idx, 59) * 0.01
        elif idx == 90:
            close = 11.3
        else:
            close = 11.3 + (idx - 90) * 0.04
        rows.append(
            {
                "code": "AAA.SH",
                "date": date,
                "open": close * 0.99,
                "high": close * 1.01,
                "low": close * 0.98,
                "close": close,
                "volume": 1000 + idx * 5,
            }
        )
    return pd.DataFrame(rows)


def test_detect_label_and_feature_stock_breakout_events():
    config = StockBreakoutConfig(lookback_window=60, min_history_days=60, min_volume_ratio=0.0)
    prices = _price_frame()

    events = detect_stock_breakout_events(prices, config)
    labeled = label_stock_breakout_events(prices, events, config)
    features = build_stock_breakout_feature_panel(prices, labeled, config)

    assert not events.empty
    first = features.iloc[0]
    assert first["asset_class"] == "stock"
    assert first["side"] == "long"
    assert first["breakout_strength"] > 0
    assert pd.notna(first["future_return_13d"])
    assert pd.notna(first["return_20d"])
    assert "entry_price" in stock_breakout_feature_columns(features)


def test_detect_stock_breakout_events_accepts_quality_filters():
    prices = _price_frame()

    loose = detect_stock_breakout_events(
        prices,
        StockBreakoutConfig(lookback_window=60, min_history_days=60, min_volume_ratio=0.0),
    )
    capped_breakout = detect_stock_breakout_events(
        prices,
        StockBreakoutConfig(lookback_window=60, min_history_days=60, min_volume_ratio=0.0, max_breakout_pct=0.001),
    )
    high_close_position = detect_stock_breakout_events(
        prices,
        StockBreakoutConfig(lookback_window=60, min_history_days=60, min_volume_ratio=0.0, min_close_position=0.95),
    )

    assert not loose.empty
    assert capped_breakout.empty
    assert high_close_position.empty


def test_build_research_frame_signal_frame_and_metrics():
    config = StockBreakoutConfig(lookback_window=60, min_history_days=60, min_volume_ratio=0.0)
    research = build_stock_breakout_research_frame(_price_frame(), config)
    research["signal_score"] = research["future_return_13d"]

    signals = build_stock_breakout_signal_frame(research, model_id="breakout-demo", feature_date="2024-05-10", config=config)
    metrics = evaluate_stock_breakout_scores(research)

    assert not signals.empty
    assert set(["event_id", "code", "signal_date", "signal_score", "pred_return_13d"]).issubset(signals.columns)
    assert signals.iloc[0]["model_id"] == "breakout-demo"
    assert metrics["event_count"] == len(research)
    assert metrics["top20_excess_return"] is not None
    assert metrics["score_unique_ratio"] is not None


def test_signal_frame_preserves_raw_prediction_when_score_is_calibrated():
    config = StockBreakoutConfig(label_horizon_days=13)
    scored = pd.DataFrame(
        {
            "event_id": ["e1", "e2"],
            "code": ["AAA.SH", "BBB.SH"],
            "event_date": ["2024-01-02", "2024-01-02"],
            "signal_score": [1.0, 0.5],
            "raw_signal_score": [0.08, -0.02],
            "entry_price": [10.0, 20.0],
            "ref_high": [9.8, 19.8],
            "ref_low": [9.1, 18.7],
        }
    )

    signals = build_stock_breakout_signal_frame(scored, model_id="calibrated", config=config)

    assert signals.loc[0, "signal_score"] == pytest.approx(1.0)
    assert signals.loc[0, "pred_return_13d"] == pytest.approx(0.08)
    assert signals.loc[0, "raw_signal_score"] == pytest.approx(0.08)


def test_daily_excess_target_demeans_same_date_candidates():
    research = pd.DataFrame(
        {
            "event_date": ["2024-01-02", "2024-01-02", "2024-01-03"],
            "future_return_13d": [0.10, -0.02, 0.03],
        }
    )

    result, label_column = breakout_script._attach_training_target(research, horizon_days=13, target_mode="daily_excess_return")

    assert label_column == "future_daily_excess_return_13d"
    assert result.loc[0, label_column] == pytest.approx(0.06)
    assert result.loc[1, label_column] == pytest.approx(-0.06)
    assert result.loc[2, label_column] == pytest.approx(0.0)


def test_path_adjusted_target_penalizes_drawdown_and_can_demean():
    research = pd.DataFrame(
        {
            "event_date": ["2024-01-02", "2024-01-02"],
            "future_return_13d": [0.10, 0.02],
            "mfe_return_13d": [0.20, 0.04],
            "mae_return_13d": [-0.08, -0.01],
        }
    )

    result, label_column = breakout_script._attach_training_target(research, horizon_days=13, target_mode="path_adjusted_return")
    excess, excess_label = breakout_script._attach_training_target(research, horizon_days=13, target_mode="daily_excess_path_adjusted_return")

    assert label_column == "future_path_adjusted_return_13d"
    assert result.loc[0, label_column] == pytest.approx(0.12)
    assert result.loc[1, label_column] == pytest.approx(0.03)
    assert excess_label == "future_daily_excess_path_adjusted_return_13d"
    assert excess.loc[0, excess_label] == pytest.approx(0.045)
    assert excess.loc[1, excess_label] == pytest.approx(-0.045)


def test_outcome_metrics_keep_realized_return_views_for_path_target():
    research = pd.DataFrame(
        {
            "event_date": ["2024-01-02", "2024-01-02", "2024-01-03"],
            "future_return_13d": [0.10, -0.02, 0.03],
            "mfe_return_13d": [0.20, 0.04, 0.05],
            "mae_return_13d": [-0.08, -0.01, -0.02],
            "signal_score": [0.8, 0.1, 0.4],
            "dataset_split": ["train", "train", "test"],
        }
    )
    labeled, label_column = breakout_script._attach_training_target(
        research,
        horizon_days=13,
        target_mode="daily_excess_path_adjusted_return",
    )
    scored = breakout_script._attach_outcome_columns(labeled, horizon_days=13)

    outcomes = breakout_script._evaluate_outcome_metrics(scored, horizon_days=13, label_column=label_column)

    assert set(["training_label", "absolute_return", "daily_excess_return"]).issubset(outcomes)
    assert outcomes["absolute_return"]["top20_mean_return"] == pytest.approx(0.10)
    assert outcomes["daily_excess_return"]["top20_mean_return"] == pytest.approx(0.06)


def test_score_transform_date_rank_preserves_raw_signal_score():
    scored = pd.DataFrame(
        {
            "event_date": ["2024-01-02", "2024-01-02", "2024-01-03"],
            "signal_score": [0.2, 0.8, -0.1],
        }
    )

    result = breakout_script._apply_score_transform(scored, "date_rank")

    assert result["raw_signal_score"].tolist() == pytest.approx([0.2, 0.8, -0.1])
    assert result["signal_score"].tolist() == pytest.approx([0.5, 1.0, 1.0])
    assert result["score_transform"].tolist() == ["date_rank", "date_rank", "date_rank"]


def test_date_topk_selection_metrics_score_each_event_date():
    scored = pd.DataFrame(
        {
            "event_date": ["2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
            "signal_score": [0.2, 0.8, 0.9, 0.1],
            "future_return_13d": [-0.02, 0.10, 0.04, -0.01],
        }
    )

    metrics = breakout_script._date_topk_selection_metrics(scored, return_column="future_return_13d")

    assert metrics["date_count"] == 2
    assert metrics["top1_per_date_count"] == 2
    assert metrics["top1_per_date_mean_return"] == pytest.approx(0.07)
    assert metrics["top1_per_date_excess_return"] == pytest.approx(0.0425)
    assert metrics["top1_per_date_hit_rate"] == pytest.approx(1.0)


def test_feature_profile_stable_slim_removes_market_and_empty_fundamentals():
    config = StockBreakoutConfig(lookback_window=60, min_history_days=60, min_volume_ratio=0.0)
    research = build_stock_breakout_research_frame(_price_frame(), config)

    columns = breakout_script._feature_columns_for_profile(research, "stable_slim")

    assert "me_market_return_5d" not in columns
    assert "fn_roe" not in columns
    assert "entry_price" not in columns
    assert "breakout_strength" in columns


def test_fresh_score_snapshot_filters_stale_signals():
    signals = pd.DataFrame(
        {
            "code": ["AAA.SH", "AAA.SH", "BBB.SH"],
            "signal_date": ["2024-06-28", "2024-06-30", "2024-06-01"],
            "feature_date": ["2024-06-28", "2024-06-30", "2024-06-01"],
            "signal_score": [0.9, 0.2, 0.8],
            "pred_return_13d": [0.9, 0.2, 0.8],
        }
    )

    fresh = breakout_script._fresh_score_snapshot_frame(signals, horizon_days=13, feature_date="2024-06-30", fresh_signal_days=7)

    assert fresh["code"].tolist() == ["AAA.SH"]
    assert fresh.iloc[0]["signal_age_days"] == 0
    assert fresh.iloc[0]["qlib_score"] == pytest.approx(0.2)


def test_train_and_score_stock_breakout_model_without_sklearn():
    pytest.importorskip("lightgbm")
    config = StockBreakoutConfig(lookback_window=60, min_history_days=60, min_volume_ratio=0.0)
    research = build_stock_breakout_research_frame(_price_frame(), config)
    feature_columns = stock_breakout_feature_columns(research)

    model, trained_features = train_lightgbm_stock_breakout_model(research, "future_return_13d", feature_columns)
    scored = score_stock_breakout_events(model, research, trained_features)

    assert trained_features
    assert "signal_score" in scored.columns
    assert scored["signal_score"].notna().any()


def test_walk_forward_fold_plan_uses_recent_max_folds():
    research = pd.DataFrame({"event_date": pd.date_range("2024-01-01", periods=120, freq="D")})

    plan = breakout_script._walk_forward_fold_plan(
        research,
        train_days=30,
        valid_days=10,
        test_days=7,
        step_days=14,
        max_folds=3,
    )

    assert len(plan) == 3
    assert plan["fold_id"].tolist() == ["wf_001", "wf_002", "wf_003"]
    assert pd.to_datetime(plan["train_end"]).lt(pd.to_datetime(plan["valid_start"])).all()
    assert pd.to_datetime(plan["valid_end"]).lt(pd.to_datetime(plan["test_start"])).all()
    assert pd.to_datetime(plan["test_end"]).le(pd.Timestamp("2024-04-29")).all()


def test_research_frame_accepts_fdh_time_symbol_columns():
    prices = _price_frame().rename(columns={"code": "symbol", "date": "time"})
    config = StockBreakoutConfig(lookback_window=60, min_history_days=60, min_volume_ratio=0.0)

    research = build_stock_breakout_research_frame(prices, config)

    assert not research.empty
    assert research.iloc[0]["code"] == "AAA.SH"


def test_research_frame_accepts_fdh_timezone_aware_time_columns():
    prices = _price_frame().rename(columns={"code": "symbol", "date": "time"})
    prices["time"] = pd.to_datetime(prices["time"], utc=True)
    config = StockBreakoutConfig(lookback_window=60, min_history_days=60, min_volume_ratio=0.0)

    research = build_stock_breakout_research_frame(prices, config)

    assert not research.empty
    assert pd.notna(research.iloc[0]["future_return_13d"])


def test_parse_symbols_deduplicates_cli_and_file(tmp_path):
    symbols_file = tmp_path / "symbols.csv"
    symbols_file.write_text("aaa.sh\nBBB.SZ,ignored\n#comment\n", encoding="utf-8")

    symbols = breakout_script._parse_symbols("AAA.SH,CCC.SH", str(symbols_file))

    assert symbols == ["AAA.SH", "CCC.SH", "BBB.SZ"]


@pytest.mark.anyio
async def test_resolve_effective_symbols_prefers_explicit_symbols(monkeypatch):
    async def fake_resolve_universe_symbols(**_kwargs):
        raise AssertionError("universe profile should not be resolved when explicit symbols are provided")

    monkeypatch.setattr(breakout_script, "resolve_universe_symbols", fake_resolve_universe_symbols)
    args = type(
        "Args",
        (),
        {
            "symbols": "AAA.SH,BBB.SZ",
            "symbols_file": None,
            "universe_profile": "csi300",
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "universe_mode": "fixed_universe",
        },
    )()

    symbols, profile = await breakout_script._resolve_effective_symbols(args)

    assert symbols == ["AAA.SH", "BBB.SZ"]
    assert profile is None


@pytest.mark.anyio
async def test_resolve_effective_symbols_uses_universe_profile(monkeypatch):
    async def fake_resolve_universe_symbols(**kwargs):
        assert kwargs["universe_profile"] == "csi500"
        assert kwargs["universe_mode"] == "fixed_universe"
        return ["AAA.SH", "BBB.SZ"], {}

    monkeypatch.setattr(breakout_script, "resolve_universe_symbols", fake_resolve_universe_symbols)
    args = type(
        "Args",
        (),
        {
            "symbols": None,
            "symbols_file": None,
            "universe_profile": "csi500",
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "universe_mode": "fixed_universe",
        },
    )()

    symbols, profile = await breakout_script._resolve_effective_symbols(args)

    assert symbols == ["AAA.SH", "BBB.SZ"]
    assert profile == "csi500"
