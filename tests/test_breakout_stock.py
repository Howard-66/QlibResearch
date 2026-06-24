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
