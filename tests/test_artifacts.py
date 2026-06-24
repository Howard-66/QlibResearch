import json

import pandas as pd

from qlib_research.io.artifacts import (
    QlibScoreStore,
    StrategySignalStore,
    build_portfolio_targets,
    publish_portfolio_targets,
    publish_score_snapshot,
    publish_strategy_signals,
)


def test_publish_score_snapshot_writes_manifest_and_latest(tmp_path):
    frame = pd.DataFrame({"code": ["AAA.SH", "BBB.SZ"], "qlib_score": [0.1, 0.2]})
    snapshot_path = publish_score_snapshot(frame, model_id="demo", feature_date="2026-04-01", artifacts_dir=tmp_path)
    latest = json.loads((tmp_path / 'latest_model.json').read_text(encoding='utf-8'))
    assert snapshot_path.exists()
    assert latest['model_id'] == 'demo'
    assert latest['snapshot_path'] == 'demo/scores.csv'


def test_build_portfolio_targets_uses_equal_weights_for_topk():
    frame = pd.DataFrame({"code": ["AAA.SH", "BBB.SZ", "CCC.SH"], "qlib_score": [0.5, 0.9, 0.1]})
    targets = build_portfolio_targets(frame, model_id='demo', feature_date='2026-04-01', topk=2)
    assert list(targets['code']) == ['BBB.SZ', 'AAA.SH']
    assert targets['target_weight'].tolist() == [0.5, 0.5]
    assert list(targets['rank']) == [1, 2]


def test_build_portfolio_targets_respects_selected_code_order():
    frame = pd.DataFrame({"code": ["AAA.SH", "BBB.SZ", "CCC.SH"], "qlib_score": [0.5, 0.9, 0.1]})
    targets = build_portfolio_targets(
        frame,
        model_id='demo',
        feature_date='2026-04-01',
        topk=2,
        selected_codes=['AAA.SH', 'CCC.SH'],
    )
    assert list(targets['code']) == ['AAA.SH', 'CCC.SH']
    assert targets['target_weight'].tolist() == [0.5, 0.5]
    assert list(targets['rank']) == [1, 2]


def test_build_portfolio_targets_returns_empty_when_selected_codes_have_no_matches():
    frame = pd.DataFrame({"code": ["AAA.SH", "BBB.SZ"], "qlib_score": [0.5, 0.9]})
    targets = build_portfolio_targets(
        frame,
        model_id='demo',
        feature_date='2026-04-01',
        topk=2,
        selected_codes=['CCC.SH'],
    )
    assert list(targets.columns) == ["trade_date", "model_id", "feature_date", "code", "target_weight", "score", "rank"]
    assert targets.empty


def test_publish_portfolio_targets_and_load_snapshot_contract(tmp_path):
    frame = pd.DataFrame({"code": ["AAA.SH", "BBB.SZ"], "qlib_score": [0.3, 0.7], "pred_return_4w": [0.02, 0.05]})
    publish_score_snapshot(frame, model_id='demo', feature_date='2026-04-01', artifacts_dir=tmp_path)
    targets = build_portfolio_targets(frame, model_id='demo', feature_date='2026-04-01', topk=2)
    target_path = publish_portfolio_targets(targets, model_id='demo', artifacts_dir=tmp_path)
    snapshot = QlibScoreStore(tmp_path).load_snapshot()
    assert target_path.exists()
    assert snapshot.records['BBB.SZ'].qlib_rank == 1


def test_publish_strategy_signals_and_load_by_code(tmp_path):
    signals = pd.DataFrame(
        {
            "event_id": ["stock_breakout:AAA.SH:2024-05-10", "stock_breakout:BBB.SZ:2024-05-10"],
            "model_id": ["breakout-demo", "breakout-demo"],
            "code": ["AAA.SH", "BBB.SZ"],
            "signal_date": ["2024-05-10", "2024-05-10"],
            "feature_date": ["2024-05-10", "2024-05-10"],
            "signal_type": ["breakout", "breakout"],
            "side": ["long", "long"],
            "signal_score": [0.12, 0.08],
            "pred_return_13d": [0.12, 0.08],
            "entry_price": [10.5, 20.5],
        }
    )

    publish_score_snapshot(
        pd.DataFrame({"code": ["AAA.SH"], "qlib_score": [0.12]}),
        model_id="breakout-demo",
        feature_date="2024-05-10",
        artifacts_dir=tmp_path,
    )
    signal_path = publish_strategy_signals(signals, model_id="breakout-demo", artifacts_dir=tmp_path)
    loaded = StrategySignalStore(tmp_path).load_signals_for_code("aaa.sh", model_id="breakout-demo")
    latest_loaded = StrategySignalStore(tmp_path).load_signals_for_code("AAA.SH")

    assert signal_path.exists()
    assert len(loaded) == 1
    assert len(latest_loaded) == 1
    assert loaded[0].event_id == "stock_breakout:AAA.SH:2024-05-10"
    assert loaded[0].signal_score == 0.12
