import json

import pandas as pd

from qlib_research.io.artifacts import (
    QlibScoreStore,
    build_portfolio_targets,
    publish_portfolio_targets,
    publish_score_snapshot,
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
