import json

import pandas as pd

from qlib_research.io.artifacts import publish_portfolio_targets, publish_score_snapshot
from qlib_research.io.sync import sync_model_artifacts


def test_sync_model_artifacts_supports_dry_run_and_latest(tmp_path):
    source = tmp_path / 'source'
    target = tmp_path / 'target'
    frame = pd.DataFrame({'code': ['AAA.SH'], 'qlib_score': [0.4]})
    publish_score_snapshot(frame, model_id='demo', feature_date='2026-04-01', artifacts_dir=source)
    publish_portfolio_targets(pd.DataFrame({
        'trade_date': ['2026-04-01'],
        'model_id': ['demo'],
        'feature_date': ['2026-04-01'],
        'code': ['AAA.SH'],
        'target_weight': [1.0],
        'score': [0.4],
        'rank': [1],
    }), model_id='demo', artifacts_dir=source)
    preview = sync_model_artifacts('demo', source_root=source, target_root=target, set_latest=True, dry_run=True)
    assert preview['dry_run'] is True
    result = sync_model_artifacts('demo', source_root=source, target_root=target, set_latest=True, dry_run=False)
    latest = json.loads((target / 'latest_model.json').read_text(encoding='utf-8'))
    assert result['synced'] is True
    assert (target / 'demo' / 'scores.csv').exists()
    assert latest['snapshot_path'] == 'demo/scores.csv'
