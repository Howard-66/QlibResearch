import json

from qlib_research.io.artifacts import QlibScoreStore


def test_score_store_loads_latest_snapshot_and_assigns_ranks(tmp_path):
    scores_path = tmp_path / "scores.json"
    scores_path.write_text(json.dumps({
        "AAA.SH": {"qlib_score": 0.42, "pred_return_4w": 0.03},
        "BBB.SZ": {"qlib_score": 0.88, "pred_return_4w": 0.05},
    }), encoding="utf-8")

    manifest_path = tmp_path / "latest_model.json"
    manifest_path.write_text(json.dumps({
        "model_id": "weekly-lgbm-v1",
        "feature_date": "2026-03-20",
        "generated_at": "2026-03-21T00:00:00Z",
        "snapshot_path": "scores.json",
    }), encoding="utf-8")

    store = QlibScoreStore(tmp_path)
    snapshot = store.load_snapshot()

    assert snapshot.model_id == "weekly-lgbm-v1"
    assert snapshot.feature_date == "2026-03-20"
    assert snapshot.records["BBB.SZ"].qlib_rank == 1
    assert snapshot.records["AAA.SH"].qlib_rank == 2
    assert snapshot.records["BBB.SZ"].pred_return_4w == 0.05
