import pandas as pd

from qlib_research.core.qlib_pipeline import (
    apply_industry_normalization,
    exclude_feature_columns,
    init_qlib_runtime,
    normalize_feature_name_list,
    resolve_feature_columns,
)


def test_apply_industry_normalization_falls_back_to_market_when_group_too_small():
    panel = pd.DataFrame(
        {
            "datetime": pd.to_datetime(["2026-01-02", "2026-01-02", "2026-01-02"]),
            "instrument": ["AAA.SH", "BBB.SZ", "CCC.SH"],
            "l1_name": ["消费", "消费", None],
            "pe_ttm": [10.0, 12.0, 14.0],
            "signal_strength": [70.0, 75.0, 80.0],
        }
    )

    normalized = apply_industry_normalization(
        panel,
        feature_columns=["pe_ttm", "signal_strength"],
        method="l1_weekly_robust",
        min_group_size=5,
    )

    expected = pd.Series([-1.0, 0.0, 1.0], name="pe_ttm")
    pd.testing.assert_series_equal(normalized["pe_ttm"].round(6), expected.round(6), check_names=False)
    pd.testing.assert_series_equal(normalized["signal_strength"], panel["signal_strength"], check_names=False)


def test_apply_industry_normalization_uses_industry_cross_section_when_group_is_large_enough():
    panel = pd.DataFrame(
        {
            "datetime": pd.to_datetime(["2026-01-02"] * 6),
            "instrument": [f"S{i:03d}.SH" for i in range(6)],
            "l1_name": ["消费"] * 5 + ["科技"],
            "pe_ttm": [10.0, 12.0, 14.0, 16.0, 18.0, 100.0],
        }
    )

    normalized = apply_industry_normalization(
        panel,
        feature_columns=["pe_ttm"],
        method="l1_weekly_robust",
        min_group_size=5,
    )

    expected_consumer = pd.Series([-1.0, -0.5, 0.0, 0.5, 1.0], index=range(5), dtype=float)
    pd.testing.assert_series_equal(
        normalized.loc[:4, "pe_ttm"].round(6),
        expected_consumer.round(6),
        check_names=False,
    )
    assert round(float(normalized.loc[5, "pe_ttm"]), 6) == 17.0


def test_apply_industry_normalization_keeps_macro_features_raw():
    panel = pd.DataFrame(
        {
            "datetime": pd.to_datetime(["2026-01-02", "2026-01-02", "2026-01-02"]),
            "instrument": ["AAA.SH", "BBB.SZ", "CCC.SH"],
            "l1_name": ["消费", "消费", "科技"],
            "pe_ttm": [10.0, 12.0, 14.0],
            "macro_credit_impulse": [3.0, 3.0, 3.0],
        }
    )

    normalized = apply_industry_normalization(
        panel,
        feature_columns=["pe_ttm", "macro_credit_impulse"],
        method="l1_weekly_robust",
        min_group_size=2,
    )

    pd.testing.assert_series_equal(
        normalized["macro_credit_impulse"],
        panel["macro_credit_impulse"],
        check_names=False,
    )


def test_feature_exclusion_helpers_resolve_manual_excludes():
    assert normalize_feature_name_list([" pe_ttm ", "macro_phase_y,macro_industry_match", "pe_ttm"]) == (
        "pe_ttm",
        "macro_phase_y",
        "macro_industry_match",
    )
    assert exclude_feature_columns(
        ("pe_ttm", "pb", "macro_phase_y"),
        excluded_features=["pb", "macro_phase_y"],
    ) == ("pe_ttm",)
    assert resolve_feature_columns(
        feature_columns=("pe_ttm", "macro_phase_y", "macro_industry_match"),
        excluded_features="macro_phase_y",
    ) == ("pe_ttm", "macro_industry_match")


def test_init_qlib_runtime_skips_reinitialization_when_registered(tmp_path, monkeypatch):
    observed: dict[str, object] = {}

    class FakeQlib:
        def init(self, **kwargs):
            observed["kwargs"] = kwargs

    monkeypatch.setattr("qlib_research.core.qlib_pipeline.require_qlib", lambda: FakeQlib())
    monkeypatch.setattr("qlib_research.core.qlib_pipeline.get_qlib_artifacts_dir", lambda: tmp_path)

    result = init_qlib_runtime(exp_name="demo-exp")

    assert isinstance(result, FakeQlib)
    assert observed["kwargs"]["skip_if_reg"] is True
    assert observed["kwargs"]["region"] == "cn"
    assert observed["kwargs"]["exp_manager"]["kwargs"]["default_exp_name"] == "demo-exp"
