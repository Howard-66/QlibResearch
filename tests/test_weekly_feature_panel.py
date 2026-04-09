import asyncio
import threading

import numpy as np
import pandas as pd

from qlib_research.core.qlib_pipeline import LABEL_COLUMN, build_training_frame
from qlib_research.core.weekly_feature_panel import (
    _filter_panel_by_universe_profile,
    _harmonize_feature_columns,
    _merge_asof_by_symbol,
    build_weekly_feature_panel,
    engineer_research_features,
    export_weekly_feature_panel,
)


def test_harmonize_feature_columns_coalesces_duplicate_valuation_fields():
    frame = pd.DataFrame(
        {
            "symbol": ["AAA.SH", "BBB.SZ"],
            "time": ["2026-01-02", "2026-01-02"],
            "pe_ttm_x": [10.5, None],
            "pe_ttm_y": [9.8, 12.2],
            "pb_x": [1.2, None],
            "pb_y": [1.1, 1.5],
            "ps_ttm_x": [2.3, None],
            "ps_ttm_y": [2.1, 2.6],
            "dv_ttm_x": [3.4, None],
            "dv_ttm_y": [3.1, 3.8],
            "peg_x": [0.9, None],
            "peg_y": [0.8, 1.2],
        }
    )

    harmonized = _harmonize_feature_columns(frame)

    assert "pe_ttm" in harmonized.columns
    assert "pb" in harmonized.columns
    assert "ps_ttm" in harmonized.columns
    assert "dv_ttm" in harmonized.columns
    assert "peg" in harmonized.columns
    assert "pe_ttm_x" not in harmonized.columns
    assert "pe_ttm_y" not in harmonized.columns
    assert harmonized.loc[0, "pe_ttm"] == 10.5
    assert harmonized.loc[1, "pe_ttm"] == 12.2
    assert harmonized.loc[1, "pb"] == 1.5


def test_build_training_frame_includes_fundamental_quality_features():
    panel = pd.DataFrame(
        {
            "datetime": pd.to_datetime(["2026-01-02", "2026-01-09"]),
            "instrument": ["AAA.SH", "AAA.SH"],
            "nda_value": [2.0, 1.0],
            "volume_confirmed": [1, 1],
            "pe_ttm": [10.5, 10.9],
            "pb": [1.2, 1.3],
            "ps_ttm": [2.2, 2.4],
            "dv_ttm": [3.1, 3.2],
            "peg": [0.8, 0.85],
            "core_indicator_pct_1250d": [18.0, 22.0],
            "core_indicator_industry_pct": [15.0, 18.0],
            "f_score": [7, 8],
            "f_roa": [1, 1],
            "f_cfo": [1, 1],
            "f_delta_roa": [0, 1],
            "f_accrual": [1, 1],
            "f_delta_lever": [1, 1],
            "f_delta_liquid": [1, 1],
            "f_eq_offer": [0, 0],
            "f_delta_margin": [1, 1],
            "f_delta_turn": [0, 1],
            "roe_5y_avg": [18.5, 18.7],
            "ni_cfo_corr_3y": [0.92, 0.93],
            "debt_ratio": [45.0, 44.5],
            "current_ratio": [1.8, 1.85],
            "roa_ttm": [7.2, 7.3],
            "cfo_ttm": [10_000_000.0, 10_200_000.0],
            "ni_ttm": [8_400_000.0, 8_500_000.0],
            "gpm_ttm": [35.0, 35.5],
            "at_ttm": [0.62, 0.64],
            "npm_ttm": [18.0, 18.4],
            "gpm_ttm_12q_std": [2.2, 2.1],
            "gpm_ttm_12q_delta": [3.0, 3.2],
            "npm_ttm_12q_std": [1.5, 1.4],
            "cfo_to_ni_ttm": [1.2, 1.22],
            "buffett_gpm_flag": [1, 1],
            "buffett_npm_stable_flag": [1, 1],
            "buffett_roa_flag": [0, 0],
            "buffett_cashflow_flag": [1, 1],
            "industry_npm_ttm_rank_pct": [0.75, 0.8],
            "buffett_npm_flag": [1, 1],
            "buffett_moat_coverage": [4, 4],
            "buffett_moat_score": [3.0, 3.0],
            "buffett_moat_pass": [1, 1],
            "macro_credit_impulse": [3.0, 3.2],
            "macro_pmi": [50.5, 51.0],
            "macro_phase_y": [0, 1],
            "macro_phase_changed": [0, 1],
            "macro_phase_reflation": [1, 0],
            "macro_phase_recovery": [0, 1],
            "macro_phase_overheat": [0, 0],
            "macro_phase_stagflation": [0, 0],
            "macro_industry_match": [1, 1],
            LABEL_COLUMN: [0.05, 0.02],
        }
    )

    qlib_frame, feature_columns = build_training_frame(panel)

    for column in (
        "pe_ttm",
        "pb",
        "ps_ttm",
        "dv_ttm",
        "peg",
        "nda_value",
        "volume_confirmed",
        "f_score",
        "f_roa",
        "f_cfo",
        "f_delta_roa",
        "f_accrual",
        "f_delta_lever",
        "f_delta_liquid",
        "f_eq_offer",
        "f_delta_margin",
        "f_delta_turn",
        "roe_5y_avg",
        "ni_cfo_corr_3y",
        "debt_ratio",
        "current_ratio",
        "roa_ttm",
        "cfo_ttm",
        "ni_ttm",
        "gpm_ttm",
        "at_ttm",
        "npm_ttm",
        "gpm_ttm_12q_std",
        "gpm_ttm_12q_delta",
        "npm_ttm_12q_std",
        "cfo_to_ni_ttm",
        "buffett_gpm_flag",
        "buffett_npm_stable_flag",
        "buffett_roa_flag",
        "buffett_cashflow_flag",
        "industry_npm_ttm_rank_pct",
        "buffett_npm_flag",
        "buffett_moat_coverage",
        "buffett_moat_score",
        "buffett_moat_pass",
        "macro_credit_impulse",
        "macro_pmi",
        "macro_phase_y",
        "macro_phase_changed",
        "macro_phase_reflation",
        "macro_phase_recovery",
        "macro_phase_overheat",
        "macro_phase_stagflation",
        "macro_industry_match",
    ):
        assert column in feature_columns
        assert ("feature", column) in qlib_frame.columns


def test_export_weekly_feature_panel_supports_running_event_loop(tmp_path, monkeypatch):
    caller_thread = threading.current_thread().name
    observed: dict[str, object] = {}

    async def fake_build_weekly_feature_panel(**kwargs):
        observed["build_thread"] = threading.current_thread().name
        observed["kwargs"] = kwargs
        return pd.DataFrame(
            {
                "symbol": ["AAA.SH"],
                "time": [pd.Timestamp("2026-01-02")],
                "instrument": ["AAA.SH"],
                "datetime": [pd.Timestamp("2026-01-02")],
                "close": [10.0],
            }
        )

    async def fake_close_fdh():
        observed["close_called"] = True

    monkeypatch.setattr("qlib_research.core.weekly_feature_panel.build_weekly_feature_panel", fake_build_weekly_feature_panel)
    monkeypatch.setattr("qlib_research.core.weekly_feature_panel.close_fdh", fake_close_fdh)

    async def invoke_export() -> None:
        output_path = tmp_path / "panel.csv"
        result = export_weekly_feature_panel(
            output_path=output_path,
            universe_profile="watchlist",
        )
        observed["result"] = result

    asyncio.run(invoke_export())

    assert observed["build_thread"] != caller_thread
    assert observed["close_called"] is True
    assert observed["result"] == (tmp_path / "panel.csv").resolve()
    exported = pd.read_csv(tmp_path / "panel.csv")
    assert exported["symbol"].tolist() == ["AAA.SH"]


def test_filter_panel_by_universe_profile_restricts_csi300_rows():
    panel = pd.DataFrame(
        {
            "symbol": ["AAA.SH", "BBB.SZ", "CCC.SH"],
            "time": pd.to_datetime(["2026-01-02", "2026-01-02", "2026-01-09"]),
            "in_csi300": [True, False, True],
            "in_csi500": [False, True, False],
        }
    )

    filtered = _filter_panel_by_universe_profile(panel, "csi300")

    assert filtered["symbol"].tolist() == ["AAA.SH", "CCC.SH"]
    assert filtered["in_csi300"].all()


def test_merge_asof_by_symbol_accepts_multi_symbol_weekly_order():
    left = pd.DataFrame(
        {
            "symbol": ["AAA.SH", "BBB.SZ", "AAA.SH", "BBB.SZ"],
            "time": pd.to_datetime(["2026-01-03", "2026-01-03", "2026-01-10", "2026-01-10"]),
            "close": [10.0, 20.0, 10.5, 20.5],
        }
    )
    right = pd.DataFrame(
        {
            "symbol": ["AAA.SH", "BBB.SZ", "AAA.SH", "BBB.SZ"],
            "time": pd.to_datetime(["2026-01-02", "2026-01-02", "2026-01-09", "2026-01-09"]),
            "pe_ttm": [12.0, 22.0, 13.0, 23.0],
        }
    )

    merged = _merge_asof_by_symbol(left, right)

    assert merged["pe_ttm"].tolist() == [12.0, 22.0, 13.0, 23.0]


def test_build_weekly_feature_panel_merges_macro_cycle_features(monkeypatch):
    class FakeFdh:
        async def get_processed_weekly_async(self, symbols=None, start_date=None, end_date=None):
            return pd.DataFrame(
                {
                    "symbol": ["AAA.SH"],
                    "time": [pd.Timestamp("2026-02-06")],
                    "close": [10.0],
                    "open": [9.8],
                    "high": [10.2],
                    "low": [9.7],
                    "volume": [1000.0],
                    "amount": [10000.0],
                    "nda_value": [2.0],
                    "volume_confirmed": [True],
                }
            )

        async def get_fundamental_combined_async(self, symbols=None, start_date=None, end_date=None, include_fscore=True):
            return pd.DataFrame(
                {
                    "symbol": ["AAA.SH"],
                    "time": [pd.Timestamp("2026-02-05")],
                    "npm_ttm": [18.2],
                    "gpm_ttm_12q_std": [2.3],
                    "gpm_ttm_12q_delta": [3.1],
                    "npm_ttm_12q_std": [1.4],
                    "cfo_to_ni_ttm": [1.18],
                    "buffett_gpm_flag": [1],
                    "buffett_npm_stable_flag": [1],
                    "buffett_roa_flag": [0],
                    "buffett_cashflow_flag": [1],
                }
            )

        async def get_industry_valuation_async(self, symbols=None, start_date=None, end_date=None, include_exempted=True):
            return pd.DataFrame(
                {
                    "symbol": ["AAA.SH"],
                    "time": [pd.Timestamp("2026-02-05")],
                    "l1_name": ["科技"],
                    "l3_name": ["软件开发"],
                    "core_indicator_type": ["PS"],
                    "core_indicator_value": [3.2],
                    "core_indicator_pct_1250d": [22.0],
                    "core_indicator_industry_pct": [35.0],
                }
            )

        async def get_basic_async(self, symbols=None):
            return pd.DataFrame({"symbol": ["AAA.SH"], "name": ["测试软件"], "industry": ["科技"]})

        async def get_cn_macro_cycle_async(self, start_date=None, end_date=None, phase_mode="stable"):
            return pd.DataFrame(
                {
                    "time": [pd.Timestamp("2026-01-31 15:00", tz="Asia/Shanghai")],
                    "observation_time": [pd.Timestamp("2025-12-31 15:00", tz="Asia/Shanghai")],
                    "m2_yoy": [8.0],
                    "gdp_yoy": [5.0],
                    "ppi_yoy": [0.5],
                    "pmi": [51.2],
                    "credit_impulse": [3.0],
                    "phase": ["RECOVERY"],
                    "phase_y": [1],
                    "phase_changed": [1],
                }
            )

        async def get_cn_macro_cycle_industries_async(
            self,
            start_date=None,
            end_date=None,
            preferred_only=False,
            phase_mode="stable",
        ):
            return pd.DataFrame(
                {
                    "time": [pd.Timestamp("2026-01-31 15:00", tz="Asia/Shanghai")],
                    "l3_name": ["软件开发"],
                    "config_macro_cycle": ["RECOVERY"],
                    "matches_stable_phase": [True],
                }
            )

    async def fake_get_fdh():
        return FakeFdh()

    async def fake_resolve_universe_symbols(universe_profile=None, symbols=None, start_date=None, end_date=None):
        return ["AAA.SH"], None

    monkeypatch.setattr("qlib_research.core.weekly_feature_panel.get_fdh", fake_get_fdh)
    monkeypatch.setattr("qlib_research.core.weekly_feature_panel.resolve_universe_symbols", fake_resolve_universe_symbols)

    panel = asyncio.run(
        build_weekly_feature_panel(
            symbols=["AAA.SH"],
            start_date="2026-02-01",
            end_date="2026-02-28",
            batch_size=10,
        )
    )

    assert "macro_credit_impulse" in panel.columns
    assert "macro_phase_recovery" in panel.columns
    assert "macro_industry_match" in panel.columns
    assert "nda_value" in panel.columns
    assert "npm_ttm" in panel.columns
    assert float(panel.loc[0, "macro_credit_impulse"]) == 3.0
    assert int(panel.loc[0, "macro_phase_recovery"]) == 1
    assert int(panel.loc[0, "macro_industry_match"]) == 1
    assert float(panel.loc[0, "nda_value"]) == 2.0


def test_engineer_research_features_adds_cross_sectional_and_macro_interaction_features():
    dates = list(pd.date_range("2026-01-02", periods=8, freq="W-FRI"))
    symbols = [f"A{i:02d}.SH" for i in range(5)] + [f"B{i:02d}.SZ" for i in range(5)]
    rows = []
    for idx, symbol in enumerate(symbols):
        base_close = 10 + idx
        for week_idx, dt in enumerate(dates):
            rows.append(
                {
                    "symbol": symbol,
                    "time": dt,
                    "datetime": dt,
                    "close": base_close + week_idx,
                    "amount": 100 + idx * 10 + week_idx,
                    "volume": 10 + idx + week_idx,
                    "nda_value": float((week_idx % 3) - 1),
                    "volume_confirmed": int((week_idx % 3) >= 1),
                    "pe_ttm": 10.0 + idx,
                    "pb": 1.0 + idx * 0.01,
                    "ps_ttm": 2.0 + idx * 0.02,
                    "pe_ttm_pct_1250d": 0.2 + week_idx * 0.05,
                    "f_score": 6 + (week_idx // 2),
                    "roa_ttm": 12.0 + week_idx * 0.2,
                    "current_ratio": 1.0 + week_idx * 0.05,
                    "npm_ttm": 12.0 + idx + week_idx * 0.1,
                    "buffett_gpm_flag": 1 if idx < 5 else 0,
                    "buffett_npm_stable_flag": 1,
                    "buffett_roa_flag": 1 if week_idx >= 4 else 0,
                    "buffett_cashflow_flag": 1,
                    "l1_name": "Tech",
                    "l3_name": "Software" if idx < 5 else "Hardware",
                    "macro_phase_reflation": 1,
                    "macro_phase_recovery": 0,
                    "macro_phase_overheat": 0,
                    "macro_phase_stagflation": 0,
                    "macro_industry_match": 1,
                }
            )
    panel = pd.DataFrame(rows)

    features = engineer_research_features(panel)

    for column in (
        "mom_4w",
        "rev_4w",
        "volatility_8w",
        "amount_change_4w",
        "pe_ttm_delta_4w",
        "f_score_delta_8w",
        "industry_mom_4w_rank_pct",
        "industry_npm_ttm_rank_pct",
        "buffett_npm_flag",
        "buffett_moat_coverage",
        "buffett_moat_score",
        "buffett_moat_pass",
        "macro_reflation_x_mom_4w",
        "macro_industry_match_x_mom_4w",
    ):
        assert column in features.columns
    assert features["macro_reflation_x_mom_4w"].notna().any()
    assert features["industry_npm_ttm_rank_pct"].notna().any()
    assert features["buffett_moat_score"].notna().any()


def test_engineer_research_features_handles_object_columns_with_array_values():
    panel = pd.DataFrame(
        {
            "symbol": ["AAA.SH", "AAA.SH", "AAA.SH", "AAA.SH"],
            "time": pd.to_datetime(["2026-01-02", "2026-01-09", "2026-01-16", "2026-01-23"]),
            "datetime": pd.to_datetime(["2026-01-02", "2026-01-09", "2026-01-16", "2026-01-23"]),
            "close": [10.0, 11.0, 12.0, 13.0],
            "amount": [100.0, 105.0, 110.0, 115.0],
            "volume": [10.0, 11.0, 12.0, 13.0],
            "l1_name": ["Tech", "Tech", "Tech", "Tech"],
            "macro_phase_reflation": [1, 1, 1, 1],
            "macro_phase_recovery": [0, 0, 0, 0],
            "macro_phase_overheat": [0, 0, 0, 0],
            "macro_phase_stagflation": [0, 0, 0, 0],
            "macro_industry_match": [1, 1, 1, 1],
            "opaque_object": [np.array([1, 2]), np.array([1, 2]), np.array([1, 2]), np.array([1, 2])],
        }
    )

    features = engineer_research_features(panel)

    assert "mom_1w" in features.columns
    assert "opaque_object" in features.columns
