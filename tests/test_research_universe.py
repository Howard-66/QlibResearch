import asyncio

import numpy as np
import pandas as pd

from qlib_research.core.research_universe import (
    _expand_index_weight_query_window,
    _select_fixed_universe_symbols,
    attach_universe_flags,
    build_index_membership_flag_frame,
    fetch_universe_profile_history,
    resolve_universe_symbols,
)


def test_build_index_membership_flag_frame_uses_latest_snapshot_membership():
    weekly_dates = pd.to_datetime(["2026-01-02", "2026-01-09", "2026-01-16"])
    history = pd.DataFrame(
        {
            "symbol": ["AAA.SH", "BBB.SZ", "AAA.SH"],
            "trade_date": pd.to_datetime(["2025-12-31", "2025-12-31", "2026-01-14"]),
            "weight": [2.5, 1.8, 3.1],
        }
    )

    flags = build_index_membership_flag_frame(
        weekly_dates=weekly_dates,
        history_frame=history,
        flag_column="in_csi300",
    )

    assert bool(flags.loc[(flags["time"] == pd.Timestamp("2026-01-02")) & (flags["symbol"] == "AAA.SH"), "in_csi300"].iloc[0])
    assert bool(flags.loc[(flags["time"] == pd.Timestamp("2026-01-09")) & (flags["symbol"] == "BBB.SZ"), "in_csi300"].iloc[0])
    assert bool(flags.loc[(flags["time"] == pd.Timestamp("2026-01-16")) & (flags["symbol"] == "AAA.SH"), "in_csi300"].iloc[0])
    assert flags.loc[(flags["time"] == pd.Timestamp("2026-01-16")) & (flags["symbol"] == "BBB.SZ")].empty


def test_build_index_membership_flag_frame_normalizes_merge_key_precision():
    weekly_dates = np.array(["2026-01-02", "2026-01-09"], dtype="datetime64[us]")
    history = pd.DataFrame(
        {
            "symbol": ["AAA.SH", "BBB.SZ"],
            "trade_date": np.array(["2026-01-01", "2026-01-08"], dtype="datetime64[s]"),
            "weight": [1.0, 1.0],
        }
    )

    flags = build_index_membership_flag_frame(
        weekly_dates=weekly_dates,
        history_frame=history,
        flag_column="in_csi300",
    )

    assert str(flags["time"].dtype) == "datetime64[ns]"
    assert set(flags["symbol"]) == {"AAA.SH", "BBB.SZ"}
    assert bool(flags.loc[(flags["time"] == pd.Timestamp("2026-01-02")) & (flags["symbol"] == "AAA.SH"), "in_csi300"].iloc[0])
    assert bool(flags.loc[(flags["time"] == pd.Timestamp("2026-01-09")) & (flags["symbol"] == "BBB.SZ"), "in_csi300"].iloc[0])


def test_attach_universe_flags_sets_false_for_non_members():
    panel = pd.DataFrame(
        {
            "symbol": ["AAA.SH", "BBB.SZ", "AAA.SH", "BBB.SZ"],
            "time": pd.to_datetime(["2026-01-02", "2026-01-02", "2026-01-16", "2026-01-16"]),
            "close": [10.0, 8.0, 10.5, 7.9],
        }
    )
    history_frames = {
        "in_csi300": pd.DataFrame(
            {
                "symbol": ["AAA.SH"],
                "trade_date": pd.to_datetime(["2025-12-31"]),
                "weight": [1.0],
            }
        )
    }

    enriched = attach_universe_flags(panel, history_frames)

    assert bool(enriched.loc[enriched["symbol"] == "AAA.SH", "in_csi300"].all())
    assert not bool(enriched.loc[enriched["symbol"] == "BBB.SZ", "in_csi300"].any())


def test_expand_index_weight_query_window_includes_previous_month_start_and_month_end():
    query_start, query_end = _expand_index_weight_query_window("2026-01-01", "2026-03-15")

    assert query_start == "2025-12-01"
    assert query_end == "2026-03-31"


def test_fetch_universe_profile_history_pads_index_weight_query_window(monkeypatch):
    observed: list[dict[str, str]] = []

    class FakeFdh:
        async def get_index_weight_async(self, **kwargs):
            observed.append(kwargs)
            return pd.DataFrame(
                {
                    "con_code": ["AAA.SH"],
                    "trade_date": ["2025-12-31"],
                    "weight": [1.0],
                }
            )

    async def fake_get_fdh():
        return FakeFdh()

    monkeypatch.setattr("qlib_research.core.research_universe.get_fdh", fake_get_fdh)

    history = asyncio.run(
        fetch_universe_profile_history(
            universe_profile="csi300",
            start_date="2026-01-01",
            end_date="2026-03-15",
        )
    )

    assert observed == [
        {
            "index_code": "000300.SH",
            "start_date": "2025-12-01",
            "end_date": "2026-03-31",
        }
    ]
    assert "in_csi300" in history
    assert history["in_csi300"]["symbol"].tolist() == ["AAA.SH"]


def test_select_fixed_universe_symbols_uses_latest_available_snapshot_before_end_date():
    history = {
        "in_csi300": pd.DataFrame(
            {
                "symbol": ["AAA.SH", "BBB.SZ", "CCC.SH", "DDD.SZ"],
                "trade_date": pd.to_datetime(["2016-01-29", "2016-01-29", "2020-12-31", "2020-12-31"]),
                "weight": [1.0, 1.0, 1.0, 1.0],
            }
        )
    }

    symbols_2018 = _select_fixed_universe_symbols(history, as_of_date="2018-01-01")
    symbols_2010 = _select_fixed_universe_symbols(history, as_of_date="2010-01-01")

    assert symbols_2018 == ["AAA.SH", "BBB.SZ"]
    assert symbols_2010 == ["AAA.SH", "BBB.SZ"]


def test_resolve_universe_symbols_supports_fixed_universe_mode(monkeypatch):
    async def fake_fetch_universe_profile_history(universe_profile=None, start_date=None, end_date=None):
        return {
            "in_csi300": pd.DataFrame(
                {
                    "symbol": ["AAA.SH", "BBB.SZ"],
                    "trade_date": pd.to_datetime(["2016-01-29", "2016-01-29"]),
                    "weight": [1.0, 1.0],
                }
            )
        }

    monkeypatch.setattr(
        "qlib_research.core.research_universe.fetch_universe_profile_history",
        fake_fetch_universe_profile_history,
    )

    symbols, history = asyncio.run(
        resolve_universe_symbols(
            universe_profile="csi300",
            start_date="2010-01-01",
            end_date=None,
            universe_mode="fixed_universe",
        )
    )

    assert symbols == ["AAA.SH", "BBB.SZ"]
    assert history == {}
