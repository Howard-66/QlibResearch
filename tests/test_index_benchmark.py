from decimal import Decimal

import pandas as pd

from qlib_research.core.index_benchmark import (
    build_blended_index_benchmark_frame,
    build_index_benchmark_frame,
    build_index_benchmark_series,
    build_universe_benchmark_frame,
    load_index_technical_frame,
    normalize_index_period,
    resolve_benchmark_components,
)


class _FakeFetcher:
    def fetch_index_daily(self, ts_code, start_date, end_date):
        base_close = 10.0 if ts_code in {"000001.SH", "000300.SH"} else 20.0 if ts_code == "000905.SH" else 30.0
        return pd.DataFrame(
            {
                "trade_date": pd.to_datetime(
                    [
                        "2026-01-05",
                        "2026-01-06",
                        "2026-01-07",
                        "2026-01-08",
                        "2026-01-09",
                        "2026-01-12",
                        "2026-01-13",
                        "2026-01-14",
                        "2026-01-15",
                        "2026-01-16",
                    ]
                ),
                "open": [10, 10, 10, 10, 10, 11, 11, 11, 11, 11],
                "high": [11, 11, 11, 11, 11, 12, 12, 12, 12, 12],
                "low": [9, 9, 9, 9, 9, 10, 10, 10, 10, 10],
                "close": [
                    base_close,
                    base_close + 0.2,
                    base_close + 0.4,
                    base_close + 0.5,
                    base_close + 0.8,
                    base_close + 1.0,
                    base_close + 1.1,
                    base_close + 1.2,
                    base_close + 1.4,
                    base_close + 1.5,
                ],
                "volume": [100] * 10,
            }
        )

    def resample_index_to_weekly(self, df_daily):
        weekly = (
            df_daily.set_index("trade_date")
            .sort_index()
            .resample("W-FRI", closed="right", label="right")
            .agg(
                {
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum",
                }
            )
            .dropna()
            .reset_index()
            .rename(columns={"trade_date": "date"})
        )
        return weekly


class _DecimalFakeFetcher(_FakeFetcher):
    def fetch_index_daily(self, ts_code, start_date, end_date):
        frame = super().fetch_index_daily(ts_code, start_date, end_date).copy()
        for column in ("open", "high", "low", "close", "volume"):
            frame[column] = frame[column].apply(lambda value: Decimal(str(value)))
        return frame


def test_normalize_index_period_accepts_native_aliases():
    assert normalize_index_period("week") == "W"
    assert normalize_index_period("1week") == "W"
    assert normalize_index_period("month") == "M"
    assert normalize_index_period("D") == "D"


def test_load_index_technical_frame_respects_api_weekly_resample():
    frame = load_index_technical_frame(
        ts_code="000001.SH",
        period="week",
        start_date="2026-01-01",
        end_date="2026-01-16",
        fetcher=_FakeFetcher(),
    )

    assert frame["date"].dt.strftime("%Y-%m-%d").tolist() == ["2026-01-09", "2026-01-16"]
    assert frame["close"].tolist() == [10.8, 11.5]


def test_build_index_benchmark_frame_aligns_to_workflow_calendar():
    calendar = pd.to_datetime(["2026-01-09", "2026-01-16"])
    benchmark_frame = build_index_benchmark_frame(
        calendar_dates=calendar,
        ts_code="000001.SH",
        period="W",
        initial_capital=1_000_000.0,
        fetcher=_FakeFetcher(),
    )

    assert benchmark_frame["datetime"].dt.strftime("%Y-%m-%d").tolist() == ["2026-01-09", "2026-01-16"]
    assert benchmark_frame["benchmark_close"].tolist() == [10.8, 11.5]
    assert benchmark_frame["benchmark_return"].round(6).tolist() == [0.0, round(11.5 / 10.8 - 1.0, 6)]
    assert benchmark_frame["benchmark_value"].round(2).tolist() == [1_000_000.0, round(1_000_000.0 * 11.5 / 10.8, 2)]


def test_build_index_benchmark_frame_coerces_decimal_prices_to_float():
    calendar = pd.to_datetime(["2026-01-09", "2026-01-16"])
    benchmark_frame = build_index_benchmark_frame(
        calendar_dates=calendar,
        ts_code="000001.SH",
        period="W",
        initial_capital=1_000_000.0,
        fetcher=_DecimalFakeFetcher(),
    )

    assert str(benchmark_frame["benchmark_close"].dtype) == "float64"
    assert str(benchmark_frame["benchmark_return"].dtype) == "float64"
    assert benchmark_frame["benchmark_value"].round(2).tolist() == [1_000_000.0, round(1_000_000.0 * 11.5 / 10.8, 2)]


def test_build_index_benchmark_series_returns_qlib_ready_period_returns():
    calendar = pd.to_datetime(["2026-01-09", "2026-01-16"])
    benchmark_series = build_index_benchmark_series(
        calendar_dates=calendar,
        ts_code="000001.SH",
        period="week",
        fetcher=_FakeFetcher(),
    )

    assert benchmark_series.index.strftime("%Y-%m-%d").tolist() == ["2026-01-16"]
    assert benchmark_series.round(6).tolist() == [round(11.5 / 10.8 - 1.0, 6)]


def test_resolve_benchmark_components_maps_universe_profiles():
    assert [item.ts_code for item in resolve_benchmark_components("csi300")] == ["000300.SH"]
    assert [item.ts_code for item in resolve_benchmark_components("csi500")] == ["000905.SH"]
    assert [item.ts_code for item in resolve_benchmark_components("merged_csi300_500")] == ["000300.SH", "000905.SH"]


def test_resolve_benchmark_components_accepts_manual_index_override():
    components = resolve_benchmark_components("csi300", benchmark_mode="000001.SH@上证指数")

    assert len(components) == 1
    assert components[0].ts_code == "000001.SH"
    assert components[0].label == "上证指数"


def test_build_universe_benchmark_frame_blends_csi300_and_csi500():
    calendar = pd.to_datetime(["2026-01-09", "2026-01-16"])
    benchmark_frame = build_universe_benchmark_frame(
        calendar_dates=calendar,
        universe_profile="merged_csi300_500",
        benchmark_mode="auto",
        period="W",
        initial_capital=1_000_000.0,
        fetcher=_FakeFetcher(),
    )

    assert benchmark_frame["benchmark_label"].iloc[0] == "沪深300 50% / 中证500 50%"
    assert benchmark_frame["benchmark_close"].round(2).tolist() == [15.8, 16.5]
    expected_second_return = round(((11.5 / 10.8 - 1.0) + (21.5 / 20.8 - 1.0)) / 2.0, 6)
    assert benchmark_frame["benchmark_return"].round(6).tolist() == [0.0, expected_second_return]


def test_build_universe_benchmark_frame_supports_manual_index_override():
    calendar = pd.to_datetime(["2026-01-09", "2026-01-16"])
    benchmark_frame = build_universe_benchmark_frame(
        calendar_dates=calendar,
        universe_profile="csi300",
        benchmark_mode="000001.SH",
        period="W",
        initial_capital=1_000_000.0,
        fetcher=_FakeFetcher(),
    )

    assert benchmark_frame["benchmark_label"].iloc[0] == "上证指数"
    assert benchmark_frame["benchmark_close"].tolist() == [10.8, 11.5]
