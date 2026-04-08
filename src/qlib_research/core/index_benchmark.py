"""
Helpers for building notebook-friendly index benchmark curves from the backend's
index technical data logic.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Sequence

import pandas as pd

from qlib_research.core.fetcher import TushareFetcher
from qlib_research.core.index_data_provider import fetch_index_daily_with_fallback


def normalize_index_period(period: str) -> str:
    normalized = str(period).strip().upper()
    period_aliases = {
        "D": "D",
        "DAY": "D",
        "1D": "D",
        "DAILY": "D",
        "W": "W",
        "WEEK": "W",
        "1W": "W",
        "1WEEK": "W",
        "WEEKLY": "W",
        "M": "M",
        "MONTH": "M",
        "1M": "M",
        "1MONTH": "M",
        "MONTHLY": "M",
    }
    if normalized not in period_aliases:
        raise ValueError(f"Unsupported index period: {period}")
    return period_aliases[normalized]


@dataclass(frozen=True)
class BenchmarkComponent:
    ts_code: str
    weight: float
    label: str


AUTO_UNIVERSE_BENCHMARKS = {
    "csi300": (
        BenchmarkComponent(ts_code="000300.SH", weight=1.0, label="沪深300"),
    ),
    "csi500": (
        BenchmarkComponent(ts_code="000905.SH", weight=1.0, label="中证500"),
    ),
    "merged_csi300_500": (
        BenchmarkComponent(ts_code="000300.SH", weight=0.5, label="沪深300"),
        BenchmarkComponent(ts_code="000905.SH", weight=0.5, label="中证500"),
    ),
}

KNOWN_INDEX_LABELS = {
    "000001.SH": "上证指数",
    "000300.SH": "沪深300",
    "000905.SH": "中证500",
}


def _manual_benchmark_component(benchmark_mode: str) -> tuple[BenchmarkComponent, ...]:
    raw_mode = str(benchmark_mode).strip()
    normalized_mode = raw_mode.upper()
    spec = raw_mode[len("manual:"):].strip() if raw_mode.lower().startswith("manual:") else raw_mode
    ts_code, label = spec, None
    if "@" in spec:
        ts_code, label = spec.split("@", 1)
    elif "|" in spec:
        ts_code, label = spec.split("|", 1)
    ts_code = str(ts_code).strip().upper()
    if not ts_code:
        raise ValueError(f"Unsupported benchmark mode: {benchmark_mode}")
    resolved_label = str(label).strip() if label is not None and str(label).strip() else KNOWN_INDEX_LABELS.get(ts_code, ts_code)
    return (BenchmarkComponent(ts_code=ts_code, weight=1.0, label=resolved_label),)


def resolve_benchmark_components(
    universe_profile: str | None = None,
    benchmark_mode: str | None = "auto",
) -> tuple[BenchmarkComponent, ...]:
    raw_mode = str(benchmark_mode or "auto").strip()
    normalized_mode = raw_mode.lower()
    if normalized_mode == "flat_zero":
        return tuple()
    if normalized_mode not in {"auto", "flat_zero"}:
        return _manual_benchmark_component(raw_mode)
    if not universe_profile:
        return (
            BenchmarkComponent(ts_code="000001.SH", weight=1.0, label="上证指数"),
        )
    return AUTO_UNIVERSE_BENCHMARKS.get(
        universe_profile,
        (BenchmarkComponent(ts_code="000001.SH", weight=1.0, label="上证指数"),),
    )


def load_index_technical_frame(
    ts_code: str = "000001.SH",
    period: str = "W",
    start_date: str | pd.Timestamp | None = None,
    end_date: str | pd.Timestamp | None = None,
    fetcher: TushareFetcher | None = None,
) -> pd.DataFrame:
    """
    Load index bars with the same resampling semantics used by the stock API's
    `get_index_technical`.
    """

    normalized_period = normalize_index_period(period)
    resolved_end = pd.Timestamp(end_date) if end_date is not None else pd.Timestamp(datetime.now().date())
    resolved_start = (
        pd.Timestamp(start_date)
        if start_date is not None
        else resolved_end - pd.Timedelta(days=365 * 10)
    )
    resolved_fetcher = fetcher or TushareFetcher()

    df_daily = fetch_index_daily_with_fallback(
        ts_code=ts_code,
        start_date=resolved_start,
        end_date=resolved_end,
        fetcher=resolved_fetcher,
    )
    if df_daily is None or df_daily.empty:
        raise RuntimeError(f"No index data available for {ts_code}")

    if normalized_period == "W":
        bars = resolved_fetcher.resample_index_to_weekly(df_daily)
    elif normalized_period == "M":
        monthly = df_daily.copy()
        if "trade_date" in monthly.columns:
            monthly["trade_date"] = pd.to_datetime(monthly["trade_date"])
            monthly = monthly.set_index("trade_date").sort_index()
        else:
            monthly.index = pd.to_datetime(monthly.index)
            monthly = monthly.sort_index()
        bars = (
            monthly.resample("ME", closed="right", label="right")
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
    else:
        daily = df_daily.copy()
        if "trade_date" in daily.columns:
            daily["trade_date"] = pd.to_datetime(daily["trade_date"])
            daily["date"] = daily["trade_date"]
        elif "date" in daily.columns:
            daily["date"] = pd.to_datetime(daily["date"])
        else:
            raise ValueError("index daily data must include trade_date or date")
        bars = daily

    bars = bars.copy()
    bars["date"] = pd.to_datetime(bars["date"])
    required_columns = ["date", "open", "high", "low", "close"]
    missing = [column for column in required_columns if column not in bars.columns]
    if missing:
        raise ValueError(f"index bars missing required columns: {missing}")
    if "volume" not in bars.columns:
        bars["volume"] = 0.0
    for column in ("open", "high", "low", "close", "volume"):
        bars[column] = pd.to_numeric(bars[column], errors="coerce")
    return bars[["date", "open", "high", "low", "close", "volume"]].dropna(subset=["date", "close"]).sort_values("date")


def build_index_benchmark_frame(
    calendar_dates: Sequence[pd.Timestamp] | pd.Index | pd.Series,
    ts_code: str = "000001.SH",
    period: str = "W",
    initial_capital: float = 1_000_000.0,
    fetcher: TushareFetcher | None = None,
) -> pd.DataFrame:
    """
    Align an index close series to the workflow's calendar and derive benchmark
    returns / value curves.
    """

    calendar_index = pd.DatetimeIndex(pd.to_datetime(calendar_dates)).sort_values().unique()
    if len(calendar_index) == 0:
        return pd.DataFrame(columns=["datetime", "benchmark_close", "benchmark_return", "benchmark_value"])

    index_frame = load_index_technical_frame(
        ts_code=ts_code,
        period=period,
        start_date=calendar_index.min() - timedelta(days=40),
        end_date=calendar_index.max() + timedelta(days=7),
        fetcher=fetcher,
    )
    aligned_close = (
        index_frame[["date", "close"]]
        .assign(date=lambda frame: pd.to_datetime(frame["date"]))
        .set_index("date")
        .sort_index()["close"]
        .reindex(calendar_index)
        .ffill()
    )
    aligned_close = aligned_close.dropna()
    if aligned_close.empty:
        return pd.DataFrame(columns=["datetime", "benchmark_close", "benchmark_return", "benchmark_value"])

    benchmark_frame = aligned_close.to_frame("benchmark_close").reset_index().rename(columns={"index": "datetime"})
    benchmark_frame["benchmark_return"] = benchmark_frame["benchmark_close"].pct_change().fillna(0.0)
    benchmark_frame["benchmark_value"] = float(initial_capital) * (1.0 + benchmark_frame["benchmark_return"]).cumprod()
    return benchmark_frame


def build_blended_index_benchmark_frame(
    calendar_dates: Sequence[pd.Timestamp] | pd.Index | pd.Series,
    components: Sequence[BenchmarkComponent],
    period: str = "W",
    initial_capital: float = 1_000_000.0,
    fetcher: TushareFetcher | None = None,
) -> pd.DataFrame:
    normalized_components = [component for component in components if float(component.weight) > 0]
    if not normalized_components:
        return pd.DataFrame(columns=["datetime", "benchmark_close", "benchmark_return", "benchmark_value", "benchmark_label"])

    component_frames: list[pd.DataFrame] = []
    for component in normalized_components:
        frame = build_index_benchmark_frame(
            calendar_dates=calendar_dates,
            ts_code=component.ts_code,
            period=period,
            initial_capital=initial_capital,
            fetcher=fetcher,
        )
        if frame.empty:
            continue
        component_frames.append(
            frame.rename(
                columns={
                    "benchmark_close": f"close__{component.ts_code}",
                    "benchmark_return": f"return__{component.ts_code}",
                    "benchmark_value": f"value__{component.ts_code}",
                }
            )
        )

    if not component_frames:
        return pd.DataFrame(columns=["datetime", "benchmark_close", "benchmark_return", "benchmark_value", "benchmark_label"])

    merged = component_frames[0]
    for frame in component_frames[1:]:
        merged = merged.merge(frame, on="datetime", how="outer")
    merged = merged.sort_values("datetime").ffill()

    total_weight = sum(float(component.weight) for component in normalized_components)
    if total_weight <= 0:
        raise ValueError("Benchmark component weights must sum to a positive value")

    close_columns = []
    return_columns = []
    for component in normalized_components:
        close_column = f"close__{component.ts_code}"
        return_column = f"return__{component.ts_code}"
        if close_column in merged.columns:
            close_columns.append((close_column, float(component.weight)))
        if return_column in merged.columns:
            return_columns.append((return_column, float(component.weight)))

    merged["benchmark_close"] = sum(merged[column].ffill() * weight for column, weight in close_columns) / total_weight
    merged["benchmark_return"] = sum(merged[column].fillna(0.0) * weight for column, weight in return_columns) / total_weight
    merged["benchmark_value"] = float(initial_capital) * (1.0 + merged["benchmark_return"]).cumprod()
    merged["benchmark_label"] = " / ".join(
        f"{component.label} {component.weight:.0%}" if component.weight != 1.0 else component.label
        for component in normalized_components
    )
    return merged[["datetime", "benchmark_close", "benchmark_return", "benchmark_value", "benchmark_label"]]


def build_index_benchmark_series(
    calendar_dates: Sequence[pd.Timestamp] | pd.Index | pd.Series,
    ts_code: str = "000001.SH",
    period: str = "W",
    fetcher: TushareFetcher | None = None,
) -> pd.Series:
    """
    Return qlib-ready benchmark returns indexed by trade date. The first aligned
    date is dropped because qlib backtests start from the second signal date.
    """

    benchmark_frame = build_index_benchmark_frame(
        calendar_dates=calendar_dates,
        ts_code=ts_code,
        period=period,
        fetcher=fetcher,
    )
    if benchmark_frame.empty:
        return pd.Series(dtype=float, name="benchmark_return")
    benchmark_series = benchmark_frame.set_index("datetime")["benchmark_return"].astype(float)
    if len(benchmark_series) >= 2:
        benchmark_series = benchmark_series.iloc[1:]
    benchmark_series.name = "benchmark_return"
    return benchmark_series


def build_universe_benchmark_frame(
    calendar_dates: Sequence[pd.Timestamp] | pd.Index | pd.Series,
    universe_profile: str | None = None,
    benchmark_mode: str | None = "auto",
    period: str = "W",
    initial_capital: float = 1_000_000.0,
    fetcher: TushareFetcher | None = None,
) -> pd.DataFrame:
    components = resolve_benchmark_components(universe_profile=universe_profile, benchmark_mode=benchmark_mode)
    if not components:
        return pd.DataFrame(columns=["datetime", "benchmark_close", "benchmark_return", "benchmark_value", "benchmark_label"])
    if len(components) == 1 and components[0].weight == 1.0:
        frame = build_index_benchmark_frame(
            calendar_dates=calendar_dates,
            ts_code=components[0].ts_code,
            period=period,
            initial_capital=initial_capital,
            fetcher=fetcher,
        )
        if frame.empty:
            return frame
        frame["benchmark_label"] = components[0].label
        return frame
    return build_blended_index_benchmark_frame(
        calendar_dates=calendar_dates,
        components=components,
        period=period,
        initial_capital=initial_capital,
        fetcher=fetcher,
    )


def build_universe_benchmark_series(
    calendar_dates: Sequence[pd.Timestamp] | pd.Index | pd.Series,
    universe_profile: str | None = None,
    benchmark_mode: str | None = "auto",
    period: str = "W",
    fetcher: TushareFetcher | None = None,
) -> pd.Series:
    frame = build_universe_benchmark_frame(
        calendar_dates=calendar_dates,
        universe_profile=universe_profile,
        benchmark_mode=benchmark_mode,
        period=period,
        fetcher=fetcher,
    )
    if frame.empty:
        return pd.Series(dtype=float, name="benchmark_return")
    series = frame.set_index("datetime")["benchmark_return"].astype(float)
    if len(series) >= 2:
        series = series.iloc[1:]
    series.name = "benchmark_return"
    return series
