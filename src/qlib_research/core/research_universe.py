"""
Universe helpers for qlib-style offline research.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal, Optional, Sequence

import pandas as pd

from qlib_research.config import get_fdh, get_project_root


INDEX_FLAG_CONFIG = {
    "csi300": {"index_code": "000300.SH", "flag_column": "in_csi300"},
    "csi500": {"index_code": "000905.SH", "flag_column": "in_csi500"},
}

SUPPORTED_UNIVERSE_PROFILES = {
    "watchlist",
    "csi300",
    "csi500",
    "merged_csi300_500",
}

UniverseSelectionMode = Literal["historical_membership", "fixed_universe"]


def _normalize_symbol(values: pd.Series) -> pd.Series:
    return values.astype(str).str.upper()


def _normalize_time(values: pd.Series) -> pd.Series:
    return (
        pd.to_datetime(values, utc=True, errors="coerce")
        .dt.tz_localize(None)
        .dt.normalize()
        .astype("datetime64[ns]")
    )


def _normalize_index_weight_frame(frame: Optional[pd.DataFrame]) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["symbol", "trade_date", "weight"])

    result = frame.copy()
    symbol_col = "con_code" if "con_code" in result.columns else "ts_code"
    time_col = "trade_date" if "trade_date" in result.columns else "time"
    if symbol_col != "symbol":
        result = result.rename(columns={symbol_col: "symbol"})
    if time_col != "trade_date":
        result = result.rename(columns={time_col: "trade_date"})

    result["symbol"] = _normalize_symbol(result["symbol"])
    result["trade_date"] = _normalize_time(result["trade_date"])
    result["weight"] = pd.to_numeric(result.get("weight", 1.0), errors="coerce").fillna(0.0)
    result = result.dropna(subset=["symbol", "trade_date"])
    return result[["symbol", "trade_date", "weight"]].drop_duplicates().sort_values(
        ["trade_date", "symbol"]
    ).reset_index(drop=True)


def _load_watchlist_symbols() -> list[str]:
    watchlist_path = get_project_root() / "data" / "watchlist.json"
    if not watchlist_path.exists():
        return []

    records = json.loads(watchlist_path.read_text(encoding="utf-8"))
    symbols = []
    for record in records:
        ticker = str(record.get("ticker") or "").strip().upper()
        if ticker:
            symbols.append(ticker)
    return sorted(set(symbols))


def _expand_index_weight_query_window(
    start_date: str | None,
    end_date: str | None,
) -> tuple[str | None, str | None]:
    query_start = start_date
    query_end = end_date

    if start_date:
        start_ts = pd.to_datetime(start_date, errors="coerce")
        if not pd.isna(start_ts):
            month_start = pd.Timestamp(start_ts).normalize().replace(day=1)
            query_start = ((month_start - pd.DateOffset(months=1)).replace(day=1)).strftime("%Y-%m-%d")

    if end_date:
        end_ts = pd.to_datetime(end_date, errors="coerce")
        if not pd.isna(end_ts):
            query_end = (pd.Timestamp(end_ts).normalize() + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")

    return query_start, query_end


async def fetch_universe_profile_history(
    universe_profile: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Fetch historical membership snapshots for a supported universe profile.
    """
    if universe_profile not in SUPPORTED_UNIVERSE_PROFILES:
        raise ValueError(f"Unsupported universe profile: {universe_profile}")

    if universe_profile == "watchlist":
        return {}

    fdh = await get_fdh()
    history: dict[str, pd.DataFrame] = {}
    query_start_date, query_end_date = _expand_index_weight_query_window(start_date, end_date)

    profile_keys = (
        ["csi300", "csi500"]
        if universe_profile == "merged_csi300_500"
        else [universe_profile]
    )
    for profile_key in profile_keys:
        config = INDEX_FLAG_CONFIG[profile_key]
        history[config["flag_column"]] = _normalize_index_weight_frame(
            await fdh.get_index_weight_async(
                index_code=config["index_code"],
                start_date=query_start_date,
                end_date=query_end_date,
            )
        )

    return history


def _select_fixed_universe_symbols(
    history_frames: dict[str, pd.DataFrame],
    *,
    as_of_date: str | None = None,
) -> list[str]:
    if not history_frames:
        return []

    as_of_ts = pd.to_datetime(as_of_date, errors="coerce") if as_of_date else None
    if pd.isna(as_of_ts):
        as_of_ts = None

    symbols: set[str] = set()
    for frame in history_frames.values():
        if frame.empty:
            continue
        normalized = frame.copy()
        normalized["trade_date"] = _normalize_time(normalized["trade_date"])
        normalized["symbol"] = _normalize_symbol(normalized["symbol"])
        normalized = normalized.dropna(subset=["trade_date", "symbol"])
        if normalized.empty:
            continue

        if as_of_ts is not None:
            eligible = normalized.loc[normalized["trade_date"] <= pd.Timestamp(as_of_ts)].copy()
        else:
            eligible = normalized

        if eligible.empty:
            snapshot_date = normalized["trade_date"].min()
            eligible = normalized.loc[normalized["trade_date"] == snapshot_date].copy()
        else:
            snapshot_date = eligible["trade_date"].max()
            eligible = eligible.loc[eligible["trade_date"] == snapshot_date].copy()

        symbols.update(eligible["symbol"].dropna().astype(str).tolist())

    return sorted(symbols)


async def resolve_universe_symbols(
    universe_profile: str | None = None,
    symbols: Optional[Sequence[str]] = None,
    start_date: str | None = None,
    end_date: str | None = None,
    universe_mode: UniverseSelectionMode = "historical_membership",
) -> tuple[list[str], dict[str, pd.DataFrame]]:
    """
    Resolve the effective symbol list and optional historical membership frames.
    """
    explicit_symbols = sorted({str(symbol).strip().upper() for symbol in (symbols or []) if str(symbol).strip()})
    if explicit_symbols:
        return explicit_symbols, {}

    if not universe_profile:
        return [], {}

    if universe_profile == "watchlist":
        return _load_watchlist_symbols(), {}

    history = await fetch_universe_profile_history(
        universe_profile=universe_profile,
        start_date=start_date,
        end_date=end_date,
    )
    if universe_mode == "fixed_universe":
        return _select_fixed_universe_symbols(history, as_of_date=end_date), {}
    resolved = sorted({
        symbol
        for frame in history.values()
        for symbol in frame["symbol"].unique().tolist()
    })
    return resolved, history


def build_index_membership_flag_frame(
    weekly_dates: Sequence[pd.Timestamp],
    history_frame: pd.DataFrame,
    flag_column: str,
) -> pd.DataFrame:
    """
    Expand historical index snapshots into weekly membership flags.
    """
    if history_frame.empty or len(weekly_dates) == 0:
        return pd.DataFrame(columns=["time", "symbol", flag_column])

    normalized_history = history_frame.copy()
    normalized_history["trade_date"] = _normalize_time(normalized_history["trade_date"])
    normalized_history["symbol"] = _normalize_symbol(normalized_history["symbol"])
    normalized_history = normalized_history.dropna(subset=["trade_date", "symbol"]).sort_values(["trade_date", "symbol"]).reset_index(drop=True)

    weekly_index = pd.DataFrame({"time": sorted(pd.to_datetime(pd.Index(weekly_dates)).unique())})
    weekly_index["time"] = _normalize_time(weekly_index["time"])
    weekly_index = weekly_index.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)

    snapshots = pd.DataFrame({"snapshot_date": sorted(normalized_history["trade_date"].dropna().unique())})
    snapshots["snapshot_date"] = _normalize_time(snapshots["snapshot_date"])
    snapshots = snapshots.dropna(subset=["snapshot_date"]).sort_values("snapshot_date").reset_index(drop=True)
    if snapshots.empty:
        return pd.DataFrame(columns=["time", "symbol", flag_column])

    if not weekly_index["time"].is_monotonic_increasing:
        raise ValueError("weekly_dates must be globally sorted before universe membership mapping")
    if not snapshots["snapshot_date"].is_monotonic_increasing:
        raise ValueError("history snapshot dates must be globally sorted before universe membership mapping")

    mapping = pd.merge_asof(
        weekly_index,
        snapshots,
        left_on="time",
        right_on="snapshot_date",
        direction="backward",
    ).dropna(subset=["snapshot_date"])

    rows: list[pd.DataFrame] = []
    for snapshot_date, mapped_frame in mapping.groupby("snapshot_date"):
        symbols = normalized_history.loc[normalized_history["trade_date"] == snapshot_date, "symbol"].dropna().unique()
        if len(symbols) == 0:
            continue
        for weekly_date in mapped_frame["time"].tolist():
            rows.append(
                pd.DataFrame(
                    {
                        "time": pd.Timestamp(weekly_date),
                        "symbol": symbols,
                        flag_column: True,
                    }
                )
            )

    if not rows:
        return pd.DataFrame(columns=["time", "symbol", flag_column])

    flags = pd.concat(rows, ignore_index=True)
    flags["symbol"] = _normalize_symbol(flags["symbol"])
    flags["time"] = pd.to_datetime(flags["time"]).dt.normalize()
    return flags.drop_duplicates(subset=["time", "symbol"]).sort_values(["time", "symbol"]).reset_index(drop=True)


def attach_universe_flags(
    panel: pd.DataFrame,
    history_frames: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Attach per-week universe membership flags to a weekly feature panel.
    """
    if panel.empty or not history_frames:
        return panel

    result = panel.copy()
    result["symbol"] = _normalize_symbol(result["symbol"])
    result["time"] = pd.to_datetime(result["time"]).dt.normalize()
    weekly_dates = sorted(result["time"].dropna().unique())

    for flag_column, history_frame in history_frames.items():
        flags = build_index_membership_flag_frame(
            weekly_dates=weekly_dates,
            history_frame=history_frame,
            flag_column=flag_column,
        )
        if flags.empty:
            result[flag_column] = False
            continue
        result = result.merge(flags, on=["time", "symbol"], how="left")
        result[flag_column] = result[flag_column].astype("boolean").fillna(False).astype(bool)

    return result
