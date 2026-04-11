"""
Build a weekly factor panel from FinanceDataHub for qlib-style research.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
from decimal import Decimal
import json
from pathlib import Path
import threading
from typing import Iterable, List, Literal, Optional, Sequence

import numpy as np
import pandas as pd

from qlib_research.config import close_fdh, get_fdh
from qlib_research.core.research_universe import attach_universe_flags, resolve_universe_symbols


WEEKLY_RENAME_MAP = {
    "ma_20": "ma20",
    "ma_50": "ma50",
    "macd_dif": "dif",
    "macd_dea": "dea",
    "macd_hist": "macd_hist",
    "rsi_14": "rsi",
    "atr_14": "atr",
}

INDUSTRY_KEEP_COLUMNS = {
    "symbol",
    "time",
    "l1_code",
    "l1_name",
    "l2_code",
    "l2_name",
    "l3_code",
    "l3_name",
    "core_indicator_type",
    "core_indicator_value",
    "core_indicator_pct_1250d",
    "core_indicator_industry_pct",
    "ref_indicator_type",
    "ref_indicator_value",
    "ref_indicator_pct_1250d",
    "ref_indicator_industry_pct",
    "is_exempted",
    "exemption_reason",
    "processed_at",
    "fscore_exemptions",
}

CANONICAL_FEATURE_COLUMNS = {
    "pe_ttm": ("pe_ttm", "pe_ttm_x", "pe_ttm_y"),
    "pb": ("pb", "pb_x", "pb_y"),
    "ps_ttm": ("ps_ttm", "ps_ttm_x", "ps_ttm_y"),
    "dv_ttm": ("dv_ttm", "dv_ttm_x", "dv_ttm_y"),
    "peg": ("peg", "peg_x", "peg_y"),
}

MACRO_PHASE_Y_MAP = {
    "REFLATION": 0,
    "RECOVERY": 1,
    "OVERHEAT": 2,
    "STAGFLATION": 3,
}

PanelEnrichmentScope = Literal["none", "symbol_local", "research_full"]

SYMBOL_LOCAL_DERIVED_COLUMNS = {
    "mom_4w",
    "rev_4w",
    "volatility_8w",
    "downside_volatility_8w",
    "amount_change_4w",
    "volume_change_4w",
    "amount_zscore_4w",
    "pe_ttm_delta_4w",
    "f_score_delta_8w",
}

RESEARCH_CROSS_SECTIONAL_DERIVED_COLUMNS = {
    "industry_mom_4w_rank_pct",
    "industry_pe_ttm_pct_rank_pct",
    "industry_f_score_rank_pct",
    "industry_npm_ttm_rank_pct",
    "buffett_npm_flag",
    "buffett_moat_coverage",
    "buffett_moat_score",
    "buffett_moat_pass",
    "macro_reflation_x_mom_4w",
    "macro_recovery_x_mom_8w",
    "macro_overheat_x_volatility_8w",
    "macro_stagflation_x_f_score",
    "macro_industry_match_x_mom_4w",
}

_ENRICHMENT_SCOPE_ORDER: dict[PanelEnrichmentScope, int] = {
    "none": 0,
    "symbol_local": 1,
    "research_full": 2,
}


def _batched(values: Optional[Sequence[str]], batch_size: int) -> Iterable[Optional[List[str]]]:
    if not values:
        yield None
        return
    for start in range(0, len(values), batch_size):
        yield list(values[start:start + batch_size])


def _normalize_time_symbol_frame(
    frame: Optional[pd.DataFrame],
    rename_map: Optional[dict] = None,
) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["symbol", "time"])

    result = frame.copy()
    if rename_map:
        result = result.rename(columns=rename_map)

    symbol_col = "symbol" if "symbol" in result.columns else "ts_code"
    time_col = "time"
    if time_col not in result.columns:
        for candidate in ("trade_date", "end_date_time", "ann_date_time"):
            if candidate in result.columns:
                time_col = candidate
                break

    if symbol_col != "symbol":
        result = result.rename(columns={symbol_col: "symbol"})
    if time_col != "time":
        result = result.rename(columns={time_col: "time"})

    for column in result.columns:
        if column in {"symbol", "time"}:
            continue
        if result[column].dtype == object:
            result[column] = result[column].apply(
                lambda value: float(value) if isinstance(value, Decimal) else value
            )

    result["symbol"] = result["symbol"].astype(str)
    result["time"] = (
        pd.to_datetime(result["time"], utc=True, errors="coerce")
        .dt.tz_localize(None)
        .dt.normalize()
    )
    result = result.dropna(subset=["symbol", "time"])
    return result.sort_values(["time", "symbol"]).reset_index(drop=True)


def _normalize_timestamp_series(series: pd.Series) -> pd.Series:
    result = pd.to_datetime(series, errors="coerce")
    if getattr(result.dt, "tz", None) is not None:
        result = result.dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
    return result.dt.normalize()


def _normalize_time_frame(
    frame: Optional[pd.DataFrame],
    rename_map: Optional[dict] = None,
) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["time"])

    result = frame.copy()
    if rename_map:
        result = result.rename(columns=rename_map)

    time_col = "time"
    if time_col not in result.columns:
        for candidate in ("trade_date", "observation_time", "effective_time"):
            if candidate in result.columns:
                time_col = candidate
                break

    if time_col != "time":
        result = result.rename(columns={time_col: "time"})

    result["time"] = _normalize_timestamp_series(result["time"])
    return result.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)


def _merge_asof_frame(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    left_on: str = "time",
    right_on: str = "time",
    by: str | Sequence[str] | None = None,
) -> pd.DataFrame:
    if right.empty:
        return left

    def _sort_for_merge(frame: pd.DataFrame, on: str, by_columns: Sequence[str] | None = None) -> pd.DataFrame:
        if by_columns is None:
            ordered = frame.sort_values(on).reset_index(drop=True)
        else:
            ordered = frame.sort_values([on, *by_columns]).reset_index(drop=True)
        if not ordered[on].is_monotonic_increasing:
            raise ValueError(f"{on} must be globally sorted for merge_asof")
        return ordered

    if by is None:
        left_sorted = _sort_for_merge(left, left_on)
        right_sorted = _sort_for_merge(right, right_on)
    else:
        by_columns = [by] if isinstance(by, str) else list(by)
        # pandas.merge_asof requires the "on" key to be globally sorted even when
        # grouping by additional columns, so time must come before symbol here.
        left_sorted = _sort_for_merge(left, left_on, by_columns)
        right_sorted = _sort_for_merge(right, right_on, by_columns)

    return pd.merge_asof(
        left_sorted,
        right_sorted,
        left_on=left_on,
        right_on=right_on,
        by=by,
        direction="backward",
    )


def _merge_asof_by_symbol(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    return _merge_asof_frame(left, right, by="symbol")


def _trim_industry_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    keep = [column for column in frame.columns if column in INDUSTRY_KEEP_COLUMNS]
    return frame[keep].copy()


def _coalesce_columns(frame: pd.DataFrame, target: str, candidates: Sequence[str]) -> None:
    series = None
    for candidate in candidates:
        if candidate not in frame.columns:
            continue
        current = pd.to_numeric(frame[candidate], errors="coerce")
        series = current if series is None else series.combine_first(current)

    if series is None:
        return

    frame[target] = series
    for candidate in candidates:
        if candidate != target and candidate in frame.columns:
            frame.drop(columns=candidate, inplace=True)


def _harmonize_feature_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame

    result = frame.copy()
    for target, candidates in CANONICAL_FEATURE_COLUMNS.items():
        _coalesce_columns(result, target, candidates)
    return result


def _prepare_macro_phase_frame(frame: Optional[pd.DataFrame]) -> pd.DataFrame:
    normalized = _normalize_time_frame(frame)
    if normalized.empty:
        return pd.DataFrame(
            columns=[
                "macro_effective_time",
                "macro_observation_time",
                "macro_m2_yoy",
                "macro_gdp_yoy",
                "macro_ppi_yoy",
                "macro_pmi",
                "macro_credit_impulse",
                "macro_phase",
                "macro_phase_y",
                "macro_phase_changed",
            ]
        )

    result = normalized.copy()
    result = result.rename(columns={"time": "macro_effective_time"})
    if "observation_time" in result.columns:
        result["macro_observation_time"] = _normalize_timestamp_series(result["observation_time"])
    else:
        result["macro_observation_time"] = pd.NaT

    result["macro_phase"] = result.get("phase")
    result["macro_phase_y"] = pd.to_numeric(
        result.get("phase_y", result["macro_phase"].map(MACRO_PHASE_Y_MAP)),
        errors="coerce",
    )
    result["macro_phase_changed"] = (
        result.get("phase_changed", pd.Series(False, index=result.index))
        .fillna(False)
        .astype("int8")
    )
    result["macro_m2_yoy"] = pd.to_numeric(result.get("m2_yoy"), errors="coerce")
    result["macro_gdp_yoy"] = pd.to_numeric(result.get("gdp_yoy"), errors="coerce")
    result["macro_ppi_yoy"] = pd.to_numeric(result.get("ppi_yoy"), errors="coerce")
    result["macro_pmi"] = pd.to_numeric(result.get("pmi"), errors="coerce")
    result["macro_credit_impulse"] = pd.to_numeric(result.get("credit_impulse"), errors="coerce")

    return result[
        [
            "macro_effective_time",
            "macro_observation_time",
            "macro_m2_yoy",
            "macro_gdp_yoy",
            "macro_ppi_yoy",
            "macro_pmi",
            "macro_credit_impulse",
            "macro_phase",
            "macro_phase_y",
            "macro_phase_changed",
        ]
    ].copy()


def _prepare_macro_industry_frame(frame: Optional[pd.DataFrame]) -> pd.DataFrame:
    normalized = _normalize_time_frame(frame)
    if normalized.empty:
        return pd.DataFrame(
            columns=["macro_effective_time", "l3_name", "macro_config_phase", "macro_industry_match"]
        )

    result = normalized.copy()
    result = result.rename(columns={"time": "macro_effective_time"})
    result["macro_config_phase"] = result.get("config_macro_cycle")
    match_col = "matches_stable_phase" if "matches_stable_phase" in result.columns else "matches_raw_phase"
    result["macro_industry_match"] = (
        result.get(match_col, pd.Series(False, index=result.index))
        .fillna(False)
        .astype("int8")
    )
    return result[
        ["macro_effective_time", "l3_name", "macro_config_phase", "macro_industry_match"]
    ].copy()


def _attach_macro_phase_flags(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    phase_series = result.get("macro_phase")
    if phase_series is None:
        phase_series = pd.Series(index=result.index, dtype="object")

    for phase_name in MACRO_PHASE_Y_MAP:
        column = f"macro_phase_{phase_name.lower()}"
        result[column] = phase_series.eq(phase_name).fillna(False).astype("int8")

    if "macro_phase_changed" in result.columns:
        result["macro_phase_changed"] = pd.to_numeric(result["macro_phase_changed"], errors="coerce").fillna(0).astype("int8")
    else:
        result["macro_phase_changed"] = 0

    if "macro_industry_match" in result.columns:
        result["macro_industry_match"] = pd.to_numeric(result["macro_industry_match"], errors="coerce").fillna(0).astype("int8")
    else:
        result["macro_industry_match"] = 0

    return result


def _attach_future_labels(panel: pd.DataFrame, horizons: Sequence[int]) -> pd.DataFrame:
    if panel.empty:
        return panel

    result = panel.sort_values(["symbol", "time"]).copy()
    grouped_close = result.groupby("symbol")["close"]

    for horizon in horizons:
        future_close = grouped_close.shift(-horizon)
        raw_return_col = f"future_return_{horizon}w"
        label_col = f"label_excess_return_{horizon}w"
        result[raw_return_col] = (future_close / result["close"]) - 1.0
        result[label_col] = result[raw_return_col] - result.groupby("time")[raw_return_col].transform("mean")

    return result


def _replace_numeric_infinities(panel: pd.DataFrame) -> pd.DataFrame:
    if panel.empty:
        return panel.copy()
    result = panel.copy()
    numeric_columns = result.select_dtypes(include=[np.number]).columns
    if len(numeric_columns) > 0:
        result.loc[:, numeric_columns] = result.loc[:, numeric_columns].replace([np.inf, -np.inf], np.nan)
    return result


def resolve_panel_enrichment_scope(
    include_derived_features: bool | None = True,
    enrichment_scope: PanelEnrichmentScope | str | None = None,
) -> PanelEnrichmentScope:
    if enrichment_scope is None:
        return "research_full" if include_derived_features else "none"

    resolved = str(enrichment_scope).strip().lower()
    if resolved not in _ENRICHMENT_SCOPE_ORDER:
        raise ValueError(f"Unsupported panel enrichment scope: {enrichment_scope}")
    return resolved  # type: ignore[return-value]


def detect_panel_enrichment_scope(panel: pd.DataFrame) -> PanelEnrichmentScope:
    if panel.empty:
        return "none"

    columns = set(panel.columns)
    if columns.intersection(RESEARCH_CROSS_SECTIONAL_DERIVED_COLUMNS):
        return "research_full"
    if columns.intersection(SYMBOL_LOCAL_DERIVED_COLUMNS):
        return "symbol_local"
    return "none"


def engineer_symbol_local_features(panel: pd.DataFrame) -> pd.DataFrame:
    """
    Add deterministic per-symbol derived features on top of the weekly raw panel.
    """
    if panel.empty:
        return panel.copy()

    result = panel.sort_values(["symbol", "time"]).copy()
    if "nda_value" in result.columns:
        result["nda_value"] = pd.to_numeric(result["nda_value"], errors="coerce")
    if "volume_confirmed" in result.columns:
        result["volume_confirmed"] = result["volume_confirmed"].apply(
            lambda value: np.nan if pd.isna(value) else int(bool(value))
        )
    for column in (
        "buffett_gpm_flag",
        "buffett_npm_stable_flag",
        "buffett_roa_flag",
        "buffett_cashflow_flag",
    ):
        if column in result.columns:
            result[column] = pd.to_numeric(result[column], errors="coerce")

    grouped_close = result.groupby("symbol")["close"]

    for horizon in (1, 2, 4, 8):
        mom_column = f"mom_{horizon}w"
        rev_column = f"rev_{horizon}w"
        result[mom_column] = grouped_close.pct_change(periods=horizon)
        result[rev_column] = -result[mom_column]

    weekly_return = grouped_close.pct_change()
    result["volatility_4w"] = weekly_return.groupby(result["symbol"]).transform(
        lambda values: values.rolling(4, min_periods=2).std()
    )
    result["volatility_8w"] = weekly_return.groupby(result["symbol"]).transform(
        lambda values: values.rolling(8, min_periods=4).std()
    )
    downside_returns = weekly_return.where(weekly_return < 0.0, 0.0)
    result["downside_volatility_8w"] = downside_returns.groupby(result["symbol"]).transform(
        lambda values: values.rolling(8, min_periods=4).std()
    )

    for column, output_prefix in (("amount", "amount"), ("volume", "volume")):
        if column not in result.columns:
            continue
        numeric_series = pd.to_numeric(result[column], errors="coerce")
        result[f"{output_prefix}_change_4w"] = numeric_series.groupby(result["symbol"]).transform(
            lambda values: values.pct_change(periods=4)
        )
    if "amount" in result.columns:
        amount_series = pd.to_numeric(result["amount"], errors="coerce")
        amount_mean = amount_series.groupby(result["symbol"]).transform(lambda values: values.rolling(4, min_periods=2).mean())
        amount_std = amount_series.groupby(result["symbol"]).transform(lambda values: values.rolling(4, min_periods=2).std())
        result["amount_zscore_4w"] = (amount_series - amount_mean) / amount_std.replace(0.0, np.nan)

    for column in ("pe_ttm", "pb", "ps_ttm", "pe_ttm_pct_1250d"):
        if column not in result.columns:
            continue
        result[f"{column}_delta_4w"] = pd.to_numeric(result[column], errors="coerce").groupby(result["symbol"]).transform(
            lambda values: values.diff(4)
        )

    for column in ("f_score", "roa_ttm", "current_ratio"):
        if column not in result.columns:
            continue
        result[f"{column}_delta_8w"] = pd.to_numeric(result[column], errors="coerce").groupby(result["symbol"]).transform(
            lambda values: values.diff(8)
        )

    return _replace_numeric_infinities(result)


def engineer_cross_sectional_research_features(panel: pd.DataFrame) -> pd.DataFrame:
    """
    Add universe-scoped research features on top of a symbol-local enriched panel.
    """
    if panel.empty:
        return panel.copy()

    result = panel.copy()
    if detect_panel_enrichment_scope(result) == "none":
        result = engineer_symbol_local_features(result)

    def _cross_rank(values: pd.Series) -> pd.Series:
        numeric = pd.to_numeric(values, errors="coerce")
        if numeric.notna().sum() <= 1:
            return pd.Series(np.nan, index=values.index)
        return numeric.rank(pct=True)

    cross_section_time = "datetime" if "datetime" in result.columns else "time"
    if {cross_section_time, "l1_name", "mom_4w"}.issubset(result.columns):
        result["industry_mom_4w_rank_pct"] = result.groupby([cross_section_time, "l1_name"])["mom_4w"].transform(_cross_rank)
    if {cross_section_time, "l1_name", "pe_ttm_pct_1250d"}.issubset(result.columns):
        result["industry_pe_ttm_pct_rank_pct"] = result.groupby([cross_section_time, "l1_name"])["pe_ttm_pct_1250d"].transform(_cross_rank)
    if {cross_section_time, "l1_name", "f_score"}.issubset(result.columns):
        result["industry_f_score_rank_pct"] = result.groupby([cross_section_time, "l1_name"])["f_score"].transform(_cross_rank)

    if {cross_section_time, "npm_ttm"}.issubset(result.columns):
        l1_rank = None
        l3_rank = None
        l3_counts = None
        if "l1_name" in result.columns:
            l1_rank = result.groupby([cross_section_time, "l1_name"])["npm_ttm"].transform(_cross_rank)
        if "l3_name" in result.columns:
            l3_rank = result.groupby([cross_section_time, "l3_name"])["npm_ttm"].transform(_cross_rank)
            l3_counts = result.groupby([cross_section_time, "l3_name"])["npm_ttm"].transform("count")

        if l1_rank is not None and l3_rank is not None and l3_counts is not None:
            use_l3 = l3_rank.notna() & (l3_counts >= 5)
            result["industry_npm_ttm_rank_pct"] = l1_rank.where(~use_l3, l3_rank)
        elif l3_rank is not None:
            result["industry_npm_ttm_rank_pct"] = l3_rank
        elif l1_rank is not None:
            result["industry_npm_ttm_rank_pct"] = l1_rank

    stable_flag = pd.to_numeric(
        result["buffett_npm_stable_flag"] if "buffett_npm_stable_flag" in result.columns else pd.Series(np.nan, index=result.index),
        errors="coerce",
    )
    industry_npm_rank = pd.to_numeric(
        result["industry_npm_ttm_rank_pct"] if "industry_npm_ttm_rank_pct" in result.columns else pd.Series(np.nan, index=result.index),
        errors="coerce",
    )
    if stable_flag.notna().any() or industry_npm_rank.notna().any():
        valid_npm_flag = stable_flag.notna() & industry_npm_rank.notna()
        result["buffett_npm_flag"] = np.where(
            valid_npm_flag,
            ((stable_flag >= 1) & (industry_npm_rank >= 0.7)).astype(float),
            np.nan,
        )

    moat_columns = [
        column for column in (
            "buffett_gpm_flag",
            "buffett_npm_flag",
            "buffett_roa_flag",
            "buffett_cashflow_flag",
        )
        if column in result.columns
    ]
    if moat_columns:
        moat_frame = result[moat_columns].apply(pd.to_numeric, errors="coerce")
        result["buffett_moat_coverage"] = moat_frame.notna().sum(axis=1)
        moat_mean = moat_frame.mean(axis=1, skipna=True)
        result["buffett_moat_score"] = moat_mean * 4.0
        result["buffett_moat_pass"] = np.where(
            moat_mean.notna(),
            (moat_mean >= 0.75).astype(float),
            np.nan,
        )

    interaction_specs = (
        ("macro_phase_reflation", "mom_4w", "macro_reflation_x_mom_4w"),
        ("macro_phase_recovery", "mom_8w", "macro_recovery_x_mom_8w"),
        ("macro_phase_overheat", "volatility_8w", "macro_overheat_x_volatility_8w"),
        ("macro_phase_stagflation", "f_score", "macro_stagflation_x_f_score"),
        ("macro_industry_match", "mom_4w", "macro_industry_match_x_mom_4w"),
    )
    for phase_column, factor_column, output_column in interaction_specs:
        if phase_column not in result.columns or factor_column not in result.columns:
            continue
        phase_values = pd.to_numeric(result[phase_column], errors="coerce").fillna(0.0)
        factor_values = pd.to_numeric(result[factor_column], errors="coerce")
        result[output_column] = phase_values * factor_values

    return _replace_numeric_infinities(result)


def ensure_panel_enrichment(
    panel: pd.DataFrame,
    target_scope: PanelEnrichmentScope | str,
) -> pd.DataFrame:
    resolved_scope = resolve_panel_enrichment_scope(enrichment_scope=target_scope)
    if resolved_scope == "none" or panel.empty:
        result = panel.copy()
        result.attrs["enrichment_scope"] = detect_panel_enrichment_scope(result)
        return result

    current_scope = detect_panel_enrichment_scope(panel)
    result = panel.copy()
    if _ENRICHMENT_SCOPE_ORDER[current_scope] < _ENRICHMENT_SCOPE_ORDER["symbol_local"]:
        result = engineer_symbol_local_features(result)
        current_scope = "symbol_local"
    if _ENRICHMENT_SCOPE_ORDER[current_scope] < _ENRICHMENT_SCOPE_ORDER[resolved_scope]:
        result = engineer_cross_sectional_research_features(result)
    result.attrs["enrichment_scope"] = detect_panel_enrichment_scope(result)
    return result


def engineer_research_features(panel: pd.DataFrame) -> pd.DataFrame:
    """
    Backward-compatible wrapper that materializes the full research feature layer.
    """
    return ensure_panel_enrichment(panel, "research_full")


def _filter_panel_by_universe_profile(
    panel: pd.DataFrame,
    universe_profile: str | None,
) -> pd.DataFrame:
    if panel.empty or not universe_profile:
        return panel
    if universe_profile == "merged_csi300_500" and {"in_csi300", "in_csi500"}.issubset(panel.columns):
        return panel.loc[panel["in_csi300"].fillna(False) | panel["in_csi500"].fillna(False)].copy()
    if universe_profile == "csi300" and "in_csi300" in panel.columns:
        return panel.loc[panel["in_csi300"].fillna(False)].copy()
    if universe_profile == "csi500" and "in_csi500" in panel.columns:
        return panel.loc[panel["in_csi500"].fillna(False)].copy()
    return panel


async def build_weekly_feature_panel(
    symbols: Optional[Sequence[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    batch_size: int = 300,
    horizons: Sequence[int] = (4, 8),
    universe_profile: str | None = None,
    filter_to_universe_membership: bool = True,
    include_derived_features: bool = True,
    enrichment_scope: PanelEnrichmentScope | str | None = None,
) -> pd.DataFrame:
    """
    Build a weekly feature panel aligned to weekly processed prices.

    Daily valuation and quarterly quality data are merged backward by symbol/time
    so each weekly row only sees information available by that feature date.
    """
    resolved_scope = resolve_panel_enrichment_scope(
        include_derived_features=include_derived_features,
        enrichment_scope=enrichment_scope,
    )

    resolved_symbols, history_frames = await resolve_universe_symbols(
        universe_profile=universe_profile,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
    )
    symbols = resolved_symbols or symbols
    fdh = await get_fdh()
    panels = []

    macro_start_date = None
    if start_date:
        macro_start_date = str((pd.Timestamp(start_date) - pd.Timedelta(days=62)).date())

    macro_phase = _prepare_macro_phase_frame(
        await fdh.get_cn_macro_cycle_async(
            start_date=macro_start_date,
            end_date=end_date,
            phase_mode="stable",
        )
    )
    macro_industry = _prepare_macro_industry_frame(
        await fdh.get_cn_macro_cycle_industries_async(
            start_date=macro_start_date,
            end_date=end_date,
            preferred_only=False,
            phase_mode="stable",
        )
    )

    for batch in _batched(symbols, batch_size):
        weekly = _normalize_time_symbol_frame(
            await fdh.get_processed_weekly_async(
                symbols=batch,
                start_date=start_date,
                end_date=end_date,
            ),
            rename_map=WEEKLY_RENAME_MAP,
        )
        if weekly.empty:
            continue

        fundamentals = _normalize_time_symbol_frame(
            await fdh.get_fundamental_combined_async(
                symbols=batch,
                start_date=start_date,
                end_date=end_date,
                include_fscore=True,
            )
        )
        industry_valuation = _normalize_time_symbol_frame(
            await fdh.get_industry_valuation_async(
                symbols=batch,
                start_date=start_date,
                end_date=end_date,
                include_exempted=True,
            )
        )
        industry_valuation = _trim_industry_columns(industry_valuation)
        basic = await fdh.get_basic_async(symbols=batch)

        panel = _merge_asof_by_symbol(weekly, fundamentals)
        panel = _merge_asof_by_symbol(panel, industry_valuation)
        panel = _harmonize_feature_columns(panel)
        panel = _merge_asof_frame(
            panel,
            macro_phase,
            left_on="time",
            right_on="macro_effective_time",
        )
        if (
            not macro_industry.empty
            and {"macro_effective_time", "l3_name"}.issubset(panel.columns)
        ):
            panel = panel.merge(
                macro_industry,
                on=["macro_effective_time", "l3_name"],
                how="left",
            )
        panel = _attach_macro_phase_flags(panel)

        if basic is not None and not basic.empty:
            basic_frame = basic.copy()
            symbol_col = "symbol" if "symbol" in basic_frame.columns else "ts_code"
            if symbol_col != "symbol":
                basic_frame = basic_frame.rename(columns={symbol_col: "symbol"})
            static_columns = [col for col in ("symbol", "name", "market", "industry", "area", "list_status") if col in basic_frame.columns]
            panel = panel.merge(
                basic_frame[static_columns].drop_duplicates(subset=["symbol"]),
                on="symbol",
                how="left",
                suffixes=("", "_basic"),
            )

        panels.append(panel)

    if not panels:
        return pd.DataFrame()

    feature_panel = pd.concat(panels, ignore_index=True)
    if history_frames:
        feature_panel = attach_universe_flags(feature_panel, history_frames)
    feature_panel = _attach_future_labels(feature_panel, horizons=horizons)
    feature_panel = ensure_panel_enrichment(feature_panel, resolved_scope)
    if history_frames and filter_to_universe_membership:
        feature_panel = _filter_panel_by_universe_profile(feature_panel, universe_profile)
    feature_panel["instrument"] = feature_panel["symbol"]
    feature_panel["datetime"] = feature_panel["time"]
    feature_panel = feature_panel.sort_values(["instrument", "datetime"]).reset_index(drop=True)
    feature_panel.attrs["enrichment_scope"] = resolved_scope
    return feature_panel


def feature_panel_metadata_path(output_path: str | Path) -> Path:
    path = Path(output_path).expanduser().resolve()
    return path.with_name(f"{path.name}.metadata.json")


def write_feature_panel_metadata(
    output_path: str | Path,
    *,
    enrichment_scope: PanelEnrichmentScope,
) -> Path:
    metadata_path = feature_panel_metadata_path(output_path)
    metadata_path.write_text(
        json.dumps(
            {
                "panel_enrichment_scope": enrichment_scope,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return metadata_path


def load_feature_panel_enrichment_scope(
    output_path: str | Path,
    panel: pd.DataFrame | None = None,
) -> PanelEnrichmentScope:
    metadata_path = feature_panel_metadata_path(output_path)
    if metadata_path.exists():
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            if metadata.get("panel_enrichment_scope") is not None:
                return resolve_panel_enrichment_scope(enrichment_scope=metadata.get("panel_enrichment_scope"))
        except (json.JSONDecodeError, ValueError, TypeError):
            pass
    if panel is not None:
        return detect_panel_enrichment_scope(panel)
    return "none"


def write_feature_panel(
    frame: pd.DataFrame,
    output_path: str | Path,
    *,
    enrichment_scope: PanelEnrichmentScope | str | None = None,
) -> Path:
    """Persist a feature panel to csv or parquet based on output suffix."""
    path = Path(output_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)

    suffix = path.suffix.lower()
    if suffix == ".csv":
        frame.to_csv(path, index=False)
    elif suffix in {".parquet", ".pq"}:
        frame.to_parquet(path, index=False)
    else:
        raise ValueError(f"Unsupported panel output format: {path.suffix}")
    resolved_scope = resolve_panel_enrichment_scope(
        enrichment_scope=enrichment_scope or frame.attrs.get("enrichment_scope", detect_panel_enrichment_scope(frame))
    )
    write_feature_panel_metadata(path, enrichment_scope=resolved_scope)
    return path


def _run_coroutine_sync(awaitable):
    """
    Run an async task from sync code, including notebook environments that
    already have an event loop running in the main thread.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(awaitable)

    future: concurrent.futures.Future = concurrent.futures.Future()

    def _runner() -> None:
        try:
            future.set_result(asyncio.run(awaitable))
        except BaseException as exc:  # pragma: no cover - re-raised via future.result
            future.set_exception(exc)

    thread = threading.Thread(target=_runner, name="weekly-feature-panel-export", daemon=True)
    thread.start()
    thread.join()
    return future.result()


def export_weekly_feature_panel(
    output_path: str | Path,
    symbols: Optional[Sequence[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    batch_size: int = 300,
    universe_profile: str | None = None,
    filter_to_universe_membership: bool = True,
    include_derived_features: bool = True,
    enrichment_scope: PanelEnrichmentScope | str | None = None,
) -> Path:
    """Synchronous wrapper for CLI usage."""
    resolved_scope = resolve_panel_enrichment_scope(
        include_derived_features=include_derived_features,
        enrichment_scope=enrichment_scope,
    )

    async def _build_and_close() -> pd.DataFrame:
        try:
            return await build_weekly_feature_panel(
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                batch_size=batch_size,
                universe_profile=universe_profile,
                filter_to_universe_membership=filter_to_universe_membership,
                include_derived_features=include_derived_features,
                enrichment_scope=resolved_scope,
            )
        finally:
            await close_fdh()

    panel = _run_coroutine_sync(_build_and_close())
    return write_feature_panel(panel, output_path, enrichment_scope=resolved_scope)
