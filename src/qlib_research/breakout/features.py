"""Stock breakout feature engineering.

The feature set intentionally mirrors the strategy note: nine feature groups
and 80 numeric columns. Calculations use only event-date and prior rows.
Missing market/fundamental inputs are allowed and produce NaN values.
"""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

FEATURE_GROUPS: dict[str, list[str]] = {
    "breakout_strength": [
        "bs_amplitude",
        "bs_body_ratio",
        "bs_upper_shadow",
        "bs_return",
        "bs_atr_ratio",
        "bs_volume_ratio",
        "bs_gap_up",
        "bs_close_position",
    ],
    "prev_high_pattern": [
        "ph_age_days",
        "ph_height_pct",
        "ph_volume_ratio",
        "ph_retracement",
        "ph_touch_count",
        "ph_breakout_margin",
    ],
    "consolidation": [
        "cs_volatility_5d",
        "cs_volatility_10d",
        "cs_volatility_20d",
        "cs_volatility_30d",
        "cs_tightness_5d",
        "cs_tightness_10d",
        "cs_tightness_20d",
        "cs_volume_trend",
        "cs_volume_shrink",
        "cs_ma_convergence",
        "cs_range_contraction",
        "cs_flat_days",
    ],
    "volume_price": [
        "vp_corr_5d",
        "vp_corr_20d",
        "vp_obv_divergence",
        "vp_mfi",
        "vp_vwap_ratio",
        "vp_up_volume_ratio",
        "vp_accumulation",
        "vp_volume_ma_ratio_5_20",
        "vp_volume_std_20d",
    ],
    "trend_momentum": [
        "tm_ma5_slope",
        "tm_ma10_slope",
        "tm_ma20_slope",
        "tm_ma60_slope",
        "tm_rsi_6",
        "tm_rsi_14",
        "tm_macd_hist",
        "tm_macd_cross_dist",
        "tm_adx",
        "tm_ma_arrangement",
    ],
    "market_env": [
        "me_market_return_5d",
        "me_market_return_20d",
        "me_market_breadth",
        "me_market_volume_ratio",
        "me_market_volatility",
        "me_market_drawdown",
        "me_market_rsi",
        "me_market_trend",
        "me_market_momentum",
    ],
    "price_momentum": [
        "pm_return_5d",
        "pm_return_10d",
        "pm_return_20d",
        "pm_max_drawdown_20d",
        "pm_gap_count_10d",
        "pm_consecutive_up",
        "pm_distance_ma20",
        "pm_distance_ma60",
    ],
    "fundamentals": [
        "fn_roe",
        "fn_pe",
        "fn_pb",
        "fn_ps",
        "fn_gross_margin",
        "fn_net_margin",
        "fn_debt_ratio",
        "fn_revenue_growth",
        "fn_profit_growth",
        "fn_market_cap_log",
        "fn_turnover_rate",
        "fn_sector_return_5d",
        "fn_sector_return_20d",
        "fn_sector_breadth",
        "fn_relative_strength",
    ],
    "interaction": [
        "ix_amplitude_x_volume",
        "ix_tightness_x_age",
        "ix_gap_x_consecutive",
    ],
}

ALL_FEATURE_COLUMNS: list[str] = [name for group in FEATURE_GROUPS.values() for name in group]
LEGACY_FEATURE_COLUMNS = [
    "volatility_20d",
    "volume_ratio_20d",
    "range_20d",
    "return_5d",
    "return_10d",
    "return_20d",
    "ma_ratio_5d",
    "ma_ratio_10d",
    "ma_ratio_20d",
]

_EPS = 1e-12


def build_stock_breakout_features(
    price_frame: pd.DataFrame,
    events: pd.DataFrame,
    *,
    extra_feature_columns: Iterable[str] = (),
) -> pd.DataFrame:
    """Attach the stock breakout v1 feature set to an event frame."""

    if events.empty:
        output = events.copy()
        for column in ALL_FEATURE_COLUMNS:
            if column not in output.columns:
                output[column] = pd.Series(dtype=float)
        return output

    prices = _normalize_prices(price_frame)
    if prices.empty:
        result = events.copy()
        for column in ALL_FEATURE_COLUMNS:
            result[column] = np.nan
        return result

    market = _market_features(prices)
    feature_frames: list[pd.DataFrame] = []
    event_feature_rows: list[dict[str, object]] = []

    for code, group in prices.groupby("code", sort=True):
        group = group.sort_values("date").reset_index(drop=True).copy()
        daily = _symbol_daily_features(group)
        if not market.empty:
            daily = daily.merge(market, on="date", how="left")
        keep = ["code", "date", *ALL_FEATURE_COLUMNS, *LEGACY_FEATURE_COLUMNS]
        for column in extra_feature_columns:
            if column in group.columns and column not in keep:
                daily[column] = group[column]
                keep.append(column)
        feature_frames.append(daily[[column for column in keep if column in daily.columns]])

        code_events = events[events.get("code", "").astype(str).str.upper() == str(code).upper()]
        if not code_events.empty:
            event_feature_rows.extend(_event_pattern_features(group, code_events))

    daily_features = pd.concat(feature_frames, ignore_index=True) if feature_frames else pd.DataFrame()
    result = events.copy()
    result["_event_date"] = pd.to_datetime(result["event_date"], errors="coerce")
    result = result.merge(daily_features, left_on=["code", "_event_date"], right_on=["code", "date"], how="left").drop(columns=["date", "_event_date"])

    event_specific = pd.DataFrame(event_feature_rows)
    if not event_specific.empty:
        result = result.merge(event_specific, on="event_id", how="left", suffixes=("", "_event"))
        for column in FEATURE_GROUPS["prev_high_pattern"]:
            event_col = f"{column}_event"
            if event_col in result.columns:
                result[column] = result[event_col].combine_first(result[column] if column in result.columns else pd.Series(np.nan, index=result.index))
                result = result.drop(columns=[event_col])
        if "ix_tightness_x_age" in result.columns and {"cs_tightness_20d", "ph_age_days"}.issubset(result.columns):
            result["ix_tightness_x_age"] = pd.to_numeric(result["cs_tightness_20d"], errors="coerce") * pd.to_numeric(result["ph_age_days"], errors="coerce")

    for column in ALL_FEATURE_COLUMNS:
        if column not in result.columns:
            result[column] = np.nan
        result[column] = pd.to_numeric(result[column], errors="coerce")

    return result


def feature_group_for(column: str) -> str | None:
    for group, columns in FEATURE_GROUPS.items():
        if column in columns:
            return group
    return None


def _normalize_prices(price_frame: pd.DataFrame) -> pd.DataFrame:
    if price_frame.empty:
        return pd.DataFrame(columns=["code", "date", "open", "high", "low", "close", "volume"])
    frame = price_frame.copy()
    rename_map = {
        "symbol": "code",
        "instrument": "code",
        "ts_code": "code",
        "datetime": "date",
        "time": "date",
        "trade_date": "date",
        "vol": "volume",
    }
    frame = frame.rename(columns={k: v for k, v in rename_map.items() if k in frame.columns and v not in frame.columns})
    for column in ("code", "date", "open", "high", "low", "close"):
        if column not in frame.columns:
            return pd.DataFrame(columns=["code", "date", "open", "high", "low", "close", "volume"])
    if "volume" not in frame.columns:
        frame["volume"] = np.nan
    frame["code"] = frame["code"].astype(str).str.strip().str.upper()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce", utc=True).dt.tz_localize(None).dt.normalize()
    numeric_candidates = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "turnover_rate",
        "circ_mv",
        "total_mv",
        "pe",
        "pb",
        "ps",
        "roe",
        "gross_margin",
        "net_margin",
        "debt_ratio",
        "revenue_growth",
        "profit_growth",
    ]
    for column in numeric_candidates:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.dropna(subset=["code", "date", "open", "high", "low", "close"]).sort_values(["code", "date"]).reset_index(drop=True)


def _symbol_daily_features(group: pd.DataFrame) -> pd.DataFrame:
    out = group[["code", "date"]].copy()
    open_ = group["open"].astype(float)
    high = group["high"].astype(float)
    low = group["low"].astype(float)
    close = group["close"].astype(float)
    volume = group["volume"].astype(float)
    prev_close = close.shift(1)
    ret = close.pct_change()
    day_range = (high - low).replace(0, np.nan)
    atr14 = _atr(high, low, close, 14)
    ma5 = close.rolling(5, min_periods=3).mean()
    ma10 = close.rolling(10, min_periods=5).mean()
    ma20 = close.rolling(20, min_periods=10).mean()
    ma60 = close.rolling(60, min_periods=20).mean()
    volume_ma5 = volume.rolling(5, min_periods=3).mean()
    volume_ma20 = volume.rolling(20, min_periods=10).mean()
    rolling_high20 = high.rolling(20, min_periods=5).max()
    rolling_low20 = low.rolling(20, min_periods=5).min()

    out["bs_amplitude"] = (high - low) / low.replace(0, np.nan)
    out["bs_body_ratio"] = (close - open_).abs() / (day_range + _EPS)
    out["bs_upper_shadow"] = (high - pd.concat([open_, close], axis=1).max(axis=1)) / (day_range + _EPS)
    out["bs_return"] = close / prev_close.replace(0, np.nan) - 1.0
    out["bs_atr_ratio"] = (high - low) / atr14.replace(0, np.nan)
    out["bs_volume_ratio"] = volume / volume_ma20.replace(0, np.nan)
    out["bs_gap_up"] = (open_ - prev_close).clip(lower=0) / prev_close.replace(0, np.nan)
    out["bs_close_position"] = (close - low) / (day_range + _EPS)

    # Event-specific previous-high fields are filled after merging. These
    # placeholders keep the schema stable for non-event browsing.
    for column in FEATURE_GROUPS["prev_high_pattern"]:
        out[column] = np.nan

    for window in (5, 10, 20, 30):
        out[f"cs_volatility_{window}d"] = ret.rolling(window, min_periods=max(3, window // 2)).std()
    for window in (5, 10, 20):
        rolling_high = high.rolling(window, min_periods=max(3, window // 2)).max()
        rolling_low = low.rolling(window, min_periods=max(3, window // 2)).min()
        out[f"cs_tightness_{window}d"] = (rolling_high - rolling_low) / close.replace(0, np.nan)
    out["cs_volume_trend"] = volume_ma5 / volume_ma20.replace(0, np.nan) - 1.0
    out["cs_volume_shrink"] = 1.0 - volume_ma5 / volume_ma20.replace(0, np.nan)
    out["cs_ma_convergence"] = pd.concat([ma5, ma10, ma20], axis=1).std(axis=1) / close.replace(0, np.nan)
    out["cs_range_contraction"] = out["cs_tightness_5d"] / out["cs_tightness_20d"].replace(0, np.nan)
    out["cs_flat_days"] = (ret.abs() < 0.01).rolling(20, min_periods=5).sum()

    out["vp_corr_5d"] = close.rolling(5, min_periods=3).corr(volume)
    out["vp_corr_20d"] = close.rolling(20, min_periods=10).corr(volume)
    obv = (np.sign(ret.fillna(0)) * volume.fillna(0)).cumsum()
    out["vp_obv_divergence"] = (obv / obv.rolling(20, min_periods=5).mean().replace(0, np.nan) - 1.0) - (close / ma20.replace(0, np.nan) - 1.0)
    out["vp_mfi"] = _mfi(high, low, close, volume, 14)
    vwap20 = ((close * volume).rolling(20, min_periods=5).sum() / volume.rolling(20, min_periods=5).sum().replace(0, np.nan))
    out["vp_vwap_ratio"] = close / vwap20.replace(0, np.nan) - 1.0
    up_volume = volume.where(ret > 0, 0.0).rolling(20, min_periods=5).sum()
    down_volume = volume.where(ret < 0, 0.0).rolling(20, min_periods=5).sum()
    out["vp_up_volume_ratio"] = up_volume / down_volume.replace(0, np.nan)
    out["vp_accumulation"] = ((close - low) - (high - close)) / (day_range + _EPS) * volume
    out["vp_accumulation"] = out["vp_accumulation"].rolling(20, min_periods=5).sum() / volume.rolling(20, min_periods=5).sum().replace(0, np.nan)
    out["vp_volume_ma_ratio_5_20"] = volume_ma5 / volume_ma20.replace(0, np.nan)
    out["vp_volume_std_20d"] = volume.rolling(20, min_periods=5).std() / volume_ma20.replace(0, np.nan)

    for window, ma in ((5, ma5), (10, ma10), (20, ma20), (60, ma60)):
        out[f"tm_ma{window}_slope"] = ma / ma.shift(window).replace(0, np.nan) - 1.0
    out["tm_rsi_6"] = _rsi(close, 6)
    out["tm_rsi_14"] = _rsi(close, 14)
    macd, macd_signal, macd_hist = _macd(close)
    out["tm_macd_hist"] = macd_hist
    out["tm_macd_cross_dist"] = macd - macd_signal
    out["tm_adx"] = _adx(high, low, close, 14)
    out["tm_ma_arrangement"] = ((ma5 > ma10).astype(float) + (ma10 > ma20).astype(float) + (ma20 > ma60).astype(float)) / 3.0

    for window in (5, 10, 20):
        out[f"pm_return_{window}d"] = close / close.shift(window).replace(0, np.nan) - 1.0
    out["pm_max_drawdown_20d"] = close / close.rolling(20, min_periods=5).max().replace(0, np.nan) - 1.0
    out["pm_gap_count_10d"] = ((open_ / prev_close.replace(0, np.nan) - 1.0).abs() > 0.02).rolling(10, min_periods=3).sum()
    out["pm_consecutive_up"] = _consecutive_true(ret > 0)
    out["pm_distance_ma20"] = close / ma20.replace(0, np.nan) - 1.0
    out["pm_distance_ma60"] = close / ma60.replace(0, np.nan) - 1.0

    out["fn_roe"] = _optional(group, "roe")
    out["fn_pe"] = _optional(group, "pe")
    out["fn_pb"] = _optional(group, "pb")
    out["fn_ps"] = _optional(group, "ps")
    out["fn_gross_margin"] = _optional(group, "gross_margin")
    out["fn_net_margin"] = _optional(group, "net_margin")
    out["fn_debt_ratio"] = _optional(group, "debt_ratio")
    out["fn_revenue_growth"] = _optional(group, "revenue_growth")
    out["fn_profit_growth"] = _optional(group, "profit_growth")
    market_cap = _optional(group, "circ_mv").combine_first(_optional(group, "total_mv"))
    out["fn_market_cap_log"] = np.log(market_cap.where(market_cap > 0))
    out["fn_turnover_rate"] = _optional(group, "turnover_rate")
    # Sector fields are optional for v1; use market-relative placeholders when
    # no sector source exists.
    out["fn_sector_return_5d"] = np.nan
    out["fn_sector_return_20d"] = np.nan
    out["fn_sector_breadth"] = np.nan
    out["fn_relative_strength"] = out["pm_return_20d"]

    out["ix_amplitude_x_volume"] = out["bs_amplitude"] * out["bs_volume_ratio"]
    out["ix_tightness_x_age"] = out["cs_tightness_20d"] * out["ph_age_days"]
    out["ix_gap_x_consecutive"] = out["bs_gap_up"] * out["pm_consecutive_up"]

    # Backward-compatible v1 aliases used by existing docs/tests.
    out["volatility_20d"] = out["cs_volatility_20d"]
    out["volume_ratio_20d"] = out["bs_volume_ratio"]
    out["range_20d"] = out["cs_tightness_20d"]
    out["return_5d"] = out["pm_return_5d"]
    out["return_10d"] = out["pm_return_10d"]
    out["return_20d"] = out["pm_return_20d"]
    out["ma_ratio_5d"] = close / ma5.replace(0, np.nan) - 1.0
    out["ma_ratio_10d"] = close / ma10.replace(0, np.nan) - 1.0
    out["ma_ratio_20d"] = close / ma20.replace(0, np.nan) - 1.0
    return out


def _event_pattern_features(group: pd.DataFrame, events: pd.DataFrame) -> list[dict[str, object]]:
    group = group.sort_values("date").reset_index(drop=True)
    dates = pd.to_datetime(group["date"])
    high = group["high"].astype(float)
    low = group["low"].astype(float)
    close = group["close"].astype(float)
    volume = group["volume"].astype(float)
    rows: list[dict[str, object]] = []
    for _, event in events.iterrows():
        event_date = pd.to_datetime(event.get("event_date"), errors="coerce")
        if pd.isna(event_date):
            continue
        pos_candidates = np.flatnonzero(dates.to_numpy(dtype="datetime64[ns]") <= np.datetime64(event_date))
        if len(pos_candidates) == 0:
            continue
        pos = int(pos_candidates[-1])
        history = group.iloc[: pos + 1]
        ref_high = _to_float(event.get("ref_high"))
        ref_low = _to_float(event.get("ref_low"))
        if not np.isfinite(ref_high) or ref_high <= 0:
            ref_high = float(history["high"].iloc[:-1].max()) if len(history) > 1 else np.nan
        lookback = history.iloc[max(0, len(history) - 80) : max(0, len(history) - 1)]
        prev_high_idx = lookback["high"].idxmax() if not lookback.empty and lookback["high"].notna().any() else None
        prev_high_date = group.loc[prev_high_idx, "date"] if prev_high_idx is not None else pd.NaT
        age_days = (event_date - pd.Timestamp(prev_high_date)).days if pd.notna(prev_high_date) else np.nan
        prev_high_volume = float(group.loc[prev_high_idx, "volume"]) if prev_high_idx is not None else np.nan
        current_close = float(close.iloc[pos])
        current_volume = float(volume.iloc[pos])
        retracement = (ref_high - ref_low) / ref_high if np.isfinite(ref_high) and np.isfinite(ref_low) and ref_high else np.nan
        touches = ((lookback["high"] >= ref_high * 0.98) & (lookback["high"] <= ref_high * 1.01)).sum() if np.isfinite(ref_high) and not lookback.empty else np.nan
        rows.append(
            {
                "event_id": event.get("event_id"),
                "ph_age_days": age_days,
                "ph_height_pct": current_close / ref_high - 1.0 if np.isfinite(ref_high) and ref_high else np.nan,
                "ph_volume_ratio": prev_high_volume / lookback["volume"].mean() if not lookback.empty and lookback["volume"].mean() else np.nan,
                "ph_retracement": retracement,
                "ph_touch_count": float(touches) if np.isfinite(touches) else np.nan,
                "ph_breakout_margin": current_close / ref_high - 1.0 if np.isfinite(ref_high) and ref_high else np.nan,
            }
        )
        if pd.isna(rows[-1]["ph_volume_ratio"]) and np.isfinite(current_volume):
            volume_base = history["volume"].rolling(20, min_periods=5).mean().iloc[-1]
            rows[-1]["ph_volume_ratio"] = current_volume / volume_base if volume_base else np.nan
    return rows


def _market_features(prices: pd.DataFrame) -> pd.DataFrame:
    frame = prices[["date", "code", "close", "volume"]].copy()
    frame["return"] = frame.sort_values(["code", "date"]).groupby("code")["close"].pct_change()
    daily = frame.groupby("date", as_index=False).agg(
        market_return=("return", "mean"),
        breadth=("return", lambda s: float((s > 0).mean()) if len(s.dropna()) else np.nan),
        market_volume=("volume", "sum"),
    )
    daily["market_index"] = (1.0 + daily["market_return"].fillna(0.0)).cumprod()
    daily["me_market_return_5d"] = daily["market_index"] / daily["market_index"].shift(5).replace(0, np.nan) - 1.0
    daily["me_market_return_20d"] = daily["market_index"] / daily["market_index"].shift(20).replace(0, np.nan) - 1.0
    daily["me_market_breadth"] = daily["breadth"]
    daily["me_market_volume_ratio"] = daily["market_volume"] / daily["market_volume"].rolling(20, min_periods=5).mean().replace(0, np.nan)
    daily["me_market_volatility"] = daily["market_return"].rolling(20, min_periods=5).std()
    daily["me_market_drawdown"] = daily["market_index"] / daily["market_index"].rolling(60, min_periods=10).max().replace(0, np.nan) - 1.0
    daily["me_market_rsi"] = _rsi(daily["market_index"], 14)
    daily["me_market_trend"] = daily["market_index"] / daily["market_index"].rolling(20, min_periods=5).mean().replace(0, np.nan) - 1.0
    daily["me_market_momentum"] = daily["me_market_return_5d"] - daily["me_market_return_20d"]
    return daily[["date", *FEATURE_GROUPS["market_env"]]]


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=max(3, period // 2)).mean()


def _rsi(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(period, min_periods=max(3, period // 2)).mean()
    loss = (-delta.clip(upper=0)).rolling(period, min_periods=max(3, period // 2)).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100.0 - 100.0 / (1.0 + rs)


def _macd(close: pd.Series) -> tuple[pd.Series, pd.Series, pd.Series]:
    ema12 = close.ewm(span=12, adjust=False, min_periods=6).mean()
    ema26 = close.ewm(span=26, adjust=False, min_periods=13).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False, min_periods=5).mean()
    return macd, signal, macd - signal


def _adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
    atr = _atr(high, low, close, period).replace(0, np.nan)
    plus_di = 100 * plus_dm.rolling(period, min_periods=max(3, period // 2)).sum() / atr
    minus_di = 100 * minus_dm.rolling(period, min_periods=max(3, period // 2)).sum() / atr
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100
    return dx.rolling(period, min_periods=max(3, period // 2)).mean()


def _mfi(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series, period: int) -> pd.Series:
    typical = (high + low + close) / 3.0
    money_flow = typical * volume
    direction = typical.diff()
    positive = money_flow.where(direction > 0, 0.0).rolling(period, min_periods=max(3, period // 2)).sum()
    negative = money_flow.where(direction < 0, 0.0).rolling(period, min_periods=max(3, period // 2)).sum()
    ratio = positive / negative.replace(0, np.nan)
    return 100.0 - 100.0 / (1.0 + ratio)


def _consecutive_true(mask: pd.Series) -> pd.Series:
    values: list[int] = []
    current = 0
    for item in mask.fillna(False).astype(bool):
        current = current + 1 if item else 0
        values.append(current)
    return pd.Series(values, index=mask.index, dtype=float)


def _optional(group: pd.DataFrame, column: str) -> pd.Series:
    if column in group.columns:
        return pd.to_numeric(group[column], errors="coerce")
    return pd.Series(np.nan, index=group.index, dtype=float)


def _to_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")
