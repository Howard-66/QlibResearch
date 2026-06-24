"""Feature engineering for breakout events (80 features in 9 groups).

All features are computed using only data on or before ``event.date``; no
look-ahead bias. Each feature is wrapped in a try/except so that any single
failure produces ``np.nan`` rather than aborting the whole extraction.
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

from qlib_research.breakout.config import BreakoutEvent
from qlib_research.breakout.utils import (
    compute_atr,
    compute_ema,
    compute_ma,
    compute_macd,
    compute_rsi,
    compute_volume_ratio,
    ensure_datetime_index,
)

_EPS = 1e-10

# ---------------------------------------------------------------------------
# Feature group definitions
# ---------------------------------------------------------------------------

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

ALL_FEATURE_COLUMNS: list[str] = [
    name for group in FEATURE_GROUPS.values() for name in group
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _safe(fn, *args, **kwargs) -> float:
    """Run ``fn`` and convert any exception / non-finite value to NaN."""
    try:
        value = fn(*args, **kwargs)
        if value is None:
            return float("nan")
        value = float(value)
        if not math.isfinite(value):
            return float("nan")
        return value
    except Exception:
        return float("nan")


def _slice_until(bars: pd.DataFrame, event_date: pd.Timestamp) -> pd.DataFrame:
    """Return rows on or before ``event_date`` (strict, no look-ahead)."""
    event_date = pd.Timestamp(event_date)
    return bars.loc[bars.index <= event_date]


def _last(series: pd.Series) -> float:
    if series is None or len(series) == 0:
        return float("nan")
    val = series.iloc[-1]
    if pd.isna(val):
        return float("nan")
    return float(val)


def _pct_change_window(series: pd.Series, window: int) -> float:
    if len(series) <= window:
        return float("nan")
    start = float(series.iloc[-window - 1])
    end = float(series.iloc[-1])
    if start == 0 or not math.isfinite(start):
        return float("nan")
    return (end - start) / start


def _slope_pct(series: pd.Series, window: int) -> float:
    """Relative slope: (s[-1] - s[-window-1]) / s[-window-1]."""
    return _pct_change_window(series, window)


def _linreg_slope(values: np.ndarray) -> float:
    n = len(values)
    if n < 2:
        return float("nan")
    x = np.arange(n, dtype=float)
    y = values.astype(float)
    mask = np.isfinite(y)
    if mask.sum() < 2:
        return float("nan")
    x = x[mask]
    y = y[mask]
    x_mean = x.mean()
    y_mean = y.mean()
    denom = ((x - x_mean) ** 2).sum()
    if denom == 0:
        return float("nan")
    return float(((x - x_mean) * (y - y_mean)).sum() / denom)


# ---------------------------------------------------------------------------
# Feature group computation
# ---------------------------------------------------------------------------


def _breakout_strength(bars: pd.DataFrame, event: BreakoutEvent) -> dict[str, float]:
    out: dict[str, float] = {}
    last = bars.iloc[-1]
    prev_close = float(bars["close"].iloc[-2]) if len(bars) >= 2 else float("nan")
    high = float(last["high"])
    low = float(last["low"])
    open_ = float(last["open"])
    close = float(last["close"])
    rng = high - low

    out["bs_amplitude"] = _safe(lambda: (high - low) / low if low > 0 else float("nan"))
    out["bs_body_ratio"] = _safe(lambda: abs(close - open_) / (rng + _EPS))
    out["bs_upper_shadow"] = _safe(
        lambda: (high - max(open_, close)) / (rng + _EPS)
    )
    out["bs_return"] = _safe(
        lambda: (close - prev_close) / prev_close if prev_close > 0 else float("nan")
    )

    def _atr_ratio() -> float:
        atr = compute_atr(bars, period=14)
        atr_val = _last(atr)
        if not math.isfinite(atr_val) or atr_val <= 0:
            return float("nan")
        return (high - low) / atr_val

    out["bs_atr_ratio"] = _safe(_atr_ratio)

    def _vol_ratio() -> float:
        vr = compute_volume_ratio(bars, period=20)
        return _last(vr)

    out["bs_volume_ratio"] = _safe(_vol_ratio)
    out["bs_gap_up"] = _safe(
        lambda: max(0.0, open_ - prev_close) / prev_close
        if prev_close > 0
        else float("nan")
    )
    out["bs_close_position"] = _safe(lambda: (close - low) / (rng + _EPS))
    return out


def _prev_high_pattern(bars: pd.DataFrame, event: BreakoutEvent) -> dict[str, float]:
    out: dict[str, float] = {}
    prev_high_date = pd.Timestamp(event.prev_high_date)
    prev_high_price = float(event.prev_high_price)

    # Window between prev_high_date and event.date (inclusive of prev_high_date,
    # exclusive of the breakout day for the "consolidation" semantics).
    between = bars.loc[(bars.index >= prev_high_date) & (bars.index < bars.index[-1])]

    def _age_days() -> float:
        # Number of trading days between prev_high_date and event.date.
        idx = bars.index
        try:
            i_prev = idx.get_indexer([prev_high_date], method="nearest")[0]
        except Exception:
            return float("nan")
        return float(len(idx) - 1 - i_prev)

    out["ph_age_days"] = _safe(_age_days)

    def _height_pct() -> float:
        # (prev_high_price - lowest close in consolidation) / prev_high_price.
        seg = between["close"]
        if len(seg) == 0 or prev_high_price <= 0:
            return float("nan")
        return (prev_high_price - float(seg.min())) / prev_high_price

    out["ph_height_pct"] = _safe(_height_pct)

    def _ph_vol_ratio() -> float:
        # Volume ratio on the prev-high day relative to its trailing 20-day mean.
        if prev_high_date not in bars.index:
            # try nearest
            idx = bars.index.get_indexer([prev_high_date], method="nearest")
            if len(idx) == 0 or idx[0] < 0:
                return float("nan")
            pos = idx[0]
        else:
            pos = bars.index.get_loc(prev_high_date)
        if pos < 1:
            return float("nan")
        window = bars["volume"].iloc[max(0, pos - 20): pos]
        if len(window) == 0:
            return float("nan")
        mean_vol = float(window.mean())
        if mean_vol <= 0:
            return float("nan")
        return float(bars["volume"].iloc[pos]) / mean_vol

    out["ph_volume_ratio"] = _safe(_ph_vol_ratio)

    def _retracement() -> float:
        seg = between["low"]
        if len(seg) == 0 or prev_high_price <= 0:
            return float("nan")
        return (prev_high_price - float(seg.min())) / prev_high_price

    out["ph_retracement"] = _safe(_retracement)

    def _touch_count() -> float:
        seg = between["high"]
        if len(seg) == 0 or prev_high_price <= 0:
            return float("nan")
        upper = prev_high_price * 1.02
        lower = prev_high_price * 0.98
        return float(((seg >= lower) & (seg <= upper)).sum())

    out["ph_touch_count"] = _safe(_touch_count)

    out["ph_breakout_margin"] = _safe(
        lambda: (float(event.breakout_price) - prev_high_price) / prev_high_price
        if prev_high_price > 0
        else float("nan")
    )
    return out


def _consolidation(bars: pd.DataFrame, event: BreakoutEvent) -> dict[str, float]:
    out: dict[str, float] = {}
    close = bars["close"]
    high = bars["high"]
    low = bars["low"]
    volume = bars["volume"]
    returns = close.pct_change()

    for win in (5, 10, 20, 30):
        out[f"cs_volatility_{win}d"] = _safe(
            lambda w=win: float(returns.iloc[-w:].std(ddof=0))
            if len(returns) >= w
            else float("nan")
        )

    for win in (5, 10, 20):
        def _tight(w: int = win) -> float:
            if len(close) < w:
                return float("nan")
            seg_h = high.iloc[-w:].max()
            seg_l = low.iloc[-w:].min()
            mean_c = close.iloc[-w:].mean()
            if mean_c <= 0 or not math.isfinite(mean_c):
                return float("nan")
            return float((seg_h - seg_l) / mean_c)

        out[f"cs_tightness_{win}d"] = _safe(_tight)

    def _vol_trend() -> float:
        if len(volume) < 30:
            return float("nan")
        recent = float(volume.iloc[-10:].mean())
        prior = float(volume.iloc[-30:-10].mean())
        if prior <= 0:
            return float("nan")
        return recent / prior

    out["cs_volume_trend"] = _safe(_vol_trend)

    def _vol_shrink() -> float:
        if len(volume) < 30:
            return float("nan")
        recent_min = float(volume.iloc[-10:].min())
        prior_mean = float(volume.iloc[-30:-10].mean())
        if prior_mean <= 0:
            return float("nan")
        return recent_min / prior_mean

    out["cs_volume_shrink"] = _safe(_vol_shrink)

    def _ma_convergence() -> float:
        ma5 = _last(compute_ma(close, 5))
        ma10 = _last(compute_ma(close, 10))
        ma20 = _last(compute_ma(close, 20))
        if not all(math.isfinite(v) for v in (ma5, ma10, ma20)) or ma20 == 0:
            return float("nan")
        return float(np.std([ma5, ma10, ma20], ddof=0) / ma20)

    out["cs_ma_convergence"] = _safe(_ma_convergence)

    def _range_contraction() -> float:
        atr5 = _last(compute_atr(bars, period=5))
        atr20 = _last(compute_atr(bars, period=20))
        if not math.isfinite(atr5) or not math.isfinite(atr20) or atr20 <= 0:
            return float("nan")
        return atr5 / atr20

    out["cs_range_contraction"] = _safe(_range_contraction)

    def _flat_days() -> float:
        if len(returns) < 30:
            return float("nan")
        recent = returns.iloc[-30:].abs()
        flat = (recent < 0.01).sum()
        return float(flat / 30.0)

    out["cs_flat_days"] = _safe(_flat_days)
    return out


def _volume_price(bars: pd.DataFrame, event: BreakoutEvent) -> dict[str, float]:
    out: dict[str, float] = {}
    close = bars["close"]
    high = bars["high"]
    low = bars["low"]
    open_ = bars["open"]
    volume = bars["volume"]
    close_change = close.pct_change()
    volume_change = volume.pct_change()

    def _corr(win: int) -> float:
        if len(close_change) < win + 1:
            return float("nan")
        seg_p = close_change.iloc[-win:]
        seg_v = volume_change.iloc[-win:]
        if seg_p.std(ddof=0) == 0 or seg_v.std(ddof=0) == 0:
            return float("nan")
        return float(seg_p.corr(seg_v))

    out["vp_corr_5d"] = _safe(lambda: _corr(5))
    out["vp_corr_20d"] = _safe(lambda: _corr(20))

    def _obv_divergence() -> float:
        if len(close) < 6:
            return float("nan")
        sign = np.sign(close.diff().fillna(0.0))
        obv = (sign * volume).cumsum()
        price_slope = _linreg_slope(close.iloc[-5:].to_numpy())
        obv_slope = _linreg_slope(obv.iloc[-5:].to_numpy())
        if not math.isfinite(price_slope) or not math.isfinite(obv_slope):
            return float("nan")
        # Use sign-based divergence indicator: +1 same direction, -1 opposite.
        return float(np.sign(obv_slope) - np.sign(price_slope))

    out["vp_obv_divergence"] = _safe(_obv_divergence)

    def _mfi(period: int = 14) -> float:
        if len(close) < period + 1:
            return float("nan")
        typical = (high + low + close) / 3.0
        money_flow = typical * volume
        tp_diff = typical.diff()
        pos_flow = money_flow.where(tp_diff > 0, 0.0)
        neg_flow = money_flow.where(tp_diff < 0, 0.0)
        pos_sum = float(pos_flow.iloc[-period:].sum())
        neg_sum = float(neg_flow.iloc[-period:].sum())
        if neg_sum <= 0:
            if pos_sum <= 0:
                return float("nan")
            return 100.0
        ratio = pos_sum / neg_sum
        return 100.0 - 100.0 / (1.0 + ratio)

    out["vp_mfi"] = _safe(_mfi)

    def _vwap_ratio() -> float:
        if len(close) < 20:
            return float("nan")
        typical = (high + low + close) / 3.0
        seg_tp = typical.iloc[-20:]
        seg_vol = volume.iloc[-20:]
        total_vol = float(seg_vol.sum())
        if total_vol <= 0:
            return float("nan")
        vwap = float((seg_tp * seg_vol).sum() / total_vol)
        if vwap <= 0:
            return float("nan")
        return float(close.iloc[-1]) / vwap

    out["vp_vwap_ratio"] = _safe(_vwap_ratio)

    def _up_volume_ratio() -> float:
        if len(close) < 20:
            return float("nan")
        seg_close = close.iloc[-20:]
        seg_open = open_.iloc[-20:]
        seg_vol = volume.iloc[-20:]
        up_mask = seg_close > seg_open
        total = float(seg_vol.sum())
        if total <= 0:
            return float("nan")
        return float(seg_vol[up_mask].sum() / total)

    out["vp_up_volume_ratio"] = _safe(_up_volume_ratio)

    def _accumulation() -> float:
        if len(close) < 20:
            return float("nan")
        seg = bars.iloc[-20:]
        mid = (seg["high"] + seg["low"]) / 2.0
        lower_half_mask = seg["close"] < mid
        total = float(seg["volume"].sum())
        if total <= 0:
            return float("nan")
        return float(seg["volume"][lower_half_mask].sum() / total)

    out["vp_accumulation"] = _safe(_accumulation)

    def _vol_ma_ratio() -> float:
        if len(volume) < 20:
            return float("nan")
        ma5 = float(volume.iloc[-5:].mean())
        ma20 = float(volume.iloc[-20:].mean())
        if ma20 <= 0:
            return float("nan")
        return ma5 / ma20

    out["vp_volume_ma_ratio_5_20"] = _safe(_vol_ma_ratio)

    def _vol_std_20d() -> float:
        if len(volume) < 20:
            return float("nan")
        seg = volume.iloc[-20:]
        mean = float(seg.mean())
        if mean <= 0:
            return float("nan")
        return float(seg.std(ddof=0) / mean)

    out["vp_volume_std_20d"] = _safe(_vol_std_20d)
    return out


def _trend_momentum(bars: pd.DataFrame, event: BreakoutEvent) -> dict[str, float]:
    out: dict[str, float] = {}
    close = bars["close"]
    high = bars["high"]
    low = bars["low"]

    ma5 = compute_ma(close, 5)
    ma10 = compute_ma(close, 10)
    ma20 = compute_ma(close, 20)
    ma60 = compute_ma(close, 60)

    out["tm_ma5_slope"] = _safe(lambda: _slope_pct(ma5, 5))
    out["tm_ma10_slope"] = _safe(lambda: _slope_pct(ma10, 5))
    out["tm_ma20_slope"] = _safe(lambda: _slope_pct(ma20, 5))
    out["tm_ma60_slope"] = _safe(lambda: _slope_pct(ma60, 5))

    out["tm_rsi_6"] = _safe(lambda: _last(compute_rsi(close, period=6)))
    out["tm_rsi_14"] = _safe(lambda: _last(compute_rsi(close, period=14)))

    macd_line, signal_line, hist = compute_macd(close)
    out["tm_macd_hist"] = _safe(lambda: _last(hist))

    def _macd_cross_dist() -> float:
        diff = (macd_line - signal_line).dropna()
        if len(diff) < 2:
            return float("nan")
        sign = np.sign(diff.to_numpy())
        # Find last index where sign flipped (compared to previous bar).
        flips = np.where(np.diff(sign) != 0)[0]
        if len(flips) == 0:
            return float("nan")
        last_flip = flips[-1]  # index in diff where sign[i+1] != sign[i]
        days_since = len(diff) - 1 - (last_flip + 1)
        # Positive if currently above zero (golden cross), negative if below.
        current_sign = sign[-1]
        if current_sign == 0:
            current_sign = sign[-2] if len(sign) >= 2 else 1
        return float(current_sign * days_since)

    out["tm_macd_cross_dist"] = _safe(_macd_cross_dist)

    def _adx() -> float:
        if len(bars) < 16:
            return float("nan")
        up_move = high.diff()
        down_move = -low.diff()
        plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
        minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
        atr = compute_atr(bars, period=14)
        plus_di = 100.0 * plus_dm.rolling(14, min_periods=1).mean() / (atr + _EPS)
        minus_di = 100.0 * minus_dm.rolling(14, min_periods=1).mean() / (atr + _EPS)
        ratio = (plus_di - minus_di).abs() / (plus_di + minus_di + _EPS)
        return float(ratio.iloc[-14:].mean())

    out["tm_adx"] = _safe(_adx)

    def _arrangement() -> float:
        v5 = _last(ma5)
        v10 = _last(ma10)
        v20 = _last(ma20)
        v60 = _last(ma60)
        if not all(math.isfinite(v) for v in (v5, v10, v20, v60)):
            return float("nan")
        score = int(v5 > v10) + int(v10 > v20) + int(v20 > v60)
        return score / 3.0

    out["tm_ma_arrangement"] = _safe(_arrangement)
    return out


def _market_env(
    market_bars: pd.DataFrame | None, event: BreakoutEvent
) -> dict[str, float]:
    cols = FEATURE_GROUPS["market_env"]
    if market_bars is None or len(market_bars) == 0:
        return {c: float("nan") for c in cols}

    mb = ensure_datetime_index(market_bars.copy())
    mb = _slice_until(mb, event.date)
    out: dict[str, float] = {}
    if len(mb) == 0:
        return {c: float("nan") for c in cols}

    close = mb["close"]
    volume = mb["volume"] if "volume" in mb.columns else None

    out["me_market_return_5d"] = _safe(lambda: _pct_change_window(close, 5))
    out["me_market_return_20d"] = _safe(lambda: _pct_change_window(close, 20))
    # Market breadth requires constituent data not available here -> NaN.
    out["me_market_breadth"] = float("nan")

    def _vol_ratio() -> float:
        if volume is None or len(volume) < 20:
            return float("nan")
        mean_vol = float(volume.iloc[-20:].mean())
        if mean_vol <= 0:
            return float("nan")
        return float(volume.iloc[-1]) / mean_vol

    out["me_market_volume_ratio"] = _safe(_vol_ratio)

    def _volatility() -> float:
        rets = close.pct_change().iloc[-20:]
        if len(rets) < 5:
            return float("nan")
        return float(rets.std(ddof=0))

    out["me_market_volatility"] = _safe(_volatility)

    def _drawdown() -> float:
        if len(close) < 20:
            return float("nan")
        seg = close.iloc[-20:]
        peak = float(seg.max())
        if peak <= 0:
            return float("nan")
        return float((peak - float(close.iloc[-1])) / peak)

    out["me_market_drawdown"] = _safe(_drawdown)
    out["me_market_rsi"] = _safe(lambda: _last(compute_rsi(close, period=14)))

    def _trend() -> float:
        ma20 = _last(compute_ma(close, 20))
        if not math.isfinite(ma20):
            return float("nan")
        return 1.0 if float(close.iloc[-1]) > ma20 else 0.0

    out["me_market_trend"] = _safe(_trend)

    def _momentum() -> float:
        ma60 = _last(compute_ma(close, 60))
        if not math.isfinite(ma60) or ma60 <= 0:
            return float("nan")
        return float(close.iloc[-1]) / ma60 - 1.0

    out["me_market_momentum"] = _safe(_momentum)
    return out


def _price_momentum(bars: pd.DataFrame, event: BreakoutEvent) -> dict[str, float]:
    out: dict[str, float] = {}
    close = bars["close"]
    open_ = bars["open"]
    high = bars["high"]
    low = bars["low"]

    # "突破前" 收益: use close prior to breakout day as endpoint.
    # 5d return = close[-2] / close[-7] - 1, etc.
    def _return_pre(window: int) -> float:
        if len(close) < window + 2:
            return float("nan")
        end = float(close.iloc[-2])
        start = float(close.iloc[-window - 2])
        if start <= 0:
            return float("nan")
        return end / start - 1.0

    out["pm_return_5d"] = _safe(lambda: _return_pre(5))
    out["pm_return_10d"] = _safe(lambda: _return_pre(10))
    out["pm_return_20d"] = _safe(lambda: _return_pre(20))

    def _max_drawdown_20d() -> float:
        if len(close) < 20:
            return float("nan")
        seg = close.iloc[-20:]
        running_max = seg.cummax()
        dd = (running_max - seg) / running_max.replace(0, np.nan)
        return float(dd.max())

    out["pm_max_drawdown_20d"] = _safe(_max_drawdown_20d)

    def _gap_count_10d() -> float:
        if len(close) < 11:
            return float("nan")
        seg_open = open_.iloc[-10:]
        prev_close = close.shift(1).iloc[-10:]
        gap_up = (seg_open > prev_close * 1.01)
        gap_down = (seg_open < prev_close * 0.99)
        return float((gap_up | gap_down).sum())

    out["pm_gap_count_10d"] = _safe(_gap_count_10d)

    def _consecutive_up() -> float:
        diff = close.diff().fillna(0.0)
        count = 0
        for val in reversed(diff.to_numpy()):
            if val > 0:
                count += 1
            else:
                break
        return float(count)

    out["pm_consecutive_up"] = _safe(_consecutive_up)

    def _dist(window: int) -> float:
        ma = _last(compute_ma(close, window))
        if not math.isfinite(ma) or ma <= 0:
            return float("nan")
        return float(close.iloc[-1]) / ma - 1.0

    out["pm_distance_ma20"] = _safe(lambda: _dist(20))
    out["pm_distance_ma60"] = _safe(lambda: _dist(60))
    return out


def _fundamentals(
    fundamentals: dict[str, float] | None, event: BreakoutEvent
) -> dict[str, float]:
    cols = FEATURE_GROUPS["fundamentals"]
    if fundamentals is None:
        return {c: float("nan") for c in cols}

    # Map fundamentals keys: accept both the feature name (with "fn_" prefix)
    # and the bare key. ``market_cap`` is automatically log-transformed.
    out: dict[str, float] = {}
    aliases = {
        "fn_roe": ("fn_roe", "roe"),
        "fn_pe": ("fn_pe", "pe"),
        "fn_pb": ("fn_pb", "pb"),
        "fn_ps": ("fn_ps", "ps"),
        "fn_gross_margin": ("fn_gross_margin", "gross_margin"),
        "fn_net_margin": ("fn_net_margin", "net_margin"),
        "fn_debt_ratio": ("fn_debt_ratio", "debt_ratio"),
        "fn_revenue_growth": ("fn_revenue_growth", "revenue_growth"),
        "fn_profit_growth": ("fn_profit_growth", "profit_growth"),
        "fn_turnover_rate": ("fn_turnover_rate", "turnover_rate"),
        "fn_sector_return_5d": ("fn_sector_return_5d", "sector_return_5d"),
        "fn_sector_return_20d": ("fn_sector_return_20d", "sector_return_20d"),
        "fn_sector_breadth": ("fn_sector_breadth", "sector_breadth"),
        "fn_relative_strength": ("fn_relative_strength", "relative_strength"),
    }
    for feat, keys in aliases.items():
        value = float("nan")
        for k in keys:
            if k in fundamentals and fundamentals[k] is not None:
                try:
                    candidate = float(fundamentals[k])
                except Exception:
                    continue
                if math.isfinite(candidate):
                    value = candidate
                    break
        out[feat] = value

    # market cap (log transform).
    def _market_cap_log() -> float:
        for k in ("fn_market_cap_log", "market_cap_log"):
            if k in fundamentals and fundamentals[k] is not None:
                try:
                    return float(fundamentals[k])
                except Exception:
                    pass
        for k in ("market_cap", "fn_market_cap"):
            if k in fundamentals and fundamentals[k] is not None:
                try:
                    mc = float(fundamentals[k])
                    if mc > 0:
                        return math.log(mc)
                except Exception:
                    pass
        return float("nan")

    out["fn_market_cap_log"] = _safe(_market_cap_log)
    return out


def _interaction(features: dict[str, float]) -> dict[str, float]:
    out: dict[str, float] = {}

    def _mul(a: str, b: str) -> float:
        va = features.get(a, float("nan"))
        vb = features.get(b, float("nan"))
        if not (math.isfinite(va) and math.isfinite(vb)):
            return float("nan")
        return va * vb

    out["ix_amplitude_x_volume"] = _safe(lambda: _mul("bs_amplitude", "bs_volume_ratio"))

    def _tightness_x_age() -> float:
        tight = features.get("cs_tightness_20d", float("nan"))
        age = features.get("ph_age_days", float("nan"))
        if not (math.isfinite(tight) and math.isfinite(age)):
            return float("nan")
        # Normalize age to [0, 1] using a reference horizon of 120 trading days.
        age_norm = max(0.0, min(1.0, age / 120.0))
        return tight * age_norm

    out["ix_tightness_x_age"] = _safe(_tightness_x_age)
    out["ix_gap_x_consecutive"] = _safe(
        lambda: _mul("bs_gap_up", "pm_consecutive_up")
    )
    return out


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def extract_features(
    event: BreakoutEvent,
    bars: pd.DataFrame,
    market_bars: pd.DataFrame | None = None,
    fundamentals: dict[str, float] | None = None,
) -> dict[str, float]:
    """Extract all 80 features for a single breakout event.

    Args:
        event: The breakout event to compute features for.
        bars: OHLCV bars for the event's symbol. Should contain at least
            ~250 trailing days; missing history yields NaN for those features.
        market_bars: Market index OHLCV bars (e.g. CSI300) for the market env
            group. If ``None`` the market_env features are all NaN.
        fundamentals: Optional dict of fundamental data. If ``None`` the
            fundamentals features are all NaN.

    Returns:
        Mapping of ``feature_name`` -> ``float``. Missing values are NaN.
    """
    nan_features = {c: float("nan") for c in ALL_FEATURE_COLUMNS}

    if bars is None or len(bars) == 0:
        return nan_features

    try:
        df = ensure_datetime_index(bars.copy())
        df = _slice_until(df, event.date)
    except Exception:
        return nan_features

    if len(df) == 0:
        return nan_features

    features: dict[str, float] = {}
    try:
        features.update(_breakout_strength(df, event))
    except Exception:
        for c in FEATURE_GROUPS["breakout_strength"]:
            features.setdefault(c, float("nan"))

    try:
        features.update(_prev_high_pattern(df, event))
    except Exception:
        for c in FEATURE_GROUPS["prev_high_pattern"]:
            features.setdefault(c, float("nan"))

    try:
        features.update(_consolidation(df, event))
    except Exception:
        for c in FEATURE_GROUPS["consolidation"]:
            features.setdefault(c, float("nan"))

    try:
        features.update(_volume_price(df, event))
    except Exception:
        for c in FEATURE_GROUPS["volume_price"]:
            features.setdefault(c, float("nan"))

    try:
        features.update(_trend_momentum(df, event))
    except Exception:
        for c in FEATURE_GROUPS["trend_momentum"]:
            features.setdefault(c, float("nan"))

    try:
        features.update(_market_env(market_bars, event))
    except Exception:
        for c in FEATURE_GROUPS["market_env"]:
            features.setdefault(c, float("nan"))

    try:
        features.update(_price_momentum(df, event))
    except Exception:
        for c in FEATURE_GROUPS["price_momentum"]:
            features.setdefault(c, float("nan"))

    try:
        features.update(_fundamentals(fundamentals, event))
    except Exception:
        for c in FEATURE_GROUPS["fundamentals"]:
            features.setdefault(c, float("nan"))

    try:
        features.update(_interaction(features))
    except Exception:
        for c in FEATURE_GROUPS["interaction"]:
            features.setdefault(c, float("nan"))

    # Ensure every declared feature is present and is a finite float / NaN.
    result: dict[str, float] = {}
    for col in ALL_FEATURE_COLUMNS:
        val = features.get(col, float("nan"))
        try:
            fval = float(val)
        except Exception:
            fval = float("nan")
        if not math.isfinite(fval):
            fval = float("nan")
        result[col] = fval
    return result


def extract_features_batch(
    events: list[BreakoutEvent],
    bars_dict: dict[str, pd.DataFrame],
    market_bars: pd.DataFrame | None = None,
    fundamentals_dict: dict[str, dict[str, float]] | None = None,
) -> pd.DataFrame:
    """Extract features for multiple events.

    Returns:
        DataFrame with one row per event and ``ALL_FEATURE_COLUMNS`` as columns.
        The returned DataFrame's index is a simple ``RangeIndex``.
    """
    rows: list[dict[str, Any]] = []
    for event in events:
        bars = bars_dict.get(event.symbol)
        fundamentals = (
            fundamentals_dict.get(event.symbol) if fundamentals_dict else None
        )
        feats = extract_features(
            event=event,
            bars=bars if bars is not None else pd.DataFrame(),
            market_bars=market_bars,
            fundamentals=fundamentals,
        )
        rows.append(feats)

    if not rows:
        return pd.DataFrame(columns=ALL_FEATURE_COLUMNS)

    df = pd.DataFrame(rows, columns=ALL_FEATURE_COLUMNS)
    df.index = pd.RangeIndex(len(df))
    return df
