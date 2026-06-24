"""Shared utility functions for breakout research."""

from __future__ import annotations

import numpy as np
import pandas as pd


def compute_atr(bars: pd.DataFrame, period: int = 14) -> pd.Series:
    """Compute Average True Range."""
    high = bars["high"]
    low = bars["low"]
    close = bars["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(window=period, min_periods=1).mean()


def compute_volume_ratio(bars: pd.DataFrame, period: int = 20) -> pd.Series:
    """Compute volume ratio relative to N-day average."""
    avg_vol = bars["volume"].rolling(window=period, min_periods=1).mean()
    return bars["volume"] / avg_vol.replace(0, np.nan)


def compute_ma(series: pd.Series, period: int) -> pd.Series:
    """Simple moving average."""
    return series.rolling(window=period, min_periods=1).mean()


def compute_ema(series: pd.Series, period: int) -> pd.Series:
    """Exponential moving average."""
    return series.ewm(span=period, adjust=False).mean()


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Relative Strength Index."""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def compute_macd(
    series: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """MACD indicator: returns (macd_line, signal_line, histogram)."""
    ema_fast = compute_ema(series, fast)
    ema_slow = compute_ema(series, slow)
    macd_line = ema_fast - ema_slow
    signal_line = compute_ema(macd_line, signal)
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def ensure_datetime_index(bars: pd.DataFrame) -> pd.DataFrame:
    """Ensure DataFrame has a DatetimeIndex named 'date'."""
    if not isinstance(bars.index, pd.DatetimeIndex):
        if "date" in bars.columns:
            bars = bars.set_index("date")
        bars.index = pd.to_datetime(bars.index)
    bars.index.name = "date"
    return bars
