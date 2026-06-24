"""Breakout event detection from historical bars."""

from __future__ import annotations

import numpy as np
import pandas as pd

from qlib_research.breakout.config import BreakoutEvent, DetectorConfig
from qlib_research.breakout.utils import compute_volume_ratio, ensure_datetime_index


def detect_breakouts(
    bars: pd.DataFrame,
    symbol: str,
    config: DetectorConfig | None = None,
) -> list[BreakoutEvent]:
    """Detect price breakout events from historical OHLCV bars.

    Args:
        bars: DataFrame with columns [open, high, low, close, volume] and a
            DatetimeIndex (or a ``date`` column convertible via
            :func:`ensure_datetime_index`).
        symbol: Stock symbol (e.g. ``"000001.SZ"``).
        config: Detection configuration. Uses defaults if ``None``.

    Returns:
        List of detected :class:`BreakoutEvent` objects in chronological order.

    Notes:
        Detection only uses information available up to and including day ``T``
        (no look-ahead bias). After a breakout is recorded, subsequent days are
        skipped for ``min_consolidation`` bars to avoid duplicate detections.
    """
    if config is None:
        config = DetectorConfig()

    if bars is None or len(bars) == 0:
        return []

    bars = ensure_datetime_index(bars).sort_index()

    required_cols = {"high", "close", "volume"}
    missing = required_cols - set(bars.columns)
    if missing:
        raise ValueError(f"bars missing required columns: {sorted(missing)}")

    n = len(bars)
    # Need at least one bar in the lookback window beyond the consolidation gap.
    if n <= config.min_consolidation:
        return []

    high = bars["high"].to_numpy(dtype=float)
    close = bars["close"].to_numpy(dtype=float)
    vol_ratio = compute_volume_ratio(bars).to_numpy(dtype=float)
    index = bars.index

    events: list[BreakoutEvent] = []
    cooldown_until = -1  # Next index allowed to detect a breakout.

    for t in range(n):
        if t < cooldown_until:
            continue

        # Window for prev-high search: exclude the most recent
        # ``min_consolidation`` bars (and bar T itself).
        window_end = t - config.min_consolidation  # exclusive
        window_start = max(0, t - config.lookback)
        if window_end <= window_start:
            continue

        prev_high_window = high[window_start:window_end]
        prev_high = float(prev_high_window.max())
        prev_high_idx = window_start + int(np.argmax(prev_high_window))

        # Breakout: today's close exceeds the prior high.
        if not (close[t] > prev_high):
            continue

        # Volume filter: breakout-day volume ratio must clear the threshold.
        ratio = vol_ratio[t]
        if not np.isfinite(ratio) or ratio < config.volume_threshold:
            continue

        events.append(
            BreakoutEvent(
                symbol=symbol,
                date=index[t],
                frequency=config.frequency,
                breakout_price=float(close[t]),
                prev_high_price=prev_high,
                prev_high_date=index[prev_high_idx],
                volume_ratio=float(ratio),
            )
        )
        cooldown_until = t + config.min_consolidation + 1

    return events
