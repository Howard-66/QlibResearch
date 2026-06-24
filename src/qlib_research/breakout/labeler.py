"""Auto-labeling of breakout events based on future returns."""

from __future__ import annotations

import numpy as np
import pandas as pd

from qlib_research.breakout.config import BreakoutEvent, LabelConfig
from qlib_research.breakout.utils import ensure_datetime_index


_OUTPUT_COLUMNS: list[str] = [
    "symbol",
    "date",
    "frequency",
    "breakout_price",
    "prev_high_price",
    "prev_high_date",
    "volume_ratio",
    "future_return",
    "label",
    "horizon_close",
]


def label_events(
    events: list[BreakoutEvent],
    bars_dict: dict[str, pd.DataFrame],
    config: LabelConfig | None = None,
) -> pd.DataFrame:
    """Label breakout events with forward returns.

    Args:
        events: List of detected breakout events.
        bars_dict: Mapping of symbol -> OHLCV DataFrame indexed by date.
        config: Labeling configuration. Uses defaults if ``None``.

    Returns:
        DataFrame whose rows mirror the input events plus the labeling fields
        ``future_return``, ``label`` and ``horizon_close``. Events whose forward
        window extends past the available data have NaN labels.
    """
    if config is None:
        config = LabelConfig()

    if not events:
        return pd.DataFrame(columns=_OUTPUT_COLUMNS)

    # Pre-normalize bars per symbol for repeated lookups.
    normalized: dict[str, pd.DataFrame] = {}
    for sym, bars in bars_dict.items():
        if bars is None or len(bars) == 0:
            continue
        normalized[sym] = ensure_datetime_index(bars).sort_index()

    rows: list[dict] = []
    for event in events:
        bars = normalized.get(event.symbol)
        if bars is None:
            continue

        # Locate the breakout bar. ``get_indexer`` tolerates missing dates.
        loc = bars.index.get_indexer([event.date])
        t = int(loc[0])
        if t < 0:
            continue

        breakout_high = float(bars["high"].iloc[t])
        future_idx = t + config.horizon_days

        if future_idx >= len(bars) or breakout_high <= 0:
            future_return = float("nan")
            horizon_close = float("nan")
            label: float = float("nan")
        else:
            horizon_close = float(bars["close"].iloc[future_idx])
            future_return = (horizon_close - breakout_high) / breakout_high
            label = 1.0 if future_return >= config.success_threshold else 0.0

        rows.append(
            {
                "symbol": event.symbol,
                "date": event.date,
                "frequency": event.frequency,
                "breakout_price": event.breakout_price,
                "prev_high_price": event.prev_high_price,
                "prev_high_date": event.prev_high_date,
                "volume_ratio": event.volume_ratio,
                "future_return": future_return,
                "label": label,
                "horizon_close": horizon_close,
            }
        )

    if not rows:
        return pd.DataFrame(columns=_OUTPUT_COLUMNS)

    df = pd.DataFrame(rows, columns=_OUTPUT_COLUMNS)
    return df
