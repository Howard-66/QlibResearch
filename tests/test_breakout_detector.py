"""Tests for breakout detector and labeler - verify no lookahead bias."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from qlib_research.breakout.config import BreakoutEvent, DetectorConfig, LabelConfig
from qlib_research.breakout.detector import detect_breakouts
from qlib_research.breakout.labeler import label_events


def _make_bars(prices: list[float], volumes: list[float] | None = None) -> pd.DataFrame:
    """Helper: create synthetic OHLCV bars."""
    n = len(prices)
    dates = pd.bdate_range("2025-01-01", periods=n)
    if volumes is None:
        volumes = [1_000_000.0] * n
    return pd.DataFrame(
        {
            "open": prices,
            "high": [p * 1.02 for p in prices],
            "low": [p * 0.98 for p in prices],
            "close": prices,
            "volume": volumes,
        },
        index=dates,
    )


class TestDetector:
    def test_simple_breakout(self):
        """A clear breakout after consolidation should be detected."""
        prices = [10.0] * 30 + [12.0]
        volumes = [1_000_000.0] * 30 + [3_000_000.0]
        bars = _make_bars(prices, volumes)
        events = detect_breakouts(
            bars, "TEST.SZ", DetectorConfig(lookback=30, min_consolidation=5)
        )
        assert len(events) >= 1
        assert events[0].symbol == "TEST.SZ"
        assert events[0].breakout_price > 10.0

    def test_no_breakout_without_consolidation(self):
        """A new high within min_consolidation days should not trigger."""
        prices = list(range(1, 20))
        bars = _make_bars([float(p) for p in prices])
        events = detect_breakouts(
            bars, "TEST.SZ", DetectorConfig(lookback=20, min_consolidation=10)
        )
        assert len(events) == 0

    def test_no_lookahead_in_detection(self):
        """Detection on day T must not use data from day T+1."""
        prices = [10.0] * 30 + [12.0, 8.0]
        volumes = [1_000_000.0] * 30 + [3_000_000.0, 3_000_000.0]
        bars = _make_bars(prices, volumes)
        events = detect_breakouts(
            bars, "TEST.SZ", DetectorConfig(lookback=30, min_consolidation=5)
        )
        assert any(e.date == bars.index[30] for e in events)


class TestLabeler:
    def test_positive_label(self):
        """Event with strong future return should be labeled 1."""
        prices = [10.0] * 30 + [12.0] + [13.0] * 20
        volumes = [1_000_000.0] * 51
        bars = _make_bars(prices, volumes)
        event = BreakoutEvent(
            symbol="TEST.SZ",
            date=bars.index[30],
            frequency="daily",
            breakout_price=12.0,
            prev_high_price=10.2,
            prev_high_date=bars.index[0],
            volume_ratio=3.0,
        )
        df = label_events([event], {"TEST.SZ": bars}, LabelConfig(horizon_days=13))
        assert df.iloc[0]["label"] == 1
        assert df.iloc[0]["future_return"] > 0

    def test_negative_label(self):
        """Event with negative future return should be labeled 0."""
        prices = [10.0] * 30 + [12.0] + [9.0] * 20
        volumes = [1_000_000.0] * 51
        bars = _make_bars(prices, volumes)
        event = BreakoutEvent(
            symbol="TEST.SZ",
            date=bars.index[30],
            frequency="daily",
            breakout_price=12.0,
            prev_high_price=10.2,
            prev_high_date=bars.index[0],
            volume_ratio=3.0,
        )
        df = label_events([event], {"TEST.SZ": bars}, LabelConfig(horizon_days=13))
        assert df.iloc[0]["label"] == 0
        assert df.iloc[0]["future_return"] < 0

    def test_insufficient_future_data(self):
        """Event near end of data should have NaN future_return."""
        prices = [10.0] * 30 + [12.0]
        volumes = [1_000_000.0] * 31
        bars = _make_bars(prices, volumes)
        event = BreakoutEvent(
            symbol="TEST.SZ",
            date=bars.index[30],
            frequency="daily",
            breakout_price=12.0,
            prev_high_price=10.2,
            prev_high_date=bars.index[0],
            volume_ratio=3.0,
        )
        df = label_events([event], {"TEST.SZ": bars}, LabelConfig(horizon_days=13))
        assert pd.isna(df.iloc[0]["future_return"])
