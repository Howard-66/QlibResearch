import asyncio

import pandas as pd

from qlib_research.core.index_data_provider import fetch_index_daily_with_fallback


def test_fetch_index_daily_with_fallback_uses_fresh_sync_fdh_client_per_call(monkeypatch):
    observed: list[dict[str, object]] = []

    class FakeFinanceDataHub:
        def __init__(self, **kwargs):
            self.record = {"init_kwargs": kwargs}
            observed.append(self.record)

        async def initialize(self):
            self.record["initialize_loop"] = id(asyncio.get_running_loop())

        async def get_index_daily_async(self, **kwargs):
            self.record["call_loop"] = id(asyncio.get_running_loop())
            self.record["call_kwargs"] = kwargs
            return pd.DataFrame(
                {
                    "trade_date": pd.to_datetime(["2026-01-09", "2026-01-16"]),
                    "close": [10.8, 11.5],
                    "open": [10.0, 11.0],
                    "high": [11.0, 12.0],
                    "low": [9.0, 10.0],
                    "volume": [100.0, 100.0],
                }
            )

        async def close(self):
            self.record["close_loop"] = id(asyncio.get_running_loop())

    monkeypatch.setattr("finance_data_hub.FinanceDataHub", FakeFinanceDataHub)
    monkeypatch.setattr("finance_data_hub.config.get_settings", lambda: object())

    first = fetch_index_daily_with_fallback("000300.SH", "2026-01-01", "2026-01-31", fetcher=None)
    second = fetch_index_daily_with_fallback("000300.SH", "2026-01-01", "2026-01-31", fetcher=None)

    assert not first.empty
    assert not second.empty
    assert len(observed) == 2
    assert observed[0]["initialize_loop"] == observed[0]["call_loop"] == observed[0]["close_loop"]
    assert observed[1]["initialize_loop"] == observed[1]["call_loop"] == observed[1]["close_loop"]
    assert observed[0]["initialize_loop"] != observed[1]["initialize_loop"]
