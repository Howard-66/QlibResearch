"""
Unified index data access helpers.

Routing rules:
- China A-share index codes (e.g. 000300.SH/399001.SZ): prefer FinanceDataHub.
- Non-A-share index codes (e.g. US indices): keep the existing fetcher path.
- Any FinanceDataHub failure/empty result falls back to the existing fetcher path.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
import re
import threading
from typing import Any

import pandas as pd

from qlib_research.config import get_fdh
from qlib_research.core.fetcher import TushareFetcher

logger = logging.getLogger(__name__)

_CN_INDEX_CODE_PATTERN = re.compile(r"^\d{6}\.(SH|SZ)$", re.IGNORECASE)
_sync_fdh_client = None
_sync_fdh_lock = threading.Lock()


def is_cn_index_code(ts_code: str | None) -> bool:
    normalized = str(ts_code or "").strip().upper()
    return bool(_CN_INDEX_CODE_PATTERN.fullmatch(normalized))


def _normalize_ts_code(ts_code: str) -> str:
    return str(ts_code).strip().upper()


def _to_date_str(value: str | pd.Timestamp | None, fmt: str) -> str | None:
    if value is None:
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert("Asia/Shanghai").tz_localize(None)
    return timestamp.strftime(fmt)


def _to_fdh_date(value: str | pd.Timestamp | None) -> str | None:
    return _to_date_str(value, "%Y-%m-%d")


def _to_tushare_date(value: str | pd.Timestamp | None) -> str | None:
    return _to_date_str(value, "%Y%m%d")


def _normalize_trade_date_column(frame: pd.DataFrame, column: str = "trade_date") -> pd.DataFrame:
    if frame.empty or column not in frame.columns:
        return frame
    normalized = frame.copy()
    trade_dates = pd.to_datetime(normalized[column], errors="coerce")
    if trade_dates.dt.tz is not None:
        trade_dates = trade_dates.dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
    normalized[column] = trade_dates
    return normalized


def _standardize_index_daily_frame(data: pd.DataFrame | None) -> pd.DataFrame:
    if data is None or data.empty:
        return pd.DataFrame()
    frame = data.copy()
    if "trade_date" not in frame.columns and "date" in frame.columns:
        frame = frame.rename(columns={"date": "trade_date"})
    if "vol" in frame.columns and "volume" not in frame.columns:
        frame = frame.rename(columns={"vol": "volume"})
    if "volume" not in frame.columns:
        frame["volume"] = 0.0
    frame = _normalize_trade_date_column(frame, "trade_date")
    return frame.sort_values("trade_date").reset_index(drop=True)


def _standardize_index_valuation_frame(data: pd.DataFrame | None) -> pd.DataFrame:
    if data is None or data.empty:
        return pd.DataFrame()
    frame = data.copy()
    if "trade_date" not in frame.columns and "date" in frame.columns:
        frame = frame.rename(columns={"date": "trade_date"})
    if "pe_ttm" not in frame.columns and "pe" in frame.columns:
        frame["pe_ttm"] = frame["pe"]
    frame = _normalize_trade_date_column(frame, "trade_date")
    return frame.sort_values("trade_date").reset_index(drop=True)


def _run_sync_call(func, *args, **kwargs):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return func(*args, **kwargs)

    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="fdh-index-sync") as executor:
        future = executor.submit(lambda: func(*args, **kwargs))
        return future.result()


def _get_sync_fdh_client():
    global _sync_fdh_client
    if _sync_fdh_client is not None:
        return _sync_fdh_client

    with _sync_fdh_lock:
        if _sync_fdh_client is None:
            from finance_data_hub import FinanceDataHub
            from finance_data_hub.config import get_settings

            settings = get_settings()
            _sync_fdh_client = FinanceDataHub(
                settings=settings,
                backend="postgresql",
                router_config_path="sources.yml",
            )
    return _sync_fdh_client


def _should_try_fdh(
    ts_code: str,
    fetcher: Any | None,
    prefer_fdh: bool,
) -> bool:
    if not prefer_fdh or not is_cn_index_code(ts_code):
        return False
    if fetcher is None:
        return True
    return isinstance(fetcher, TushareFetcher)


def _fallback_fetch_index_daily(
    ts_code: str,
    start_date: str | pd.Timestamp | None,
    end_date: str | pd.Timestamp | None,
    fetcher: TushareFetcher | None,
) -> pd.DataFrame:
    resolved_fetcher = fetcher or TushareFetcher()
    fallback = resolved_fetcher.fetch_index_daily(
        ts_code,
        _to_tushare_date(start_date),
        _to_tushare_date(end_date),
    )
    return _standardize_index_daily_frame(fallback)


def _fallback_fetch_index_valuation(
    ts_code: str,
    start_date: str | pd.Timestamp | None,
    end_date: str | pd.Timestamp | None,
    fetcher: TushareFetcher | None,
) -> pd.DataFrame:
    resolved_fetcher = fetcher or TushareFetcher()
    fallback = resolved_fetcher.fetch_index_valuation(
        ts_code,
        _to_tushare_date(start_date),
        _to_tushare_date(end_date),
    )
    return _standardize_index_valuation_frame(fallback)


def fetch_index_daily_with_fallback(
    ts_code: str,
    start_date: str | pd.Timestamp | None,
    end_date: str | pd.Timestamp | None,
    fetcher: TushareFetcher | None = None,
    prefer_fdh: bool = True,
) -> pd.DataFrame:
    normalized_code = _normalize_ts_code(ts_code)
    if _should_try_fdh(normalized_code, fetcher, prefer_fdh):
        try:
            fdh = _get_sync_fdh_client()
            frame = _run_sync_call(
                fdh.get_index_daily,
                ts_code=normalized_code,
                start_date=_to_fdh_date(start_date),
                end_date=_to_fdh_date(end_date),
            )
            normalized = _standardize_index_daily_frame(frame)
            if not normalized.empty:
                return normalized
            logger.warning(
                "FDH index_daily returned empty for %s, falling back to existing fetcher",
                normalized_code,
            )
        except Exception as exc:
            logger.warning(
                "FDH index_daily failed for %s, falling back to existing fetcher: %s",
                normalized_code,
                exc,
            )
    return _fallback_fetch_index_daily(normalized_code, start_date, end_date, fetcher)


async def fetch_index_daily_with_fallback_async(
    ts_code: str,
    start_date: str | pd.Timestamp | None,
    end_date: str | pd.Timestamp | None,
    fetcher: TushareFetcher | None = None,
    prefer_fdh: bool = True,
) -> pd.DataFrame:
    normalized_code = _normalize_ts_code(ts_code)
    if _should_try_fdh(normalized_code, fetcher, prefer_fdh):
        try:
            fdh = await get_fdh()
            frame = await fdh.get_index_daily_async(
                ts_code=normalized_code,
                start_date=_to_fdh_date(start_date),
                end_date=_to_fdh_date(end_date),
            )
            normalized = _standardize_index_daily_frame(frame)
            if not normalized.empty:
                return normalized
            logger.warning(
                "FDH index_daily returned empty for %s, falling back to existing fetcher",
                normalized_code,
            )
        except Exception as exc:
            logger.warning(
                "FDH index_daily failed for %s, falling back to existing fetcher: %s",
                normalized_code,
                exc,
            )
    return _fallback_fetch_index_daily(normalized_code, start_date, end_date, fetcher)


def fetch_index_valuation_with_fallback(
    ts_code: str,
    start_date: str | pd.Timestamp | None,
    end_date: str | pd.Timestamp | None,
    fetcher: TushareFetcher | None = None,
    prefer_fdh: bool = True,
) -> pd.DataFrame:
    normalized_code = _normalize_ts_code(ts_code)
    if _should_try_fdh(normalized_code, fetcher, prefer_fdh):
        try:
            fdh = _get_sync_fdh_client()
            frame = _run_sync_call(
                fdh.get_index_dailybasic,
                ts_code=normalized_code,
                start_date=_to_fdh_date(start_date),
                end_date=_to_fdh_date(end_date),
            )
            normalized = _standardize_index_valuation_frame(frame)
            if not normalized.empty:
                return normalized
            logger.warning(
                "FDH index_dailybasic returned empty for %s, falling back to existing fetcher",
                normalized_code,
            )
        except Exception as exc:
            logger.warning(
                "FDH index_dailybasic failed for %s, falling back to existing fetcher: %s",
                normalized_code,
                exc,
            )
    return _fallback_fetch_index_valuation(normalized_code, start_date, end_date, fetcher)


async def fetch_index_valuation_with_fallback_async(
    ts_code: str,
    start_date: str | pd.Timestamp | None,
    end_date: str | pd.Timestamp | None,
    fetcher: TushareFetcher | None = None,
    prefer_fdh: bool = True,
) -> pd.DataFrame:
    normalized_code = _normalize_ts_code(ts_code)
    if _should_try_fdh(normalized_code, fetcher, prefer_fdh):
        try:
            fdh = await get_fdh()
            frame = await fdh.get_index_dailybasic_async(
                ts_code=normalized_code,
                start_date=_to_fdh_date(start_date),
                end_date=_to_fdh_date(end_date),
            )
            normalized = _standardize_index_valuation_frame(frame)
            if not normalized.empty:
                return normalized
            logger.warning(
                "FDH index_dailybasic returned empty for %s, falling back to existing fetcher",
                normalized_code,
            )
        except Exception as exc:
            logger.warning(
                "FDH index_dailybasic failed for %s, falling back to existing fetcher: %s",
                normalized_code,
                exc,
            )
    return _fallback_fetch_index_valuation(normalized_code, start_date, end_date, fetcher)

