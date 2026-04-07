"""
Helpers for qlib-native portfolio analysis on top of repository-managed weekly panels.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from copy import deepcopy
from functools import partial
import io
from pathlib import Path
from typing import Any, Sequence
import warnings

import numpy as np
import pandas as pd

from qlib_research.core.portfolio import BacktestTradingConfig


NATIVE_SIGNAL_COLUMN = "score"


def _require_qlib():
    from qlib_research.core.qlib_pipeline import require_qlib

    return require_qlib()


_require_qlib()
with contextlib.redirect_stderr(io.StringIO()):
    from qlib.backtest.decision import Order  # type: ignore  # noqa: E402
    from qlib.backtest.exchange import Exchange  # type: ignore  # noqa: E402
    from qlib.backtest.high_performance_ds import PandasQuote  # type: ignore  # noqa: E402
    from qlib.contrib.strategy.order_generator import OrderGenWInteract  # type: ignore  # noqa: E402
    from qlib.contrib.strategy.signal_strategy import WeightStrategyBase  # type: ignore  # noqa: E402


def _ensure_datetime_series(values: Sequence[pd.Timestamp] | pd.Index | pd.Series) -> pd.DatetimeIndex:
    index = pd.DatetimeIndex(pd.to_datetime(values))
    return pd.DatetimeIndex(sorted(index.unique()))


def _native_freq_label(time_per_step: str) -> str:
    normalized = str(time_per_step).strip().lower()
    return normalized if normalized.startswith("1") else f"1{normalized}"


def _flat_benchmark_from_signal(signal_frame: pd.DataFrame) -> pd.Series:
    dates = _ensure_datetime_series(signal_frame.index.get_level_values("datetime"))
    if len(dates) >= 2:
        benchmark_index = dates[1:]
    else:
        benchmark_index = dates
    return pd.Series(0.0, index=benchmark_index)


def _coerce_signal_frame(signal_frame: pd.DataFrame | pd.Series, score_column: str = NATIVE_SIGNAL_COLUMN) -> pd.DataFrame:
    if isinstance(signal_frame, pd.Series):
        signal_frame = signal_frame.to_frame(score_column)
    frame = signal_frame.copy()
    if not isinstance(frame.index, pd.MultiIndex):
        raise ValueError("signal_frame must be indexed by MultiIndex(datetime, instrument)")
    if frame.index.nlevels != 2:
        raise ValueError("signal_frame index must have exactly two levels: datetime and instrument")
    if score_column not in frame.columns:
        first_column = frame.columns[0]
        frame = frame[[first_column]].rename(columns={first_column: score_column})
    frame.index = frame.index.set_names(["datetime", "instrument"])
    frame = frame.sort_index()
    frame[score_column] = pd.to_numeric(frame[score_column], errors="coerce")
    frame = frame.dropna(subset=[score_column])
    if frame.empty:
        raise ValueError("signal_frame has no valid score rows")
    ordered_columns = [score_column] + [column for column in frame.columns if column != score_column]
    return frame[ordered_columns]


def patch_qlib_resam_compat() -> None:
    """
    Patch qlib's resampling helpers to avoid pandas' deprecated fillna(method=...) path.

    qlib still calls Series.fillna(method="ffill"/"bfill") inside qlib.utils.resam.get_valid_value,
    which emits a FutureWarning on newer pandas. We patch the helper once per process so native
    backtests stay quiet and remain compatible with future pandas releases.
    """

    from qlib.utils import resam as qlib_resam  # type: ignore

    if getattr(qlib_resam, "_tradingnexus_resam_patched", False):
        return

    def _get_valid_value(series: pd.Series, last: bool = True):
        if last:
            return series.ffill().iloc[-1]
        return series.bfill().iloc[0]

    def _ts_data_valid(ts_feature, last: bool = False):
        if isinstance(ts_feature, pd.DataFrame):
            return ts_feature.apply(lambda column: _get_valid_value(column, last=last))
        if isinstance(ts_feature, pd.Series):
            return _get_valid_value(ts_feature, last=last)
        raise TypeError(f"ts_feature should be pd.DataFrame/Series, not {type(ts_feature)}")

    qlib_resam.get_valid_value = _get_valid_value
    qlib_resam._ts_data_valid = _ts_data_valid
    qlib_resam.ts_data_last = partial(_ts_data_valid, last=True)
    qlib_resam.ts_data_first = partial(_ts_data_valid, last=False)
    qlib_resam._tradingnexus_resam_patched = True


def patch_qlib_index_data_compat() -> None:
    """
    Patch qlib's IndexData.mean to avoid noisy RuntimeWarning on empty/all-NaN slices.

    qlib's default implementation delegates to ``np.nanmean`` directly, which emits
    ``RuntimeWarning: Mean of empty slice`` when execution indicators have no samples
    for a given step. The warning is noisy but the desired result is still ``NaN``.
    We preserve that behavior while avoiding the warning.
    """

    from qlib.utils import index_data as qlib_index_data  # type: ignore

    if getattr(qlib_index_data, "_tradingnexus_index_data_patched", False):
        return

    def _safe_nanmean(data: np.ndarray, axis=None):
        arr = np.asarray(data, dtype=float)
        if axis is None:
            valid_count = int(np.sum(~np.isnan(arr)))
            if valid_count == 0:
                return np.nan
            return float(np.nansum(arr) / valid_count)

        valid_count = np.sum(~np.isnan(arr), axis=axis)
        summed = np.nansum(arr, axis=axis)
        out = np.full(np.shape(summed), np.nan, dtype=float)
        return np.divide(summed, valid_count, out=out, where=valid_count != 0)

    def _mean(self, axis=None, dtype=None, out=None):
        assert out is None and dtype is None, "`out` is just for compatible with numpy's aggregating function"
        if axis is None:
            return _safe_nanmean(self.data)
        if axis == 0:
            tmp_data = _safe_nanmean(self.data, axis=0)
            return qlib_index_data.SingleData(tmp_data, self.columns)
        if axis == 1:
            tmp_data = _safe_nanmean(self.data, axis=1)
            return qlib_index_data.SingleData(tmp_data, self.index)
        raise ValueError("axis must be None, 0 or 1")

    qlib_index_data.IndexData.mean = _mean
    qlib_index_data._tradingnexus_index_data_patched = True


def ensure_minimal_qlib_provider(
    provider_dir: str | Path,
    calendar_dates: Sequence[pd.Timestamp] | pd.Index | pd.Series,
    instruments: Sequence[str],
    market_name: str = "all",
    freq: str = "week",
) -> Path:
    provider_path = Path(provider_dir).expanduser().resolve()
    calendars_dir = provider_path / "calendars"
    instruments_dir = provider_path / "instruments"
    calendars_dir.mkdir(parents=True, exist_ok=True)
    instruments_dir.mkdir(parents=True, exist_ok=True)

    calendar_index = _ensure_datetime_series(calendar_dates)
    if len(calendar_index) >= 2:
        step = calendar_index[-1] - calendar_index[-2]
    else:
        step = pd.Timedelta(days=7)
    calendar_with_future = calendar_index.append(pd.DatetimeIndex([calendar_index[-1] + step]))
    calendar_lines = [ts.strftime("%Y-%m-%d") for ts in calendar_with_future]
    normalized_freq = _native_freq_label(freq)
    calendar_file_names = {
        f"{freq}.txt",
        f"{freq}_future.txt",
        f"{normalized_freq}.txt",
        f"{normalized_freq}_future.txt",
    }
    for file_name in sorted(calendar_file_names):
        (calendars_dir / file_name).write_text("\n".join(calendar_lines) + ("\n" if calendar_lines else ""), encoding="utf-8")

    start_date = calendar_index.min().strftime("%Y-%m-%d")
    end_date = calendar_with_future.max().strftime("%Y-%m-%d")
    instrument_lines = [
        f"{str(instrument)}\t{start_date}\t{end_date}"
        for instrument in sorted({str(item) for item in instruments})
    ]
    (instruments_dir / f"{market_name}.txt").write_text(
        "\n".join(instrument_lines) + ("\n" if instrument_lines else ""),
        encoding="utf-8",
    )
    return provider_path


def build_native_quote_frame(
    panel: pd.DataFrame,
    symbols: Sequence[str] | None = None,
) -> pd.DataFrame:
    required_columns = {"datetime", "instrument", "open", "close"}
    missing = required_columns.difference(panel.columns)
    if missing:
        raise ValueError(f"panel missing required columns for native quote: {sorted(missing)}")

    frame = panel.copy()
    if symbols:
        symbol_set = {str(symbol) for symbol in symbols}
        frame = frame.loc[frame["instrument"].astype(str).isin(symbol_set)].copy()
    if frame.empty:
        raise ValueError("panel has no rows for native quote generation")

    frame["datetime"] = pd.to_datetime(frame["datetime"])
    frame["instrument"] = frame["instrument"].astype(str)
    frame["$open"] = pd.to_numeric(frame["open"], errors="coerce")
    frame["$close"] = pd.to_numeric(frame["close"], errors="coerce")
    volume_source = "volume" if "volume" in frame.columns else "amount" if "amount" in frame.columns else None
    if volume_source is None:
        frame["$volume"] = 0.0
    else:
        frame["$volume"] = pd.to_numeric(frame[volume_source], errors="coerce").fillna(0.0)
    frame["$factor"] = 1.0
    frame = frame.sort_values(["instrument", "datetime"]).reset_index(drop=True)

    calendar_dates = _ensure_datetime_series(frame["datetime"])
    all_symbols = sorted(frame["instrument"].astype(str).unique().tolist())
    full_index = pd.MultiIndex.from_product(
        [all_symbols, calendar_dates],
        names=["instrument", "datetime"],
    )

    quote = frame.set_index(["instrument", "datetime"])[["$open", "$close", "$volume", "$factor"]].sort_index()
    quote = quote.reindex(full_index)
    raw_presence = quote["$close"].notna()

    quote["$close"] = quote.groupby(level="instrument", sort=False)["$close"].ffill()
    quote["$factor"] = quote.groupby(level="instrument", sort=False)["$factor"].ffill().fillna(1.0)
    quote["$volume"] = quote["$volume"].fillna(0.0)
    quote["$open"] = quote["$open"].where(raw_presence, np.nan)
    quote["$change"] = (
        quote.groupby(level="instrument", sort=False)["$close"]
        .pct_change(fill_method=None)
        .replace([pd.NA, pd.NaT], 0.0)
        .fillna(0.0)
    )
    quote["limit_buy"] = ~raw_presence
    quote["limit_sell"] = ~raw_presence
    quote = quote[["$open", "$close", "$volume", "$factor", "$change", "limit_buy", "limit_sell"]].sort_index()
    return quote


def build_native_signal_frame(
    score_frame: pd.DataFrame | pd.Series,
    date_column: str = "feature_date",
    instrument_column: str = "instrument",
    score_column: str = NATIVE_SIGNAL_COLUMN,
) -> pd.DataFrame:
    if isinstance(score_frame, pd.Series):
        return _coerce_signal_frame(score_frame, score_column=score_column)
    frame = score_frame.copy()
    if isinstance(frame.index, pd.MultiIndex):
        return _coerce_signal_frame(frame, score_column=score_column)
    required_columns = {date_column, instrument_column, score_column}
    missing = required_columns.difference(frame.columns)
    if missing:
        raise ValueError(f"score_frame missing required columns for native signal: {sorted(missing)}")
    frame[date_column] = pd.to_datetime(frame[date_column])
    frame[instrument_column] = frame[instrument_column].astype(str)
    signal_columns = [date_column, instrument_column] + [
        column
        for column in frame.columns
        if column not in {date_column, instrument_column}
    ]
    signal = frame[signal_columns].copy()
    signal = signal.set_index([date_column, instrument_column]).sort_index()
    signal.index = signal.index.set_names(["datetime", "instrument"])
    return _coerce_signal_frame(signal, score_column=score_column)


def _score_frame_to_series_and_meta(
    score: pd.DataFrame | pd.Series,
    score_column: str = NATIVE_SIGNAL_COLUMN,
) -> tuple[pd.Series, pd.DataFrame]:
    if isinstance(score, pd.DataFrame):
        score_frame = score.copy()
        if score_column not in score_frame.columns:
            first_column = score_frame.columns[0]
            score_frame = score_frame.rename(columns={first_column: score_column})
    else:
        score_frame = pd.Series(score).to_frame(score_column)
    numeric_scores = pd.to_numeric(score_frame[score_column], errors="coerce").dropna().sort_values(ascending=False)
    meta = score_frame.reindex(numeric_scores.index).copy()
    meta[score_column] = numeric_scores
    meta.index = meta.index.astype(str)
    numeric_scores.index = numeric_scores.index.astype(str)
    return numeric_scores, meta


def _current_position_stock_list(current: Any) -> list[str]:
    if current is None or not hasattr(current, "get_stock_list"):
        return []
    return [str(code) for code in current.get_stock_list()]


def _current_position_weight_dict(current: Any) -> dict[str, float]:
    if current is None or not hasattr(current, "get_stock_weight_dict"):
        return {}
    weights = current.get_stock_weight_dict(only_stock=True)
    total_weight = sum(float(value) for value in weights.values())
    if total_weight <= 0:
        return {}
    return {
        str(code): float(value) / total_weight
        for code, value in weights.items()
        if float(value) > 0
    }


def _code_rank_map(series: pd.Series) -> dict[str, int]:
    return {
        str(code): rank
        for rank, code in enumerate(series.index.astype(str).tolist(), start=1)
    }


def _row_numeric_value(meta: pd.DataFrame, code: str, column: str) -> float | None:
    if column not in meta.columns or code not in meta.index:
        return None
    value = pd.to_numeric(pd.Series([meta.at[code, column]]), errors="coerce").iloc[0]
    if pd.isna(value):
        return None
    return float(value)


def _row_string_value(meta: pd.DataFrame, code: str, column: str) -> str | None:
    if column not in meta.columns or code not in meta.index:
        return None
    value = meta.at[code, column]
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def select_topk_with_buffer(
    score: pd.DataFrame | pd.Series,
    *,
    current_holdings: Sequence[str] | None = None,
    topk: int,
    hold_buffer_rank: int | None = None,
    min_liquidity_filter: float = 0.0,
    min_score_spread: float = 0.0,
    industry_max_weight: float | None = None,
    buyable_codes: Sequence[str] | None = None,
    score_column: str = NATIVE_SIGNAL_COLUMN,
) -> list[str]:
    scores, meta = _score_frame_to_series_and_meta(score, score_column=score_column)
    if scores.empty:
        return []

    holdings = [str(code) for code in (current_holdings or [])]
    buyable_code_set = None if buyable_codes is None else {str(code) for code in buyable_codes}
    topk = max(int(topk), 1)
    buffer_rank = max(int(hold_buffer_rank or topk), topk)
    rank_map = _code_rank_map(scores)
    industry_cap_count: int | None = None
    if industry_max_weight is not None and 0 < float(industry_max_weight) < 1.0:
        industry_cap_count = max(1, int(np.floor(float(industry_max_weight) * topk)))

    def _passes_liquidity(code: str) -> bool:
        if min_liquidity_filter <= 0:
            return True
        amount_value = _row_numeric_value(meta, code, "amount")
        if amount_value is not None:
            return amount_value >= float(min_liquidity_filter)
        volume_value = _row_numeric_value(meta, code, "volume")
        if volume_value is not None:
            return volume_value >= float(min_liquidity_filter)
        return False

    selected: list[str] = []
    industry_counts: dict[str, int] = {}

    def _accept(code: str) -> bool:
        if code in selected:
            return False
        if buyable_code_set is not None and code not in holdings and code not in buyable_code_set:
            return False
        if code in holdings:
            return True
        if not _passes_liquidity(code):
            return False
        if industry_cap_count is None:
            return True
        industry_name = _row_string_value(meta, code, "l1_name")
        if not industry_name:
            return True
        return industry_counts.get(industry_name, 0) < industry_cap_count

    def _append(code: str) -> None:
        selected.append(code)
        industry_name = _row_string_value(meta, code, "l1_name")
        if industry_name:
            industry_counts[industry_name] = industry_counts.get(industry_name, 0) + 1

    retained_current = [
        code
        for code in holdings
        if code in rank_map and rank_map[code] <= buffer_rank
    ]
    retained_current = sorted(retained_current, key=lambda code: (-scores.loc[code], code))
    for code in retained_current:
        if _accept(code):
            _append(code)
        if len(selected) >= topk:
            return selected[:topk]

    displaced_current = [
        code
        for code in holdings
        if code in rank_map and code not in selected
    ]
    displaced_current = sorted(displaced_current, key=lambda code: (-scores.loc[code], code))

    for code in scores.index.astype(str).tolist():
        if len(selected) >= topk:
            break
        if code in selected or code in retained_current:
            continue
        if not _accept(code):
            continue
        if min_score_spread > 0 and code not in holdings and displaced_current:
            best_displaced = displaced_current[0]
            if float(scores.loc[code]) - float(scores.loc[best_displaced]) < float(min_score_spread):
                continue
        _append(code)

    return selected[:topk]


class StaticQuoteExchange(Exchange):
    """
    A qlib Exchange-compatible class that bypasses D.features and relies on a provided quote frame.
    """

    @staticmethod
    def _init_exchange(instance: Any, extra_quote: pd.DataFrame) -> None:
        quote = extra_quote.copy()
        if not isinstance(quote.index, pd.MultiIndex):
            raise ValueError("extra_quote must be a MultiIndex DataFrame indexed by instrument and datetime")
        quote.index = quote.index.set_names(["instrument", "datetime"])
        quote = quote.sort_index()
        missing = set(instance.all_fields).difference(quote.columns)
        if missing:
            raise ValueError(f"extra_quote missing required qlib fields: {sorted(missing)}")

        instance.quote_df = quote[instance.all_fields].copy()
        if (instance.quote_df["$factor"].isna() & ~instance.quote_df["$close"].isna()).any():
            instance.trade_w_adj_price = True
        else:
            instance.trade_w_adj_price = False
        instance._update_limit(instance.limit_threshold)

    def get_quote_from_qlib(self) -> None:
        if self.extra_quote is None:
            raise ValueError("StaticQuoteExchange requires extra_quote")
        self._init_exchange(self, self.extra_quote)


class AShareQuoteExchange(StaticQuoteExchange):
    """
    A-share flavored qlib Exchange with separate commission / fees / stamp duty.

    qlib's default Exchange collapses all transaction costs into `open_cost/close_cost`
    and applies `min_cost` to the total. For A-shares we want minimum commission to only
    affect the broker commission component, while exchange fees, transfer fees, and
    stamp duty remain proportional.
    """

    def __init__(
        self,
        *args: Any,
        broker_commission_rate: float = 0.0,
        exchange_fee_rate: float = 0.0,
        transfer_fee_rate: float = 0.0,
        stamp_duty_sell_rate: float = 0.0,
        min_commission: float = 0.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.broker_commission_rate = float(broker_commission_rate)
        self.exchange_fee_rate = float(exchange_fee_rate)
        self.transfer_fee_rate = float(transfer_fee_rate)
        self.stamp_duty_sell_rate = float(stamp_duty_sell_rate)
        self.min_commission = float(min_commission)

    def _calculate_trade_cost(
        self,
        trade_val: float,
        *,
        side: str,
        adj_cost_ratio: float,
    ) -> float:
        if trade_val <= 0:
            return 0.0
        commission = max(trade_val * self.broker_commission_rate, self.min_commission)
        exchange_fee = trade_val * self.exchange_fee_rate
        transfer_fee = trade_val * self.transfer_fee_rate
        stamp_duty = trade_val * self.stamp_duty_sell_rate if side == "sell" else 0.0
        impact_cost = trade_val * adj_cost_ratio
        return commission + exchange_fee + transfer_fee + stamp_duty + impact_cost

    def _clip_buy_amount_to_cash(
        self,
        order: Any,
        *,
        trade_price: float,
        cash: float,
        adj_cost_ratio: float,
    ) -> float:
        deal_amount = self.round_amount_by_trade_unit(order.deal_amount, order.factor)
        trade_unit = self.trade_unit or 1
        while deal_amount > 0:
            trade_val = deal_amount * trade_price
            total_cost = self._calculate_trade_cost(trade_val, side="buy", adj_cost_ratio=adj_cost_ratio)
            if cash + 1e-9 >= trade_val + total_cost:
                return float(deal_amount)
            deal_amount -= trade_unit
        return 0.0

    def _calc_trade_info_by_order(
        self,
        order: Any,
        position: Any,
        dealt_order_amount: dict,
    ) -> tuple[float, float, float]:
        trade_price = float(
            self.get_deal_price(order.stock_id, order.start_time, order.end_time, direction=order.direction)
        )
        total_trade_val = float(self.get_volume(order.stock_id, order.start_time, order.end_time)) * trade_price
        order.factor = self.get_factor(order.stock_id, order.start_time, order.end_time)
        order.deal_amount = order.amount
        self._clip_amount_by_volume(order, dealt_order_amount)

        trade_val = float(order.deal_amount) * trade_price
        if not total_trade_val or np.isnan(total_trade_val):
            adj_cost_ratio = float(self.impact_cost)
        else:
            adj_cost_ratio = float(self.impact_cost) * (trade_val / total_trade_val) ** 2

        if order.direction == Order.SELL:
            if position is not None:
                current_amount = position.get_stock_amount(order.stock_id) if position.check_stock(order.stock_id) else 0
                if not np.isclose(order.deal_amount, current_amount):
                    order.deal_amount = self.round_amount_by_trade_unit(
                        min(current_amount, order.deal_amount),
                        order.factor,
                    )
                trade_val = float(order.deal_amount) * trade_price
                if position.get_cash() + trade_val < self._calculate_trade_cost(
                    trade_val,
                    side="sell",
                    adj_cost_ratio=adj_cost_ratio,
                ):
                    order.deal_amount = 0.0
            side = "sell"
        elif order.direction == Order.BUY:
            if position is not None:
                order.deal_amount = self._clip_buy_amount_to_cash(
                    order,
                    trade_price=trade_price,
                    cash=float(position.get_cash()),
                    adj_cost_ratio=adj_cost_ratio,
                )
            else:
                order.deal_amount = self.round_amount_by_trade_unit(order.deal_amount, order.factor)
            side = "buy"
        else:
            raise NotImplementedError(f"Unsupported order direction: {order.direction}")

        trade_val = float(order.deal_amount) * trade_price
        trade_cost = self._calculate_trade_cost(trade_val, side=side, adj_cost_ratio=adj_cost_ratio)
        if trade_val <= 1e-5:
            trade_cost = 0.0
        return trade_price, trade_val, trade_cost


class WeeklyTopKEqualWeightStrategy(WeightStrategyBase):
    """
    Thin qlib-native strategy adapter for weekly equal-weight TopK rebalancing.
    """

    def __init__(
        self,
        *,
        topk: int,
        only_tradable: bool = True,
        score_column: str = NATIVE_SIGNAL_COLUMN,
        risk_degree: float = 1.0,
        hold_buffer_rank: int | None = None,
        rebalance_interval_steps: int = 1,
        min_liquidity_filter: float = 0.0,
        min_score_spread: float = 0.0,
        industry_max_weight: float | None = None,
        **kwargs: Any,
    ):
        super().__init__(
            order_generator_cls_or_obj=OrderGenWInteract(),
            risk_degree=risk_degree,
            **kwargs,
        )
        self.topk = int(topk)
        self.only_tradable = bool(only_tradable)
        self.score_column = score_column
        self.hold_buffer_rank = int(hold_buffer_rank) if hold_buffer_rank is not None else self.topk
        self.rebalance_interval_steps = max(int(rebalance_interval_steps), 1)
        self.min_liquidity_filter = float(min_liquidity_filter)
        self.min_score_spread = float(min_score_spread)
        self.industry_max_weight = industry_max_weight

    def _tradable_codes(
        self,
        codes: Sequence[str],
        trade_start_time: pd.Timestamp,
        trade_end_time: pd.Timestamp,
    ) -> set[str]:
        tradable: set[str] = set()
        for code in codes:
            if self.trade_exchange.is_stock_tradable(
                stock_id=code,
                start_time=trade_start_time,
                end_time=trade_end_time,
            ):
                tradable.add(str(code))
        return tradable

    def generate_target_weight_position(self, score, current, trade_start_time, trade_end_time):
        if self.rebalance_interval_steps > 1:
            trade_step = self.trade_calendar.get_trade_step()
            if trade_step % self.rebalance_interval_steps != 0:
                current_weights = _current_position_weight_dict(current)
                if current_weights:
                    return current_weights

        current_holdings = _current_position_stock_list(current)
        tradable_codes = None
        if self.only_tradable:
            tradable_codes = self._tradable_codes(
                _score_frame_to_series_and_meta(score, score_column=self.score_column)[0].index.tolist(),
                trade_start_time=trade_start_time,
                trade_end_time=trade_end_time,
            )
        score_frame = score.copy() if isinstance(score, pd.DataFrame) else pd.Series(score).to_frame(self.score_column)

        selected = select_topk_with_buffer(
            score_frame,
            current_holdings=current_holdings,
            topk=self.topk,
            hold_buffer_rank=self.hold_buffer_rank,
            min_liquidity_filter=self.min_liquidity_filter,
            min_score_spread=self.min_score_spread,
            industry_max_weight=self.industry_max_weight,
            buyable_codes=tradable_codes,
            score_column=self.score_column,
        )
        if not selected:
            current_weights = _current_position_weight_dict(current)
            return current_weights if current_weights else {}
        weight = 1.0 / len(selected)
        return {str(code): weight for code in selected}


@dataclass(frozen=True)
class NativePortfolioArtifacts:
    signal_frame: pd.DataFrame
    quote_frame: pd.DataFrame
    report_normal: pd.DataFrame
    positions_normal: dict[pd.Timestamp, Any]
    port_analysis: pd.DataFrame
    indicators_normal: pd.DataFrame
    signal_diagnostics: pd.DataFrame | None = None
    portfolio_diagnostics: pd.DataFrame | None = None
    recorder_id: str | None = None


@dataclass(frozen=True)
class NativePortfolioRunResult:
    config: dict[str, Any]
    artifacts: NativePortfolioArtifacts


def build_native_portana_config(
    signal_frame: pd.DataFrame | pd.Series,
    quote_frame: pd.DataFrame,
    topk: int,
    trading_config: BacktestTradingConfig | None = None,
    account: float = 1_000_000.0,
    benchmark: str | None = None,
    market_name: str = "all",
    time_per_step: str = "week",
    risk_degree: float = 1.0,
    only_tradable: bool = True,
    hold_buffer_rank: int | None = None,
    rebalance_interval_steps: int = 1,
    min_liquidity_filter: float = 0.0,
    min_score_spread: float = 0.0,
    industry_max_weight: float | None = None,
) -> dict[str, Any]:
    signal = _coerce_signal_frame(signal_frame)
    if quote_frame.empty:
        raise ValueError("quote_frame is empty")

    normalized_time_per_step = _native_freq_label(time_per_step)
    dates = _ensure_datetime_series(signal.index.get_level_values("datetime"))
    if len(dates) < 2:
        raise ValueError("qlib native backtest requires at least two signal dates")
    config = trading_config or BacktestTradingConfig()
    codes = sorted(quote_frame.index.get_level_values("instrument").astype(str).unique().tolist())

    exchange_kwargs = config.qlib_exchange_kwargs(deal_price="$open")
    exchange_kwargs["exchange"] = {
        "class": "AShareQuoteExchange",
        "module_path": "qlib_research.core.qlib_native_backtest",
        "kwargs": {
            "freq": normalized_time_per_step,
            "codes": codes,
            "deal_price": "$open",
            "impact_cost": exchange_kwargs["impact_cost"],
            "trade_unit": exchange_kwargs["trade_unit"],
            "broker_commission_rate": config.broker_commission_rate,
            "exchange_fee_rate": config.exchange_fee_rate,
            "transfer_fee_rate": config.transfer_fee_rate,
            "stamp_duty_sell_rate": config.stamp_duty_sell_rate,
            "min_commission": config.min_commission,
            "quote_cls": PandasQuote,
            "extra_quote": quote_frame.copy(),
        },
    }

    return {
        "strategy": {
            "class": "WeeklyTopKEqualWeightStrategy",
            "module_path": "qlib_research.core.qlib_native_backtest",
            "kwargs": {
                "signal": "<PRED>",
                "topk": int(topk),
                "risk_degree": float(risk_degree),
                "only_tradable": bool(only_tradable),
                "hold_buffer_rank": int(hold_buffer_rank) if hold_buffer_rank is not None else int(topk),
                "rebalance_interval_steps": max(int(rebalance_interval_steps), 1),
                "min_liquidity_filter": float(min_liquidity_filter),
                "min_score_spread": float(min_score_spread),
                "industry_max_weight": industry_max_weight,
            },
        },
        "executor": {
            "class": "SimulatorExecutor",
            "module_path": "qlib.backtest.executor",
            "kwargs": {
                "time_per_step": normalized_time_per_step,
                "generate_portfolio_metrics": True,
            },
        },
        "backtest": {
            "start_time": dates[1],
            "end_time": dates[-1],
            "account": float(account),
            "benchmark": benchmark,
            "exchange_kwargs": exchange_kwargs,
            "pos_type": "Position",
        },
        "provider": {
            "market_name": market_name,
            "time_per_step": normalized_time_per_step,
            "codes": codes,
        },
    }


def load_native_portfolio_artifacts(recorder: Any, time_per_step: str = "week") -> NativePortfolioArtifacts:
    freq_label = _native_freq_label(time_per_step)
    signal_frame = recorder.load_object("pred.pkl")
    quote_frame = recorder.load_object("native_quote.pkl")
    return NativePortfolioArtifacts(
        signal_frame=signal_frame,
        quote_frame=quote_frame,
        report_normal=recorder.load_object(f"portfolio_analysis/report_normal_{freq_label}.pkl"),
        positions_normal=recorder.load_object(f"portfolio_analysis/positions_normal_{freq_label}.pkl"),
        port_analysis=recorder.load_object(f"portfolio_analysis/port_analysis_{freq_label}.pkl"),
        indicators_normal=recorder.load_object(f"portfolio_analysis/indicators_normal_{freq_label}.pkl"),
        recorder_id=str(recorder.id),
    )


def build_signal_diagnostics(
    signal_frame: pd.DataFrame | pd.Series,
    topk: int,
    score_column: str = NATIVE_SIGNAL_COLUMN,
) -> pd.DataFrame:
    signal = build_native_signal_frame(signal_frame, score_column=score_column)
    rows: list[dict[str, Any]] = []
    previous_top_codes: list[str] = []
    for signal_date in _ensure_datetime_series(signal.index.get_level_values("datetime")):
        slice_frame = signal.xs(signal_date, level="datetime").copy()
        scores = pd.to_numeric(slice_frame[score_column], errors="coerce").dropna().sort_values(ascending=False)
        top_scores = scores.head(topk)
        top_codes = top_scores.index.astype(str).tolist()
        row = {
            "signal_date": pd.Timestamp(signal_date),
            "coverage": int(len(scores)),
            "score_dispersion": float(scores.std()) if len(scores) > 1 else 0.0,
            "score_unique_count": int(scores.nunique()),
            "topk_unique_score_ratio": float(top_scores.nunique() / max(min(topk, len(top_scores)), 1)),
            "topk_overlap_prev": float(len(set(previous_top_codes) & set(top_codes)) / max(topk, 1)) if previous_top_codes else np.nan,
        }
        if "future_return_4w" in slice_frame.columns:
            ranked = slice_frame.reindex(scores.index)
            decile_count = max(int(np.ceil(len(ranked) / 10.0)), 1)
            top_decile = pd.to_numeric(ranked["future_return_4w"].head(decile_count), errors="coerce")
            bottom_decile = pd.to_numeric(ranked["future_return_4w"].tail(decile_count), errors="coerce")
            row["future_return_top_bottom_decile_spread"] = float(top_decile.mean() - bottom_decile.mean())
        if "label_excess_return_4w" in slice_frame.columns:
            ranked = slice_frame.reindex(scores.index)
            decile_count = max(int(np.ceil(len(ranked) / 10.0)), 1)
            top_decile = pd.to_numeric(ranked["label_excess_return_4w"].head(decile_count), errors="coerce")
            bottom_decile = pd.to_numeric(ranked["label_excess_return_4w"].tail(decile_count), errors="coerce")
            row["excess_return_top_bottom_decile_spread"] = float(top_decile.mean() - bottom_decile.mean())
        rows.append(row)
        previous_top_codes = top_codes
    return pd.DataFrame(rows)


def _position_holdings_snapshot(position: Any) -> list[str]:
    if position is None or not hasattr(position, "get_stock_list"):
        return []
    return [str(code) for code in position.get_stock_list()]


def _tradability_sets_for_date(
    quote_frame: pd.DataFrame,
    trade_date: pd.Timestamp,
) -> tuple[set[str], set[str]]:
    if quote_frame.empty:
        return set(), set()
    if trade_date not in quote_frame.index.get_level_values("datetime"):
        return set(), set()
    day_quote = quote_frame.xs(trade_date, level="datetime").copy()
    day_quote.index = day_quote.index.astype(str)
    buyable = {
        str(code)
        for code, row in day_quote.iterrows()
        if not bool(row.get("limit_buy", False)) and pd.notna(row.get("$open")) and pd.notna(row.get("$close"))
    }
    sellable = {
        str(code)
        for code, row in day_quote.iterrows()
        if not bool(row.get("limit_sell", False)) and pd.notna(row.get("$close"))
    }
    return buyable, sellable


def build_native_portfolio_diagnostics(
    signal_frame: pd.DataFrame | pd.Series,
    quote_frame: pd.DataFrame,
    positions_normal: dict[pd.Timestamp, Any],
    *,
    topk: int,
    hold_buffer_rank: int | None = None,
    rebalance_interval_steps: int = 1,
    only_tradable: bool = True,
    min_liquidity_filter: float = 0.0,
    min_score_spread: float = 0.0,
    industry_max_weight: float | None = None,
    score_column: str = NATIVE_SIGNAL_COLUMN,
) -> pd.DataFrame:
    signal = build_native_signal_frame(signal_frame, score_column=score_column)
    signal_dates = _ensure_datetime_series(signal.index.get_level_values("datetime"))
    trade_dates = sorted(pd.to_datetime(list(positions_normal.keys())))
    if len(signal_dates) < 2 or not trade_dates:
        return pd.DataFrame()

    signal_diagnostics = build_signal_diagnostics(signal, topk=topk, score_column=score_column).set_index("signal_date")
    rows: list[dict[str, Any]] = []
    previous_actual_holdings: list[str] = []

    for signal_date, trade_date in zip(signal_dates[:-1], trade_dates):
        signal_slice = signal.xs(signal_date, level="datetime").copy()
        buyable_codes, sellable_codes = _tradability_sets_for_date(quote_frame, pd.Timestamp(trade_date))
        target_holdings = select_topk_with_buffer(
            signal_slice,
            current_holdings=previous_actual_holdings,
            topk=topk,
            hold_buffer_rank=hold_buffer_rank,
            min_liquidity_filter=min_liquidity_filter,
            min_score_spread=min_score_spread,
            industry_max_weight=industry_max_weight,
            buyable_codes=buyable_codes if only_tradable else None,
            score_column=score_column,
        )
        actual_holdings = _position_holdings_snapshot(positions_normal.get(trade_date))
        blocked_sell_codes = sorted(
            code
            for code in previous_actual_holdings
            if code not in target_holdings and code not in sellable_codes and code in actual_holdings
        )
        row = {
            "signal_date": pd.Timestamp(signal_date),
            "trade_date": pd.Timestamp(trade_date),
            "target_hold_count": int(len(target_holdings)),
            "actual_hold_count": int(len(actual_holdings)),
            "blocked_sell_count": int(len(blocked_sell_codes)),
            "blocked_sell_codes": ",".join(blocked_sell_codes),
            "residual_hold_count": int(len(set(actual_holdings) - set(target_holdings))),
            "rebalance_interval_steps": int(rebalance_interval_steps),
            "hold_buffer_rank": int(hold_buffer_rank or topk),
        }
        if pd.Timestamp(signal_date) in signal_diagnostics.index:
            row.update(signal_diagnostics.loc[pd.Timestamp(signal_date)].to_dict())
        rows.append(row)
        previous_actual_holdings = actual_holdings

    return pd.DataFrame(rows)


def run_portana_record(
    recorder: Any,
    config: dict[str, Any],
    quote_frame: pd.DataFrame,
    time_per_step: str = "week",
) -> NativePortfolioArtifacts:
    qlib = _require_qlib()
    recorder.save_objects(**{"native_quote.pkl": quote_frame})
    record = qlib.workflow.record_temp.PortAnaRecord(
        recorder=recorder,
        config=config,
        risk_analysis_freq=time_per_step,
        indicator_analysis_freq=time_per_step,
    )
    record.generate()
    return load_native_portfolio_artifacts(recorder, time_per_step=time_per_step)


def run_native_backtest_analysis(
    signal_frame: pd.DataFrame | pd.Series,
    quote_frame: pd.DataFrame,
    topk: int,
    trading_config: BacktestTradingConfig | None = None,
    account: float = 1_000_000.0,
    benchmark: str | None = None,
    market_name: str = "all",
    time_per_step: str = "week",
    risk_degree: float = 1.0,
    only_tradable: bool = True,
    hold_buffer_rank: int | None = None,
    rebalance_interval_steps: int = 1,
    min_liquidity_filter: float = 0.0,
    min_score_spread: float = 0.0,
    industry_max_weight: float | None = None,
) -> NativePortfolioRunResult:
    from qlib.backtest import backtest  # type: ignore
    from qlib.contrib.evaluate import risk_analysis  # type: ignore

    patch_qlib_resam_compat()
    patch_qlib_index_data_compat()
    signal = build_native_signal_frame(signal_frame)
    effective_benchmark = benchmark if benchmark is not None else _flat_benchmark_from_signal(signal)
    config = build_native_portana_config(
        signal_frame=signal,
        quote_frame=quote_frame,
        topk=topk,
        trading_config=trading_config,
        account=account,
        benchmark=effective_benchmark,
        market_name=market_name,
        time_per_step=time_per_step,
        risk_degree=risk_degree,
        only_tradable=only_tradable,
        hold_buffer_rank=hold_buffer_rank,
        rebalance_interval_steps=rebalance_interval_steps,
        min_liquidity_filter=min_liquidity_filter,
        min_score_spread=min_score_spread,
        industry_max_weight=industry_max_weight,
    )
    direct_config = deepcopy(config)
    direct_config["strategy"]["kwargs"]["signal"] = signal
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Series.fillna with 'method' is deprecated",
            category=FutureWarning,
            module=r"qlib\.utils\.resam",
        )
        portfolio_metric_dict, indicator_dict = backtest(
            start_time=direct_config["backtest"]["start_time"],
            end_time=direct_config["backtest"]["end_time"],
            strategy=direct_config["strategy"],
            executor=direct_config["executor"],
            benchmark=effective_benchmark,
            account=account,
            exchange_kwargs=direct_config["backtest"]["exchange_kwargs"],
            pos_type=direct_config["backtest"]["pos_type"],
        )
    freq_label = _native_freq_label(time_per_step)
    report_normal, positions_normal = portfolio_metric_dict[freq_label]
    indicators_normal = indicator_dict[freq_label][0]
    port_analysis = pd.concat(
        {
            "excess_return_without_cost": risk_analysis(
                report_normal["return"] - report_normal["bench"],
                freq=freq_label,
            ),
            "excess_return_with_cost": risk_analysis(
                report_normal["return"] - report_normal["bench"] - report_normal["cost"],
                freq=freq_label,
            ),
        }
    )
    artifacts = NativePortfolioArtifacts(
        signal_frame=signal,
        quote_frame=quote_frame,
        report_normal=report_normal,
        positions_normal=positions_normal,
        port_analysis=port_analysis,
        indicators_normal=indicators_normal,
        signal_diagnostics=build_signal_diagnostics(signal, topk=topk, score_column=NATIVE_SIGNAL_COLUMN),
        portfolio_diagnostics=build_native_portfolio_diagnostics(
            signal,
            quote_frame,
            positions_normal,
            topk=topk,
            hold_buffer_rank=hold_buffer_rank,
            rebalance_interval_steps=rebalance_interval_steps,
            only_tradable=only_tradable,
            min_liquidity_filter=min_liquidity_filter,
            min_score_spread=min_score_spread,
            industry_max_weight=industry_max_weight,
            score_column=NATIVE_SIGNAL_COLUMN,
        ),
    )
    return NativePortfolioRunResult(config=config, artifacts=artifacts)


def run_native_portfolio_analysis(
    recorder: Any,
    signal_frame: pd.DataFrame | pd.Series,
    quote_frame: pd.DataFrame,
    topk: int,
    trading_config: BacktestTradingConfig | None = None,
    account: float = 1_000_000.0,
    benchmark: str | None = None,
    market_name: str = "all",
    time_per_step: str = "week",
    risk_degree: float = 1.0,
    only_tradable: bool = True,
    hold_buffer_rank: int | None = None,
    rebalance_interval_steps: int = 1,
    min_liquidity_filter: float = 0.0,
    min_score_spread: float = 0.0,
    industry_max_weight: float | None = None,
) -> NativePortfolioRunResult:
    patch_qlib_resam_compat()
    patch_qlib_index_data_compat()
    signal = build_native_signal_frame(signal_frame)
    config = build_native_portana_config(
        signal_frame=signal,
        quote_frame=quote_frame,
        topk=topk,
        trading_config=trading_config,
        account=account,
        benchmark=benchmark,
        market_name=market_name,
        time_per_step=time_per_step,
        risk_degree=risk_degree,
        only_tradable=only_tradable,
        hold_buffer_rank=hold_buffer_rank,
        rebalance_interval_steps=rebalance_interval_steps,
        min_liquidity_filter=min_liquidity_filter,
        min_score_spread=min_score_spread,
        industry_max_weight=industry_max_weight,
    )
    recorder.save_objects(**{"pred.pkl": signal, "native_quote.pkl": quote_frame})
    artifacts = run_portana_record(
        recorder=recorder,
        config=config,
        quote_frame=quote_frame,
        time_per_step=time_per_step,
    )
    enriched_artifacts = NativePortfolioArtifacts(
        signal_frame=artifacts.signal_frame,
        quote_frame=artifacts.quote_frame,
        report_normal=artifacts.report_normal,
        positions_normal=artifacts.positions_normal,
        port_analysis=artifacts.port_analysis,
        indicators_normal=artifacts.indicators_normal,
        signal_diagnostics=build_signal_diagnostics(signal, topk=topk, score_column=NATIVE_SIGNAL_COLUMN),
        portfolio_diagnostics=build_native_portfolio_diagnostics(
            signal,
            quote_frame,
            artifacts.positions_normal,
            topk=topk,
            hold_buffer_rank=hold_buffer_rank,
            rebalance_interval_steps=rebalance_interval_steps,
            only_tradable=only_tradable,
            min_liquidity_filter=min_liquidity_filter,
            min_score_spread=min_score_spread,
            industry_max_weight=industry_max_weight,
            score_column=NATIVE_SIGNAL_COLUMN,
        ),
        recorder_id=artifacts.recorder_id,
    )
    return NativePortfolioRunResult(config=config, artifacts=enriched_artifacts)
