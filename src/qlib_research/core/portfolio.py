from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any


def _coerce_valid_price(price: object) -> float | None:
    try:
        value = float(price)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value) or value <= 0:
        return None
    return value


def _empty_trade_summary() -> dict[str, float | int]:
    return {
        "trade_count": 0,
        "buy_count": 0,
        "sell_count": 0,
        "buy_notional": 0.0,
        "sell_notional": 0.0,
        "gross_trade_value": 0.0,
        "commission": 0.0,
        "exchange_fee": 0.0,
        "transfer_fee": 0.0,
        "stamp_duty": 0.0,
        "impact_cost": 0.0,
        "total_cost": 0.0,
        "turnover_ratio": 0.0,
    }


def _merge_trade_summary(base: dict[str, float | int], update: dict[str, float | int]) -> dict[str, float | int]:
    merged = dict(base)
    for key, value in update.items():
        if key not in merged:
            merged[key] = value
            continue
        merged[key] = merged[key] + value
    return merged


@dataclass(frozen=True)
class BacktestTradingConfig:
    broker_commission_rate: float = 0.0
    exchange_fee_rate: float = 0.0
    transfer_fee_rate: float = 0.0
    stamp_duty_sell_rate: float = 0.0
    impact_cost_rate: float = 0.0
    min_commission: float = 0.0
    trade_unit: int | None = None
    stop_loss_mode: str = "off"
    stop_loss_threshold: float = 0.08

    def qlib_exchange_kwargs(self, *, deal_price: str = "$open") -> dict[str, float | int | str]:
        return {
            "deal_price": deal_price,
            "open_cost": self.broker_commission_rate + self.exchange_fee_rate + self.transfer_fee_rate,
            "close_cost": self.broker_commission_rate
            + self.exchange_fee_rate
            + self.transfer_fee_rate
            + self.stamp_duty_sell_rate,
            "impact_cost": self.impact_cost_rate,
            "min_cost": self.min_commission,
            "trade_unit": self.trade_unit,
        }


class PortfolioManager:
    def __init__(
        self,
        initial_capital: float = 100000.0,
        trading_config: BacktestTradingConfig | None = None,
    ):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.trading_config = trading_config or BacktestTradingConfig()
        self.holdings: dict[str, dict[str, float]] = {}
        self.equity_curve: list[dict[str, Any]] = []
        self.history: list[dict[str, Any]] = []
        self.trade_log: list[dict[str, Any]] = []

    def get_total_value(self, current_prices: dict[str, object]) -> float:
        stock_value = 0.0
        for ticker, data in self.holdings.items():
            live_price = _coerce_valid_price(current_prices.get(ticker))
            fallback_price = _coerce_valid_price(data.get("current_price"))
            price = live_price if live_price is not None else fallback_price
            if price is None:
                continue
            data["current_price"] = price
            stock_value += float(data["quantity"]) * price
        return self.cash + stock_value

    def rebalance(
        self,
        current_prices: dict[str, object],
        target_allocation_weights: dict[str, float],
    ) -> dict[str, float | int]:
        total_value = self.get_total_value(current_prices)
        target_values = {ticker: total_value * float(weight) for ticker, weight in target_allocation_weights.items()}
        tickers = sorted(set(self.holdings) | set(target_values))
        trade_summary = _empty_trade_summary()

        desired_quantities: dict[str, int] = {}
        for ticker in tickers:
            current_price = _coerce_valid_price(current_prices.get(ticker))
            if current_price is None:
                continue
            target_value = max(target_values.get(ticker, 0.0), 0.0)
            desired_quantities[ticker] = self._target_quantity_for_value(target_value, current_price)

        for ticker in tickers:
            current_price = _coerce_valid_price(current_prices.get(ticker))
            if current_price is None:
                continue
            current_qty = int(self.holdings.get(ticker, {}).get("quantity", 0))
            desired_qty = desired_quantities.get(ticker, current_qty)
            if desired_qty >= current_qty:
                continue
            sell_qty = self._normalize_sell_quantity(current_qty=current_qty, desired_qty=desired_qty)
            if sell_qty <= 0:
                continue
            trade_summary = _merge_trade_summary(trade_summary, self._execute_trade(ticker, -sell_qty, current_price))

        buy_candidates: list[tuple[str, int, float, float]] = []
        for ticker in tickers:
            current_price = _coerce_valid_price(current_prices.get(ticker))
            if current_price is None:
                continue
            current_qty = int(self.holdings.get(ticker, {}).get("quantity", 0))
            desired_qty = desired_quantities.get(ticker, current_qty)
            if desired_qty <= current_qty:
                continue
            delta_qty = desired_qty - current_qty
            target_value = target_values.get(ticker, 0.0)
            buy_candidates.append((ticker, delta_qty, current_price, target_value))

        buy_candidates.sort(key=lambda item: item[3], reverse=True)
        for ticker, delta_qty, current_price, _target_value in buy_candidates:
            affordable_qty = self._clip_buy_quantity_to_cash(delta_qty, current_price)
            if affordable_qty <= 0:
                continue
            trade_summary = _merge_trade_summary(trade_summary, self._execute_trade(ticker, affordable_qty, current_price))

        if total_value > 0:
            trade_summary["turnover_ratio"] = float(trade_summary["gross_trade_value"]) / float(total_value)
        return trade_summary

    def _target_quantity_for_value(self, target_value: float, price: float) -> int:
        if target_value <= 0 or price <= 0:
            return 0
        raw_quantity = int(target_value / price)
        return self._round_buy_quantity(raw_quantity)

    def _round_buy_quantity(self, quantity: int) -> int:
        if quantity <= 0:
            return 0
        trade_unit = self.trading_config.trade_unit
        if not trade_unit or trade_unit <= 1:
            return int(quantity)
        return int(quantity // trade_unit * trade_unit)

    def _normalize_sell_quantity(self, current_qty: int, desired_qty: int) -> int:
        sell_qty = max(current_qty - desired_qty, 0)
        if sell_qty <= 0:
            return 0
        if desired_qty <= 0:
            return current_qty
        trade_unit = self.trading_config.trade_unit
        if not trade_unit or trade_unit <= 1:
            return sell_qty
        return int(sell_qty // trade_unit * trade_unit)

    def _calculate_trade_costs(self, qty: int, price: float, side: str) -> dict[str, float]:
        notional = float(qty) * float(price)
        if notional <= 0:
            return {
                "commission": 0.0,
                "exchange_fee": 0.0,
                "transfer_fee": 0.0,
                "stamp_duty": 0.0,
                "impact_cost": 0.0,
                "total_cost": 0.0,
            }
        commission_base = notional * float(self.trading_config.broker_commission_rate)
        commission = max(commission_base, float(self.trading_config.min_commission))
        exchange_fee = notional * float(self.trading_config.exchange_fee_rate)
        transfer_fee = notional * float(self.trading_config.transfer_fee_rate)
        stamp_duty = notional * float(self.trading_config.stamp_duty_sell_rate) if side == "sell" else 0.0
        impact_cost = notional * float(self.trading_config.impact_cost_rate)
        total_cost = commission + exchange_fee + transfer_fee + stamp_duty + impact_cost
        return {
            "commission": commission,
            "exchange_fee": exchange_fee,
            "transfer_fee": transfer_fee,
            "stamp_duty": stamp_duty,
            "impact_cost": impact_cost,
            "total_cost": total_cost,
        }

    def _clip_buy_quantity_to_cash(self, desired_qty: int, price: float) -> int:
        qty = self._round_buy_quantity(desired_qty)
        trade_unit = self.trading_config.trade_unit or 1
        while qty > 0:
            costs = self._calculate_trade_costs(qty, price, side="buy")
            total_outlay = qty * price + costs["total_cost"]
            if self.cash + 1e-9 >= total_outlay:
                return qty
            qty -= trade_unit
        return 0

    def _execute_trade(self, ticker: str, qty: int, price: float) -> dict[str, float | int]:
        if qty == 0:
            return _empty_trade_summary()

        side = "buy" if qty > 0 else "sell"
        quantity = int(abs(qty))
        costs = self._calculate_trade_costs(quantity, price, side=side)
        notional = float(quantity) * float(price)
        trade_summary = _empty_trade_summary()
        trade_summary["trade_count"] = 1
        trade_summary["gross_trade_value"] = notional
        trade_summary["commission"] = costs["commission"]
        trade_summary["exchange_fee"] = costs["exchange_fee"]
        trade_summary["transfer_fee"] = costs["transfer_fee"]
        trade_summary["stamp_duty"] = costs["stamp_duty"]
        trade_summary["impact_cost"] = costs["impact_cost"]
        trade_summary["total_cost"] = costs["total_cost"]

        if side == "buy":
            total_outlay = notional + costs["total_cost"]
            if self.cash + 1e-9 < total_outlay:
                return _empty_trade_summary()

            self.cash -= total_outlay
            if ticker not in self.holdings:
                self.holdings[ticker] = {"quantity": 0, "cost": 0.0, "current_price": float(price)}

            old_qty = int(self.holdings[ticker]["quantity"])
            old_cost = float(self.holdings[ticker]["cost"])
            effective_cost = total_outlay / quantity
            new_qty = old_qty + quantity
            new_cost = effective_cost if new_qty == quantity else ((old_cost * old_qty) + total_outlay) / new_qty

            self.holdings[ticker]["quantity"] = new_qty
            self.holdings[ticker]["cost"] = new_cost
            self.holdings[ticker]["current_price"] = float(price)
            trade_summary["buy_count"] = 1
            trade_summary["buy_notional"] = notional
        else:
            if ticker not in self.holdings:
                return _empty_trade_summary()
            current_qty = int(self.holdings[ticker]["quantity"])
            if current_qty < quantity:
                return _empty_trade_summary()
            revenue = notional - costs["total_cost"]
            self.cash += revenue
            remaining_qty = current_qty - quantity
            if remaining_qty == 0:
                del self.holdings[ticker]
            else:
                self.holdings[ticker]["quantity"] = remaining_qty
                self.holdings[ticker]["current_price"] = float(price)
            trade_summary["sell_count"] = 1
            trade_summary["sell_notional"] = notional

        self.trade_log.append(
            {
                "ticker": ticker,
                "side": side,
                "quantity": quantity,
                "price": float(price),
                "notional": notional,
                **costs,
            }
        )
        return trade_summary

    def check_stop_loss(
        self,
        execution_prices: dict[str, object],
        trigger_prices: dict[str, object] | None = None,
        threshold: float | None = None,
    ) -> dict[str, float | int]:
        if self.trading_config.stop_loss_mode == "off":
            return _empty_trade_summary()

        trigger_source = trigger_prices or execution_prices
        stop_threshold = float(threshold if threshold is not None else self.trading_config.stop_loss_threshold)
        trade_summary = _empty_trade_summary()
        for ticker in list(self.holdings.keys()):
            data = self.holdings[ticker]
            trigger_price = _coerce_valid_price(trigger_source.get(ticker))
            execution_price = _coerce_valid_price(execution_prices.get(ticker))
            if trigger_price is None or execution_price is None:
                continue
            if trigger_price < float(data["cost"]) * (1.0 - stop_threshold):
                quantity = int(data["quantity"])
                trade_summary = _merge_trade_summary(
                    trade_summary,
                    self._execute_trade(ticker, -quantity, execution_price),
                )
        return trade_summary
