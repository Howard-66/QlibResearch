from __future__ import annotations

import numpy as np
import pandas as pd

from qlib_research.core.portfolio import BacktestTradingConfig, PortfolioManager


class BacktestEngine:
    def __init__(
        self,
        initial_capital: float = 100000.0,
        trading_config: BacktestTradingConfig | None = None,
    ):
        self.trading_config = trading_config or BacktestTradingConfig()
        self.portfolio = PortfolioManager(initial_capital, trading_config=self.trading_config)

    def run(
        self,
        price_data: pd.DataFrame,
        signal_data: pd.DataFrame | None = None,
        execution_price_data: pd.DataFrame | None = None,
        stop_price_data: pd.DataFrame | None = None,
    ) -> None:
        """
        Run backtest simulation.

        price_data:
            DataFrame indexed by evaluation date. Portfolio is marked to market with these prices.
        signal_data:
            DataFrame indexed by execution/rebalance date and containing target weights.
        execution_price_data:
            DataFrame indexed by execution/rebalance date and containing成交 prices. Defaults to price_data.
        stop_price_data:
            Optional trigger-price frame used by stop-loss logic. Defaults to price_data.
        """
        if price_data.empty:
            print("No price data provided.")
            return

        mark_prices = price_data.sort_index()
        execution_prices = (execution_price_data if execution_price_data is not None else price_data).sort_index()
        stop_prices = (stop_price_data if stop_price_data is not None else price_data).sort_index()
        dates = mark_prices.index.sort_values()

        for date in dates:
            current_mark_prices = mark_prices.loc[date].to_dict()
            current_execution_prices = (
                execution_prices.loc[date].to_dict() if date in execution_prices.index else current_mark_prices
            )
            current_stop_prices = stop_prices.loc[date].to_dict() if date in stop_prices.index else current_mark_prices

            trade_summary = {
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

            if self.trading_config.stop_loss_mode != "off":
                stop_summary = self.portfolio.check_stop_loss(
                    current_execution_prices,
                    trigger_prices=current_stop_prices,
                )
                for key, value in stop_summary.items():
                    trade_summary[key] += value

            if signal_data is not None and date in signal_data.index:
                target_weights = signal_data.loc[date].to_dict()
                target_weights = {key: value for key, value in target_weights.items() if not pd.isna(value)}
                rebalance_summary = self.portfolio.rebalance(current_execution_prices, target_weights)
                for key, value in rebalance_summary.items():
                    trade_summary[key] += value

            total_value = self.portfolio.get_total_value(current_mark_prices)
            market_value = total_value - self.portfolio.cash
            self.portfolio.history.append(
                {
                    "date": date,
                    "total_value": total_value,
                    "cash": self.portfolio.cash,
                    "market_value": market_value,
                    **trade_summary,
                }
            )

    def get_performance_metrics(self) -> dict[str, float]:
        if not self.portfolio.history:
            return {}

        df = pd.DataFrame(self.portfolio.history).set_index("date")
        df["returns"] = df["total_value"].pct_change().fillna(0.0)

        total_return = (df["total_value"].iloc[-1] / self.portfolio.initial_capital) - 1.0
        days = (df.index[-1] - df.index[0]).days
        cagr = (1.0 + total_return) ** (365.0 / days) - 1.0 if days > 0 else 0.0
        df["cummax"] = df["total_value"].cummax()
        df["drawdown"] = (df["total_value"] - df["cummax"]) / df["cummax"]
        max_drawdown = df["drawdown"].min()
        return_std = df["returns"].std()
        daily_sharpe = 0.0 if pd.isna(return_std) or return_std == 0 else df["returns"].mean() / return_std
        sharpe_ratio = daily_sharpe * np.sqrt(252)

        metrics = {
            "Total Return": float(total_return),
            "CAGR": float(cagr),
            "Max Drawdown": float(max_drawdown),
            "Sharpe Ratio": float(sharpe_ratio),
        }
        cost_columns = [
            "commission",
            "exchange_fee",
            "transfer_fee",
            "stamp_duty",
            "impact_cost",
            "total_cost",
            "gross_trade_value",
        ]
        for column in cost_columns:
            if column in df.columns:
                metrics[f"Total {column.replace('_', ' ').title()}"] = float(df[column].sum())
        return metrics
