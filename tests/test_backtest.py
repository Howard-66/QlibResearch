import pandas as pd

from qlib_research.core.backtest import BacktestEngine
from qlib_research.core.portfolio import BacktestTradingConfig, PortfolioManager


def test_portfolio_execution_applies_a_share_net_costs():
    config = BacktestTradingConfig(
        broker_commission_rate=0.0002,
        exchange_fee_rate=0.0000341,
        transfer_fee_rate=0.00001,
        stamp_duty_sell_rate=0.0005,
        impact_cost_rate=0.0005,
        min_commission=5.0,
        trade_unit=100,
    )
    pm = PortfolioManager(initial_capital=10_000, trading_config=config)

    buy_summary = pm._execute_trade("AAA.SH", 100, 10.0)
    assert pm.holdings["AAA.SH"]["quantity"] == 100
    assert round(pm.cash, 4) == round(10_000 - 1_000 - buy_summary["total_cost"], 4)
    assert round(buy_summary["commission"], 4) == 5.0
    assert round(buy_summary["stamp_duty"], 4) == 0.0

    sell_summary = pm._execute_trade("AAA.SH", -100, 10.0)
    assert "AAA.SH" not in pm.holdings
    assert round(sell_summary["stamp_duty"], 4) == round(1_000 * 0.0005, 4)
    expected_cash = 10_000 - buy_summary["total_cost"] - sell_summary["total_cost"]
    assert round(pm.cash, 4) == round(expected_cash, 4)


def test_portfolio_rebalance_rounds_buys_to_board_lots():
    config = BacktestTradingConfig(trade_unit=100)
    pm = PortfolioManager(initial_capital=10_000, trading_config=config)
    current_prices = {"AAA.SH": 23.0}

    summary = pm.rebalance(current_prices, {"AAA.SH": 1.0})

    assert pm.holdings["AAA.SH"]["quantity"] == 400
    assert summary["buy_count"] == 1
    assert pm.cash == 10_000 - (400 * 23.0)


def test_backtest_engine_uses_execution_prices_and_marks_to_close():
    dates = pd.to_datetime(["2024-01-05", "2024-01-12"])
    mark_prices = pd.DataFrame({"AAA.SH": [100.0, 120.0]}, index=dates)
    execution_prices = pd.DataFrame({"AAA.SH": [100.0, 110.0]}, index=dates)
    signal_data = pd.DataFrame({"AAA.SH": [0.0, 1.0]}, index=dates)

    engine = BacktestEngine(initial_capital=10_000)
    engine.run(mark_prices, signal_data=signal_data, execution_price_data=execution_prices)

    history = pd.DataFrame(engine.portfolio.history).set_index("date")
    assert len(history) == 2
    assert history.loc[pd.Timestamp("2024-01-12"), "total_value"] > 10_000
    assert history.loc[pd.Timestamp("2024-01-12"), "market_value"] == 120.0 * 90


def test_backtest_engine_does_not_trigger_stop_loss_when_disabled():
    config = BacktestTradingConfig(stop_loss_mode="off")
    engine = BacktestEngine(initial_capital=10_000, trading_config=config)

    engine.portfolio._execute_trade("AAA.SH", 100, 10.0)
    date = pd.Timestamp("2024-01-12")
    mark_prices = pd.DataFrame({"AAA.SH": [10.0]}, index=[date])
    execution_prices = pd.DataFrame({"AAA.SH": [10.0]}, index=[date])
    stop_prices = pd.DataFrame({"AAA.SH": [7.0]}, index=[date])

    engine.run(mark_prices, execution_price_data=execution_prices, stop_price_data=stop_prices)

    assert "AAA.SH" in engine.portfolio.holdings
