import pathlib
from datetime import timedelta
from decimal import Decimal

import numpy as np
import pandas as pd

from src.backtest import backtest_util
from src.backtest.ftx_data_types import (BackTestConfig, BaseState, HedgeType,
                                         MarketOrder, Side)
from src.indicator.base_indicator import BaseIndicator
from src.indicator.bollinger import BollingerParams


def run_backtest(
    indicator: BaseIndicator,
    config: BackTestConfig,
):
    trades = pd.read_parquet(indicator.get_trades_path())
    spot_klines = pd.read_parquet(indicator.get_spot_klines_path())
    future_klines = pd.read_parquet(indicator.get_future_klines_path())

    for boll_mult in np.arange(1, 3, 0.1):
        boll_mult = round(boll_mult, 1)
        save_path = pathlib.Path(f"local/backtest/bollinger_{boll_mult}")
        summary_path = save_path / "summary.json"
        if summary_path.exists():
            continue

        params: BollingerParams = BollingerParams(length=20, std_mult=boll_mult)

        upper_threshold_df, lower_threshold_df = indicator.compute_thresholds(
            spot_klines, future_klines, params, as_df=True
        )

        # run backtest
        state = BaseState()
        logs = []

        trades_iter = trades.itertuples()
        while True:
            try:
                trade = next(trades_iter)
            except StopIteration:
                break
            dt: pd.Timestamp = trade.Index
            ts: float = dt.timestamp()
            basis = Decimal(str(trade.basis))
            future_side = trade.f_side
            spot_price = Decimal(str(trade.s_price))
            spot_size = Decimal(str(trade.s_size))
            future_price = Decimal(str(trade.f_price))
            future_size = Decimal(str(trade.f_size))
            max_available_size = min(spot_size, future_size)
            state.basis = basis

            # expiry liquidation
            if ts >= config["ts_to_expiry"]:
                break

            # indicator ready
            dt_truncate = dt.replace(minute=0, second=0, microsecond=0) - timedelta(
                hours=1
            )
            if dt_truncate not in upper_threshold_df.index:
                continue

            boll_up = upper_threshold_df[dt_truncate]
            boll_low = lower_threshold_df[dt_truncate]
            if np.isnan(boll_low):
                continue

            # open position
            if (
                future_side == "SELL"
                and basis > boll_up
                and ts < config["ts_to_stop_open"]
            ):
                spot_market_order = MarketOrder(
                    symbol="spot",
                    side=Side.BUY,
                    price=spot_price,
                    size=max_available_size,
                    create_timestamp=ts,
                    fee_rate=config["fee_rate"],
                )
                future_market_order = MarketOrder(
                    symbol="future",
                    side=Side.SELL,
                    price=future_price,
                    size=max_available_size,
                    create_timestamp=ts,
                    fee_rate=config["fee_rate"],
                )
                fee = future_market_order.fee + spot_market_order.fee
                expected_return = (
                    future_market_order.order_value
                    - spot_market_order.order_value
                    - 2 * fee
                )
                if expected_return > 0:
                    state.open_position(
                        spot_market_order,
                        future_market_order,
                        config["collateral_weight"],
                        config["leverage"],
                    )
                    state.append_hedge_trade(ts, HedgeType.OPEN, basis)

            # close position
            if state.spot_position > 0:
                entry_basis = state.future_entry_price - state.spot_entry_price
                if (
                    future_side == "BUY"
                    and basis <= max(0, boll_low)
                    and entry_basis > basis
                ):
                    close_size = min(state.spot_position, max_available_size)
                    spot_market_order = MarketOrder(
                        symbol="spot",
                        side=Side.SELL,
                        price=spot_price,
                        size=close_size,
                        create_timestamp=ts,
                        fee_rate=config["fee_rate"],
                    )
                    future_market_order = MarketOrder(
                        symbol="future",
                        side=Side.BUY,
                        price=future_price,
                        size=close_size,
                        create_timestamp=ts,
                        fee_rate=config["fee_rate"],
                    )
                    state.close_position(
                        spot_market_order,
                        future_market_order,
                        config["collateral_weight"],
                        config["leverage"],
                    )
                    state.append_hedge_trade(ts, HedgeType.CLOSE, basis)

            # log
            logs.append(state.to_log_state(timestamp=ts))

        # liquidation after expiry
        if state.spot_position > 0:
            liquidation_size = state.spot_position
            spot_market_order = MarketOrder(
                symbol="spot",
                side=Side.SELL,
                price=config["expiration_price"],
                size=liquidation_size,
                create_timestamp=config["ts_to_expiry"],
                fee_rate=config["fee_rate"],
            )
            future_market_order = MarketOrder(
                symbol="future",
                side=Side.BUY,
                price=config["expiration_price"],
                size=liquidation_size,
                create_timestamp=config["ts_to_expiry"],
                fee_rate=config["fee_rate"],
            )
            state.close_position(
                spot_market_order,
                future_market_order,
                config["collateral_weight"],
                config["leverage"],
            )
            state.append_hedge_trade(
                config["ts_to_expiry"], HedgeType.CLOSE, Decimal(0)
            )
            logs.append(state.to_log_state(config["ts_to_expiry"]))

        # save summary
        save_path = f"local/backtest/bollinger_{boll_mult}/summary.json"
        backtest_util.save_summary(logs, save_path)

        # plot
        save_path = f"local/backtest/bollinger_{boll_mult}/plot.jpg"
        backtest_util.plot_logs(logs, state.hedge_trades, save_path, to_show=False)
