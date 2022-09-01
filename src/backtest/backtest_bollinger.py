import pathlib
from dataclasses import asdict
from datetime import timedelta
from decimal import Decimal
from os.path import exists

import numpy as np
import pandas as pd

from src.backtest import backtest_util
from src.backtest.ftx_data_types import BaseState, HedgeType, MarketOrder, Side
from src.indicator.base_indicator import BaseIndicator


def run_backtest(backtest_indicator: BaseIndicator):
    trades = pd.read_parquet(backtest_indicator.get_trades_path())
    spot_klines = pd.read_parquet(backtest_indicator.get_spot_klines_path())
    future_klines = pd.read_parquet(backtest_indicator.get_future_klines_path())

    save_path = backtest_indicator.get_save_path()
    save_path_obj = pathlib.Path(save_path)
    summary_path_obj = save_path_obj / "summary.json"
    if summary_path_obj.exists():
        print(f"summary file {summary_path_obj} exist")
        return

    summary_list = []
    index = 0
    for params in backtest_indicator.generate_params():
        print(f"params: {params}")

        upper_threshold_df, lower_threshold_df = backtest_indicator.compute_thresholds(
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
            if ts >= backtest_indicator.config.ts_to_expiry:
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
                and ts < backtest_indicator.config.ts_to_stop_open
            ):
                spot_market_order = MarketOrder(
                    symbol="spot",
                    side=Side.BUY,
                    price=spot_price,
                    size=max_available_size,
                    create_timestamp=ts,
                    fee_rate=backtest_indicator.config.fee_rate,
                )
                future_market_order = MarketOrder(
                    symbol="future",
                    side=Side.SELL,
                    price=future_price,
                    size=max_available_size,
                    create_timestamp=ts,
                    fee_rate=backtest_indicator.config.fee_rate,
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
                        backtest_indicator.config.collateral_weight,
                        backtest_indicator.config.leverage,
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
                        fee_rate=backtest_indicator.config.fee_rate,
                    )
                    future_market_order = MarketOrder(
                        symbol="future",
                        side=Side.BUY,
                        price=future_price,
                        size=close_size,
                        create_timestamp=ts,
                        fee_rate=backtest_indicator.config.fee_rate,
                    )
                    state.close_position(
                        spot_market_order,
                        future_market_order,
                        backtest_indicator.config.collateral_weight,
                        backtest_indicator.config.leverage,
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
                price=backtest_indicator.config.expiration_price,
                size=liquidation_size,
                create_timestamp=backtest_indicator.config.ts_to_expiry,
                fee_rate=backtest_indicator.config.fee_rate,
            )
            future_market_order = MarketOrder(
                symbol="future",
                side=Side.BUY,
                price=backtest_indicator.config.expiration_price,
                size=liquidation_size,
                create_timestamp=backtest_indicator.config.ts_to_expiry,
                fee_rate=backtest_indicator.config.fee_rate,
            )
            state.close_position(
                spot_market_order,
                future_market_order,
                backtest_indicator.config.collateral_weight,
                backtest_indicator.config.leverage,
            )
            state.append_hedge_trade(
                backtest_indicator.config.ts_to_expiry, HedgeType.CLOSE, Decimal(0)
            )
            logs.append(state.to_log_state(backtest_indicator.config.ts_to_expiry))

        # save summary
        summary_dict = backtest_util.logs_to_summary(logs)
        summary_dict["index"] = index
        summary_dict["params"] = asdict(params)
        summary_list.append(summary_dict)

        # plot if not exist
        plot_path = f"{save_path}/plot_{str(index)}.jpg"
        if not exists(plot_path):
            backtest_util.plot_logs(logs, state.hedge_trades, plot_path, to_show=False)

        index += 1

    summary_path = f"{save_path}/summary.json"
    result_dict = {
        # can put other info
        "results": summary_list,
    }
    backtest_util.save_summary(result_dict, summary_path)
