from datetime import datetime

import pytz
from apscheduler.schedulers.blocking import BlockingScheduler

from src.backtest.backtest import run_backtest
from src.backtest.binance.binance_config import (binance_future_config_options,
                                                 binance_get_backtest_config)
from src.backtest.binance.binance_prepare_backtest_data import \
    run_binance_data_prepare_process
from src.backtest.ftx.ftx_config import (ftx_future_config_options,
                                         ftx_get_backtest_config)
from src.backtest.ftx.ftx_prepare_backtest_data import \
    run_ftx_data_prepare_process
from src.exchange.binance.binance_client import (
    BinanceSpotExchange, BinanceUSDMarginFuturesExchange)
from src.exchange.binance.binance_data_type import (BinanceCandleResolution,
                                                    BinanceUSDTQuaterHedgePair)
from src.exchange.ftx.ftx_client import FtxExchange
from src.exchange.ftx.ftx_data_type import FtxCandleResolution, FtxHedgePair
from src.indicator.macd_bollinger import (MACDBollingerBacktest,
                                          MACDBollingerParams)

# from src.indicator.bollinger import BollingerBacktest
# from src.indicator.donchian import DonchianBacktest
# from src.indicator.keltner import KeltnerBacktest
# from src.indicator.macd import MACDBacktest
# from src.indicator.rsi import RSIBacktest


def cron_run(config):
    print(f"{datetime.now()} Running {config['subaccount']} backtest")
    backtest_class = config["backtest_class"]
    exchange_option = config["exchange_option"]
    hedge_config = config["hedge_config"]
    if exchange_option == "Ftx":
        spot_client = FtxExchange("", "")
        future_client = spot_client
        resolution = FtxCandleResolution.ONE_HOUR
        hedge_pair: FtxHedgePair = hedge_config["hedge_pair"]
        backtest_config = ftx_get_backtest_config(hedge_config)
        run_ftx_data_prepare_process(
            hedge_pair,
            backtest_config.start_timestamp,
            backtest_config.end_timestamp,
            force_run=True,
        )
    elif exchange_option == "Binance":
        spot_client = BinanceSpotExchange("", "")
        future_client = BinanceUSDMarginFuturesExchange("", "")
        resolution = BinanceCandleResolution.ONE_HOUR
        hedge_pair: BinanceUSDTQuaterHedgePair = hedge_config["hedge_pair"]
        backtest_config = binance_get_backtest_config(hedge_config)
        run_binance_data_prepare_process(
            hedge_pair,
            backtest_config.start_timestamp,
            backtest_config.end_timestamp,
            force_run=True,
        )

    indicator = backtest_class(
        hedge_pair=hedge_pair,
        kline_resolution=resolution,
        spot_client=spot_client,
        future_client=future_client,
        backtest_config=backtest_config,
    )

    print("running backtest...")
    run_backtest(indicator, params_list=[config["params"]], force_run=True)


def main():
    subaccount_configs = [
        {
            "subaccount": "PROD_ARB_HW",
            "backtest_class": MACDBollingerBacktest,
            "exchange_option": "Ftx",
            "hedge_config": ftx_future_config_options[3],
            "params": MACDBollingerParams(
                macd_std_mult=1.2,
                boll_std_mult=0.7,
                lower_bound_factor=0.95,
                dea_positive_filter=True,
                negtive_filter_type="macd",
            ),
        },
        {
            "subaccount": "PROD_FARB_HW",
            "backtest_class": MACDBollingerBacktest,
            "exchange_option": "Ftx",
            "hedge_config": ftx_future_config_options[-1],
            "params": MACDBollingerParams(
                macd_std_mult=1.2,
                boll_std_mult=0.7,
                lower_bound_factor=0.95,
                dea_positive_filter=False,
                negtive_filter_type="macd",
            ),
        },
    ]
    for config in subaccount_configs:
        cron_run(config)


if __name__ == "__main__":
    sched = BlockingScheduler()
    sched.add_job(main, "cron", hour=0, minute=5, timezone=pytz.utc)
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        pass
