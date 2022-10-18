from pick import pick

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
from src.indicator.bollinger import BollingerBacktest
from src.indicator.donchian import DonchianBacktest
from src.indicator.keltner import KeltnerBacktest
from src.indicator.macd import MACDBacktest
from src.indicator.macd_bollinger import MACDBollingerBacktest
from src.indicator.rsi import RSIBacktest


def main():
    class_title = "Choose Strategy Class:"
    class_options = [
        BollingerBacktest,
        DonchianBacktest,
        KeltnerBacktest,
        MACDBollingerBacktest,
        MACDBacktest,
        RSIBacktest,
    ]

    class_option, _ = pick(class_options, class_title, indicator="=>")
    backtest_class = class_option

    exchange_title = "Choose Exchange:"
    exchange_options = ["Ftx", "Binance"]

    exchange_option, _ = pick(exchange_options, exchange_title, indicator="=>")
    if exchange_option == "Ftx":
        spot_client = FtxExchange("", "")
        future_client = spot_client
        resolution = FtxCandleResolution.ONE_HOUR
        future_title = "Choose Backtest Future Options"
        _, index = pick(
            [option["name"] for option in ftx_future_config_options],
            future_title,
            indicator="=>",
        )
        hedge_config = ftx_future_config_options[index]
        hedge_pair: FtxHedgePair = hedge_config["hedge_pair"]
        backtest_config = ftx_get_backtest_config(hedge_config)
        run_ftx_data_prepare_process(
            hedge_pair, backtest_config.start_timestamp, backtest_config.end_timestamp
        )
    elif exchange_option == "Binance":
        spot_client = BinanceSpotExchange("", "")
        future_client = BinanceUSDMarginFuturesExchange("", "")
        resolution = BinanceCandleResolution.ONE_HOUR
        future_title = "Choose Backtest Future Options"
        _, index = pick(
            [option["name"] for option in binance_future_config_options],
            future_title,
            indicator="=>",
        )
        hedge_config = binance_future_config_options[index]
        hedge_pair: BinanceUSDTQuaterHedgePair = hedge_config["hedge_pair"]
        backtest_config = binance_get_backtest_config(hedge_config)
        run_binance_data_prepare_process(
            hedge_pair, backtest_config.start_timestamp, backtest_config.end_timestamp
        )

    indicator = backtest_class(
        hedge_pair=hedge_pair,
        kline_resolution=resolution,
        spot_client=spot_client,
        future_client=future_client,
        backtest_config=backtest_config,
    )

    print("running backtest...")
    run_backtest(indicator)


if __name__ == "__main__":
    main()
