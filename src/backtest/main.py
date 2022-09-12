from pick import pick

from src.backtest.backtest_util import resolution_to_dir_name
from src.exchange.ftx.ftx_data_type import FtxCandleResolution
from src.indicator.bollinger import BollingerBacktest
from src.indicator.macd import MACDBacktest

from ..exchange.ftx.ftx_data_type import FtxHedgePair
from .backtest import run_backtest
from .config import future_config_options, get_backtest_config
from .ftx_prepare_backtest_data import (save_kline_from_trades,
                                        save_merged_kline, save_merged_trades)
from .ftx_trades_downloader import run_trades_data_download_process


def main():
    trades_dir = "local/trades/"

    class_title = "Choose Strategy Class:"
    class_options = [BollingerBacktest, MACDBacktest]

    class_option, _ = pick(class_options, class_title, indicator="=>")
    backtest_class = class_option

    future_title = "Choose Backtest Future Options"
    _, index = pick(
        [option["name"] for option in future_config_options],
        future_title,
        indicator="=>",
    )
    hedge_config = future_config_options[index]
    hedge_pair = hedge_config["hedge_pair"]

    resolution = FtxCandleResolution.ONE_HOUR
    backtest_config = get_backtest_config(hedge_config)

    indicator = backtest_class(
        hedge_pair=hedge_pair,
        kline_resolution=resolution,
        backtest_config=backtest_config,
    )

    print(f"download {hedge_pair.spot} trades in {trades_dir}")
    run_trades_data_download_process(
        hedge_pair.spot,
        backtest_config.start_timestamp,
        backtest_config.end_timestamp,
        trades_dir,
    )

    print(f"download {hedge_pair.future} trades in {trades_dir}")
    run_trades_data_download_process(
        hedge_pair.future,
        backtest_config.start_timestamp,
        backtest_config.end_timestamp,
        trades_dir,
    )

    spot_data_dir = trades_dir + FtxHedgePair.to_dir_name(hedge_pair.spot)
    future_data_dir = trades_dir + FtxHedgePair.to_dir_name(hedge_pair.future)
    print(f"save spot {hedge_pair.spot} kline from trades")
    save_kline_from_trades(
        spot_data_dir,
        hedge_pair.spot,
        backtest_config.start_timestamp,
        backtest_config.end_timestamp,
        resolution=resolution_to_dir_name(resolution),
        save_dir=backtest_config.save_dir + "kline",
    )
    print(f"save future {hedge_pair.future} kline from trades")
    save_kline_from_trades(
        future_data_dir,
        hedge_pair.future,
        backtest_config.start_timestamp,
        backtest_config.end_timestamp,
        resolution=resolution_to_dir_name(resolution),
        save_dir=backtest_config.save_dir + "kline",
    )
    print("save merged trades")
    save_merged_trades(
        hedge_pair.future,
        future_data_dir,
        spot_data_dir,
        backtest_config.start_timestamp,
        backtest_config.end_timestamp,
        save_path=backtest_config.save_dir + "merged_trades",
    )
    print("save merged kline")
    save_merged_kline(
        hedge_pair.future,
        future_data_dir,
        spot_data_dir,
        backtest_config.start_timestamp,
        backtest_config.end_timestamp,
        resolution=resolution_to_dir_name(resolution),
        save_path=backtest_config.save_dir + "merged_kline",
    )

    print("running backtest...")
    run_backtest(indicator)


if __name__ == "__main__":
    main()
