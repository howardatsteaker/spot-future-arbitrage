import datetime
import time
from decimal import Decimal

from pick import pick

from src.backtest.backtest_util import resolution_to_dir_name
from src.exchange.ftx.ftx_data_type import FtxCandleResolution
from src.indicator.bollinger import BollingerBacktest
from src.indicator.macd import MACDBacktest

from ..exchange.ftx.ftx_data_type import FtxHedgePair
from .backtest import run_backtest
from .ftx_data_types import BackTestConfig
from .ftx_prepare_backtest_data import (save_kline_from_trades,
                                        save_merged_kline, save_merged_trades)
from .ftx_trades_downloader import run_trades_data_download_process


def main():
    start_time = "2022/03/26"
    end_time = "2022/06/24"
    expiration_time = "2022/06/24"
    trades_dir = "local/trades/"
    save_dir = "local/"

    title = "Choose Strategy Class:"
    options = [BollingerBacktest, MACDBacktest]

    option, _ = pick(options, title, indicator="=>")

    backtest_class = option
    resolution = FtxCandleResolution.ONE_HOUR

    hedge_pair: FtxHedgePair = FtxHedgePair(
        coin="BTC", spot="BTC/USD", future="BTC-0624"
    )

    start_timestamp = int(
        time.mktime(datetime.datetime.strptime(start_time, "%Y/%m/%d").timetuple())
    )
    end_timestamp = int(
        time.mktime(datetime.datetime.strptime(end_time, "%Y/%m/%d").timetuple())
    )
    expiration_timestamp = int(
        time.mktime(datetime.datetime.strptime(expiration_time, "%Y/%m/%d").timetuple())
    )

    config: BackTestConfig = BackTestConfig(
        fee_rate=Decimal("0.000228"),
        collateral_weight=Decimal("0.975"),
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        ts_to_stop_open=expiration_timestamp - 86400,
        ts_to_expiry=expiration_timestamp,
        expiration_price=Decimal("21141.1"),
        leverage=Decimal("3"),
        save_dir=save_dir,
    )

    indicator = backtest_class(
        hedge_pair=hedge_pair, kline_resolution=resolution, backtest_config=config
    )

    print(f"download {hedge_pair.spot} trades in {trades_dir}")
    run_trades_data_download_process(
        hedge_pair.spot, start_timestamp, end_timestamp, trades_dir
    )

    print(f"download {hedge_pair.future} trades in {trades_dir}")
    run_trades_data_download_process(
        hedge_pair.future, start_timestamp, end_timestamp, trades_dir
    )

    spot_data_dir = trades_dir + FtxHedgePair.to_dir_name(hedge_pair.spot)
    future_data_dir = trades_dir + FtxHedgePair.to_dir_name(hedge_pair.future)
    print(f"save spot {hedge_pair.spot} kline from trades")
    save_kline_from_trades(
        spot_data_dir,
        hedge_pair.spot,
        start_timestamp,
        end_timestamp,
        resolution=resolution_to_dir_name(resolution),
        save_dir=save_dir + "kline",
    )
    print(f"save future {hedge_pair.future} kline from trades")
    save_kline_from_trades(
        future_data_dir,
        hedge_pair.future,
        start_timestamp,
        end_timestamp,
        resolution=resolution_to_dir_name(resolution),
        save_dir=save_dir + "kline",
    )
    print("save merged trades")
    save_merged_trades(
        hedge_pair.future,
        future_data_dir,
        spot_data_dir,
        start_timestamp,
        end_timestamp,
        save_path=save_dir + "merged_trades",
    )
    print("save merged kline")
    save_merged_kline(
        hedge_pair.future,
        future_data_dir,
        spot_data_dir,
        start_timestamp,
        end_timestamp,
        resolution=resolution_to_dir_name(resolution),
        save_path=save_dir + "merged_kline",
    )

    print("running backtest...")
    run_backtest(indicator)


if __name__ == "__main__":
    main()
