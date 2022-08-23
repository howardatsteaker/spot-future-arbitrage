import time
import datetime

from .ftx_trades_downloader import run_trades_data_download_process
from .ftx_prepare_backtest_data import (
    save_kline_from_trades,
    save_merged_trades,
    save_merged_kline
)
from .backtest_bollinger import run_backtest

from ..exchange.ftx.ftx_data_type import FtxHedgePair

def main():
    start_time = "2022/08/01"
    end_time = "2022/08/22"
    trades_dir = "trades/"
    save_dir = "local/"
    resolution = '1H'

    hedge_pair: FtxHedgePair = FtxHedgePair(
        coin='BTC',
        spot='BTC/USD',
        future='BTC-0930'
    )

    start_timestamp = int(time.mktime(datetime.datetime.strptime(start_time, "%Y/%m/%d").timetuple()))
    end_timestamp = int(time.mktime(datetime.datetime.strptime(end_time, "%Y/%m/%d").timetuple()))

    print(f'download {hedge_pair.spot} trades in {trades_dir}')
    run_trades_data_download_process(hedge_pair.spot, start_timestamp, end_timestamp, trades_dir)

    print(f'download {hedge_pair.future} trades in {trades_dir}')
    run_trades_data_download_process(hedge_pair.future, start_timestamp, end_timestamp, trades_dir)

    spot_data_dir = trades_dir + FtxHedgePair.to_dir_name(hedge_pair.spot)
    future_data_dir = trades_dir + FtxHedgePair.to_dir_name(hedge_pair.future)
    print(f'save spot {hedge_pair.spot} kline from trades')
    save_kline_from_trades(
        spot_data_dir,
        hedge_pair.spot,
        start_timestamp,
        end_timestamp,
        resolution=resolution,
        save_dir=save_dir+'kline'
    )
    print(f'save future {hedge_pair.future} kline from trades')
    save_kline_from_trades(
        future_data_dir,
        hedge_pair.future,
        start_timestamp,
        end_timestamp,
        resolution=resolution,
        save_dir=save_dir+'kline'
    )
    print('save merged trades')
    save_merged_trades(
        hedge_pair.future,
        future_data_dir,
        spot_data_dir,
        start_timestamp,
        end_timestamp,
        save_path=save_dir+'merged_trades'
    )
    print('save merged kline')
    save_merged_kline(
        hedge_pair.future,
        future_data_dir,
        spot_data_dir,
        start_timestamp,
        end_timestamp,
        resolution='1H',
        save_path=save_dir+'merged_kline'
    )

    trades_path = save_dir + 'merged_trades/' + FtxHedgePair.to_dir_name(hedge_pair.future) + \
        '/' + str(start_timestamp) + '_' + str(end_timestamp) + '.parquet'
    spot_klines_path = save_dir + 'kline/' + FtxHedgePair.to_dir_name(hedge_pair.spot) + \
        '/' + str(start_timestamp) + '_' + str(end_timestamp) + '_' + str(resolution) + '.parquet'
    future_klines_path = save_dir + 'kline/' + FtxHedgePair.to_dir_name(hedge_pair.future) + \
        '/' + str(start_timestamp) + '_' + str(end_timestamp) + '_' + str(resolution) + '.parquet'

    print('running backtest...')
    run_backtest(trades_path, spot_klines_path, future_klines_path)

if __name__ == '__main__':
    main()
