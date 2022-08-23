import os
from typing import List
import pickle
import pathlib
import requests
from argparse import ArgumentParser
import dateutil.parser
import multiprocessing as mp
import itertools
from src.backtest.ftx_data_types import Trade

ONE_DAY = 86400


def get_one_day_trades_from_ftx(symbol: str, start: float) -> List[Trade]:
    start = start // ONE_DAY * ONE_DAY
    end = start + ONE_DAY
    session = requests.Session()
    trades = []

    while True:
        parmas = {'market_name': symbol, 'start_time': start, 'end_time': end}
        res = session.get(f"https://ftx.com/api/markets/{symbol}/trades", params=parmas)
        data = res.json()['result']
        if len(data) == 0:
            break
        trades.extend(list(map(Trade.ftx_map, data)))
        min_time: str = min(data, key=lambda x: dateutil.parser.parse(x['time']))['time']
        end = dateutil.parser.parse(min_time).timestamp() - 0.00001

    return trades


def download_ftx_trades_if_file_not_exist(symbol: str, start: int, save_path: str):
    symbol_name_for_path = symbol.replace('-', '_').replace('/', '_')
    start = start // ONE_DAY * ONE_DAY
    path = os.path.join(save_path, symbol_name_for_path, f"{start}.pickle")
    if os.path.exists(path):
        print(f"{path} exists")
        return
    trades = get_one_day_trades_from_ftx(symbol, start)
    if len(trades) > 0:
        save_pickle(trades, symbol_name_for_path, f"{start}", save_path)
    else:
        print(f"{symbol} {start} is empty")


def save_pickle(trades: List[Trade], symbol: str, start_str: str, save_path: str):
    trades_json = [t.to_json() for t in trades]
    path = os.path.join(save_path, symbol)
    if not os.path.exists(path):
        try:
            pathlib.Path(path).mkdir(parents=True, exist_ok=True)
        except OSError:
            pass
    filename = os.path.join(path, f"{start_str}.pickle")
    with open(filename, 'wb') as handle:
        pickle.dump(trades_json, handle, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"{len(trades)} rows saved in {filename}")


def run_trades_data_download_process(market: str, start_ts: int, end_ts: int, save_path: str):
    time_range = range(start_ts, end_ts, ONE_DAY)

    with mp.Pool(os.cpu_count() // 2) as pool:
        pool.starmap(download_ftx_trades_if_file_not_exist, zip(
            itertools.repeat(market, len(time_range)),
            time_range,
            itertools.repeat(save_path, len(time_range))))


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-m", "--market", required=True, help="market symbol")
    parser.add_argument("-s", "--start_ts", required=True, type=int, help="start timestamp")
    parser.add_argument("-e", "--end_ts", required=True, type=int, help="end timestamp")
    parser.add_argument("-d", "--dir", required=True, help="directory to save trades data")
    args = parser.parse_args()
    run_trades_data_download_process(args.market, args.start_ts, args.end_ts, args.dir)
