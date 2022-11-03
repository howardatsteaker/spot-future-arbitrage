import itertools
import multiprocessing as mp
import os
import pathlib
import pickle
import time
from typing import List

import dateutil.parser
import requests

from src.exchange.exchange_data_type import HedgePair

ONE_DAY = 86400


def get_one_day_trades_from_ftx(symbol: str, start: float) -> List[dict]:
    start = start // ONE_DAY * ONE_DAY
    end = start + ONE_DAY
    session = requests.Session()
    all_trades = []

    while True:
        parmas = {"market_name": symbol, "start_time": start, "end_time": end}
        res = session.get(f"https://ftx.com/api/markets/{symbol}/trades", params=parmas)
        trades: List[dict] = res.json()["result"]
        if len(trades) == 0:
            break
        all_trades.extend(trades)
        min_time: str = min(
            trades, key=lambda trade: dateutil.parser.parse(trade["time"])
        )["time"]
        min_time_ts: float = dateutil.parser.parse(min_time).timestamp()
        end = min_time_ts - 0.00001

    return all_trades


def download_ftx_trades_if_file_not_exist(symbol: str, start: float, save_path: str):
    symbol_name_for_path = HedgePair.to_dir_name(symbol)
    start = start // ONE_DAY * ONE_DAY
    path = os.path.join(save_path, symbol_name_for_path, f"{start}.pickle")
    if os.path.exists(path):
        print(f"{path} exists")
        return
    trades = get_one_day_trades_from_ftx(symbol, start)
    if len(trades) > 0:
        save_pickle(trades, path)
    else:
        print(f"{symbol} {start} is empty")


def save_pickle(trades: List[dict], save_path: str):
    path = pathlib.Path(save_path)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass
    with open(path, "wb") as handle:
        pickle.dump(trades, handle, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"{len(trades)} rows saved in {path}")


def run_trades_data_download_process(
    market: str, start_ts: float, end_ts: float, save_path: str = "local/ftx/trades"
):
    end_ts = min(end_ts, time.time())
    time_range = range(int(start_ts), int(end_ts), ONE_DAY)

    with mp.Pool(os.cpu_count() // 2) as pool:
        pool.starmap(
            download_ftx_trades_if_file_not_exist,
            zip(
                itertools.repeat(market, len(time_range)),
                time_range,
                itertools.repeat(save_path, len(time_range)),
            ),
        )
