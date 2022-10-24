import itertools
import multiprocessing as mp
import os
import pathlib
import sys
import urllib.request
from datetime import datetime

from src.exchange.binance.binance_data_type import BinanceUSDTQuaterHedgePair

ONE_DAY = 86400
BASE_URL = "https://data.binance.vision"


def download_file(download_url: str, save_path: str):
    try:
        dl_file = urllib.request.urlopen(download_url)
        length = dl_file.getheader("content-length")
        if length:
            length = int(length)
            blocksize = max(4096, length // 100)

        with open(save_path, "wb") as out_file:
            dl_progress = 0
            print("File Download: {}".format(save_path))
            while True:
                buf = dl_file.read(blocksize)
                if not buf:
                    break
                dl_progress += len(buf)
                out_file.write(buf)
                done = int(50 * dl_progress / length)
                sys.stdout.write("\r[%s%s]" % ("#" * done, "." * (50 - done)))
                sys.stdout.flush()

    except urllib.error.HTTPError:
        print("File not found: {}".format(download_url))
        pass


def download_binance_trades_if_file_not_exist(
    market: str, start_ts: float, save_path: str
):
    start_ts = start_ts // ONE_DAY * ONE_DAY
    start_dt = datetime.utcfromtimestamp(start_ts)
    start_dt_str = start_dt.strftime("%Y-%m-%d")
    filename = f"{start_ts}.zip"
    path = os.path.join(save_path, market, filename)
    if not os.path.exists(path):
        pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
    else:
        print(f"file already exists! {path}")
        return
    if BinanceUSDTQuaterHedgePair.is_spot(market):
        url = (
            BASE_URL
            + f"/data/spot/daily/aggTrades/{market}/{market}-aggTrades-{start_dt_str}.zip"
        )
    elif BinanceUSDTQuaterHedgePair.is_future(market, market.split("_")[1]):
        url = (
            BASE_URL
            + f"/data/futures/um/daily/aggTrades/{market}/{market}-aggTrades-{start_dt_str}.zip"
        )
    else:
        return
    download_file(url, path)


def run_trades_data_download_process(
    market: str, start_ts: float, end_ts: float, save_path: str = "local/binance/trades"
):
    time_range = range(int(start_ts), int(end_ts), ONE_DAY)

    with mp.Pool(os.cpu_count() // 2) as pool:
        pool.starmap(
            download_binance_trades_if_file_not_exist,
            zip(
                itertools.repeat(market, len(time_range)),
                time_range,
                itertools.repeat(save_path, len(time_range)),
            ),
        )
