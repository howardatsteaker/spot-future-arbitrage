import multiprocessing as mp
import os
import pathlib

import pandas as pd

from src.backtest.binance.binance_data_loader import BinanceDataLoader
from src.backtest.binance.binance_trades_downloader import \
    run_trades_data_download_process
from src.exchange.binance.binance_data_type import (BinanceCandleResolution,
                                                    BinanceUSDTQuaterHedgePair)
from src.exchange.exchange_data_type import HedgePair


def save_kline_from_trades(
    data_dir: str,
    symbol: str,
    start_ts: int,
    end_ts: int,
    resolution: BinanceCandleResolution = BinanceCandleResolution.ONE_HOUR,
    save_dir: str = "local/binance/kline",
    force_run: bool = False,
):
    symbol_path_name = HedgePair.to_dir_name(symbol)
    resolution_rule_str = resolution.to_pandas_resample_rule()
    filename = os.path.join(
        save_dir, symbol_path_name, f"{start_ts}_{end_ts}_{resolution_rule_str}.parquet"
    )
    if force_run or not os.path.exists(filename):
        data_loader = BinanceDataLoader(data_dir)
        trades_df = data_loader.get_trades_df(start_ts, end_ts)
        klines_df = data_loader.trades_df_2_klines(
            trades_df, resolution=resolution_rule_str
        )
        pathlib.Path(filename).parent.mkdir(parents=True, exist_ok=True)
        klines_df.to_parquet(filename)
    else:
        print(f"file {filename} exist")


def save_merged_trades(
    future: str,
    future_dir: str,
    spot_dir: str,
    start_ts: int,
    end_ts: int,
    save_path: str = "local/binance/merged_trades",
    force_run: bool = False,
):
    future_path_name = HedgePair.to_dir_name(future)
    save_path = os.path.join(save_path, future_path_name)
    filename = os.path.join(save_path, f"{start_ts}_{end_ts}.parquet")
    if not os.path.exists(save_path):
        pathlib.Path(save_path).mkdir(parents=True, exist_ok=True)
    if force_run or not os.path.exists(filename):
        spot_data_loader = BinanceDataLoader(spot_dir)
        future_data_loader = BinanceDataLoader(future_dir)
        spot_trades_df = spot_data_loader.get_trades_df(start_ts, end_ts)
        future_trades_df = future_data_loader.get_trades_df(start_ts, end_ts)
        spot_trades_df.rename(
            columns={
                "id": "s_id",
                "price": "s_price",
                "size": "s_size",
                "taker_side": "s_taker_side",
            },
            inplace=True,
        )
        future_trades_df.rename(
            columns={
                "id": "f_id",
                "price": "f_price",
                "size": "f_size",
                "taker_side": "f_taker_side",
            },
            inplace=True,
        )
        resample_1s_spot_df = spot_trades_df.resample("1s").last()
        resample_1s_future_df = future_trades_df.resample("1s").last()
        concat_df = pd.concat([resample_1s_spot_df, resample_1s_future_df], axis=1)
        concat_df.dropna(inplace=True)
        concat_df = concat_df[concat_df["s_taker_side"] != concat_df["f_taker_side"]]
        concat_df["basis"] = concat_df["f_price"] - concat_df["s_price"]
        concat_df.to_parquet(filename)


def save_merged_kline(
    future: str,
    future_dir: str,
    spot_dir: str,
    start_ts: int,
    end_ts: int,
    resolution: BinanceCandleResolution = BinanceCandleResolution.ONE_HOUR,
    save_path: str = "local/binance/merged_kline",
    force_run: bool = False,
):
    future_path_name = HedgePair.to_dir_name(future)
    resolution_rule_str = resolution.to_pandas_resample_rule()
    filename = os.path.join(
        save_path,
        future_path_name,
        f"{start_ts}_{end_ts}_{resolution_rule_str}.parquet",
    )
    path = pathlib.Path(filename)
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    if force_run or not path.exists():
        spot_data_loader = BinanceDataLoader(spot_dir)
        future_data_loader = BinanceDataLoader(future_dir)
        spot_trades_df = spot_data_loader.get_trades_df(start_ts, end_ts)
        future_trades_df = future_data_loader.get_trades_df(start_ts, end_ts)
        spot_trades_df.rename(
            columns={
                "id": "s_id",
                "price": "s_price",
                "size": "s_size",
                "taker_side": "s_taker_side",
            },
            inplace=True,
        )
        future_trades_df.rename(
            columns={
                "id": "f_id",
                "price": "f_price",
                "size": "f_size",
                "taker_side": "f_taker_side",
            },
            inplace=True,
        )
        resample_1s_spot_df = spot_trades_df.resample("1s").last()
        resample_1s_future_df = future_trades_df.resample("1s").last()
        concat_df = pd.concat([resample_1s_spot_df, resample_1s_future_df], axis=1)
        concat_df.dropna(inplace=True)
        concat_df = concat_df[concat_df["s_taker_side"] != concat_df["f_taker_side"]]
        concat_df["basis"] = concat_df["f_price"] - concat_df["s_price"]
        resample: pd.DataFrame = concat_df.resample(resolution_rule_str).agg(
            {"basis": "ohlc"}
        )
        resample = resample.droplevel(0, axis=1)
        resample.to_parquet(path)


def run_binance_data_prepare_process(
    hedge_pair: BinanceUSDTQuaterHedgePair,
    start: float,
    end: float,
    data_dir: str = "local/binance/trades",
    force_run: bool = False,
):
    spot_data_dir = os.path.join(data_dir, HedgePair.to_dir_name(hedge_pair.spot))
    future_data_dir = os.path.join(data_dir, HedgePair.to_dir_name(hedge_pair.future))

    print(f"Download trades in {data_dir}")
    spot_p = mp.Process(
        target=run_trades_data_download_process, args=(hedge_pair.spot, start, end)
    )
    spot_p.start()
    future_p = mp.Process(
        target=run_trades_data_download_process, args=(hedge_pair.future, start, end)
    )
    future_p.start()
    spot_p.join()
    future_p.join()

    print(f"Save spot {hedge_pair.spot} kline from trades")
    save_kline_from_trades(
        spot_data_dir, hedge_pair.spot, start, end, force_run=force_run
    )

    print(f"Save future {hedge_pair.future} kline from trades")
    save_kline_from_trades(
        future_data_dir, hedge_pair.future, start, end, force_run=force_run
    )

    print("Save merged trades")
    save_merged_trades(
        hedge_pair.future,
        future_data_dir,
        spot_data_dir,
        start,
        end,
        force_run=force_run,
    )

    print("Save merged kline")
    save_merged_kline(
        hedge_pair.future,
        future_data_dir,
        spot_data_dir,
        start,
        end,
        force_run=force_run,
    )
