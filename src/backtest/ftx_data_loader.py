import glob
import os
import pathlib
import pickle
import sys
from datetime import datetime
from typing import Generator

import pandas as pd

from src.backtest.ftx_data_types import Trade

sys.setrecursionlimit(100000)


class DataLoader:
    def __init__(self, data_dir: str):
        path = os.path.join(data_dir, "*.pickle")
        self.files = sorted(glob.glob(path), key=self.parse_filename_start_timestamp)

    def parse_filename_start_timestamp(self, filename: str) -> int:
        return int(pathlib.Path(filename).stem)

    def get_trades_generator(
        self, start: int, end: int
    ) -> Generator[Trade, None, None]:
        files = filter(
            lambda x: start <= self.parse_filename_start_timestamp(x) < end, self.files
        )
        for file in files:
            with open(file, "rb") as f:
                trades = pickle.load(f)
            trades = list(map(Trade.from_json, trades))
            trades = sorted(trades, key=lambda trade: trade.time)
            for trade in trades:
                yield trade

    def get_trades_df(self, start: int, end: int) -> pd.DataFrame:
        files = filter(
            lambda x: start <= self.parse_filename_start_timestamp(x) < end, self.files
        )
        trades_records = []
        for file in files:
            with open(file, "rb") as f:
                trades = pickle.load(f)
            trades_records.extend(trades)
        df = pd.DataFrame.from_records(trades_records)
        df.index = df["time"].apply(datetime.fromtimestamp)
        df.drop(columns="time", inplace=True)
        df.sort_values("id", axis=0, inplace=True)
        return df

    @staticmethod
    def trades_df_2_klines(df: pd.DataFrame, resolution: str) -> pd.DataFrame:
        assert df.index.inferred_type == "datetime64", "must have a datetime index"
        assert "price" in df.columns, "must have a 'price' column"
        assert "size" in df.columns, "must have a 'size' column"
        resampled_df: pd.DataFrame = df.resample(rule=resolution).agg(
            {"price": "ohlc", "size": "sum"}
        )
        resampled_df.columns = resampled_df.columns.droplevel(0)
        return resampled_df
