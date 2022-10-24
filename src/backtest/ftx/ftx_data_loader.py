import glob
import os
import pathlib
import pickle
import sys
from typing import List

import pandas as pd

sys.setrecursionlimit(100000)


class FtxDataLoader:
    def __init__(self, data_dir: str):
        path = os.path.join(data_dir, "*.pickle")
        self.files = sorted(glob.glob(path), key=self.parse_filename_start_timestamp)

    def parse_filename_start_timestamp(self, filename: str) -> float:
        return float(pathlib.Path(filename).stem)

    def get_trades_df(self, start: float, end: float) -> pd.DataFrame:
        files = filter(
            lambda x: start <= self.parse_filename_start_timestamp(x) < end, self.files
        )
        all_trades = []
        for file in files:
            with open(file, "rb") as f:
                trades: List[dict] = pickle.load(f)
            all_trades.extend(trades)
        df = pd.DataFrame.from_records(all_trades, exclude=["liquidation"])
        df.index = pd.to_datetime(df["time"], utc=True)
        df.rename(columns={"side": "taker_side"}, inplace=True)
        df.drop(columns=["time"], inplace=True)
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
