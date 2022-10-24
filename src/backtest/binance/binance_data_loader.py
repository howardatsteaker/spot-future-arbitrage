import glob
import os
import pathlib

import numpy as np
import pandas as pd


class BinanceDataLoader:
    def __init__(self, data_dir: str):
        path = os.path.join(data_dir, "*.zip")
        self.files = glob.glob(path)

    def parse_filename_start_timestamp(self, filename: str) -> float:
        return float(pathlib.Path(filename).stem)

    def get_trades_df(self, start_ts: float, end_ts: float) -> pd.DataFrame:
        files = filter(
            lambda x: start_ts <= self.parse_filename_start_timestamp(x) < end_ts,
            self.files,
        )
        dfs = []
        csv_header = [
            "agg_trade_id",
            "price",
            "quantity",
            "first_trade_id",
            "last_trade_id",
            "transact_time",
            "is_buyer_maker",
        ]
        dtype = {
            "agg_trade_id": np.int64,
            "price": np.float64,
            "quantity": np.float64,
            "first_trade_id": np.int64,
            "last_trade_id": np.int64,
            "transact_time": np.int64,
            "is_buyer_maker": np.bool8,
        }
        for file in files:
            # some file does not have header, so try to read the first line
            test_df = pd.read_csv(file, compression="zip", nrows=1)
            if test_df.columns[0] == "agg_trade_id":
                df = pd.read_csv(
                    file, compression="zip", usecols=[0, 1, 2, 3, 4, 5, 6], dtype=dtype
                )
            else:
                df = pd.read_csv(
                    file,
                    compression="zip",
                    header=None,
                    usecols=[0, 1, 2, 3, 4, 5, 6],
                    dtype=dtype,
                )
                df.columns = csv_header
            df.rename(
                columns={
                    "transact_time": "time",
                    "agg_trade_id": "id",
                    "quantity": "size",
                },
                inplace=True,
            )
            df["taker_side"] = df["is_buyer_maker"].apply(
                lambda x: "sell" if x else "buy"
            )
            df.index = pd.to_datetime(df["time"], unit="ms", utc=True)
            df.drop(
                columns=["time", "first_trade_id", "last_trade_id", "is_buyer_maker"],
                inplace=True,
            )
            dfs.append(df)

        df = pd.concat(dfs, axis=0)
        df.sort_values(by="id", inplace=True)

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
