import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import List

import dateutil.parser
import numpy as np
import pandas as pd

from src.backtest.ftx_data_types import BackTestConfig
from src.exchange.ftx.ftx_client import FtxExchange
from src.exchange.ftx.ftx_data_type import FtxCandleResolution, FtxHedgePair
from src.indicator.base_indicator import BaseIndicator


@dataclass
class BollingerParams:
    length: int
    std_mult: float


class Bollinger(BaseIndicator):
    """To use Bollinger Band indicator, one should set parameters in the yaml config file.
    For example:

    indicator:
        name: 'bollinger'
        params:
            resolution: 3600  # Enum of (15, 60, 300, 900, 3600, 14400, 86400) in seconds
            length: 20
            std_mult: 2.0
    """

    def __init__(
        self,
        hedge_pair: FtxHedgePair,
        kline_resolution: FtxCandleResolution,
        params: BollingerParams = None,
    ):
        super().__init__(kline_resolution)
        self.hedge_pair = hedge_pair
        if not params:
            # default params
            self.params: BollingerParams = BollingerParams(length=20, std_mult=2.0)
        else:
            self.params: BollingerParams = params

    @staticmethod
    def compute_thresholds(
        merged_candles_df: pd.DataFrame, params: BollingerParams, as_df=False
    ):
        rolling = merged_candles_df["close"].rolling(params.length)
        merged_candles_df["ma"] = rolling.mean()
        merged_candles_df["std"] = rolling.std()

        if as_df:
            return (
                merged_candles_df["ma"] + params.std_mult * merged_candles_df["std"],
                merged_candles_df["ma"] - params.std_mult * merged_candles_df["std"],
            )
        else:
            ma = merged_candles_df["ma"].iloc[-1]
            std = merged_candles_df["std"].iloc[-1]

            upper_threshold = ma + params.std_mult * std
            lower_threshold = ma - params.std_mult * std

            return upper_threshold, lower_threshold

    # for live trade usage
    async def update_indicator_info(self):
        client = FtxExchange("", "")
        resolution = self._kline_resolution
        end_ts = (time.time() // resolution.value - 1) * resolution.value
        start_ts = end_ts - self.params.length * resolution.value
        spot_candles = await client.get_candles(
            self.hedge_pair.spot, resolution, start_ts, end_ts
        )
        if len(spot_candles) == 0:
            return
        future_candles = await client.get_candles(
            self.hedge_pair.future, resolution, start_ts, end_ts
        )
        if len(future_candles) == 0:
            return

        await client.close()

        spot_df = self.candles_to_df(spot_candles)
        future_df = self.candles_to_df(future_candles)
        spot_close = spot_df["close"].rename("s_close")
        future_close = future_df["close"].rename("f_close")
        merged_df = pd.concat([spot_close, future_close], axis=1)
        merged_df["close"] = merged_df["f_close"] - merged_df["s_close"]

        upper_threshold, lower_threshold = self.compute_thresholds(
            merged_df, self.params
        )

        self._upper_threshold = Decimal(str(upper_threshold))
        self._lower_threshold = Decimal(str(lower_threshold))
        self._last_kline_start_timestamp = spot_df.index[-1].timestamp()

    def candles_to_df(self, candles: List[dict]) -> pd.DataFrame:
        df = pd.DataFrame.from_records(candles)
        df["startTime"] = df["startTime"].apply(dateutil.parser.parse)
        df["close"] = df["close"].astype("float32")
        df.set_index("startTime", inplace=True)
        df.sort_index(inplace=True)
        return df


class BollingerBacktest(Bollinger):
    def __init__(
        self,
        hedge_pair: FtxHedgePair,
        kline_resolution: FtxCandleResolution,
        backtest_config: BackTestConfig,
    ):
        super().__init__(hedge_pair, kline_resolution)
        self.config = backtest_config

    def generate_params(self) -> list[BollingerParams]:
        params = []
        for boll_mult in np.arange(0.8, 2.5, 0.1):
            boll_mult = round(boll_mult, 1)
            for length in np.arange(10, 50, 5):
                length = int(length)
                params.append(BollingerParams(length=length, std_mult=boll_mult))
        return params

    def get_save_path(self) -> str:
        from_datatime = datetime.fromtimestamp(self.config.start_timestamp)
        from_date_str = from_datatime.strftime("%Y%m%d")
        to_datatime = datetime.fromtimestamp(self.config.end_timestamp)
        to_data_str = to_datatime.strftime("%Y%m%d")
        return f"local/backtest/bollinger_{self.hedge_pair.coin}_{from_date_str}_{to_data_str}"
