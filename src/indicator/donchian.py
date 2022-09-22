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
class DonchianParams:
    length: int


class Donchian(BaseIndicator):
    """To use Donchian indicator, one should set parameters in the yaml config file.
    For example:

    indicator:
        name: 'donchian'
        params:
            resolution: 3600  # Enum of (15, 60, 300, 900, 3600, 14400, 86400) in seconds
            length: 20
    """

    def __init__(
        self,
        hedge_pair: FtxHedgePair,
        kline_resolution: FtxCandleResolution,
        params: DonchianParams = None,
    ):
        super().__init__(kline_resolution)
        self.hedge_pair = hedge_pair
        if not params:
            # default params
            self.params: DonchianParams = DonchianParams(length=20)
        else:
            self.params: DonchianParams = params

    @staticmethod
    def compute_thresholds(
        merged_candles_df: pd.DataFrame, params: DonchianParams, as_df=False
    ):
        rolling_high = merged_candles_df["high"].rolling(params.length)
        rolling_low = merged_candles_df["low"].rolling(params.length)
        merged_candles_df["up"] = rolling_high.max()
        merged_candles_df["low"] = rolling_low.min()
        merged_candles_df["mid"] = (merged_candles_df["up"] + merged_candles_df["low"]) / 2

        upper_threshold = merged_candles_df["up"]
        lower_threshold = merged_candles_df["low"]

        if as_df:
            return (
                upper_threshold,
                lower_threshold,
            )
        else:
            upper_threshold.iloc[-1]
            lower_threshold.iloc[-1]

            return upper_threshold, lower_threshold

    # for live trade usage
    async def update_indicator_info(self):
        client = FtxExchange("", "")
        resolution = self._kline_resolution
        end_ts = (time.time() // resolution.value - 1) * resolution.value
        start_ts = end_ts - 2 * self.params.length * resolution.value
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


class DonchianBacktest(Donchian):
    def __init__(
        self,
        hedge_pair: FtxHedgePair,
        kline_resolution: FtxCandleResolution,
        backtest_config: BackTestConfig,
    ):
        super().__init__(hedge_pair, kline_resolution)
        self.config = backtest_config

    def generate_params(self) -> list[DonchianParams]:
        params = []
        for length in np.arange(20, 40, 20):
            length = int(length)
            params.append(DonchianParams(length=length))
        return params

    def get_save_path(self) -> str:
        from_datatime = datetime.fromtimestamp(self.config.start_timestamp)
        from_date_str = from_datatime.strftime("%Y%m%d")
        to_datatime = datetime.fromtimestamp(self.config.end_timestamp)
        to_data_str = to_datatime.strftime("%Y%m%d")
        return f"local/backtest/donchain_{self.hedge_pair.future}_{from_date_str}_{to_data_str}"
