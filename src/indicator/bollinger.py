import time
from decimal import Decimal
from typing import List

import dateutil.parser
import pandas as pd

from src.exchange.ftx.ftx_client import FtxExchange
from src.exchange.ftx.ftx_data_type import FtxCandleResolution, FtxHedgePair
from src.indicator.base_indicator import BaseIndicator


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
        length: int = 20,
        std_mult: float = 2.0,
    ):
        super().__init__(kline_resolution)
        self.hedge_pair = hedge_pair
        self.length = length
        self.std_mult = std_mult

    async def update_indicator_info(self):
        client = FtxExchange("", "")
        resolution = self._kline_resolution
        end_ts = (time.time() // resolution.value - 1) * resolution.value
        start_ts = end_ts - self.length * resolution.value
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
        concat_df = pd.concat([spot_close, future_close], axis=1)
        concat_df["basis"] = concat_df["f_close"] - concat_df["s_close"]
        rolling = concat_df["basis"].rolling(self.length)
        concat_df["ma"] = rolling.mean()
        concat_df["std"] = rolling.std()

        ma = concat_df["ma"].iloc[-1]
        std = concat_df["std"].iloc[-1]

        upper_threshold = ma + self.std_mult * std
        lower_threshold = ma - self.std_mult * std

        self._upper_threshold = Decimal(str(upper_threshold))
        self._lower_threshold = Decimal(str(lower_threshold))
        self._last_kline_start_timestamp = concat_df.index[-1].timestamp()

    def candles_to_df(self, candles: List[dict]) -> pd.DataFrame:
        df = pd.DataFrame.from_records(candles)
        df["startTime"] = df["startTime"].apply(dateutil.parser.parse)
        df["close"] = df["close"].astype("float32")
        df.set_index("startTime", inplace=True)
        df.sort_index(inplace=True)
        return df
