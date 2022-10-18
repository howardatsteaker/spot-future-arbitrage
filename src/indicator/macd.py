import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import List

import dateutil.parser
import numpy as np
import pandas as pd

from src.backtest.backtest_data_type import BackTestConfig
from src.exchange.exchange_data_type import (CandleResolution, ExchangeBase,
                                             HedgePair, Kline)
from src.indicator.base_indicator import BaseIndicator


@dataclass
class MACDParams:
    fast_length: int
    slow_length: float
    signal_length: int
    std_length: int
    std_mult: float

    @property
    def alpha_fast(self):
        return 2 / (self.fast_length + 1)

    @property
    def alpha_slow(self):
        return 2 / (self.slow_length + 1)

    @property
    def alpha_macd(self):
        return 2 / (self.signal_length + 1)


class MACD(BaseIndicator):
    """To use MACD indicator, one should set parameters in the yaml config file.
    For example:

    indicator:
        name: 'macd'
        params:
            kline_resolution: 3600  # Enum of (15, 60, 300, 900, 3600, 14400, 86400) in seconds
            fast_length: 12
            slow_length: 26
            signal_length: 9
            std_length: 20
            std_mult: 1.0
    """

    def __init__(
        self,
        hedge_pair: HedgePair,
        kline_resolution: CandleResolution,
        spot_client: ExchangeBase,
        future_client: ExchangeBase,
        params: MACDParams = None,
    ):
        super().__init__(kline_resolution)
        self.hedge_pair = hedge_pair
        self.spot_client: ExchangeBase = spot_client
        self.future_client: ExchangeBase = future_client
        if not params:
            # default params
            self.params: MACDParams = MACDParams(
                fast_length=12,
                slow_length=26,
                signal_length=9,
                std_length=20,
                std_mult=1.0,
            )
        else:
            self.params: MACDParams = params

    @staticmethod
    def compute_thresholds(merged_candles_df, params: MACDParams, as_df=False):
        merged_candles_df["fast_ema"] = (
            merged_candles_df["close"].ewm(span=params.fast_length).mean()
        )
        merged_candles_df["slow_ema"] = (
            merged_candles_df["close"].ewm(span=params.slow_length).mean()
        )
        merged_candles_df["dif"] = (
            merged_candles_df["fast_ema"] - merged_candles_df["slow_ema"]
        )
        merged_candles_df["macd"] = (
            merged_candles_df["dif"].ewm(span=params.signal_length).mean()
        )
        merged_candles_df["dif_sub_macd"] = (
            merged_candles_df["dif"] - merged_candles_df["macd"]
        )
        merged_candles_df["std"] = (
            merged_candles_df["dif_sub_macd"].rolling(params.std_length).std()
        )

        upper_threshold_df = (
            (params.std_mult * merged_candles_df["std"]) / (1 - params.alpha_macd)
            + merged_candles_df["macd"]
            - (
                (1 - params.alpha_fast) * merged_candles_df["fast_ema"]
                - (1 - params.alpha_slow) * merged_candles_df["slow_ema"]
            )
        ) / (params.alpha_fast - params.alpha_slow)

        lower_threshold_df = (
            (-params.std_mult * merged_candles_df["std"]) / (1 - params.alpha_macd)
            + merged_candles_df["macd"]
            - (
                (1 - params.alpha_fast) * merged_candles_df["fast_ema"]
                - (1 - params.alpha_slow) * merged_candles_df["slow_ema"]
            )
        ) / (params.alpha_fast - params.alpha_slow)

        if as_df:
            return (upper_threshold_df, lower_threshold_df)
        else:
            return (upper_threshold_df.iloc[-1], lower_threshold_df.iloc[-1])

    async def update_indicator_info(self):
        resolution = self._kline_resolution
        end_ts = (time.time() // resolution.value - 1) * resolution.value
        start_ts = end_ts - 2 * self.params.slow_length * resolution.value
        try:
            spot_candles = await self.spot_client.get_candles(
                self.hedge_pair.spot, resolution, start_ts, end_ts
            )
            if len(spot_candles) == 0:
                return
            future_candles = await self.future_client.get_candles(
                self.hedge_pair.future, resolution, start_ts, end_ts
            )
            if len(future_candles) == 0:
                return
        finally:
            await self.spot_client.close()
            await self.future_client.close()

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


class MACDBacktest(MACD):
    def __init__(
        self,
        hedge_pair: HedgePair,
        kline_resolution: CandleResolution,
        spot_client: ExchangeBase,
        future_client: ExchangeBase,
        backtest_config: BackTestConfig,
    ):
        super().__init__(
            hedge_pair=hedge_pair,
            kline_resolution=kline_resolution,
            spot_client=spot_client,
            future_client=future_client,
        )
        self.config = backtest_config

    def generate_params(self) -> list[MACDParams]:
        params = []
        for std_mult in np.arange(0.9, 3, 0.1):
            std_mult = round(std_mult, 1)
            params.append(
                MACDParams(
                    fast_length=12,
                    slow_length=26,
                    signal_length=9,
                    std_length=20,
                    std_mult=std_mult,
                )
            )
        return params

    def get_save_path(self) -> str:
        from_datatime = datetime.fromtimestamp(self.config.start_timestamp)
        from_date_str = from_datatime.strftime("%Y%m%d")
        to_datatime = datetime.fromtimestamp(self.config.end_timestamp)
        to_data_str = to_datatime.strftime("%Y%m%d")
        return f"local/backtest/macd_{self.future_client.name}_{self.hedge_pair.future}_{from_date_str}_{to_data_str}"
