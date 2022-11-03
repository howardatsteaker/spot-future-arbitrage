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
class RSIParams:
    length: int
    upper_limit: float
    lower_limit: float


class RSI(BaseIndicator):
    """To use RSI indicator, one should set parameters in the yaml config file.
    For example:

    indicator:
        name: 'rsi'
        params:
            resolution: 3600  # Enum of (15, 60, 300, 900, 3600, 14400, 86400) in seconds
            length: 14
            upper_limit: 70
            lower_limit: 30
    """

    def __init__(
        self,
        hedge_pair: HedgePair,
        kline_resolution: CandleResolution,
        spot_client: ExchangeBase,
        future_client: ExchangeBase,
        params: RSIParams = None,
    ):
        super().__init__(kline_resolution)
        self.hedge_pair = hedge_pair
        self.spot_client: ExchangeBase = spot_client
        self.future_client: ExchangeBase = future_client
        if not params:
            # default params
            self.params: RSIParams = RSIParams(
                length=14, upper_limit=70, lower_limit=30
            )
        else:
            self.params: RSIParams = params

    @staticmethod
    def compute_thresholds(
        merged_candles_df: pd.DataFrame, params: RSIParams, as_df=False
    ):
        close = merged_candles_df["close"]
        diff = close.diff(1)
        pos = diff.apply(lambda x: max(0, x))
        neg = diff.apply(lambda x: -min(0, x))
        rolling_pos = pos.rolling(params.length).mean()
        rolling_neg = neg.rolling(params.length).mean()

        # calculate thresholds invertly
        curr_loss_with_ups = rolling_neg * (params.length - 1) / params.length
        curr_gain_wtih_downs = rolling_pos * (params.length - 1) / params.length
        upper_threshold = (
            params.upper_limit
            * curr_loss_with_ups
            / (100 - params.upper_limit)
            * params.length
            - rolling_pos * (params.length - 1)
            + close
        )
        lower_threshold = (
            close
            - (100 - params.lower_limit)
            / params.lower_limit
            * curr_gain_wtih_downs
            * params.length
            - rolling_neg * (params.length - 1)
        )
        if as_df:
            return (
                upper_threshold,
                lower_threshold,
            )
        else:
            return upper_threshold.iloc[-1], lower_threshold.iloc[-1]

    # for live trade usage
    async def update_indicator_info(self):
        resolution = self._kline_resolution
        end_ts = (time.time() // resolution.value - 1) * resolution.value
        start_ts = end_ts - 2 * self.params.length * resolution.value

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

        spot_df = self.candles_to_df(spot_candles)
        future_df = self.candles_to_df(future_candles)
        spot_close = spot_df["close"].rename("s_close")
        future_close = future_df["close"].rename("f_close")
        merged_df = pd.concat([spot_close, future_close], axis=1)
        merged_df["close"] = merged_df["f_close"] - merged_df["s_close"]
        print(merged_df)

        upper_threshold, lower_threshold = self.compute_thresholds(
            merged_df, self.params
        )

        self._upper_threshold = Decimal(str(upper_threshold))
        self._lower_threshold = Decimal(str(lower_threshold))
        self._last_kline_start_timestamp = spot_df.index[-1].timestamp()

    def candles_to_df(self, candles: List[dict]) -> pd.DataFrame:
        df = pd.DataFrame.from_records(candles)
        df["close"] = df["close"].astype("float32")
        df.set_index("start_time", inplace=True)
        df.sort_index(inplace=True)
        return df


class RSIBacktest(RSI):
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

    def generate_params(self) -> list[RSIParams]:
        params = []
        for length in np.arange(7, 28, 7):
            length = int(length)
            for lower_limit, upper_limit in zip(range(15, 40, 5), range(75, 60, -5)):
                params.append(
                    RSIParams(
                        length=length, lower_limit=lower_limit, upper_limit=upper_limit
                    )
                )
        return params

    def get_save_path(self) -> str:
        from_datatime = datetime.fromtimestamp(self.config.start_timestamp)
        from_date_str = from_datatime.strftime("%Y%m%d")
        to_datatime = datetime.fromtimestamp(self.config.end_timestamp)
        to_data_str = to_datatime.strftime("%Y%m%d")
        return f"local/backtest/rsi_{self.future_client.name}_{self.hedge_pair.future}_{from_date_str}_{to_data_str}"
