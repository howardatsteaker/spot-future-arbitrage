import asyncio
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
class KeltnerParams:
    length: int
    mult: float


class Keltner(BaseIndicator):
    """To use Keltner Channel indicator, one should set parameters in the yaml config file.
    For example:

    indicator:
        name: 'keltner'
        params:
            resolution: 3600  # Enum of (15, 60, 300, 900, 3600, 14400, 86400) in seconds
            length: 20
            mult: 1.0
    """

    def __init__(
        self,
        hedge_pair: HedgePair,
        kline_resolution: CandleResolution,
        spot_client: ExchangeBase,
        future_client: ExchangeBase,
        params: KeltnerParams = None,
    ):
        super().__init__(kline_resolution)
        self.hedge_pair = hedge_pair
        self.spot_client: ExchangeBase = spot_client
        self.future_client: ExchangeBase = future_client
        if not params:
            # default params
            self.params: KeltnerParams = KeltnerParams(length=20, mult=1.0)
        else:
            self.params: KeltnerParams = params

    @staticmethod
    def compute_thresholds(
        merged_candles_df: pd.DataFrame, params: KeltnerParams, as_df=False
    ):
        high = merged_candles_df["high"]
        low = merged_candles_df["low"]
        close = merged_candles_df["close"]
        range1 = high - low
        range2 = high - close.shift(1)
        range3 = close.shift(1) - low
        tr = pd.concat([range1, range2, range3], axis=1).max(axis=1, skipna=True)
        atr = tr.rolling(params.length).mean()
        ma = close.rolling(params.length).mean()
        upper_threshold = ma + params.mult * atr
        lower_threshold = ma - params.mult * atr

        if as_df:
            return (
                upper_threshold,
                lower_threshold,
            )
        else:
            return upper_threshold.iloc[-1], lower_threshold.iloc[-1]

    # for live trade usage
    async def update_indicator_info(self):
        raise NotImplementedError

    #     resolution = self._kline_resolution
    #     end_ts = time.time() // resolution.value * resolution.value
    #     start_ts = end_ts - 2 * self.params.length * resolution.value
    #     try:
    #         spot_trades, future_trades = await asyncio.gather(
    #             self.spot_client.get_trades(self.hedge_pair.spot, start_ts, end_ts),
    #             self.future_client.get_trades(self.hedge_pair.future, start_ts, end_ts),
    #         )
    #     finally:
    #         await self.spot_client.close()
    #         await self.future_client.close()
    #     merged_df = self.merge_trades_to_candle_df(spot_trades, future_trades)

    #     upper_threshold, lower_threshold = self.compute_thresholds(
    #         merged_df, self.params
    #     )

    #     self._upper_threshold = Decimal(str(upper_threshold))
    #     self._lower_threshold = Decimal(str(lower_threshold))
    #     self._last_kline_start_timestamp = merged_df.index[-1].timestamp()

    # def merge_trades_to_candle_df(self, spot_trades, future_trades) -> pd.DataFrame:
    #     spot_trades_df = pd.DataFrame.from_records(spot_trades)
    #     spot_trades_df["time"] = spot_trades_df["time"].apply(dateutil.parser.parse)
    #     spot_trades_df.set_index("time", inplace=True)
    #     spot_trades_df.sort_index(inplace=True)
    #     spot_trades_df.rename(
    #         columns={
    #             "id": "s_id",
    #             "price": "s_price",
    #             "size": "s_size",
    #             "side": "s_side",
    #         },
    #         inplace=True,
    #     )
    #     resample_1s_spot_df = spot_trades_df.resample("1s").last()

    #     future_trades_df = pd.DataFrame.from_records(future_trades)
    #     future_trades_df["time"] = future_trades_df["time"].apply(dateutil.parser.parse)
    #     future_trades_df.set_index("time", inplace=True)
    #     future_trades_df.sort_index(inplace=True)
    #     future_trades_df.rename(
    #         columns={
    #             "id": "f_id",
    #             "price": "f_price",
    #             "size": "f_size",
    #             "side": "f_side",
    #         },
    #         inplace=True,
    #     )
    #     resample_1s_future_df = future_trades_df.resample("1s").last()

    #     concat_df = pd.concat([resample_1s_spot_df, resample_1s_future_df], axis=1)
    #     concat_df.dropna(inplace=True)
    #     concat_df = concat_df[concat_df["s_side"] != concat_df["f_side"]]
    #     concat_df["basis"] = concat_df["f_price"] - concat_df["s_price"]
    #     resample: pd.DataFrame = concat_df.resample(
    #         self._kline_resolution.to_pandas_resample_rule()
    #     ).agg({"basis": "ohlc"})
    #     resample = resample.droplevel(0, axis=1)
    #     resample.fillna(resample["close"].ffill())
    #     return resample


class KeltnerBacktest(Keltner):
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

    def generate_params(self) -> list[KeltnerParams]:
        params = []
        for mult in np.arange(0.8, 2.5, 0.1):
            mult = round(mult, 1)
            for length in np.arange(10, 50, 5):
                length = int(length)
                params.append(KeltnerParams(length=length, mult=mult))
        return params

    def get_save_path(self) -> str:
        from_datatime = datetime.fromtimestamp(self.config.start_timestamp)
        from_date_str = from_datatime.strftime("%Y%m%d")
        to_datatime = datetime.fromtimestamp(self.config.end_timestamp)
        to_data_str = to_datatime.strftime("%Y%m%d")
        return f"local/backtest/keltner_{self.future_client.name}_{self.hedge_pair.future}_{from_date_str}_{to_data_str}"
