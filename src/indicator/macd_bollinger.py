import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import List

import dateutil.parser
import pandas as pd

from src.backtest.ftx_data_types import BackTestConfig
from src.exchange.ftx.ftx_client import FtxExchange
from src.exchange.ftx.ftx_data_type import FtxCandleResolution, FtxHedgePair
from src.indicator.base_indicator import BaseIndicator


class NegtiveSignalType(Enum):
    DIF = "dif"
    DEA = "dea"
    MACD = "macd"
    NON = "non"


@dataclass
class MACDBollingerParams:
    macd_std_mult: float
    boll_std_mult: float
    lower_bound_factor: float
    dea_positive_filter: bool
    negtive_filter_type: str
    macd_std_length: int = 20
    macd_fast_length: int = 12
    macd_slow_length: int = 26
    macd_signal_length: int = 9
    boll_length: int = 20

    @property
    def alpha_fast(self):
        return 2 / (self.macd_fast_length + 1)

    @property
    def alpha_slow(self):
        return 2 / (self.macd_slow_length + 1)

    @property
    def alpha_macd(self):
        return 2 / (self.macd_signal_length + 1)


class MACDBollinger(BaseIndicator):
    """To use MACDBollinger indicator, one should set parameters in the yaml config file.
    For example:

    indicator:
        name: 'macd_bollinger'
        params:
            kline_resolution: 3600  # Enum of (15, 60, 300, 900, 3600, 14400, 86400) in seconds
            macd_std_mult: 1.6
            boll_std_mult: 0.9
            lower_bound_factor: 0.95
            dea_positive_filter: False
            negtive_filter_type: NegtiveSignalType.NON.value
            macd_std_length: 20
            macd_fast_length: 12
            macd_slow_length: 26
            macd_signal_length: 9
            boll_length: 20
    """

    def __init__(
        self,
        hedge_pair: FtxHedgePair,
        kline_resolution: FtxCandleResolution,
        params: MACDBollingerParams = None,
    ):
        super().__init__(kline_resolution)
        self.hedge_pair = hedge_pair
        if not params:
            # default params
            self.params: MACDBollingerParams = MACDBollingerParams(
                macd_std_mult=1.6,
                boll_std_mult=0.9,
                lower_bound_factor=0.95,
                dea_positive_filter=False,
                negtive_filter_type=NegtiveSignalType.NON.value,
            )
        else:
            self.params: MACDBollingerParams = params

    @staticmethod
    def compute_thresholds(merged_candles_df, params: MACDBollingerParams, as_df=False):
        merged_candles_df["fast_ema"] = (
            merged_candles_df["close"].ewm(span=params.macd_fast_length).mean()
        )
        merged_candles_df["slow_ema"] = (
            merged_candles_df["close"].ewm(span=params.macd_slow_length).mean()
        )
        merged_candles_df["dif"] = (
            merged_candles_df["fast_ema"] - merged_candles_df["slow_ema"]
        )
        merged_candles_df["dea"] = (
            merged_candles_df["dif"].ewm(span=params.macd_signal_length).mean()
        )

        merged_candles_df["macd"] = merged_candles_df["dif"] - merged_candles_df["dea"]

        merged_candles_df["macd_std"] = (
            merged_candles_df["macd"].rolling(params.macd_std_length).std()
        )

        # more signals
        dea_positive_signal = merged_candles_df["dea"].apply(
            lambda x: 0 if x > 0 else 10000
        )

        dif_negative_signal = merged_candles_df["dif"].apply(
            lambda x: 0 if x < 0 else -10000
        )
        dea_negative_signal = merged_candles_df["dea"].apply(
            lambda x: 0 if x > 0 else -10000
        )
        macd_negative_signal = merged_candles_df["macd"].apply(
            lambda x: 0 if x > 0 else -10000
        )

        rolling = merged_candles_df["close"].rolling(params.boll_length)
        merged_candles_df["ma"] = rolling.mean()
        merged_candles_df["std"] = rolling.std()

        merged_candles_df["boll_upper"] = (
            merged_candles_df["ma"] + params.boll_std_mult * merged_candles_df["std"]
        )
        merged_candles_df["boll_lower"] = (
            merged_candles_df["ma"] - params.boll_std_mult * merged_candles_df["std"]
        )

        merged_candles_df["macd_upper"] = (
            (params.macd_std_mult * merged_candles_df["macd_std"])
            / (1 - params.alpha_macd)
            + merged_candles_df["dea"]
            - (
                (1 - params.alpha_fast) * merged_candles_df["fast_ema"]
                - (1 - params.alpha_slow) * merged_candles_df["slow_ema"]
            )
        ) / (params.alpha_fast - params.alpha_slow)

        merged_candles_df["macd_lower"] = (
            (-params.macd_std_mult * merged_candles_df["macd_std"])
            / (1 - params.alpha_macd)
            + merged_candles_df["dea"]
            - (
                (1 - params.alpha_fast) * merged_candles_df["fast_ema"]
                - (1 - params.alpha_slow) * merged_candles_df["slow_ema"]
            )
        ) / (params.alpha_fast - params.alpha_slow)

        upper_threshold_df = merged_candles_df.loc[:, ["boll_upper", "macd_upper"]].min(
            axis=1
        )
        lower_threshold_df = merged_candles_df.loc[:, ["boll_lower", "macd_lower"]].min(
            axis=1
        )

        # add lower bound factor
        lower_threshold_dt = lower_threshold_dt * params.lower_bound_factor
        if params.dea_positive_filter:
            upper_threshold_df = lower_threshold_df + dea_positive_signal

        if params.negtive_filter_type == NegtiveSignalType.DIF.value:
            lower_threshold_dt = lower_threshold_dt + dif_negative_signal
        elif params.negtive_filter_type == NegtiveSignalType.DEA.value:
            lower_threshold_dt = lower_threshold_dt + dea_negative_signal
        elif params.negtive_filter_type == NegtiveSignalType.MACD.value:
            lower_threshold_dt = lower_threshold_dt + macd_negative_signal

        if as_df:
            return (upper_threshold_df, lower_threshold_df)
        else:
            return (upper_threshold_df.iloc[-1], lower_threshold_df.iloc[-1])

    async def update_indicator_info(self):
        client = FtxExchange("", "")
        resolution = self._kline_resolution
        end_ts = (time.time() // resolution.value - 1) * resolution.value
        start_ts = end_ts - self.params.macd_slow_length * resolution.value
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


class MACDBollingerBacktest(MACDBollinger):
    def __init__(
        self,
        hedge_pair: FtxHedgePair,
        kline_resolution: FtxCandleResolution,
        backtest_config: BackTestConfig,
    ):
        super().__init__(hedge_pair, kline_resolution)
        self.config = backtest_config

    def generate_params(self) -> list[MACDBollingerParams]:
        params = []
        for macd_std_mult in [1.2, 1.6, 1.8]:
            for boll_std_mult in [0.7, 0.9]:
                for dea_positive_filter in [True, False]:
                    for lower_bound_factor in [0.95, 1]:
                        for negtive_filter_type in [
                            NegtiveSignalType.NON.value,
                            NegtiveSignalType.MACD.value,
                        ]:
                            params.append(
                                MACDBollingerParams(
                                    macd_std_mult=macd_std_mult,
                                    boll_std_mult=boll_std_mult,
                                    dea_positive_filter=dea_positive_filter,
                                    lower_bound_factor=lower_bound_factor,
                                    negtive_filter_type=negtive_filter_type,
                                )
                            )
        return params

    def get_save_path(self) -> str:
        from_datatime = datetime.fromtimestamp(self.config.start_timestamp)
        from_date_str = from_datatime.strftime("%Y%m%d")
        to_datatime = datetime.fromtimestamp(self.config.end_timestamp)
        to_data_str = to_datatime.strftime("%Y%m%d")
        return f"local/backtest/macd_boll_{self.hedge_pair.future}_{from_date_str}_{to_data_str}"
