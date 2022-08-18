import time
from typing import List
from decimal import Decimal
import pandas as pd
import dateutil.parser
from src.exchange.ftx.ftx_client import FtxExchange
from src.exchange.ftx.ftx_data_type import FtxCandleResolution, FtxHedgePair


class MACD:

    def __init__(
        self,
        hedge_pair: FtxHedgePair,
        fast_length: int = 12,
        slow_length: int = 26,
        signal_length: int = 9,
        std_length: int = 20,
        std_mult: float = 1.0,
        ):

        self.upper_threshold: Decimal = None
        self.lower_threshold: Decimal = None
        self.last_update_timestamp: float = 0.0

        self.hedge_pair = hedge_pair
        self.fast_length = fast_length
        self.slow_length = slow_length
        self.signal_length = signal_length
        self.std_length = std_length
        self.std_mult = std_mult

        self.alpha_fast = 2 / (fast_length + 1)
        self.alpha_slow = 2 / (slow_length + 1)
        self.alpha_macd = 2 / (signal_length + 1)

    async def update_indicator_info(self):
        client = FtxExchange('', '')
        resolution = FtxCandleResolution.ONE_HOUR
        end_ts = (time.time() // resolution.value - 1) * resolution.value
        start_ts = end_ts - self.slow_length * resolution.value
        spot_candles = await client.get_candles(self.hedge_pair.spot, resolution, start_ts, end_ts)
        if len(spot_candles) == 0:
            return
        future_candles = await client.get_candles(self.hedge_pair.future, resolution, start_ts, end_ts)
        if len(future_candles) == 0:
            return

        await client.close()

        spot_df = self.candles_to_df(spot_candles)
        future_df = self.candles_to_df(future_candles)

        spot_close = spot_df['close'].rename('s_close')
        future_close = future_df['close'].rename('f_close')
        concat_df = pd.concat([spot_close, future_close], axis=1)
        concat_df['basis'] = concat_df['f_close'] - concat_df['s_close']
        concat_df['fast_ema'] = concat_df['basis'].ewm(span=self.fast_length).mean()
        concat_df['slow_ema'] = concat_df['basis'].ewm(span=self.slow_length).mean()
        concat_df['dif'] = concat_df['fast_ema'] - concat_df['slow_ema']
        concat_df['macd'] = concat_df['dif'].ewm(span=self.signal_length).mean()
        concat_df['dif_sub_macd'] = concat_df['dif'] - concat_df['macd']
        concat_df['std'] = concat_df['dif_sub_macd'].rolling(self.std_length).std()

        last_fast = concat_df['fast_ema'].iloc[-1]
        last_slow = concat_df['slow_ema'].iloc[-1]
        last_macd = concat_df['macd'].iloc[-1]
        std = concat_df['std'].iloc[-1]

        upper_threshold = ((self.std_mult * std) / (1 - self.alpha_macd) + last_macd - ((1 - self.alpha_fast) * last_fast - (1 - self.alpha_slow) * last_slow)) / (self.alpha_fast - self.alpha_slow)
        lower_threshold = ((-self.std_mult * std) / (1 - self.alpha_macd) + last_macd - ((1 - self.alpha_fast) * last_fast - (1 - self.alpha_slow) * last_slow)) / (self.alpha_fast - self.alpha_slow)

        self.upper_threshold = Decimal(str(upper_threshold))
        self.lower_threshold = Decimal(str(lower_threshold))
        self.last_update_timestamp = concat_df.index[-1].timestamp()

    def candles_to_df(self, candles: List[dict]) -> pd.DataFrame:
        df = pd.DataFrame.from_records(candles)
        df['startTime'] = df['startTime'].apply(dateutil.parser.parse)
        df['close'] = df['close'].astype('float32')
        df.set_index('startTime', inplace=True)
        df.sort_index(inplace=True)
        return df
