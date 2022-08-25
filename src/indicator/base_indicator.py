import time
from decimal import Decimal

from src.exchange.ftx.ftx_data_type import FtxCandleResolution


class BaseIndicator:
    def __init__(self, kline_resolution: FtxCandleResolution):
        self._kline_resolution: FtxCandleResolution = kline_resolution
        self._lower_threshold: Decimal = None
        self._upper_threshold: Decimal = None
        self._last_kline_start_timestamp: float = 0.0

    @property
    def upper_threshold(self) -> Decimal:
        return self._upper_threshold

    @property
    def lower_threshold(self) -> Decimal:
        return self._lower_threshold

    @property
    def last_kline_start_timestamp(self) -> float:
        return self._last_kline_start_timestamp

    @property
    def kline_resolution(self) -> FtxCandleResolution:
        return self._kline_resolution

    @property
    def ready(self) -> bool:
        now_ts = time.time()
        return (
            self._upper_threshold is not None
            and self._lower_threshold is not None
            and self._last_kline_start_timestamp + self._kline_resolution.value
            <= now_ts
            and now_ts < self._last_kline_start_timestamp + 2 * self._kline_resolution
        )

    async def update_indicator_info(self, merged_candles):
        """Indicators which inherit from this base class must implement this function
        to update upper_threshold, lower_threshold, last_kline_start_timestamp"""
        raise NotImplementedError
