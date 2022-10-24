import time
from decimal import Decimal

from src.exchange.exchange_data_type import CandleResolution


class BaseIndicator:
    def __init__(self, kline_resolution: CandleResolution):
        self._kline_resolution: CandleResolution = kline_resolution
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
    def kline_resolution(self) -> CandleResolution:
        return self._kline_resolution

    @property
    def ready(self) -> bool:
        now_ts = time.time()
        return (
            self._upper_threshold is not None
            and self._lower_threshold is not None
            and self._last_kline_start_timestamp + self._kline_resolution.value
            <= now_ts
            and now_ts
            < self._last_kline_start_timestamp + 2 * self._kline_resolution.value
        )

    async def update_indicator_info(self):
        """Indicators which inherit from this base class must implement this function
        to update upper_threshold, lower_threshold, last_kline_start_timestamp"""
        raise NotImplementedError
