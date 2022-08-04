from dataclasses import dataclass, field
from enum import Enum
from decimal import Decimal


class FtxCandleResolution(Enum):
    FIFTEEN_SECONDS = 15
    ONE_MINUTE = 60
    FIVE_MINUTES = 300
    FIFTEEN_MINUTES = 900
    ONE_HOUR = 3600
    FOUR_HOURS = 14400
    ONE_DAY = 86400


@dataclass
class FtxTradingRule:
    symbol: str
    min_order_size: Decimal


@dataclass
class Ftx_EWMA_InterestRate:
    lookback_days: int
    lambda_: Decimal = Decimal('0.02')
    last_ewma: Decimal = None
    last_timestamp: float = None

    @property
    def hourly_rate(self) -> Decimal:
        return self.last_ewma

    @property
    def yearly_rate(self) -> Decimal:
        return self.last_ewma * Decimal('24') * Decimal('365')
