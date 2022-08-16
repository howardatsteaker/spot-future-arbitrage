from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from decimal import Decimal
import time
import uuid


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


@dataclass
class FtxFeeRate:
    taker_fee_rate: Decimal = None
    maker_fee_rate: Decimal = None


@dataclass
class FtxCollateralWeight:
    coin: str
    weight: Decimal


@dataclass
class FtxHedgePair:
    coin: str
    spot: str
    future: str

    @staticmethod
    def coin_to_spot(coin: str) -> str:
        return coin + '/USD'

    @staticmethod
    def coin_to_future(coin: str, season: str) -> str:
        return coin + '-' + season

    @staticmethod
    def spot_to_coin(spot: str) -> str:
        return spot.split('/')[0]

    @staticmethod
    def spot_to_future(spot: str, season: str) -> str:
        return spot.split('/')[0] + '-' + season

    @staticmethod
    def future_to_coin(future: str) -> str:
        return future.split('-')[0]

    @staticmethod
    def future_to_spot(future: str) -> str:
        return future.split('-')[0] + '/USD'

    @staticmethod
    def is_spot(symbol: str) -> bool:
        return symbol.endswith('/USD')

    @staticmethod
    def is_future(symbol: str, season: str) -> bool:
        return symbol.endswith(f"-{season}")


@dataclass
class FtxTradingRuleMessage:
    trading_rule: FtxTradingRule


@dataclass
class FtxInterestRateMessage:
    ewma_interest_rate: Ftx_EWMA_InterestRate


@dataclass
class FtxFeeRateMessage:
    fee_rate: FtxFeeRate


@dataclass
class FtxCollateralWeightMessage:
    collateral_weight: FtxCollateralWeight


@dataclass
class FtxLeverageMessage:
    leverage: Decimal


@dataclass
class FtxFundRequestMessage:
    id: uuid.UUID
    fund_needed: Decimal
    spot_notional_value: Decimal


@dataclass
class FtxFundResponseMessage:
    id: uuid.UUID
    approve: bool
    fund_supply: Decimal
    borrow: Decimal


@dataclass
class FtxFundOpenFilledMessage:
    id: uuid.UUID
    fund_used: Decimal
    spot_notional_value: Decimal


@dataclass
class FtxOrderMessage:
    id: str
    market: str
    type: FtxOrderType
    side: Side
    size: Decimal
    price: Decimal
    status: FtxOrderStatus
    filled_size: Decimal
    avg_fill_price: Decimal
    create_timestamp: float


@dataclass
class FtxTicker:
    symbol: str
    bid: Decimal
    ask: Decimal
    bid_size: Decimal
    ask_size: Decimal
    last: Decimal
    timestamp: float

    @classmethod
    def ws_entry(cls, symbol: str, ticker_info: dict) -> FtxTicker:
        return cls(
            symbol=symbol,
            bid=Decimal(str(ticker_info['bid'])),
            ask=Decimal(str(ticker_info['ask'])),
            bid_size=Decimal(str(ticker_info['bidSize'])),
            ask_size=Decimal(str(ticker_info['askSize'])),
            last=Decimal(str(ticker_info['last'])),
            timestamp=ticker_info['time'],
        )

    def is_delay(self, threshold: float) -> bool:
        return time.time() - self.timestamp > threshold

class Side(Enum):
    BUY = 'buy'
    SELL = 'sell'


class FtxOrderType(Enum):
    LIMIT ='limit'
    MARKET = 'market'


class FtxOrderStatus(Enum):
    NEW = "new"
    OPEN = "open"
    CLOSED = "closed"

    @classmethod
    def str_entry(cls, status: str) -> FtxOrderStatus:
        if status == 'new':
            return cls.NEW
        elif status == 'open':
            return cls.OPEN
        else:
            return cls.CLOSED
