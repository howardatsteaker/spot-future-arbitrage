from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum


class FtxCandleResolution(Enum):
    FIFTEEN_SECONDS = 15
    ONE_MINUTE = 60
    FIVE_MINUTES = 300
    FIFTEEN_MINUTES = 900
    ONE_HOUR = 3600
    FOUR_HOURS = 14400
    ONE_DAY = 86400

    @classmethod
    def from_seconds(cls, seconds: int) -> FtxCandleResolution:
        if seconds == 15:
            return cls.FIFTEEN_SECONDS
        elif seconds == 60:
            return cls.ONE_MINUTE
        elif seconds == 300:
            return cls.FIVE_MINUTES
        elif seconds == 900:
            return cls.FIFTEEN_MINUTES
        elif seconds == 3600:
            return cls.ONE_HOUR
        elif seconds == 14400:
            return cls.FOUR_HOURS
        elif seconds == 86400:
            return cls.ONE_DAY
        else:
            raise ValueError(
                "'seconds' must be one of (15, 60, 300, 900, 3600, 14400, 86400)"
            )


@dataclass
class FtxTradingRule:
    symbol: str
    min_order_size: Decimal


@dataclass
class Ftx_EWMA_InterestRate:
    lookback_days: int
    lambda_: Decimal = Decimal("0.02")
    last_ewma: Decimal = None
    last_timestamp: float = None

    @property
    def hourly_rate(self) -> Decimal:
        return self.last_ewma

    @property
    def yearly_rate(self) -> Decimal:
        return self.last_ewma * Decimal("24") * Decimal("365")


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
        return coin + "/USD"

    @staticmethod
    def coin_to_future(coin: str, season: str) -> str:
        return coin + "-" + season

    @staticmethod
    def spot_to_coin(spot: str) -> str:
        return spot.split("/")[0]

    @staticmethod
    def spot_to_future(spot: str, season: str) -> str:
        return spot.split("/")[0] + "-" + season

    @staticmethod
    def future_to_coin(future: str) -> str:
        return future.split("-")[0]

    @staticmethod
    def future_to_spot(future: str) -> str:
        return future.split("-")[0] + "/USD"

    @staticmethod
    def is_spot(symbol: str) -> bool:
        return symbol.endswith("/USD")

    @staticmethod
    def is_future(symbol: str, season: str) -> bool:
        return symbol.endswith(f"-{season}")

    @staticmethod
    def to_dir_name(symbol: str) -> str:
        symbol = symbol.replace("/", "_")
        symbol = symbol.replace("-", "_")
        return symbol


@dataclass
class FtxLeverageInfo:
    max_leverage: Decimal = Decimal(1)
    account_value: Decimal = Decimal(0)
    position_value: Decimal = Decimal(0)
    current_leverage: Decimal = Decimal(0)


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
    leverage: FtxLeverageInfo


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
            bid=Decimal(str(ticker_info["bid"])),
            ask=Decimal(str(ticker_info["ask"])),
            bid_size=Decimal(str(ticker_info["bidSize"])),
            ask_size=Decimal(str(ticker_info["askSize"])),
            last=Decimal(str(ticker_info["last"])),
            timestamp=ticker_info["time"],
        )

    def is_delay(self, threshold: float) -> bool:
        return time.time() - self.timestamp > threshold


class Side(Enum):
    BUY = "buy"
    SELL = "sell"


class FtxOrderType(Enum):
    LIMIT = "limit"
    MARKET = "market"


class FtxOrderStatus(Enum):
    NEW = "new"
    OPEN = "open"
    CLOSED = "closed"

    @classmethod
    def str_entry(cls, status: str) -> FtxOrderStatus:
        if status == "new":
            return cls.NEW
        elif status == "open":
            return cls.OPEN
        else:
            return cls.CLOSED
