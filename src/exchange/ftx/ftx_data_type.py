from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Union

from src.common import to_decimal_or_none


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

    def to_pandas_resample_rule(self) -> str:
        if self is FtxCandleResolution.FIFTEEN_SECONDS:
            return "15S"
        elif self is FtxCandleResolution.ONE_MINUTE:
            return "1min"
        elif self is FtxCandleResolution.FIVE_MINUTES:
            return "5min"
        elif self is FtxCandleResolution.FIFTEEN_MINUTES:
            return "15min"
        elif self is FtxCandleResolution.ONE_HOUR:
            return "1H"
        elif self is FtxCandleResolution.FOUR_HOURS:
            return "4H"
        elif self is FtxCandleResolution.ONE_DAY:
            return "1D"


@dataclass
class FtxTradingRule:
    symbol: str
    min_order_size: Decimal
    price_tick: Decimal


@dataclass
class Ftx_EWMA_InterestRate:
    lookback_days: int
    taker_fee_rate: Decimal("0.0007")
    lambda_: Decimal = Decimal("0.02")
    last_ewma: Decimal = None
    last_timestamp: float = None

    @property
    def hourly_rate(self) -> Decimal:
        return self.last_ewma * (1 + 500 * self.taker_fee_rate)

    @property
    def yearly_rate(self) -> Decimal:
        return (
            self.last_ewma
            * Decimal("24")
            * Decimal("365")
            * (1 + 500 * self.taker_fee_rate)
        )

    def set_taker_fee_rate(self, taker_fee_rate: Decimal):
        self.taker_fee_rate = taker_fee_rate


@dataclass
class FtxFeeRate:
    taker_fee_rate: Decimal = None
    maker_fee_rate: Decimal = None


@dataclass
class FtxCollateralWeight:
    coin: str
    weight: Decimal


class TradeType(Enum):
    BOTH = "both"
    CLOSE_ONLY = "close_only"
    NEITHER = "neither"


@dataclass
class FtxHedgePair:
    coin: str
    spot: str
    future: str
    trade_type: TradeType = TradeType.BOTH

    @classmethod
    def from_coin(
        cls, coin: str, season: str, trade_type: TradeType = TradeType.BOTH
    ) -> FtxHedgePair:
        return cls(
            coin=coin,
            spot=cls.coin_to_spot(coin),
            future=cls.coin_to_future(coin, season),
            trade_type=trade_type,
        )

    @classmethod
    def from_spot(
        cls, spot: str, season: str, trade_type: TradeType = TradeType.BOTH
    ) -> FtxHedgePair:
        return cls(
            coin=cls.spot_to_coin(spot),
            spot=spot,
            future=cls.spot_to_future(spot, season),
            trade_type=trade_type,
        )

    @classmethod
    def from_future(
        cls, future: str, trade_type: TradeType = TradeType.BOTH
    ) -> FtxHedgePair:
        return cls(
            coin=cls.future_to_coin(future),
            spot=cls.future_to_spot(future),
            future=future,
            trade_type=trade_type,
        )

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

    @property
    def can_open(self) -> bool:
        return self.trade_type is TradeType.BOTH

    @property
    def can_close(self) -> bool:
        return (
            self.trade_type is TradeType.CLOSE_ONLY or self.trade_type is TradeType.BOTH
        )


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
    coin: str
    fund_needed: Decimal


@dataclass
class FtxFundResponseMessage:
    coin: str
    fund_supply: Decimal


@dataclass
class FtxFundOpenFilledMessage:
    coin: str
    fund_used: Decimal


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
class FtxEntryPriceRequestMessage:
    market: str


@dataclass
class FtxEntryPriceResponseMessage:
    market: str
    entry_price: Decimal


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
            bid=to_decimal_or_none(ticker_info["bid"]),
            ask=to_decimal_or_none(ticker_info["ask"]),
            bid_size=to_decimal_or_none(ticker_info["bidSize"]),
            ask_size=to_decimal_or_none(ticker_info["askSize"]),
            last=to_decimal_or_none(ticker_info["last"]),
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


@dataclass
class FtxHedgePairSummary:
    hedge_pair: FtxHedgePair
    spot_size: Decimal = Decimal(0)
    future_size: Decimal = Decimal(0)
    spot_entry_price: Decimal = None
    future_entry_price: Decimal = None
    spot_usd_value: Decimal = Decimal(0)
    spot_price_tick: Decimal = None
    future_price_tick: Decimal = None

    @property
    def is_pair_size_equal(self) -> bool:
        return self.spot_size + self.future_size == 0

    @property
    def basis(self) -> Union[Decimal, None]:
        if (
            self.spot_entry_price
            and self.future_entry_price
            and self.spot_size > 0
            and self.future_size < 0
        ):
            return self.future_entry_price - self.spot_entry_price
        else:
            return None

    def __lt__(self, other: FtxHedgePairSummary) -> bool:
        return self.spot_usd_value < other.spot_usd_value

    def __repr__(self) -> str:
        if self.is_pair_size_equal:
            text = (
                f"{self.hedge_pair.coin} {self.spot_size} (${self.spot_usd_value:,.0f})"
            )
        else:
            text = f"{self.hedge_pair.coin} `[{self.spot_size} {self.future_size}]` (${self.spot_usd_value:,.0f})"
        basis = self.basis
        spot_price_tick = self.spot_price_tick
        future_price_tick = self.future_price_tick
        if basis:
            if spot_price_tick and future_price_tick:
                price_tick = min(spot_price_tick, future_price_tick)
                basis = basis // price_tick * price_tick
            text += f", Basis: ${basis}"
        return text


@dataclass
class OpenCloseInfo:
    """This class is used for log summary"""

    hedge_pair: FtxHedgePair
    spot_open_size: Decimal = Decimal(0)
    spot_open_value: Decimal = Decimal(0)
    spot_close_size: Decimal = Decimal(0)
    spot_close_value: Decimal = Decimal(0)
    future_open_size: Decimal = Decimal(0)
    future_open_value: Decimal = Decimal(0)
    future_close_size: Decimal = Decimal(0)
    future_close_value: Decimal = Decimal(0)

    @property
    def spot_open_price(self) -> Decimal:
        if self.spot_open_size == 0:
            return None
        else:
            return self.spot_open_value / self.spot_open_size

    @property
    def spot_close_price(self) -> Decimal:
        if self.spot_close_size == 0:
            return None
        else:
            return self.spot_close_value / self.spot_close_size

    @property
    def future_open_price(self) -> Decimal:
        if self.future_open_size == 0:
            return None
        else:
            return self.future_open_value / self.future_open_size

    @property
    def future_close_price(self) -> Decimal:
        if self.future_close_size == 0:
            return None
        else:
            return self.future_close_value / self.future_close_size

    def fill_entry(self, fill: dict):
        market = fill.get("market")
        side = fill.get("side")
        size = Decimal(str(fill.get("size")))
        price = Decimal(str(fill.get("price")))

        if market == self.hedge_pair.spot:
            if side == "buy":
                self.spot_open_size += size
                self.spot_open_value += size * price
            else:
                self.spot_close_size += size
                self.spot_close_value += size * price
        elif market == self.hedge_pair.future:
            if side == "sell":
                self.future_open_size += size
                self.future_open_value += size * price
            else:
                self.future_close_size += size
                self.future_close_value += size * price
