from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from functools import cached_property
from typing import List

from src.exchange.exchange_data_type import Side


@dataclass
class BackTestConfig:
    spot_fee_rate: Decimal
    future_fee_rate: Decimal
    collateral_weight: Decimal
    start_timestamp: int
    end_timestamp: int
    ts_to_stop_open: int
    ts_to_expiry: int
    expiration_price: Decimal
    leverage: Decimal
    save_dir: str
    exchange: str


@dataclass
class MarketOrder:
    symbol: str
    side: Side
    price: Decimal
    size: Decimal
    create_timestamp: float
    fee_rate: Decimal

    @cached_property
    def order_value(self) -> Decimal:
        return self.price * self.size

    @cached_property
    def fee(self) -> Decimal:
        return self.order_value * self.fee_rate

    def __repr__(self) -> str:
        return f"MarketOrder({self.side.name}, p: {self.price}, s: {self.size}, create time: {datetime.fromtimestamp(self.create_timestamp)}), fee: {self.fee}"


class HedgeType(Enum):
    OPEN = "open"
    CLOSE = "close"


@dataclass
class HedgeTrade:
    timestamp: float
    hedge_type: HedgeType
    basis: Decimal


@dataclass
class LogState:
    timestamp: float
    basis: Decimal
    spot_position: Decimal
    future_position: Decimal
    spot_entry_price: Decimal
    future_entry_price: Decimal
    net_deposit: Decimal
    profit: Decimal


@dataclass
class BaseState:
    basis: Decimal = Decimal("nan")
    spot_position: Decimal = Decimal(0)
    future_position: Decimal = Decimal(0)
    spot_entry_price: Decimal = Decimal("nan")
    future_entry_price: Decimal = Decimal("nan")
    hedge_trades: List[HedgeTrade] = field(default_factory=lambda: [])
    balance: Decimal = Decimal(0)
    net_deposit = Decimal(0)
    profit: Decimal = Decimal(0)

    def deposit(self, amount: Decimal):
        self.balance += amount
        self.net_deposit += amount

    def withdraw(self, amount: Decimal):
        self.balance -= amount
        self.net_deposit -= amount

    def place_market_order(self, market_order: MarketOrder):
        assert market_order.symbol == "spot" or market_order.symbol == "future"
        if market_order.symbol == "spot" and market_order.side == Side.SELL:
            assert (
                self.spot_position - market_order.size >= 0
            ), "negative spot position is not allowed"
        if market_order.symbol == "future" and market_order.side == Side.BUY:
            assert (
                self.future_position + market_order.size <= 0
            ), "positive future position is not allowed"

        if market_order.symbol == "spot":
            if market_order.side == Side.BUY:
                new_size = self.spot_position + market_order.size
                if self.spot_entry_price.is_nan():
                    self.spot_entry_price = market_order.price
                else:
                    self.spot_entry_price = (
                        self.spot_entry_price * self.spot_position
                        + market_order.order_value
                    ) / new_size
            else:
                new_size = self.spot_position - market_order.size
                if new_size == 0:
                    self.spot_entry_price = Decimal("nan")
            self.spot_position = new_size
        else:
            if market_order.side == Side.SELL:
                new_size = self.future_position - market_order.size
                if self.future_entry_price.is_nan():
                    self.future_entry_price = market_order.price
                else:
                    self.future_entry_price = (
                        self.future_entry_price * self.future_position
                        - market_order.order_value
                    ) / new_size
            else:
                new_size = self.future_position + market_order.size
                if new_size == 0:
                    self.future_entry_price = Decimal("nan")
            self.future_position = new_size

    def open_position(
        self,
        spot_market_order: MarketOrder,
        future_market_order: MarketOrder,
        collateral_weight: Decimal,
        leverage: Decimal = Decimal("1"),
    ):
        assert spot_market_order.side == Side.BUY
        assert future_market_order.side == Side.SELL
        assert collateral_weight <= Decimal(1)
        assert leverage >= Decimal(1)
        assert spot_market_order.size == future_market_order.size

        fee = spot_market_order.fee + future_market_order.fee
        fund_required = (
            spot_market_order.order_value * (1 - collateral_weight)
            + future_market_order.order_value / leverage
            + fee
        )
        self.deposit(max(0, fund_required - self.balance))
        self.balance -= fund_required
        self.profit -= fee
        self.place_market_order(spot_market_order)
        self.place_market_order(future_market_order)

    def close_position(
        self,
        spot_market_order: MarketOrder,
        future_market_order: MarketOrder,
        collateral_weight: Decimal,
        leverage: Decimal = Decimal("1"),
    ):
        assert spot_market_order.side == Side.SELL
        assert future_market_order.side == Side.BUY
        assert collateral_weight <= Decimal(1)
        assert leverage >= Decimal(1)
        assert spot_market_order.size == future_market_order.size

        size = spot_market_order.size
        fee = spot_market_order.fee + future_market_order.fee
        fund_released = spot_market_order.order_value
        fund_released += (
            self.future_entry_price * (1 + 1 / leverage) - future_market_order.price
        ) * size
        fund_released -= (
            self.spot_entry_price * spot_market_order.size * collateral_weight
        )
        fund_released -= fee
        self.balance += fund_released
        self.withdraw(max(0, self.balance))
        self.profit += (
            (self.future_entry_price - future_market_order.price)
            + (spot_market_order.price - self.spot_entry_price)
        ) * size - fee
        self.place_market_order(spot_market_order)
        self.place_market_order(future_market_order)

    def to_log_state(self, timestamp):
        return LogState(
            timestamp=timestamp,
            basis=self.basis,
            spot_position=self.spot_position,
            future_position=self.future_position,
            spot_entry_price=self.spot_entry_price,
            future_entry_price=self.future_entry_price,
            net_deposit=self.net_deposit,
            profit=self.profit,
        )

    def append_hedge_trade(
        self, timestamp: float, hedge_type: HedgeType, basis: Decimal
    ):
        self.hedge_trades.append(
            HedgeTrade(timestamp=timestamp, hedge_type=hedge_type, basis=basis)
        )
