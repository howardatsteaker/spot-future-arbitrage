from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum, auto
from typing import List
from functools import cached_property
from dateutil import parser


class Side(Enum):
    BUY = 'buy'
    SELL = 'sell'

    @classmethod
    def ftx_side_map(cls, side: str):
        if side == 'buy':
            return cls.BUY
        elif side == 'sell':
            return cls.SELL


@dataclass
class Trade:
    id: str
    price: float
    size: float
    side: Side
    time: datetime

    @classmethod
    def ftx_map(cls, row: dict) -> Trade:
        """FTX original map function
        """
        side = Side.ftx_side_map(row['side'])
        time = parser.parse(row['time'])
        return Trade(row['id'], row['price'], row['size'], side, time)

    def to_json(self) -> dict:
        return {'id': self.id, 'price': self.price, 'size': self.size, 'side': self.side.name, 'time': self.time.timestamp()}

    @classmethod
    def from_json(cls, data: dict) -> Trade:
        side = Side.BUY if data['side'] == 'BUY' else Side.SELL
        t = datetime.fromtimestamp(data['time'])
        return Trade(id=data['id'], price=data['price'], size=data['size'], side=side, time=t)

    def __repr__(self) -> str:
        return f"Trade(id: {self.id}, price: {self.price}, size: {self.size}, side: {self.side.name}, time: {self.time})"


@dataclass
class Kline:
    high: Decimal
    low: Decimal
    open: Decimal
    close: Decimal
    volume: Decimal
    time: datetime

    @classmethod
    def ftx_map(self, row: dict):
        """FTX original map function
        """
        return Kline(
            high=Decimal(str(row['high'])),
            low=Decimal(str(row['low'])),
            open=Decimal(str(row['open'])),
            close=Decimal(str(row['close'])),
            volume=Decimal(str(row['volume'])),
            time=parser.parse(row['start_time'])
        )


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
    basis: Decimal = Decimal('nan')
    spot_position: Decimal = Decimal(0)
    future_position: Decimal = Decimal(0)
    spot_entry_price: Decimal = Decimal('nan')
    future_entry_price: Decimal = Decimal('nan')
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
        assert market_order.symbol == 'spot' or market_order.symbol == 'future'
        if market_order.symbol == 'spot' and market_order.side == Side.SELL:
            assert self.spot_position - market_order.size >= 0, 'negative spot position is not allowed'
        if market_order.symbol == 'future' and market_order.side == Side.BUY:
            assert self.future_position + market_order.size <= 0, 'positive future position is not allowed'

        if market_order.symbol == 'spot':
            if market_order.side == Side.BUY:
                new_size = self.spot_position + market_order.size
                if self.spot_entry_price.is_nan():
                    self.spot_entry_price = market_order.price
                else:
                    self.spot_entry_price = (self.spot_entry_price * self.spot_position + market_order.order_value) / new_size
            else:
                new_size = self.spot_position - market_order.size
                if new_size == 0:
                    self.spot_entry_price = Decimal('nan')
            self.spot_position = new_size
        else:
            if market_order.side == Side.SELL:
                new_size = self.future_position - market_order.size
                if self.future_entry_price.is_nan():
                    self.future_entry_price = market_order.price
                else:
                    self.future_entry_price = (self.future_entry_price * self.future_position - market_order.order_value) / new_size
            else:
                new_size = self.future_position + market_order.size
                if new_size == 0:
                    self.future_entry_price = Decimal('nan')
            self.future_position = new_size

    def open_position(self, spot_market_order: MarketOrder, future_market_order: MarketOrder, collateral_weight: Decimal, leverage: Decimal = Decimal('1')):
        assert spot_market_order.side == Side.BUY
        assert future_market_order.side == Side.SELL
        assert collateral_weight <= Decimal(1)
        assert leverage >= Decimal(1)
        assert spot_market_order.size == future_market_order.size

        fee = spot_market_order.fee + future_market_order.fee
        fund_required = spot_market_order.order_value * (1 - collateral_weight) + future_market_order.order_value / leverage + fee
        self.deposit(max(0, fund_required - self.balance))
        self.balance -= fund_required
        self.profit -= fee
        self.place_market_order(spot_market_order)
        self.place_market_order(future_market_order)

    def close_position(self, spot_market_order: MarketOrder, future_market_order: MarketOrder, collateral_weight: Decimal, leverage: Decimal = Decimal('1')):
        assert spot_market_order.side == Side.SELL
        assert future_market_order.side == Side.BUY
        assert collateral_weight <= Decimal(1)
        assert leverage >= Decimal(1)
        assert spot_market_order.size == future_market_order.size

        size = spot_market_order.size
        fee = spot_market_order.fee + future_market_order.fee
        fund_released = spot_market_order.order_value
        fund_released += (self.future_entry_price * (1 + 1 / leverage)- future_market_order.price) * size
        fund_released -= self.spot_entry_price * spot_market_order.size * collateral_weight
        fund_released -= fee
        self.balance += fund_released
        self.withdraw(max(0, self.balance))
        self.profit += ((self.future_entry_price - future_market_order.price) + (spot_market_order.price - self.spot_entry_price)) * size - fee
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

    def append_hedge_trade(self, timestamp: float, hedge_type: HedgeType, basis: Decimal):
        self.hedge_trades.append(HedgeTrade(timestamp=timestamp, hedge_type=hedge_type, basis=basis))


class HedgeType(Enum):
    OPEN = 'open'
    CLOSE = 'close'


@dataclass
class HedgeTrade:
    timestamp: float
    hedge_type: HedgeType
    basis: Decimal


class CombinedModelHedgeType(Enum):
    BOTH_OPEN = auto()
    MODEL1_OPEN = auto()
    MODEL2_OPEN = auto()
    BOTH_CLOSE = auto()
    MODEL1_CLOSE = auto()
    MODEL2_CLOSE = auto()
    ONE_OPEN_THE_OTHER_CLOSE = auto()


@dataclass
class CombinedModelHedgeTrade:
    timestamp: float
    hedge_type: CombinedModelHedgeType
    basis: Decimal


@dataclass
class CombinedModelState(BaseState):
    combined_model_hedge_trades: List[CombinedModelHedgeTrade] = field(default_factory=lambda: [])

    def append_hedge_trade(self, timestamp: float, hedge_type: CombinedModelHedgeType, basis: Decimal):
        self.combined_model_hedge_trades.append(CombinedModelHedgeTrade(timestamp, hedge_type, basis))
