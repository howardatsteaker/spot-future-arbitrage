from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class TradeType(Enum):
    BOTH = "both"
    CLOSE_ONLY = "close_only"
    NEITHER = "neither"


@dataclass
class HedgePair:
    coin: str
    spot: str
    future: str
    trade_type: TradeType = TradeType.BOTH

    @classmethod
    def from_coin(
        cls, coin: str, season: str, trade_type: TradeType = TradeType.BOTH
    ) -> HedgePair:
        return cls(
            coin=coin,
            spot=cls.coin_to_spot(coin),
            future=cls.coin_to_future(coin, season),
            trade_type=trade_type,
        )

    @classmethod
    def from_spot(
        cls, spot: str, season: str, trade_type: TradeType = TradeType.BOTH
    ) -> HedgePair:
        return cls(
            coin=cls.spot_to_coin(spot),
            spot=spot,
            future=cls.spot_to_future(spot, season),
            trade_type=trade_type,
        )

    @classmethod
    def from_future(
        cls, future: str, trade_type: TradeType = TradeType.BOTH
    ) -> HedgePair:
        return cls(
            coin=cls.future_to_coin(future),
            spot=cls.future_to_spot(future),
            future=future,
            trade_type=trade_type,
        )

    @staticmethod
    def coin_to_spot(coin: str) -> str:
        raise NotImplementedError

    @staticmethod
    def coin_to_future(coin: str, season: str) -> str:
        raise NotImplementedError

    @staticmethod
    def spot_to_coin(spot: str) -> str:
        raise NotImplementedError

    @staticmethod
    def spot_to_future(spot: str, season: str) -> str:
        raise NotImplementedError

    @staticmethod
    def future_to_coin(future: str) -> str:
        raise NotImplementedError

    @staticmethod
    def future_to_spot(future: str) -> str:
        raise NotImplementedError

    @staticmethod
    def is_spot(symbol: str) -> bool:
        raise NotImplementedError

    @staticmethod
    def is_future(symbol: str, season: str) -> bool:
        raise NotImplementedError

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
