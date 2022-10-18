from __future__ import annotations

from dataclasses import dataclass

from src.exchange.exchange_data_type import (CandleResolution, HedgePair,
                                             TradeType)


@dataclass
class BinanceUSDTQuaterHedgePair(HedgePair):
    coin: str
    spot: str
    future: str
    trade_type: TradeType = TradeType.BOTH

    @staticmethod
    def coin_to_spot(coin: str) -> str:
        return coin + "USDT"

    @staticmethod
    def coin_to_future(coin: str, season: str) -> str:
        assert len(season) == 6  # YYMMDD ex. 221230 -> 2022/12/30
        return coin + "USDT_" + season

    @staticmethod
    def spot_to_coin(spot: str) -> str:
        return spot.split("USDT")[0]

    @staticmethod
    def spot_to_future(spot: str, season: str) -> str:
        assert len(season) == 6  # YYMMDD ex. 221230 -> 2022/12/30
        return spot + "_" + season

    @staticmethod
    def future_to_coin(future: str) -> str:
        return future.split("USDT")[0]

    @staticmethod
    def future_to_spot(future: str) -> str:
        return future.split("_")[0]

    @staticmethod
    def is_spot(symbol: str) -> bool:
        return symbol.endswith("USDT")

    @staticmethod
    def is_future(symbol: str, season: str) -> bool:
        assert len(season) == 6  # YYMMDD ex. 221230 -> 2022/12/30
        return symbol.endswith(f"USDT_{season}")


class BinanceCandleResolution(CandleResolution):
    ONE_MINUTE = 60
    THREE_MINUTES = 180
    FIVE_MINUTES = 300
    FIFTEEN_MINUTES = 900
    THIRTY_MINUTES = 1800
    ONE_HOUR = 3600
    TWO_HOURS = 7200
    FOUR_HOURS = 14400
    SIX_HOURS = 21600
    EIGHT_HOURS = 28800
    TWELVE_HOURS = 43200
    ONE_DAY = 86400
    THREE_DAYS = 259200
    ONE_WEEK = 604800
    ONE_MONTH = 2592000

    @classmethod
    def from_seconds(cls, seconds: int) -> BinanceCandleResolution:
        if seconds == 60:
            return cls.ONE_MINUTE
        elif seconds == 180:
            return cls.THREE_MINUTES
        elif seconds == 300:
            return cls.FIVE_MINUTES
        elif seconds == 900:
            return cls.FIFTEEN_MINUTES
        elif seconds == 1800:
            return cls.THIRTY_MINUTES
        elif seconds == 3600:
            return cls.ONE_HOUR
        elif seconds == 7200:
            return cls.TWO_HOURS
        elif seconds == 14400:
            return cls.FOUR_HOURS
        elif seconds == 21600:
            return cls.SIX_HOURS
        elif seconds == 28800:
            return cls.EIGHT_HOURS
        elif seconds == 43200:
            return cls.TWELVE_HOURS
        elif seconds == 86400:
            return cls.ONE_DAY
        elif seconds == 259200:
            return cls.THREE_DAYS
        elif seconds == 604800:
            return cls.ONE_WEEK
        elif seconds == 2592000:
            return cls.ONE_MONTH
        else:
            raise ValueError(
                "'seconds' must be one of (60, 180, 300, 900, 1800, 3600, 7200, 14400, 21600, 28800, 43200, 86400, 259200, 604800, 2592000)"
            )

    def to_pandas_resample_rule(self) -> str:
        if self is BinanceCandleResolution.ONE_MINUTE:
            return "1min"
        elif self is BinanceCandleResolution.THREE_MINUTES:
            return "3min"
        elif self is BinanceCandleResolution.FIVE_MINUTES:
            return "5min"
        elif self is BinanceCandleResolution.FIFTEEN_MINUTES:
            return "15min"
        elif self is BinanceCandleResolution.THIRTY_MINUTES:
            return "30min"
        elif self is BinanceCandleResolution.ONE_HOUR:
            return "1H"
        elif self is BinanceCandleResolution.TWO_HOURS:
            return "2H"
        elif self is BinanceCandleResolution.FOUR_HOURS:
            return "4H"
        elif self is BinanceCandleResolution.SIX_HOURS:
            return "6H"
        elif self is BinanceCandleResolution.EIGHT_HOURS:
            return "8H"
        elif self is BinanceCandleResolution.TWELVE_HOURS:
            return "12H"
        elif self is BinanceCandleResolution.ONE_DAY:
            return "1D"
        elif self is BinanceCandleResolution.THREE_DAYS:
            return "3D"
        elif self is BinanceCandleResolution.ONE_WEEK:
            return "1W"
        elif self is BinanceCandleResolution.ONE_MONTH:
            return "1MS"

    def to_binance_rest_api_request_str(self) -> str:
        if self is BinanceCandleResolution.ONE_MINUTE:
            return "1m"
        elif self is BinanceCandleResolution.THREE_MINUTES:
            return "3m"
        elif self is BinanceCandleResolution.FIVE_MINUTES:
            return "5m"
        elif self is BinanceCandleResolution.FIFTEEN_MINUTES:
            return "15m"
        elif self is BinanceCandleResolution.THIRTY_MINUTES:
            return "30m"
        elif self is BinanceCandleResolution.ONE_HOUR:
            return "1h"
        elif self is BinanceCandleResolution.TWO_HOURS:
            return "2h"
        elif self is BinanceCandleResolution.FOUR_HOURS:
            return "4h"
        elif self is BinanceCandleResolution.SIX_HOURS:
            return "6h"
        elif self is BinanceCandleResolution.EIGHT_HOURS:
            return "8h"
        elif self is BinanceCandleResolution.TWELVE_HOURS:
            return "12h"
        elif self is BinanceCandleResolution.ONE_DAY:
            return "1d"
        elif self is BinanceCandleResolution.THREE_DAYS:
            return "3d"
        elif self is BinanceCandleResolution.ONE_WEEK:
            return "1w"
        elif self is BinanceCandleResolution.ONE_MONTH:
            return "1M"
