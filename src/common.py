from __future__ import annotations
from enum import Enum, auto
from decimal import Decimal
import yaml


class AutoName(Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name


class Exchange(AutoName):
    FTX = auto()
    BINANCE = auto()

    @staticmethod
    def from_str(s: str):
        s_lower = s.lower()
        if s_lower == 'ftx':
            return Exchange.FTX
        elif s_lower == 'binance':
            raise ValueError("Exchange.BINANCE is not implement")
        else:
            raise ValueError("Exchange not available")


class Config:

    def __init__(
            self,
            exchange: Exchange,
            api_key: str,
            api_secret: str,
            subaccount_name: str,
            interest_rate_lookback_days: int,
            season: str,
            log: dict,
            indicator: dict,
            ticker_delay_threshold: float,
            apr_to_open_position: Decimal,
            min_order_size_mode: bool,
            open_order_size_multiplier: Decimal,
            close_order_size_multiplier: Decimal,
            max_leverage: Decimal,
            leverage_limit: Decimal):
        self.exchange=exchange
        self.api_key=api_key
        self.api_secret=api_secret
        self.subaccount_name=subaccount_name
        self.interest_rate_lookback_days = interest_rate_lookback_days
        self.season = season
        self.log = log
        self.indicator = indicator
        self.ticker_delay_threshold = ticker_delay_threshold
        self.apr_to_open_position = apr_to_open_position
        self.min_order_size_mode = min_order_size_mode
        self.open_order_size_multiplier = open_order_size_multiplier
        self.close_order_size_multiplier = close_order_size_multiplier
        self.max_leverage = max_leverage
        self.leverage_limit = leverage_limit


    @classmethod
    def from_yaml(cls, file_path: str) -> Config:
        with open(file_path, 'r') as f:
            data = yaml.load(f, yaml.SafeLoader)

        return Config(
            exchange=Exchange.from_str(data['exchange']['name']),
            api_key=data['exchange']['api_key'],
            api_secret=data['exchange']['api_secret'],
            subaccount_name=data['exchange']['subaccount_name'],
            interest_rate_lookback_days=data['interest_rate']['lookback_days'],
            season=data['season'],
            log=data['log'],
            indicator=data['indicator'],
            ticker_delay_threshold=data['strategy']['ticker_delay_threshold'],
            apr_to_open_position=Decimal(str(data['strategy']['apr_to_open_position'])),
            min_order_size_mode=data['strategy']['min_order_size_mode'],
            open_order_size_multiplier=Decimal(str(data['strategy']['open_order_size_multiplier'])),
            close_order_size_multiplier=Decimal(str(data['strategy']['close_order_size_multiplier'])),
            max_leverage=Decimal(str(data['strategy']['max_leverage'])),
            leverage_limit=Decimal(str(data['strategy']['leverage_limit'])),
        )
