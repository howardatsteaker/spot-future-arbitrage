from __future__ import annotations
from enum import Enum, auto
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
            log: dict):
        self.exchange=exchange
        self.api_key=api_key
        self.api_secret=api_secret
        self.subaccount_name=subaccount_name
        self.interest_rate_lookback_days = interest_rate_lookback_days
        self.season = season
        self.log = log

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
        )
