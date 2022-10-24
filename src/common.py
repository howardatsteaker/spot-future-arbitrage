from __future__ import annotations

import decimal
from decimal import Decimal
from enum import Enum, auto
from typing import List

import yaml

from src.util.funding_service import FundingServiceConfig
from src.util.rate_limit import RateLimitConfig
from src.util.slack import SlackConfig


class AutoName(Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name


class Exchange(AutoName):
    FTX = auto()
    BINANCE = auto()

    @staticmethod
    def from_str(s: str):
        s_lower = s.lower()
        if s_lower == "ftx":
            return Exchange.FTX
        elif s_lower == "binance":
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
        ignore_interest_rate: bool,
        interest_rate_lookback_days: int,
        estimated_borrowing_days: Decimal,
        season: str,
        log: dict,
        indicator: dict,
        ticker_delay_threshold: float,
        apr_to_open_position: Decimal,
        min_order_size_mode: bool,
        open_order_size_multiplier: Decimal,
        close_order_size_multiplier: Decimal,
        max_leverage: Decimal,
        leverage_limit: Decimal,
        seconds_before_expiry_to_stop_open_position: float,
        seconds_before_expiry_to_stop_close_position: float,
        release_mode: bool,
        open_fee_coverage_multiplier: Decimal,
        max_open_budget: Decimal,
        cooldown_open_budget: Decimal,
        min_leverage_to_use_cooldown_budget: Decimal,
        min_volume_usd_24h: float,
        whitelist: List[str],
        blacklist: List[str],
        slack_config: SlackConfig,
        rate_limit_config: RateLimitConfig,
        funding_service_config: FundingServiceConfig,
    ):
        self.exchange = exchange
        self.api_key = api_key
        self.api_secret = api_secret
        self.subaccount_name = subaccount_name
        self.ignore_interest_rate = ignore_interest_rate
        self.interest_rate_lookback_days = interest_rate_lookback_days
        self.estimated_borrowing_days = estimated_borrowing_days
        self.season = season
        self.log = log
        self.indicator = indicator
        self.ticker_delay_threshold = ticker_delay_threshold
        self.apr_to_open_position = apr_to_open_position
        self.min_order_size_mode = min_order_size_mode
        self.open_order_size_multiplier = open_order_size_multiplier
        self.max_open_budget = max_open_budget
        self.cooldown_open_budget = cooldown_open_budget
        self.min_leverage_to_use_cooldown_budget = min_leverage_to_use_cooldown_budget
        self.min_volume_usd_24h = min_volume_usd_24h
        self.close_order_size_multiplier = close_order_size_multiplier
        self.max_leverage = max_leverage
        self.leverage_limit = leverage_limit
        assert (
            seconds_before_expiry_to_stop_open_position
            >= seconds_before_expiry_to_stop_close_position
        ), "stop open should greater than or equal to stop close"
        assert (
            seconds_before_expiry_to_stop_close_position > 3600
        ), "stop close should greater than 3600s (1 hour)"
        self.seconds_before_expiry_to_stop_open_position = (
            seconds_before_expiry_to_stop_open_position
        )
        self.seconds_before_expiry_to_stop_close_position = (
            seconds_before_expiry_to_stop_close_position
        )
        self.release_mode = release_mode
        self.open_fee_coverage_multiplier = open_fee_coverage_multiplier
        self.whitelist = whitelist
        self.blacklist = blacklist
        self.slack_config = slack_config
        self.rate_limit_config = rate_limit_config
        self.funding_service_config = funding_service_config

    @classmethod
    def from_yaml(cls, file_path: str) -> Config:
        with open(file_path, "r") as f:
            data = yaml.load(f, yaml.SafeLoader)

        if data.get("whitelist"):
            whitelist: List[str] = data["whitelist"]
        else:
            whitelist = []

        if data.get("blacklist"):
            blacklist: List[str] = data["blacklist"]
        else:
            blacklist = []
        return Config(
            exchange=Exchange.from_str(data["exchange"]["name"]),
            api_key=data["exchange"]["api_key"],
            api_secret=data["exchange"]["api_secret"],
            subaccount_name=data["exchange"]["subaccount_name"],
            ignore_interest_rate=data["interest_rate"]["ignore"],
            interest_rate_lookback_days=data["interest_rate"]["lookback_days"],
            estimated_borrowing_days=Decimal(
                str(data["interest_rate"]["estimated_borrowing_days"])
            ),
            season=data["season"],
            log=data["log"],
            indicator=data["indicator"],
            ticker_delay_threshold=data["strategy"]["ticker_delay_threshold"],
            apr_to_open_position=Decimal(str(data["strategy"]["apr_to_open_position"])),
            min_order_size_mode=data["strategy"]["min_order_size_mode"],
            open_order_size_multiplier=Decimal(
                str(data["strategy"]["open_order_size_multiplier"])
            ),
            close_order_size_multiplier=Decimal(
                str(data["strategy"]["close_order_size_multiplier"])
            ),
            max_leverage=Decimal(str(data["strategy"]["max_leverage"])),
            leverage_limit=Decimal(str(data["strategy"]["leverage_limit"])),
            seconds_before_expiry_to_stop_open_position=data["strategy"][
                "seconds_before_expiry_to_stop_open_position"
            ],
            seconds_before_expiry_to_stop_close_position=data["strategy"][
                "seconds_before_expiry_to_stop_close_position"
            ],
            release_mode=data["strategy"]["release_mode"],
            open_fee_coverage_multiplier=Decimal(
                str(data["strategy"]["open_fee_coverage_multiplier"])
            ),
            max_open_budget=Decimal(str(data["strategy"]["max_open_budget"])),
            cooldown_open_budget=Decimal(str(data["strategy"]["cooldown_open_budget"])),
            min_leverage_to_use_cooldown_budget=Decimal(
                str(data["strategy"]["min_leverage_to_use_cooldown_budget"])
            ),
            whitelist=whitelist,
            blacklist=blacklist,
            slack_config=SlackConfig(
                enable=data["slack"]["enable"],
                auth_token=data["slack"]["auth_token"],
                summary_channel=data["slack"]["summary_channel"],
                alert_channel=data["slack"]["alert_channel"],
            ),
            rate_limit_config=RateLimitConfig(
                interval=data["rate_limit"]["interval"],
                limit=data["rate_limit"]["limit"],
            ),
            funding_service_config=FundingServiceConfig(
                enable=data["funding_service"]["enable"],
                target_leverage=Decimal(
                    str(data["funding_service"]["target_leverage"])
                ),
                leverage_upper_bound=Decimal(
                    str(data["funding_service"]["leverage_upper_bound"])
                ),
                leverage_lower_bound=Decimal(
                    str(data["funding_service"]["leverage_lower_bound"])
                ),
                min_deposit_amount=Decimal(
                    str(data["funding_service"]["min_deposit_amount"])
                ),
                min_withdraw_amount=Decimal(
                    str(data["funding_service"]["min_withdraw_amount"])
                ),
                min_remain=Decimal(str(data["funding_service"]["min_remain"])),
            ),
        )


def to_decimal_or_none(number: float | int | str) -> Decimal | None:
    if isinstance(number, (float, int)):
        return Decimal(str(number))
    elif isinstance(number, str):
        try:
            return Decimal(number)
        except decimal.InvalidOperation:
            return None
    else:
        return None
