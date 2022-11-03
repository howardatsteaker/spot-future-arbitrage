from dataclasses import dataclass
from decimal import Decimal


@dataclass
class FundingServiceConfig:
    enable: bool
    target_leverage: Decimal
    leverage_upper_bound: Decimal
    leverage_lower_bound: Decimal
    min_deposit_amount: Decimal
    min_withdraw_amount: Decimal
    min_remain: Decimal
    daily_max_net_deposit: Decimal
