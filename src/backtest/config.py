import datetime
import time
from decimal import Decimal
from typing import TypedDict

from ..exchange.ftx.ftx_data_type import FtxHedgePair
from .ftx_data_types import BackTestConfig


class HedgeConfig(TypedDict):
    name: str
    start_time: str
    end_time: str
    expiration_time: str
    expiration_price: Decimal
    hedge_pair: FtxHedgePair


future_config_options: list[HedgeConfig] = [
    {
        "name": "BTC 2022 Q1",
        "start_time": "2022/01/01",
        "end_time": "2022/03/25",
        "expiration_time": "2022/03/25",
        "expiration_price": Decimal("43990.78"),
        "hedge_pair": FtxHedgePair(coin="BTC", spot="BTC/USD", future="BTC-0325"),
    },
    {
        "name": "BTC 2022 Q2",
        "start_time": "2022/03/26",
        "end_time": "2022/06/24",
        "expiration_time": "2022/06/24",
        "expiration_price": Decimal("21141.1"),
        "hedge_pair": FtxHedgePair(coin="BTC", spot="BTC/USD", future="BTC-0624"),
    },
    {
        "name": "BTC 2022 Q3",
        "start_time": "2022/06/25",
        "end_time": "2022/09/05",
        "expiration_time": "2022/09/30",
        "expiration_price": Decimal("20200"),  # temp value
        "hedge_pair": FtxHedgePair(coin="BTC", spot="BTC/USD", future="BTC-0930"),
    },
    {
        "name": "BTC 2022 Q2 0930",
        "start_time": "2022/03/26",
        "end_time": "2022/06/24",
        "expiration_time": "2022/09/30",
        "expiration_price": Decimal("20200"),  # temp value
        "hedge_pair": FtxHedgePair(coin="BTC", spot="BTC/USD", future="BTC-0930"),
    },
    {
        "name": "BTC 2022 Q3 1230",
        "start_time": "2022/06/25",
        "end_time": "2022/09/05",
        "expiration_time": "2022/09/30",
        "expiration_price": Decimal("20200"),  # temp value
        "hedge_pair": FtxHedgePair(coin="BTC", spot="BTC/USD", future="BTC-1230"),
    },
]


def get_backtest_config(hedge_config: HedgeConfig) -> BackTestConfig:
    start_timestamp = int(
        time.mktime(
            datetime.datetime.strptime(
                hedge_config["start_time"], "%Y/%m/%d"
            ).timetuple()
        )
    )
    end_timestamp = int(
        time.mktime(
            datetime.datetime.strptime(hedge_config["end_time"], "%Y/%m/%d").timetuple()
        )
    )
    expiration_timestamp = int(
        time.mktime(
            datetime.datetime.strptime(
                hedge_config["expiration_time"], "%Y/%m/%d"
            ).timetuple()
        )
    )

    return BackTestConfig(
        fee_rate=Decimal("0.000228"),
        collateral_weight=Decimal("0.975"),
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        ts_to_stop_open=expiration_timestamp - 86400,
        ts_to_expiry=expiration_timestamp,
        expiration_price=hedge_config["expiration_price"],
        leverage=Decimal("3"),
        save_dir="local/",
    )
