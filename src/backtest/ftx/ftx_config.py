import datetime
import time
from decimal import Decimal
from typing import TypedDict

import requests

from src.backtest.backtest_data_type import BackTestConfig
from src.exchange.ftx.ftx_data_type import FtxHedgePair


class FtxHedgeConfig(TypedDict):
    name: str
    start_time: str
    end_time: str
    expiration_time: str
    expiration_price: Decimal
    hedge_pair: FtxHedgePair


ftx_future_config_options: list[FtxHedgeConfig] = [
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
        "end_time": "2022/09/30",
        "expiration_time": "2022/09/30",
        "expiration_price": Decimal("19416.4"),  # temp value
        "hedge_pair": FtxHedgePair(coin="BTC", spot="BTC/USD", future="BTC-0930"),
    },
    {
        "name": "BTC 2022 Q4",
        "start_time": "2022/09/30",
        "end_time": "2022/12/30",
        "expiration_time": "2022/12/30",
        "expiration_price": Decimal("20200"),  # temp value
        "hedge_pair": FtxHedgePair(coin="BTC", spot="BTC/USD", future="BTC-1230"),
    },
    {
        "name": "BTC 2022 Q2 0930",
        "start_time": "2022/03/26",
        "end_time": "2022/06/24",
        "expiration_time": "2022/09/30",
        "expiration_price": Decimal("19416.4"),  # temp value
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
    {
        "name": "BTC 2022 Q4 0331",
        "start_time": "2022/09/30",
        "end_time": "2022/12/30",
        "expiration_time": "2022/12/30",
        "expiration_price": Decimal("20000"),  # temp value
        "hedge_pair": FtxHedgePair(coin="BTC", spot="BTC/USD", future="BTC-0331"),
    },
]


def ftx_get_backtest_config(hedge_config: FtxHedgeConfig) -> BackTestConfig:
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

    res = requests.get("https://ftx.com/api/wallet/coins")
    res_json = res.json()
    infos = res_json["result"]
    info = next(info for info in infos if info["id"] == hedge_config["hedge_pair"].coin)
    collateral_weight = Decimal(str(info["collateralWeight"]))

    return BackTestConfig(
        spot_fee_rate=Decimal("0.000228"),
        future_fee_rate=Decimal("0.000228"),
        collateral_weight=collateral_weight,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        ts_to_stop_open=expiration_timestamp - 86400,
        ts_to_expiry=expiration_timestamp,
        expiration_price=hedge_config["expiration_price"],
        leverage=Decimal("3.0"),
        save_dir="local/",
        exchange="ftx",
    )
