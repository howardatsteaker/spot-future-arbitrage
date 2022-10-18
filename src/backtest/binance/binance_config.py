import datetime
import time
from decimal import Decimal
from typing import TypedDict

from src.backtest.backtest_data_type import BackTestConfig
from src.exchange.binance.binance_data_type import BinanceUSDTQuaterHedgePair


class BinanceHedgeConfig(TypedDict):
    name: str
    start_time: str
    end_time: str
    expiration_time: str
    expiration_price: Decimal
    hedge_pair: BinanceUSDTQuaterHedgePair


binance_future_config_options: list[BinanceHedgeConfig] = [
    {
        "name": "BTC_220930",
        "start_time": "2022/06/20",
        "end_time": "2022/09/30",
        "expiration_time": "2022/09/30",
        "expiration_price": Decimal("19416.4"),
        "hedge_pair": BinanceUSDTQuaterHedgePair(
            coin="BTC", spot="BTCUSDT", future="BTCUSDT_220930"
        ),
    },
    {
        "name": "BTC_221230",
        "start_time": "2022/09/23",
        "end_time": "2022/12/30",
        "expiration_time": "2022/12/30",
        "expiration_price": Decimal("20000"),  # temp value
        "hedge_pair": BinanceUSDTQuaterHedgePair(
            coin="BTC", spot="BTCUSDT", future="BTCUSDT_221230"
        ),
    },
]


def binance_get_backtest_config(hedge_config: BinanceHedgeConfig) -> BackTestConfig:
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
        spot_fee_rate=Decimal("0.001"),
        future_fee_rate=Decimal("0.0004"),
        collateral_weight=Decimal("0.95"),
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        ts_to_stop_open=expiration_timestamp - 86400,
        ts_to_expiry=expiration_timestamp,
        expiration_price=hedge_config["expiration_price"],
        leverage=Decimal("3.0"),
        save_dir="local/",
        exchange="binance",
    )
