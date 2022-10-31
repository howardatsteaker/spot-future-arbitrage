import asyncio
import time
from argparse import ArgumentParser
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Union

import dateutil.parser
import pandas as pd
import pytz
import uvloop

from src.common import Config
from src.exchange.ftx.ftx_client import FtxExchange
from src.exchange.ftx.ftx_data_type import FtxHedgePair


@dataclass
class HedgeState:
    hedge_pair: FtxHedgePair
    taker_fee_rate: Decimal
    spot_size: Decimal = Decimal(0)
    future_size: Decimal = Decimal(0)
    spot_entry_price: Union[Decimal, None] = None
    future_entry_price: Union[Decimal, None] = None
    pnl: Decimal = Decimal(0)
    last_spot_fill_price: Union[Decimal, str] = None

    def fill_entry(self, fill: dict):
        market: str = fill["market"]
        side: str = fill["side"]
        price: Decimal = Decimal(str(fill["price"]))
        size: Decimal = Decimal(str(fill["size"]))
        fee: Decimal = Decimal(str(fill["fee"]))
        fee_currency: str = fill["feeCurrency"]
        if market == self.hedge_pair.spot:
            if side == "buy":
                new_size = self.spot_size + size
                if self.spot_entry_price is None:
                    self.spot_entry_price = price
                else:
                    self.spot_entry_price = (
                        self.spot_entry_price * self.spot_size + price * size
                    ) / new_size
            elif side == "sell":
                if self.spot_entry_price:
                    self.pnl += (price - self.spot_entry_price) * size
                new_size = self.spot_size - size
            else:
                raise ValueError(f"Invalid side: {side}")
            self.spot_size = new_size
            if new_size == 0:
                self.spot_entry_price = None
            if fee_currency == "USD":
                self.pnl -= fee
            self.last_spot_fill_price = price
        elif market == self.hedge_pair.future:
            if side == "sell":
                new_size = self.future_size + size
                if self.future_entry_price is None:
                    self.future_entry_price = price
                else:
                    self.future_entry_price = (
                        self.future_entry_price * self.future_size + price * size
                    ) / new_size
                self.future_size = new_size
            elif side == "buy":
                if self.future_entry_price:
                    self.pnl += (self.future_entry_price - price) * size
                new_size = self.future_size - size
                if new_size == 0:
                    self.future_entry_price = None
                self.future_size = new_size
            else:
                raise ValueError(f"Invalid side: {side}")
            if fee_currency == "USD":
                self.pnl -= fee
        else:
            raise ValueError(f"Invalid market name: {market}")

    @property
    def unrealized_pnl(self) -> Decimal:
        """Assume future price expiry at last spot price"""
        if not (self.spot_entry_price and self.future_entry_price):
            return Decimal(0)

        pnl = (
            (self.last_spot_fill_price - self.spot_entry_price) * self.spot_size
            + (self.future_entry_price - self.last_spot_fill_price) * self.future_size
            - self.last_spot_fill_price * self.spot_size * self.taker_fee_rate
        )

        return pnl


@dataclass
class DepositOrWithdraw:
    type: str
    size: Decimal
    time: datetime


async def main():
    parser = ArgumentParser()
    parser.add_argument("-c", "--config", required=True, help="config file path")
    args = parser.parse_args()
    config_path = args.config
    config = Config.from_yaml(config_path)
    ftx = FtxExchange(config.api_key, config.api_secret, config.subaccount_name)

    # configs
    st = dateutil.parser.parse("2022-10-01T18:00:01.712767+00:00").timestamp()
    expiry_dt: datetime = datetime(2022, 12, 30, 16, tzinfo=pytz.utc)  # for 1230
    # expiry_dt: datetime = datetime(2023, 3, 31, 16, tzinfo=pytz.utc)  # for 0331

    results: Dict[str, HedgeState] = {}
    et: float = time.time()
    account_info = await ftx.get_account()
    taker_fee_rate = Decimal(str(account_info["takerFee"]))
    fills = await ftx.get_fills(st, et)
    for fill in fills:
        if fill["type"] == "otc":
            fill["market"] = f"{fill['baseCurrency']}/{fill['quoteCurrency']}"
        market: str = fill["market"]

        # get coin
        if FtxHedgePair.is_future(market, config.season):
            pair = FtxHedgePair.from_future(market)
        elif FtxHedgePair.is_spot(market):
            pair = FtxHedgePair.from_spot(market, config.season)
        else:
            raise ValueError(f"Invalid market: {market}")

        # create coin state map
        if not results.get(pair.coin):
            results[pair.coin] = HedgeState(pair, taker_fee_rate)

        # fill entry
        results[pair.coin].fill_entry(fill)

    # sum up all results
    all_pnl = Decimal(0)
    all_unreal_pnl = Decimal(0)
    for result in results.values():
        all_pnl += result.pnl
        all_unreal_pnl += result.unrealized_pnl
        print(
            f"{result.hedge_pair.future} realized pnl: {result.pnl:.2f}, unrealized pnl: {result.unrealized_pnl:.2f}"
        )
    print(f"All realized pnl: {all_pnl:.2f}")
    print(f"All unrealized pnl: {all_unreal_pnl:.2f}")

    # get deposit and withdraw history
    all_history: List[DepositOrWithdraw] = []
    deposit_his = await ftx.get_deposit_history(1.0, et)
    withdraw_his = await ftx.get_withdraw_history(1.0, et)
    for his in deposit_his:
        if his["coin"] in ["USD", "USDC", "USDT"]:
            all_history.append(
                DepositOrWithdraw(
                    type="d",
                    size=Decimal(str(his["size"])),
                    time=dateutil.parser.parse(his["time"]),
                )
            )
    for his in withdraw_his:
        if his["coin"] in ["USD", "USDC"]:
            all_history.append(
                DepositOrWithdraw(
                    type="w",
                    size=Decimal(str(his["size"])),
                    time=dateutil.parser.parse(his["time"]),
                )
            )
    all_history = sorted(all_history, key=lambda item: item.time)

    usd_balance = Decimal(0)
    balance_history = []
    for his in all_history:
        if his.type == "d":
            usd_balance += his.size
        elif his.type == "w":
            usd_balance -= his.size
        log = (his.time, usd_balance)
        balance_history.append(log)
    df = pd.DataFrame(balance_history, columns=["datetime", "balance"])
    df["duration"] = df["datetime"].shift(-1) - df["datetime"]
    df.loc[df.index[-1], "duration"] = expiry_dt - df["datetime"].iloc[-1]
    df["duration_ts"] = df["duration"].values.astype("int64") // 10 ** 9
    df["balance"] = df["balance"].astype("float64")

    # filter datetime with start_time
    df = df[df["datetime"] >= datetime.fromtimestamp(st, tz=pytz.utc)]
    avg_deposit = Decimal(
        str((df["balance"] * df["duration_ts"]).sum() / df["duration_ts"].sum())
    )
    print(f"Avg. deposit: {avg_deposit}")

    # APR
    roi: Decimal = (all_pnl + all_unreal_pnl) / avg_deposit
    running_days: float = (expiry_dt.timestamp() - st) / 86400
    apr: Decimal = roi * Decimal(365) / Decimal(str(running_days))
    print(f"APR: {apr:.2%}")

    await ftx.close()


if __name__ == "__main__":
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
