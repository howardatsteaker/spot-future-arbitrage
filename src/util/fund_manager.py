import asyncio
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict

from src.exchange.ftx.ftx_data_type import (FtxFundOpenFilledMessage,
                                            FtxFundRequestMessage,
                                            FtxFundResponseMessage,
                                            FtxLeverageInfo)


@dataclass
class FundManager:
    leverage_limit: Decimal = None
    free_collateral: Decimal = Decimal(0)
    collateral_freeze: Dict[str, Decimal] = field(
        default_factory=lambda: dict()
    )  # use coin as the key
    leverage_info: FtxLeverageInfo = FtxLeverageInfo()
    lock: asyncio.Lock = asyncio.Lock()

    async def update_account_state(self, account_info: dict):
        free_collateral = Decimal(str(account_info["freeCollateral"]))
        max_leverage = Decimal(str(account_info["leverage"]))
        account_value = Decimal(str(account_info["totalAccountValue"]))
        position_value = Decimal(str(account_info["totalPositionSize"]))
        current_leverage = position_value / account_value
        leverage_info = FtxLeverageInfo(
            max_leverage, account_value, position_value, current_leverage
        )
        async with self.lock:
            self.free_collateral = free_collateral
            for freeze_amount in self.collateral_freeze.values():
                self.free_collateral -= freeze_amount
            self.free_collateral = max(Decimal(0), self.free_collateral)
            self.leverage_info = leverage_info

    async def request_for_budget(
        self, request: FtxFundRequestMessage
    ) -> FtxFundResponseMessage:
        async with self.lock:
            if self.leverage_limit is not None:
                collateral = self._get_free_collateral_with_leverage_limit()
            else:
                collateral = self.free_collateral
            if collateral <= 0:
                return FtxFundResponseMessage(coin=request.coin, fund_supply=Decimal(0))
            elif request.fund_needed < collateral:
                # handle collateral change
                fund_supply = request.fund_needed
                self.collateral_freeze[request.coin] = fund_supply
                self.free_collateral -= fund_supply
                return FtxFundResponseMessage(
                    coin=request.coin, fund_supply=fund_supply
                )
            else:
                # handle collateral change
                fund_supply = collateral
                self.collateral_freeze[request.coin] = fund_supply
                self.free_collateral = Decimal(0)
                return FtxFundResponseMessage(
                    coin=request.coin, fund_supply=fund_supply
                )

    def _get_free_collateral_with_leverage_limit(self):
        available_collateral = (
            self.leverage_limit * self.leverage_info.account_value
            - self.leverage_info.position_value
        ) / (self.leverage_limit - 1)
        return max(Decimal(0), min(available_collateral, self.free_collateral))

    async def handle_open_order_filled(self, msg: FtxFundOpenFilledMessage):
        async with self.lock:
            freeze_amount = self.collateral_freeze.get(msg.coin, Decimal(0))
            # handle collateral change in freeze
            self.free_collateral += freeze_amount
            # handle collateral change
            self.free_collateral -= msg.fund_used
