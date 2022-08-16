import asyncio
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict
from uuid import UUID

from src.exchange.ftx.ftx_data_type import FtxFundOpenFilledMessage, FtxFundRequestMessage, FtxFundResponseMessage


@dataclass
class FundManager:
    free_collateral: Decimal = Decimal(0)
    free_usd: Decimal = Decimal(0)
    borrowed_usd: Decimal = Decimal(0)
    collateral_freeze: Dict[UUID, Decimal] = {}
    lock: asyncio.Lock = asyncio.Lock()

    async def update_free_collateral(self, free_collateral: Decimal):
        async with self.lock:
            self.free_collateral = free_collateral
            for freeze_amount in self.collateral_freeze.values():
                self.free_collateral -= freeze_amount
            self.free_collateral = max(Decimal(0), self.free_collateral)

    async def update_usd_state(self, free_usd: Decimal, spot_borrow: Decimal):
        async with self.lock:
            self.free_usd = free_usd
            self.borrowed_usd = spot_borrow

    async def request_for_open(self, request: FtxFundRequestMessage) -> FtxFundResponseMessage:
        async with self.lock:
            if self.free_collateral <= 0:
                return FtxFundResponseMessage(
                    id=request.id,
                    approve=False,
                    fund_supply=Decimal(0),
                    borrow=Decimal(0))
            elif request.fund_needed < self.free_collateral:
                # handle collateral change
                fund_supply = request.fund_needed
                self.collateral_freeze[request.id] = fund_supply
                self.free_collateral -= fund_supply
                # handle usd change
                borrow = max(Decimal(0), request.spot_notional_value - self.free_usd)
                self.borrowed_usd += borrow
                self.free_usd = max(Decimal(0), self.free_usd - fund_supply)
                return FtxFundResponseMessage(
                    id=request.id,
                    approve=True,
                    fund_supply=fund_supply,
                    borrow=borrow)
            else:
                # handle collateral change
                fund_supply = self.free_collateral
                self.collateral_freeze[request.id] = fund_supply
                self.free_collateral = Decimal(0)
                # handle usd change
                borrow = max(Decimal(0), request.spot_notional_value - self.free_usd)
                self.borrowed_usd += borrow
                self.free_usd = max(Decimal(0), self.free_usd - fund_supply)
                return FtxFundResponseMessage(
                    id=request.id,
                    approve=True,
                    fund_supply=fund_supply,
                    borrow=fund_supply)

    async def handle_open_order_filled(self, msg: FtxFundOpenFilledMessage):
        async with self.lock:
            freeze_amount = self.collateral_freeze.get(msg.id, Decimal(0))
            # handle collateral change in freeze
            self.free_collateral += freeze_amount
            # handle collateral change
            self.free_collateral -= msg.fund_used
            