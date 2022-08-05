import re
import time
import asyncio
from typing import Dict
from decimal import Decimal
import dateutil.parser
from src.common import Config, Exchange
from src.exchange.ftx.ftx_client import FtxExchange
from src.exchange.ftx.ftx_data_type import FtxCollateralWeight, Ftx_EWMA_InterestRate, FtxFeeRate, FtxTradingRule, FtxHedgePair


class MainProcess:
    MARKET_STATUS_POLLING_INTERVAL = 300
    INTEREST_RATE_POLLING_INTERVAL = 3600
    FEE_RATE_POLLING_INTERVAL = 300
    COLLATERAL_WEIGHT_POLLING_INTERVAL = 300

    def __init__(self, config: Config):
        self.config: Config = config
        if config.exchange == Exchange.FTX:
            self.exchange = FtxExchange(config.api_key, config.api_secret, config.subaccount_name)
            self.trading_rules: Dict[str, FtxTradingRule] = {}
            self.hedge_pairs: Dict[str, FtxHedgePair] = {}
            self.ewma_interest_rate = Ftx_EWMA_InterestRate(config.interest_rate_lookback_days)
            self.fee_rate = FtxFeeRate()
            self.collateral_weights: Dict[str, FtxCollateralWeight] = {}

            self._market_status_polling_task: asyncio.Task = None
            self._interest_rate_polling_task: asyncio.Task = None
            self._fee_rate_polling_task: asyncio.Task = None
            self._collateral_weight_polling_task: asyncio.Task = None

    @property
    def interest_rate(self) -> Decimal:
        return self.ewma_interest_rate.hourly_rate

    @property
    def status_dict(self) -> Dict[str, bool]:
        return {
            "trading_rule_initialized": len(self.trading_rules) > 0,
            "hedge_pair_initialized": len(self.hedge_pairs) > 0,
            "interest_rate_initialized": self.interest_rate is not None,
            "taker_fee_rate_initialized": self.fee_rate.taker_fee_rate is not None,
            "collateral_weight_initialized": len(self.collateral_weights) > 0,
        }

    @property
    def ready(self) -> bool:
        return all(self.status_dict.values())

    def start_network(self):
        if self._market_status_polling_task is None:
            self._market_status_polling_task = asyncio.create_task(self._market_status_polling_loop())
        if self._interest_rate_polling_task is None:
            self._interest_rate_polling_task = asyncio.create_task(self._interest_rate_polling_loop())
        if self._fee_rate_polling_task is None:
            self._fee_rate_polling_task = asyncio.create_task(self._fee_rate_polling_loop())
        if self._collateral_weight_polling_task is None:
            self._collateral_weight_polling_task = asyncio.create_task(self._collateral_weight_polling_loop())

    def stop_network(self):
        if self._market_status_polling_task is not None:
            self._market_status_polling_task.cancel()
            self._market_status_polling_task = None
        if self._interest_rate_polling_task is not None:
            self._interest_rate_polling_task.cancel()
            self._interest_rate_polling_task = None
        if self._fee_rate_polling_task is not None:
            self._fee_rate_polling_task.cancel()
            self._fee_rate_polling_task = None
        if self._collateral_weight_polling_task is not None:
            self._collateral_weight_polling_task.cancel()
            self._collateral_weight_polling_task = None

    async def _market_status_polling_loop(self):
        """Handle the market infomations. Combined the bollowing tasks to make only one request.
        1. update TradingRule
        2. uddate HedgePair
        """
        while True:
            try:
                markets = await self.exchange.get_markets()
                self._update_trading_rule(markets)
                self._update_hedge_pair(markets)
                await asyncio.sleep(self.MARKET_STATUS_POLLING_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print("Unexpected error while fetching market status.")
                print(e)
                await asyncio.sleep(1)

    def _update_trading_rule(self, market_infos: dict):
        for market in market_infos:
            symbol = market['name']
            min_order_size = Decimal(str(market['sizeIncrement']))
            self.trading_rules[symbol] = FtxTradingRule(symbol, min_order_size)

    def _update_hedge_pair(self, market_infos: dict):
        ## For testing
        # self.hedge_pairs['BTC'] = FtxHedgePair(
        #     coin='BTC',
        #     spot='BTC/USD',
        #     future=f'BTC-{self.config.season}'
        # )
        symbol_set = set([info['name'] for info in market_infos if info['enabled']])
        regex = re.compile(f"[0-9A-Z]+-{self.config.season}")
        for symbol in symbol_set:
            if regex.match(symbol) and FtxHedgePair.future_to_spot(symbol) in symbol_set:
                coin = FtxHedgePair.future_to_coin(symbol)
                spot = FtxHedgePair.coin_to_spot(coin)
                self.hedge_pairs[coin] = FtxHedgePair(
                    coin=coin,
                    spot=spot,
                    future=symbol
                )

    async def _interest_rate_polling_loop(self):
        while True:
            try:
                et = time.time()
                ewma = self.ewma_interest_rate.last_ewma
                if ewma is None:
                    st = et - self.ewma_interest_rate.lookback_days * 24 * 3600
                else:
                    st = self.ewma_interest_rate.last_timestamp + 1
                rate_info = await self.exchange.get_full_spot_margin_history(st, et)
                if len(rate_info) == 0:
                    await asyncio.sleep(self.INTEREST_RATE_POLLING_INTERVAL)
                    continue
                for info in rate_info:
                    rate = Decimal(str(info['rate']))
                    ewma = self.ewma_interest_rate.lambda_ * rate + (1 - self.ewma_interest_rate.lambda_) * ewma if ewma else rate
                self.ewma_interest_rate.last_ewma = ewma
                self.ewma_interest_rate.last_timestamp = dateutil.parser.parse(rate_info[-1]['time'])
                await asyncio.sleep(self.INTEREST_RATE_POLLING_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception:
                print("Unexpected error while fetching USD interest rate.")
                await asyncio.sleep(5)

    async def _fee_rate_polling_loop(self):
        while True:
            try:
                account = await self.exchange.get_account()
                self.fee_rate.maker_fee_rate = Decimal(str(account['makerFee']))
                self.fee_rate.taker_fee_rate = Decimal(str(account['takerFee']))
                await asyncio.sleep(self.FEE_RATE_POLLING_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception:
                print("Unexpected error while fetching account fee rate.")
                await asyncio.sleep(5)

    async def _collateral_weight_polling_loop(self):
         while True:
            try:
                coin_infos = await self.exchange.get_coins()
                for info in coin_infos:
                    self.collateral_weights[info['id']] = FtxCollateralWeight(
                        coin=info['id'],
                        weight=Decimal(str(info['collateralWeight'])))
                await asyncio.sleep(self.COLLATERAL_WEIGHT_POLLING_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception:
                print("Unexpected error while fetching account fee rate.")
                await asyncio.sleep(5)

    async def run(self):
        self.start_network()
        while True:
            if self.ready:
                pass
            await asyncio.sleep(1)
        self.stop_network()
        await self.exchange.close()
