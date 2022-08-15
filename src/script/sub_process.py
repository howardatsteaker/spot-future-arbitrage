import time
import asyncio
from decimal import Decimal
from typing import List
import logging
import pathlib
from enum import Enum
from multiprocessing.connection import Connection
import uvloop
from src.common import Config
from src.exchange.ftx.ftx_client import FtxExchange
from src.exchange.ftx.ftx_data_type import (
    Ftx_EWMA_InterestRate,
    FtxCollateralWeight,
    FtxCollateralWeightMessage,
    FtxFeeRate,
    FtxFeeRateMessage,
    FtxHedgePair,
    FtxInterestRateMessage,
    FtxTradingRule,
    FtxTradingRuleMessage)
from src.indicator.macd import MACD


class TickerNotifyType(Enum):
    SPOT = "SPOT"
    FUTURE = "FUTURE"


class SubProcess:
    INDICATOR_UPDATE_INTERVAL = 3600  # seconds
    INDICATOR_UPDATE_TIME_SHIFT = 30  # seconds

    def __init__(self, hedge_pair: FtxHedgePair, config: Config, conn: Connection):
        self.hedge_pair = hedge_pair
        self.config = config
        self.conn = conn
        self.logger = self._init_get_logger()

        self.spot_trading_rule: FtxTradingRule = None
        self.future_trading_rule: FtxTradingRule = None
        self.ewma_interest_rate: Ftx_EWMA_InterestRate = None
        self.fee_rate: FtxFeeRate = None
        self.collateral_weight: FtxCollateralWeight = None

        self.exchange = FtxExchange(config.api_key, config.api_secret, config.subaccount_name)
        self.exchange.ws_register_ticker_channel([hedge_pair.spot, hedge_pair.future])

        self._consume_main_process_msg_task: asyncio.Task = None
        self._listen_for_ws_task: asyncio.Task = None
        self._indicator_polling_loop_task: asyncio.Task = None

        self.spot_entry_price: Decimal = None
        self.future_entry_price: Decimal = None
        self.spot_position_size: Decimal = Decimal(0)
        self.future_position_size: Decimal = Decimal(0)  # negative means short postion

        self.indicator = self._init_get_indicator()
        self._last_indicator_update_ts: float = 0.0

    def _init_get_logger(self):
        log = self.config.log
        level = logging.getLevelName(log['level'].upper())
        fmt = log['fmt']
        datefmt = log['datefmt']
        formatter = logging.Formatter(fmt, datefmt)
        handlers = []
        if log['to_console']:
            ch = logging.StreamHandler()
            ch.setFormatter(formatter)
            ch.set_name('stream_formatter')
            handlers.append(ch)
        if log['to_file']:
            path = pathlib.Path(log['file_path'])
            if not path.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(log['file_path'], encoding='utf-8')
            fh.setFormatter(formatter)
            fh.set_name('file_handler')
            handlers.append(fh)
        logging.basicConfig(level=level, handlers=handlers)
        logger = logging.getLogger()
        logging.getLogger('asyncio').setLevel(logging.WARNING)
        return logger

    async def _update_position_size(self):
        balances = await self.exchange.get_balances()
        try:
            balance = next(b for b in balances if b['coin'] == self.hedge_pair.coin)
        except StopIteration:
            self.spot_position_size = Decimal(0)
        else:
            self.spot_position_size = Decimal(str(balance['total']))
        self.logger.info(f'{self.hedge_pair.coin} position size is {self.spot_position_size}')

        positions = await self.exchange.get_positions()
        try:
            position = next(p for p in positions if p['future'] == self.hedge_pair.future)
        except StopIteration:
            self.future_position_size = Decimal(0)
        else:
            self.future_position_size = Decimal(str(position['netSize']))
        self.logger.info(f'{self.hedge_pair.future} position size is {self.future_position_size}')

    async def _update_entry_price(self):
        await self._update_position_size()
        spot_fills = await self.exchange.get_fills_since_last_flat(self.hedge_pair.spot, self.spot_position_size)
        self.logger.debug(f"length of {self.hedge_pair.spot} fills is: {len(spot_fills)}")
        future_fills = await self.exchange.get_fills_since_last_flat(self.hedge_pair.future, self.future_position_size)
        self.logger.debug(f"length of {self.hedge_pair.future} fills is: {len(future_fills)}")
        self.spot_entry_price = self._compute_entry_price(self.spot_position_size, spot_fills)
        self.logger.info(f'Update {self.hedge_pair.spot} entry price: {self.spot_entry_price}')
        self.future_entry_price = self._compute_entry_price(self.future_position_size, future_fills)
        self.logger.info(f'Update {self.hedge_pair.future} entry price: {self.future_entry_price}')
        if self.spot_entry_price and self.future_entry_price:
            basis = self.future_entry_price - self.spot_entry_price
            self.logger.info(f"Update {self.hedge_pair.coin} basis: {basis}")

    def _compute_entry_price(self, position_size: Decimal, fills: List[dict]) -> Decimal:
        if position_size == 0:
            return None
        elif position_size > 0:
            temp_position_size = position_size
            my_fills = []
            for fill in reversed(fills):
                size = Decimal(str(fill['size']))
                price = Decimal(str(fill['price']))
                if fill['side'] == 'buy':
                    prev_position_size = temp_position_size - size
                    if prev_position_size <= 0:
                        my_fills.append({'side': 'buy', 'size': temp_position_size, 'price': price})
                        break
                    else:
                        my_fills.append({'side': 'buy', 'size': size, 'price': price})
                        temp_position_size = prev_position_size
                else:
                    temp_position_size += size
                    my_fills.append({'side': 'sell', 'size': size, 'price': price})
            cum_size = Decimal(0)
            entry_price = None
            for fill in reversed(my_fills):
                if fill['side'] == 'buy':
                    new_size = cum_size + fill['size']
                    if entry_price is None:
                        entry_price = fill['price']
                    else:
                        entry_price = (cum_size * entry_price + fill['size'] * fill['price']) / new_size
                    cum_size = new_size
                else:
                    # entry price remain the same when closing position
                    cum_size -= fill['size']
            return entry_price
        else:
            temp_position_size = position_size
            my_fills = []
            for fill in reversed(fills):
                size = Decimal(str(fill['size']))
                price = Decimal(str(fill['price']))
                if fill['side'] == 'sell':
                    prev_position_size = temp_position_size + size
                    if prev_position_size >= 0:
                        my_fills.append({'side': 'sell', 'size': -temp_position_size, 'price': price})
                        break
                    else:
                        my_fills.append({'side': 'sell', 'size': size, 'price': price})
                        temp_position_size = prev_position_size
                else:
                    temp_position_size -= size
                    my_fills.append({'side': 'buy', 'size': size, 'price': price})
            cum_size = Decimal(0)
            entry_price = None
            for fill in reversed(my_fills):
                if fill['side'] == 'sell':
                    new_size = cum_size + fill['size']
                    if entry_price is None:
                        entry_price = fill['price']
                    else:
                        entry_price = (cum_size * entry_price + fill['size'] * fill['price']) / new_size
                    cum_size = new_size
                else:
                    # entry price remain the same when closing position
                    cum_size -= fill['size']
            return entry_price

    def _init_get_indicator(self):
        if self.config.indicator['name'] == 'macd':
            params = self.config.indicator['params']
            return MACD(
                self.hedge_pair,
                fast_length=params['fast_length'],
                slow_length=params['slow_length'],
                signal_length=params['signal_length'],
                std_length=params['std_length'],
                std_mult=params['std_mult'])
        else:
            raise NotImplementedError(f"Sorry, {self.config.indicator['name']} is not implemented")

    def start_network(self):
        if self._consume_main_process_msg_task is None:
            self._consume_main_process_msg_task = asyncio.create_task(self._consume_main_process_msg())
        if self._listen_for_ws_task is None:
            self._listen_for_ws_task = asyncio.create_task(self.exchange.ws_start_network())
        asyncio.create_task(self._update_entry_price())
        if self._indicator_polling_loop_task is None:
            self._indicator_polling_loop_task = asyncio.create_task(self._indicator_polling_loop())

    def stop_network(self):
        if self._consume_main_process_msg_task is not None:
            self._consume_main_process_msg_task.cancel()
            self._consume_main_process_msg_task = None
        if self._listen_for_ws_task is not None:
            self._listen_for_ws_task.cancel()
            self._listen_for_ws_task = None
        if self._indicator_polling_loop_task is not None:
            self._indicator_polling_loop_task.cancel()
            self._indicator_polling_loop_task = None

    async def _consume_main_process_msg(self):
        while True:
            if self.conn.poll():
                msg = self.conn.recv()
                if type(msg) is FtxTradingRuleMessage:
                    trading_rule = msg.trading_rule
                    if trading_rule.symbol == self.hedge_pair.spot:
                        self.spot_trading_rule = trading_rule
                    elif trading_rule.symbol == self.hedge_pair.future:
                        self.future_trading_rule = trading_rule
                    self.logger.debug(f"{self.hedge_pair.coin} Receive trading rule message: {trading_rule}")
                elif type(msg) is FtxInterestRateMessage:
                    self.ewma_interest_rate = msg.ewma_interest_rate
                    self.logger.debug(f"{self.hedge_pair.coin} Receive interest rate message: {msg.ewma_interest_rate}")
                elif type(msg) is FtxFeeRateMessage:
                    self.fee_rate = msg.fee_rate
                    self.logger.debug(f"{self.hedge_pair.coin} Receive fee rate message: {msg.fee_rate}")
                elif type(msg) is FtxCollateralWeightMessage:
                    self.collateral_weight = msg.collateral_weight
                    self.logger.debug(f"{self.hedge_pair.coin} Receive collateral weight message: {msg.collateral_weight}")
                else:
                    self.logger.warning(f"{self.hedge_pair.coin} receive unknown message: {msg}")
            await asyncio.sleep(1)

    async def _indicator_polling_loop(self):
        while True:
            try:
                now_ts = time.time()
                if now_ts - self._last_indicator_update_ts > self.INDICATOR_UPDATE_INTERVAL:
                    await self.indicator.update_indicator_info()
                    up = self.indicator.upper_threshold
                    low = self.indicator.lower_threshold
                    self._last_indicator_update_ts = time.time()
                    self.logger.info(f"Indicator is updated successfully. UP: {up}, LOW: {low}")
                curr_tick = now_ts // self.INDICATOR_UPDATE_INTERVAL * self.INDICATOR_UPDATE_INTERVAL
                curr_tick_with_shift = curr_tick + self.INDICATOR_UPDATE_TIME_SHIFT
                if curr_tick_with_shift > now_ts:
                    wait_time = curr_tick_with_shift - now_ts
                else:
                    wait_time = curr_tick_with_shift + self.INDICATOR_UPDATE_INTERVAL - now_ts
                await asyncio.sleep(wait_time)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.error(f"{self.hedge_pair.coin} Error while update indicator.", exc_info=True)
                await asyncio.sleep(self.INDICATOR_UPDATE_TIME_SHIFT)

    async def wait_spot_ticker_notify(self):
        spot_cond = self.exchange.ticker_notify_conds.get(self.hedge_pair.spot)
        async with spot_cond:
            await spot_cond.wait()

    async def wait_future_ticker_notify(self):
        future_cond = self.exchange.ticker_notify_conds.get(self.hedge_pair.future)
        async with future_cond:
            await future_cond.wait()

    async def wait_either_one_ticker_condition_notify(self) -> TickerNotifyType:
        spot_task = asyncio.create_task(self.wait_spot_ticker_notify(), name='spot')
        future_task = asyncio.create_task(self.wait_future_ticker_notify(), name='future')
        done, _ = await asyncio.wait({spot_task, future_task}, return_when=asyncio.FIRST_COMPLETED)
        if spot_task in done:
            return TickerNotifyType.SPOT
        elif future_task in done:
            return TickerNotifyType.FUTURE

    async def open_position(self):
        # check conditions is not None
        spot_cond = self.exchange.ticker_notify_conds.get(self.hedge_pair.spot)
        if spot_cond is None:
            return
        future_cond = self.exchange.ticker_notify_conds.get(self.hedge_pair.future)
        if future_cond is None:
            return
        # wait ticker notify
        ticker_notify_type = await self.wait_either_one_ticker_condition_notify()
        self.logger.info(f"Get {ticker_notify_type.value} ticker notification")

    async def run(self):
        try:
            self.start_network()
            while True:
                await self.open_position()
                await asyncio.sleep(0)
        except KeyboardInterrupt:
            await self.exchange.close()


def run_sub_process(hedge_pair: FtxHedgePair, config: Config, conn: Connection):
    sub_process = SubProcess(hedge_pair, config, conn)
    sub_process.logger.debug(f'start to run {hedge_pair.coin} process')
    uvloop.install()
    asyncio.run(sub_process.run())
