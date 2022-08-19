from dataclasses import dataclass
import time
import asyncio
from decimal import Decimal
from typing import List, Dict
import logging
import pathlib
from enum import Enum
from multiprocessing.connection import Connection
import uuid
import uvloop
import dateutil.parser
from cachetools import TTLCache
from ..exchange.ftx.ftx_error import ExchangeError
from src.common import Config
from src.exchange.ftx.ftx_client import FtxExchange
from src.exchange.ftx.ftx_data_type import (
    Ftx_EWMA_InterestRate,
    FtxCollateralWeight,
    FtxCollateralWeightMessage,
    FtxFeeRate,
    FtxFeeRateMessage,
    FtxFundOpenFilledMessage,
    FtxFundRequestMessage,
    FtxFundResponseMessage,
    FtxHedgePair,
    FtxInterestRateMessage,
    FtxLeverageInfo,
    FtxLeverageMessage,
    FtxOrderMessage,
    FtxOrderStatus,
    FtxOrderType,
    FtxTradingRule,
    FtxTradingRuleMessage,
    Side)
from src.indicator.macd import MACD


@dataclass
class CombinedTradingRule:
    min_order_size: Decimal


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

        self._loop = asyncio.get_event_loop()
        self._main_process_notify_event = asyncio.Event()

        self.spot_trading_rule: FtxTradingRule = None
        self.future_trading_rule: FtxTradingRule = None
        self.combined_trading_rule: CombinedTradingRule = None
        self.ewma_interest_rate: Ftx_EWMA_InterestRate = None
        self.fee_rate: FtxFeeRate = None
        self.collateral_weight: FtxCollateralWeight = None
        self.leverage_info: FtxLeverageInfo = None

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

        self.future_expiry_ts: float = None
        self._future_expiry_ts_update_event = asyncio.Event()

        self._fund_manager_response_event: asyncio.Event = asyncio.Event()
        self._fund_manager_response_messages: Dict[uuid.UUID, FtxFundResponseMessage] = {}

        self._ws_orders: Dict[str, FtxOrderMessage] = TTLCache(maxsize=1000, ttl=60)  # using order_id: str as the mapping key
        self._ws_orders_events: Dict[str, asyncio.Event] = TTLCache(maxsize=1000, ttl=60)  # using order_id: str as the mapping key

    @property
    def ready(self) -> bool:
        # check conditions is not None
        spot_cond = self.exchange.ticker_notify_conds.get(self.hedge_pair.spot)
        if spot_cond is None:
            self.logger.debug(f"{self.hedge_pair.spot} ticker condition not ready")
            return False
        future_cond = self.exchange.ticker_notify_conds.get(self.hedge_pair.future)
        if future_cond is None:
            self.logger.debug(f"{self.hedge_pair.future} ticker condition not ready")
            return False
        if self.future_expiry_ts is None:
            self.logger.debug(f"{self.hedge_pair.coin} expiry ts not ready")
            return False
        if self.leverage_info is None:
            self.logger.debug(f"{self.hedge_pair.coin} leverage info not ready")
            return False
        if self.combined_trading_rule is None:
            self.logger.debug(f"{self.hedge_pair.coin} combined trading rule not ready")
            return False
        if time.time() - self.indicator.last_update_timestamp > 7200:
            self.logger.debug(f"{self.hedge_pair.coin} indicator delay and not ready")
            return False
        return True

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

    async def _init_update_future_expiry(self):
        result = await self.exchange.get_future(self.hedge_pair.future)
        self.future_expiry_ts = dateutil.parser.parse(result['expiry']).timestamp()
        self._future_expiry_ts_update_event.set()

    def start_network(self):
        if self._consume_main_process_msg_task is None:
            self._consume_main_process_msg_task = asyncio.create_task(self._consume_main_process_msg())
        if self._listen_for_ws_task is None:
            self._listen_for_ws_task = asyncio.create_task(self.exchange.ws_start_network())
        asyncio.create_task(self._update_entry_price())
        if self._indicator_polling_loop_task is None:
            self._indicator_polling_loop_task = asyncio.create_task(self._indicator_polling_loop())
        asyncio.create_task(self._init_update_future_expiry())

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
        self._loop.add_reader(self.conn.fileno(), self._main_process_notify_event.set)
        while True:
            if not self.conn.poll():
                await self._main_process_notify_event.wait()
            msg = self.conn.recv()
            if type(msg) is FtxTradingRuleMessage:
                trading_rule = msg.trading_rule
                if trading_rule.symbol == self.hedge_pair.spot:
                    self.spot_trading_rule = trading_rule
                elif trading_rule.symbol == self.hedge_pair.future:
                    self.future_trading_rule = trading_rule
                self.logger.debug(f"{self.hedge_pair.coin} Receive trading rule message: {trading_rule}")
                # update the least common multiple of min order size
                if self.spot_trading_rule is not None and self.future_trading_rule is not None:
                    lcm_min_order_size = max(self.spot_trading_rule.min_order_size, self.future_trading_rule.min_order_size)
                    assert lcm_min_order_size % self.spot_trading_rule.min_order_size == 0, f"{lcm_min_order_size} is not a multiple of spot min order size {self.spot_trading_rule.min_order_size}"
                    assert lcm_min_order_size % self.future_trading_rule.min_order_size == 0, f"{lcm_min_order_size} is not a multiple of future min order size {self.future_trading_rule.min_order_size}"
                    self.combined_trading_rule = CombinedTradingRule(lcm_min_order_size)
            elif type(msg) is FtxInterestRateMessage:
                self.ewma_interest_rate = msg.ewma_interest_rate
                self.logger.debug(f"{self.hedge_pair.coin} Receive interest rate message: {msg.ewma_interest_rate}")
            elif type(msg) is FtxFeeRateMessage:
                self.fee_rate = msg.fee_rate
                self.logger.debug(f"{self.hedge_pair.coin} Receive fee rate message: {msg.fee_rate}")
            elif type(msg) is FtxCollateralWeightMessage:
                self.collateral_weight = msg.collateral_weight
                self.logger.debug(f"{self.hedge_pair.coin} Receive collateral weight message: {msg.collateral_weight}")
            elif type(msg) is FtxLeverageMessage:
                self.leverage_info = msg.leverage
                self.logger.debug(f"{self.hedge_pair.coin} Receive leverage message: {msg.leverage}X")
            elif type(msg) is FtxFundResponseMessage:
                self._fund_manager_response_event.set()
                self._fund_manager_response_messages[msg.id] = msg
            elif type(msg) is FtxOrderMessage:
                self.logger.debug(f"{self.hedge_pair.coin} Receive ws order message: {msg}")
                order_id = msg.id
                self._ws_orders[order_id] = msg
                if not self._ws_orders_events.get(order_id):
                    self._ws_orders_events[order_id] = asyncio.Event()
                self._ws_orders_events[order_id].set()
            else:
                self.logger.warning(f"{self.hedge_pair.coin} receive unknown message: {msg}")
            self._main_process_notify_event.clear()

    async def _indicator_polling_loop(self):
        while True:
            try:
                now_ts = time.time()
                if now_ts - self._last_indicator_update_ts > self.INDICATOR_UPDATE_INTERVAL:
                    await self.indicator.update_indicator_info()
                    up = self.indicator.upper_threshold
                    low = self.indicator.lower_threshold
                    self._last_indicator_update_ts = time.time()
                    self.logger.info(f"{self.hedge_pair.coin} Indicator is updated successfully. UP: {up}, LOW: {low}")
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
        if self.config.release_mode:
            return
        if self.ready:
            # if current leverage is too high, openning new position is disabled
            if self.leverage_info.current_leverage > self.config.leverage_limit:
                return

            # wait ticker notify
            ticker_notify_type = await self.wait_either_one_ticker_condition_notify()
            self.logger.debug(f"Get {ticker_notify_type.value} ticker notification")

            spot_ticker = self.exchange.tickers[self.hedge_pair.spot]
            future_ticker = self.exchange.tickers[self.hedge_pair.future]

            if ticker_notify_type is TickerNotifyType.SPOT:
                if spot_ticker.is_delay(self.config.ticker_delay_threshold):
                    self.logger.warning(f"{spot_ticker.symbol} ticker is delay")
                    return
            else:
                if future_ticker.is_delay(self.config.ticker_delay_threshold):
                    self.logger.warning(f"{future_ticker.symbol} ticker is delay")
                    return

            # get expected profit, cost, fee, etc...
            future_price = future_ticker.bid
            spot_price = spot_ticker.ask
            basis = future_price - spot_price
            collateral = future_price / self.leverage_info.max_leverage - spot_price * self.collateral_weight.weight
            fee = (spot_price + future_price) * self.fee_rate.taker_fee_rate
            cost = spot_price + collateral + fee
            profit = basis - 2 * fee

            if profit < 0:
                return

            # get apr
            pnl_rate = profit / cost
            seconds_to_expiry = max(0, self.future_expiry_ts - time.time())
            days_to_expiry = Decimal(str(seconds_to_expiry / 86400))
            apr = pnl_rate * Decimal('365') / days_to_expiry

            # open signals
            to_open = False
            if apr >= self.config.apr_to_open_position:
                to_open = True
            if not to_open and basis > self.indicator.upper_threshold:
                to_open = True
            if not to_open:
                return

            # order size
            future_size = future_ticker.bid_size
            spot_size = spot_ticker.ask_size
            order_size = self._get_open_order_size(future_size, spot_size)
            if order_size <= 0:
                return

            # request fund from main process
            fund_needed = cost * order_size
            spot_notional_value = spot_price * order_size
            request_id = uuid.uuid4()
            self.conn.send(FtxFundRequestMessage(request_id, fund_needed, spot_notional_value))

            # await fund response
            try:
                await asyncio.wait_for(self._fund_manager_response_event.wait(), timeout=self.config.ticker_delay_threshold)
            except asyncio.TimeoutError:
                self.logger.warning(f"{self.hedge_pair.coin} request fund for open position timeout.")
                return
            msg = self._fund_manager_response_messages[request_id]
            if not msg.approve:
                return

            # calculate order size if fund_supply is smaller than fund_needed
            if msg.fund_supply < fund_needed:
                order_size = self._get_open_order_size_with_fund_supply(msg.fund_supply, cost)
                if order_size <= 0:
                    return

            # if borrow, calculate whether margin open is profitable or not
            if msg.borrow > 0:
                hours_to_expiry = days_to_expiry * Decimal('24')
                borrow_pnl = basis * order_size - 2 * (future_price + spot_price) * self.fee_rate.taker_fee_rate - msg.borrow * self.ewma_interest_rate.hourly_rate * hours_to_expiry
                if borrow_pnl <= 0:
                    return

            # place order
            spot_place_order = self._place_market_order_with_retry(self.hedge_pair.spot, Side.BUY, order_size)
            future_place_order = self._place_market_order_with_retry(self.hedge_pair.future, Side.SELL, order_size)
            spot_order_result, future_order_result = await asyncio.gather(spot_place_order, future_place_order, return_exceptions=True)
            both_results_ok = True
            if isinstance(spot_order_result, Exception):
                self.logger.error(f"Unexpected error while place {self.hedge_pair.spot} market order.", exc_info=True)
                both_results_ok = False
            if isinstance(future_order_result, Exception):
                self.logger.error(f"Unexpected error while place {self.hedge_pair.future} market order.", exc_info=True)
                both_results_ok = False

            if both_results_ok:
                spot_order_id = str(spot_order_result['id'])
                if not self._ws_orders_events.get(spot_order_id):
                    self._ws_orders_events[spot_order_id] = asyncio.Event()
                future_order_id = str(future_order_result['id'])
                if not self._ws_orders_events.get(future_order_id):
                    self._ws_orders_events[future_order_id] = asyncio.Event()
                spot_order_msg, future_order_msg = await asyncio.gather(self._wait_order(spot_order_id), self._wait_order(future_order_id))

                if spot_order_msg and future_order_msg:
                    # check filled size
                    if spot_order_msg.filled_size != future_order_msg.filled_size:
                        self.logger.warning(f"Filled size is not matched, {spot_order_msg.market}: {spot_order_msg.filled_size}, {future_order_msg.market}: {future_order_msg.filled_size}")
                    else:
                        # log open pnl rate, apr
                        real_basis = future_order_msg.avg_fill_price - spot_order_msg.avg_fill_price
                        real_profit = real_basis - 2 * self.fee_rate.taker_fee_rate
                        real_collateral = future_order_msg.avg_fill_price / self.leverage_info.max_leverage - spot_order_msg.avg_fill_price * self.collateral_weight.weight
                        real_fee = (future_order_msg.avg_fill_price + spot_order_msg.avg_fill_price) * self.fee_rate.taker_fee_rate
                        real_cost = spot_order_msg.avg_fill_price + real_collateral + real_fee
                        real_pnl_rate = real_profit / real_cost
                        real_apr = real_pnl_rate * Decimal('365') / days_to_expiry
                        self.logger.info(f"{self.hedge_pair.future} Open APR: {apr:.2%}, Basis: {basis}, Indicator up: {self.indicator.upper_threshold}, size: {min(spot_size, future_size)}, filled APR: {real_apr:.2%}, Basis: {real_basis}, size: {spot_order_msg.filled_size}")
                        fund_used = real_cost * spot_order_msg.filled_size
                        real_spot_notional_value = spot_order_msg.avg_fill_price * spot_order_msg.filled_size
                        self.conn.send(FtxFundOpenFilledMessage(request_id, fund_used, real_spot_notional_value))
                
                if spot_order_msg is None:
                    self.logger.warning(f"Order {spot_order_id} not found or not closed")
                else:
                    # update spot position size, entry price
                    spot_new_size = self.spot_position_size + spot_order_msg.filled_size
                    if self.spot_entry_price is None:
                        self.spot_entry_price = spot_order_msg.avg_fill_price
                    else:
                        self.spot_entry_price = (self.spot_entry_price * self.spot_position_size + spot_order_msg.avg_fill_price * spot_order_msg.filled_size) / spot_new_size
                        self.spot_position_size = spot_new_size
                
                if future_order_msg is None:
                    self.logger.warning(f"Order {future_order_id} not found or not closed")
                else:
                    # update future position size, entry price
                    future_new_size = self.future_position_size - future_order_msg.filled_size
                    if self.future_entry_price is None:
                        self.future_entry_price = future_order_msg.avg_fill_price
                    else:
                        self.future_entry_price = (self.future_entry_price * self.future_position_size + future_order_msg.avg_fill_price * future_order_msg.filled_size) / future_new_size
                        self.future_position_size = future_new_size

    async def _wait_order(self, order_id: str, timeout: float = 3.0) -> FtxOrderMessage:
        t0 = time.time()
        event = self._ws_orders_events.get(order_id)
        if event is not None:
            try:
                await asyncio.wait_for(event.wait(), timeout)
            except asyncio.TimeoutError:
                self.logger.warning(f"Wait order {order_id} event timeout: {timeout} s")
            while True:
                order_msg = self._ws_orders[order_id]
                if order_msg.status == FtxOrderStatus.CLOSED:
                    return order_msg
                if time.time() - t0 > timeout:
                    self.logger.warning(f"Order {order_id} event is set, but timeout with order not closed.")
                    break
                await asyncio.sleep(0.1)
        # try rest api
        data = await self.exchange.get_order(order_id)
        order_msg = FtxOrderMessage(
            id=str(data['id']),
            market=data['market'],
            type=FtxOrderType.LIMIT if data['type'] == 'limit' else FtxOrderType.MARKET,
            side=Side.BUY if data['side'] == 'buy' else Side.SELL,
            size=Decimal(str(data['size'])),
            price=Decimal(str(data['price'])) if isinstance(data['price'], (float, int)) else data['price'],
            status=FtxOrderStatus.str_entry(data['status']),
            filled_size=Decimal(str(data['filledSize'])),
            avg_fill_price=Decimal(str(data['avgFillPrice'])) if data['avgFillPrice'] else None,
            create_timestamp=dateutil.parser.parse(data['createdAt']).timestamp(),
        )
        if order_msg.status == FtxOrderStatus.CLOSED:
            return order_msg
        else:
            return None

    def _get_open_order_size(self, future_size: Decimal, spot_size: Decimal) -> Decimal:
        if self.config.min_order_size_mode:
            return self.combined_trading_rule.min_order_size

        available_size = min(future_size, spot_size)        
        required_size = available_size * self.config.open_order_size_multiplier
        order_size = max(required_size, self.combined_trading_rule.min_order_size)

        return order_size // self.combined_trading_rule.min_order_size * self.combined_trading_rule.min_order_size

    def _get_close_order_size(self, future_size: Decimal, spot_size: Decimal) -> Decimal:
        if self.config.min_order_size_mode:
            return self.combined_trading_rule.min_order_size

        available_size = min(future_size, spot_size)        
        required_size = available_size * self.config.close_order_size_multiplier
        order_size = max(required_size, self.combined_trading_rule.min_order_size)

        return order_size // self.combined_trading_rule.min_order_size * self.combined_trading_rule.min_order_size

    def _get_open_order_size_with_fund_supply(self, fund_supply: Decimal, cost: Decimal) -> Decimal:
        max_order_size = fund_supply / cost
        return max_order_size // self.combined_trading_rule.min_order_size * self.combined_trading_rule.min_order_size

    async def _place_market_order_with_retry(self, market: str, side: Side, size: Decimal, attempts: int = 3, sleep: float = 0.2):
        attempt = 0
        ret_error = None
        while attempt < attempts:
            attempt += 1
            try:
                ret = await self.exchange.place_market_order(market, side, size)
            except ExchangeError as error:
                self.logger.warning(f"Fail to place {market} market {side.value} order with size: {size}, attempt: {attempt}")
                if attempt == attempts:
                    ret_error = error
                    break
                await asyncio.sleep(sleep)
            else:
                return ret
        raise ret_error

    async def close_position(self):
        if not self.ready:
            return
        if self.spot_entry_price is None:
            return
        if self.future_entry_price is None:
            return
        if self.spot_position_size < self.combined_trading_rule.min_order_size:
            return
        if self.future_position_size > -self.combined_trading_rule.min_order_size:
            return
        position_size = min(self.spot_position_size, -self.future_position_size)

        # wait ticker notify
        ticker_notify_type = await self.wait_either_one_ticker_condition_notify()
        self.logger.debug(f"Get {ticker_notify_type.value} ticker notification")

        spot_ticker = self.exchange.tickers[self.hedge_pair.spot]
        future_ticker = self.exchange.tickers[self.hedge_pair.future]

        if ticker_notify_type is TickerNotifyType.SPOT:
            if spot_ticker.is_delay(self.config.ticker_delay_threshold):
                self.logger.warning(f"{spot_ticker.symbol} ticker is delay")
                return
        else:
            if future_ticker.is_delay(self.config.ticker_delay_threshold):
                self.logger.warning(f"{future_ticker.symbol} ticker is delay")
                return

        spot_price = spot_ticker.bid
        spot_size = spot_ticker.bid_size
        future_price = future_ticker.ask
        future_size = future_ticker.ask_size
        close_basis = future_price - spot_price

        to_close = False
        if close_basis <= 0:
            to_close = True

        open_basis = self.future_entry_price - self.spot_entry_price
        open_fee = (self.future_entry_price + self.spot_entry_price) * self.fee_rate.taker_fee_rate
        close_fee = (future_price + spot_price) * self.fee_rate.taker_fee_rate
        profit = open_basis - close_basis - open_fee - close_fee
        if not to_close and not profit > 0:
            return

        if not to_close and self.config.release_mode:
            to_close = True

        if not to_close and close_basis < self.indicator.lower_threshold:
            to_close = True

        if not to_close:
            return

        # order size
        order_size = self._get_close_order_size(future_size, spot_size)
        order_size = min(position_size, order_size)
        if order_size <= 0:
            return

        # place order
        spot_place_order = self._place_market_order_with_retry(self.hedge_pair.spot, Side.SELL, order_size)
        future_place_order = self._place_market_order_with_retry(self.hedge_pair.future, Side.BUY, order_size)
        spot_order_result, future_order_result = await asyncio.gather(spot_place_order, future_place_order, return_exceptions=True)
        both_results_ok = True
        if isinstance(spot_order_result, Exception):
            self.logger.error(f"Unexpected error while place {self.hedge_pair.spot} market order.", exc_info=True)
            both_results_ok = False
        if isinstance(future_order_result, Exception):
            self.logger.error(f"Unexpected error while place {self.hedge_pair.future} market order.", exc_info=True)
            both_results_ok = False

        if both_results_ok:
            spot_order_id = str(spot_order_result['id'])
            if not self._ws_orders_events.get(spot_order_id):
                self._ws_orders_events[spot_order_id] = asyncio.Event()
            future_order_id = str(future_order_result['id'])
            if not self._ws_orders_events.get(future_order_id):
                self._ws_orders_events[future_order_id] = asyncio.Event()
            spot_order_msg, future_order_msg = await asyncio.gather(self._wait_order(spot_order_id), self._wait_order(future_order_id))

            if spot_order_msg and future_order_msg:
                # check filled size
                if spot_order_msg.filled_size != future_order_msg.filled_size:
                    self.logger.warning(f"Filled size is not matched, {spot_order_msg.market}: {spot_order_msg.filled_size}, {future_order_msg.market}: {future_order_msg.filled_size}")
                else:
                    # log close pnl rate, apr
                    collateral = self.future_entry_price / self.leverage_info.max_leverage - self.spot_entry_price * self.collateral_weight.weight
                    cost = self.spot_entry_price + collateral + open_fee
                    pnl_rate = profit / cost
                    days_to_expiry = Decimal(str(self.future_expiry_ts - time.time() / 86400))
                    apr = pnl_rate * Decimal('365') / days_to_expiry
                    real_basis = future_order_msg.avg_fill_price - spot_order_msg.avg_fill_price
                    real_close_fee = (future_order_msg.avg_fill_price + spot_order_msg.avg_fill_price) * self.fee_rate.taker_fee_rate
                    real_profit = open_basis - real_basis - open_fee - real_close_fee
                    real_pnl_rate = real_profit / cost
                    real_apr = real_pnl_rate * Decimal('365') / days_to_expiry
                    self.logger.info(f"{self.hedge_pair.future} Close APR: {apr:.2%}, Basis: {close_basis}, Indicator low: {self.indicator.lower_threshold}, size: {min(spot_size, future_size)}, filled APR: {real_apr:.2%}, Basis: {real_basis}, size: {spot_order_msg.filled_size}")

            # TODO: inform main process fund release

            if spot_order_msg is None:
                self.logger.warning(f"Order {spot_order_id} not found or not closed")
            else:
                # update spot position size, entry price
                spot_new_size = self.spot_position_size - spot_order_msg.filled_size
                if spot_new_size == 0:
                    self.spot_entry_price = None
                self.spot_position_size = spot_new_size
            
            if future_order_msg is None:
                self.logger.warning(f"Order {future_order_id} not found or not closed")
            else:
                # update future position size, entry price
                future_new_size = self.future_position_size + future_order_msg.filled_size
                if future_new_size == 0:
                    self.future_entry_price = None
                self.future_position_size = future_new_size

    async def open_position_loop(self):
        await self._future_expiry_ts_update_event.wait()
        while self.future_expiry_ts - time.time() > self.config.seconds_before_expiry_to_stop_open_position:
            try:
                await self.open_position()
                await asyncio.sleep(0.2)
            except Exception:
                self.logger.error(f"Unexpected error while open {self.hedge_pair.coin} position.", exc_info=True)
                await asyncio.sleep(5)

    async def close_position_loop(self):
        await self._future_expiry_ts_update_event.wait()
        while self.future_expiry_ts - time.time()> self.config.seconds_before_expiry_to_stop_close_position:
            try:
                await self.close_position()
                await asyncio.sleep(0.2)
            except Exception:
                self.logger.error(f"Unexpected error while close {self.hedge_pair.coin} position.", exc_info=True)
                await asyncio.sleep(5)

    async def run(self):
        try:
            self.start_network()
            open_task = self.open_position_loop()
            close_task = self.close_position_loop()
            await asyncio.gather(open_task, close_task)
        except KeyboardInterrupt:
            await self.exchange.close()


def run_sub_process(hedge_pair: FtxHedgePair, config: Config, conn: Connection):
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    sub_process = SubProcess(hedge_pair, config, conn)
    sub_process.logger.debug(f'start to run {hedge_pair.coin} process')
    loop =asyncio.get_event_loop()
    loop.run_until_complete(sub_process.run())
