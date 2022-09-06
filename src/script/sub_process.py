import asyncio
import logging
import pathlib
import sys
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from multiprocessing.connection import Connection
from signal import SIGTERM, signal
from typing import Dict, List

import dateutil.parser
import uvloop
from cachetools import TTLCache

from src.common import Config
from src.exchange.ftx.ftx_client import FtxExchange
from src.exchange.ftx.ftx_data_type import (Ftx_EWMA_InterestRate,
                                            FtxCandleResolution,
                                            FtxCollateralWeight,
                                            FtxCollateralWeightMessage,
                                            FtxEntryPriceRequestMessage,
                                            FtxEntryPriceResponseMessage,
                                            FtxFeeRate, FtxFeeRateMessage,
                                            FtxFundOpenFilledMessage,
                                            FtxFundRequestMessage,
                                            FtxFundResponseMessage,
                                            FtxHedgePair,
                                            FtxInterestRateMessage,
                                            FtxLeverageInfo,
                                            FtxLeverageMessage,
                                            FtxOrderMessage, FtxOrderStatus,
                                            FtxOrderType, FtxTradingRule,
                                            FtxTradingRuleMessage, Side,
                                            TradeType)
from src.exchange.ftx.ftx_error import ExchangeError, RateLimitExceeded
from src.indicator.base_indicator import BaseIndicator
from src.indicator.bollinger import Bollinger, BollingerParams
from src.indicator.macd import MACD, MACDParams
from src.util.rate_limit import RateLimiter
from src.util.slack import SlackWrappedLogger


@dataclass
class CombinedTradingRule:
    min_order_size: Decimal


class TickerNotifyType(Enum):
    SPOT = "SPOT"
    FUTURE = "FUTURE"


class SubProcess:
    ENTRY_PRICE_POLLING_INTERVAL = 3600

    def __init__(
        self,
        hedge_pair: FtxHedgePair,
        config: Config,
        conn: Connection,
        rate_limiter: RateLimiter,
    ):
        self.hedge_pair = hedge_pair
        self.config = config
        self.conn = conn
        self.rate_limiter = rate_limiter
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

        self.exchange = FtxExchange(
            config.api_key, config.api_secret, config.subaccount_name
        )
        self.exchange.ws_register_ticker_channel([hedge_pair.spot, hedge_pair.future])

        self._consume_main_process_msg_task: asyncio.Task = None
        self._listen_for_ws_task: asyncio.Task = None
        self._indicator_polling_loop_task: asyncio.Task = None
        self._entry_price_polling_loop_task: asyncio.Task = None

        self.spot_entry_price: Decimal = None
        self.future_entry_price: Decimal = None
        self.spot_position_size: Decimal = Decimal(0)
        self.future_position_size: Decimal = Decimal(0)  # negative means short postion
        self._position_size_update_event = asyncio.Event()

        self.indicator = self._init_get_indicator()

        self.future_expiry_ts: float = None
        self._future_expiry_ts_update_event = asyncio.Event()

        self._fund_manager_response_events: Dict[uuid.UUID, asyncio.Event] = TTLCache(
            maxsize=1000, ttl=60
        )
        self._fund_manager_response_messages: Dict[
            uuid.UUID, FtxFundResponseMessage
        ] = TTLCache(maxsize=1000, ttl=60)

        self._ws_orders: Dict[str, FtxOrderMessage] = TTLCache(
            maxsize=1000, ttl=60
        )  # using order_id: str as the mapping key
        self._ws_orders_events: Dict[str, asyncio.Event] = TTLCache(
            maxsize=1000, ttl=60
        )  # using order_id: str as the mapping key

        self._state_update_lock = (
            asyncio.Lock()
        )  # this lock is used when updating position size, entry price, and open/close position

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
        if not self.indicator.ready:
            self.logger.debug(f"{self.hedge_pair.coin} indicator not ready")
            return False
        if self.collateral_weight is None:
            self.logger.debug(f"{self.hedge_pair.coin} collateral weight not ready")
            return False
        return True

    def _init_get_logger(self):
        log = self.config.log
        level = logging.getLevelName(log["level"].upper())
        fmt = log["fmt"]
        datefmt = log["datefmt"]
        formatter = logging.Formatter(fmt, datefmt)
        handlers = []
        if log["to_console"]:
            ch = logging.StreamHandler()
            ch.setFormatter(formatter)
            ch.set_name("stream_formatter")
            handlers.append(ch)
        if log["to_file"]:
            path = pathlib.Path(log["file_path"])
            if not path.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(log["file_path"], encoding="utf-8")
            fh.setFormatter(formatter)
            fh.set_name("file_handler")
            handlers.append(fh)
        logging.basicConfig(level=level, handlers=handlers)
        logger = logging.getLogger()
        logging.getLogger("asyncio").setLevel(logging.WARNING)
        logger = SlackWrappedLogger(
            logger,
            {
                "auth_token": self.config.slack_config.auth_token,
                "info_channel": self.config.slack_config.summary_channel,
                "alert_channel": self.config.slack_config.alert_channel,
            },
        )
        return logger

    async def _update_position_size(self):
        balances = await self.exchange.get_balances()
        try:
            balance = next(b for b in balances if b["coin"] == self.hedge_pair.coin)
        except StopIteration:
            self.spot_position_size = Decimal(0)
        else:
            self.spot_position_size = Decimal(str(balance["total"]))
        self.logger.info(
            f"{self.hedge_pair.coin} position size is {self.spot_position_size}"
        )

        positions = await self.exchange.get_positions()
        try:
            position = next(
                p for p in positions if p["future"] == self.hedge_pair.future
            )
        except StopIteration:
            self.future_position_size = Decimal(0)
        else:
            self.future_position_size = Decimal(str(position["netSize"]))
        self.logger.info(
            f"{self.hedge_pair.future} position size is {self.future_position_size}"
        )
        self._position_size_update_event.set()

    async def _update_entry_price(self):
        async with self._state_update_lock:
            await self._update_position_size()
            spot_fills = await self.exchange.get_fills_since_last_flat(
                self.hedge_pair.spot, self.spot_position_size
            )
            self.logger.debug(
                f"length of {self.hedge_pair.spot} fills is: {len(spot_fills)}"
            )
            future_fills = await self.exchange.get_fills_since_last_flat(
                self.hedge_pair.future, self.future_position_size
            )
            self.logger.debug(
                f"length of {self.hedge_pair.future} fills is: {len(future_fills)}"
            )
            self.spot_entry_price = self._compute_entry_price(
                self.spot_position_size, spot_fills
            )
            self.logger.info(
                f"Update {self.hedge_pair.spot} entry price: {self.spot_entry_price}"
            )
            self.future_entry_price = self._compute_entry_price(
                self.future_position_size, future_fills
            )
            self.logger.info(
                f"Update {self.hedge_pair.future} entry price: {self.future_entry_price}"
            )
            if self.spot_entry_price and self.future_entry_price:
                basis = self.future_entry_price - self.spot_entry_price
                self.logger.info(f"Update {self.hedge_pair.coin} basis: {basis}")

    async def _entry_price_polling_loop(self):
        while True:
            try:
                await self._update_entry_price()
                await asyncio.sleep(self.ENTRY_PRICE_POLLING_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.error(
                    f"{self.hedge_pair.coin} Error while polling entry price.",
                    exc_info=True,
                )
                await asyncio.sleep(10)

    def _compute_entry_price(
        self, position_size: Decimal, fills: List[dict]
    ) -> Decimal:
        if position_size == 0:
            return None
        elif position_size > 0:
            temp_position_size = position_size
            my_fills = []
            for fill in reversed(fills):
                size = Decimal(str(fill["size"]))
                price = Decimal(str(fill["price"]))
                if fill["side"] == "buy":
                    prev_position_size = temp_position_size - size
                    if prev_position_size <= 0:
                        my_fills.append(
                            {"side": "buy", "size": temp_position_size, "price": price}
                        )
                        break
                    else:
                        my_fills.append({"side": "buy", "size": size, "price": price})
                        temp_position_size = prev_position_size
                else:
                    temp_position_size += size
                    my_fills.append({"side": "sell", "size": size, "price": price})
            cum_size = Decimal(0)
            entry_price = None
            for fill in reversed(my_fills):
                if fill["side"] == "buy":
                    new_size = cum_size + fill["size"]
                    if entry_price is None:
                        entry_price = fill["price"]
                    else:
                        entry_price = (
                            cum_size * entry_price + fill["size"] * fill["price"]
                        ) / new_size
                    cum_size = new_size
                else:
                    # entry price remain the same when closing position
                    cum_size -= fill["size"]
            return entry_price
        else:
            temp_position_size = position_size
            my_fills = []
            for fill in reversed(fills):
                size = Decimal(str(fill["size"]))
                price = Decimal(str(fill["price"]))
                if fill["side"] == "sell":
                    prev_position_size = temp_position_size + size
                    if prev_position_size >= 0:
                        my_fills.append(
                            {
                                "side": "sell",
                                "size": -temp_position_size,
                                "price": price,
                            }
                        )
                        break
                    else:
                        my_fills.append({"side": "sell", "size": size, "price": price})
                        temp_position_size = prev_position_size
                else:
                    temp_position_size -= size
                    my_fills.append({"side": "buy", "size": size, "price": price})
            cum_size = Decimal(0)
            entry_price = None
            for fill in reversed(my_fills):
                if fill["side"] == "sell":
                    new_size = cum_size + fill["size"]
                    if entry_price is None:
                        entry_price = fill["price"]
                    else:
                        entry_price = (
                            cum_size * entry_price + fill["size"] * fill["price"]
                        ) / new_size
                    cum_size = new_size
                else:
                    # entry price remain the same when closing position
                    cum_size -= fill["size"]
            return entry_price

    def _init_get_indicator(self) -> BaseIndicator:
        if self.config.indicator["name"] == "macd":
            params = self.config.indicator["params"]
            macd_params = MACDParams(
                fast_length=params["fast_length"],
                slow_length=params["slow_length"],
                signal_length=params["signal_length"],
                std_length=params["std_length"],
                std_mult=params["std_mult"],
            )
            return MACD(
                self.hedge_pair,
                kline_resolution=FtxCandleResolution.from_seconds(
                    params["kline_resolution"]
                ),
                params=macd_params,
            )
        elif self.config.indicator["name"] == "bollinger":
            params = self.config.indicator["params"]
            boll_params = BollingerParams(
                length=params["length"], std_mult=params["std_mult"]
            )
            return Bollinger(
                self.hedge_pair,
                kline_resolution=FtxCandleResolution.from_seconds(
                    params["kline_resolution"]
                ),
                params=boll_params,
            )
        else:
            raise NotImplementedError(
                f"Sorry, {self.config.indicator['name']} is not implemented"
            )

    async def _init_update_future_expiry(self):
        result = await self.exchange.get_future(self.hedge_pair.future)
        self.future_expiry_ts = dateutil.parser.parse(result["expiry"]).timestamp()
        self._future_expiry_ts_update_event.set()

    def start_network(self):
        if self._consume_main_process_msg_task is None:
            self._consume_main_process_msg_task = asyncio.create_task(
                self._consume_main_process_msg()
            )
        if self._listen_for_ws_task is None:
            self._listen_for_ws_task = asyncio.create_task(
                self.exchange.ws_start_network()
            )
        if self._indicator_polling_loop_task is None:
            self._indicator_polling_loop_task = asyncio.create_task(
                self._indicator_polling_loop()
            )
        if self._entry_price_polling_loop_task is None:
            self._entry_price_polling_loop_task = asyncio.create_task(
                self._entry_price_polling_loop()
            )
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
        if self._entry_price_polling_loop_task is not None:
            self._entry_price_polling_loop_task.cancel()
            self._entry_price_polling_loop_task = None

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
                self.logger.debug(
                    f"{self.hedge_pair.coin} Receive trading rule message: {trading_rule}"
                )
                # update the least common multiple of min order size
                if (
                    self.spot_trading_rule is not None
                    and self.future_trading_rule is not None
                ):
                    lcm_min_order_size = max(
                        self.spot_trading_rule.min_order_size,
                        self.future_trading_rule.min_order_size,
                    )
                    assert (
                        lcm_min_order_size % self.spot_trading_rule.min_order_size == 0
                    ), f"{lcm_min_order_size} is not a multiple of spot min order size {self.spot_trading_rule.min_order_size}"
                    assert (
                        lcm_min_order_size % self.future_trading_rule.min_order_size
                        == 0
                    ), f"{lcm_min_order_size} is not a multiple of future min order size {self.future_trading_rule.min_order_size}"
                    self.combined_trading_rule = CombinedTradingRule(lcm_min_order_size)
            elif type(msg) is FtxInterestRateMessage:
                self.ewma_interest_rate = msg.ewma_interest_rate
                self.logger.debug(
                    f"{self.hedge_pair.coin} Receive interest rate message: {msg.ewma_interest_rate}"
                )
            elif type(msg) is FtxFeeRateMessage:
                self.fee_rate = msg.fee_rate
                self.logger.debug(
                    f"{self.hedge_pair.coin} Receive fee rate message: {msg.fee_rate}"
                )
            elif type(msg) is FtxCollateralWeightMessage:
                self.collateral_weight = msg.collateral_weight
                self.logger.debug(
                    f"{self.hedge_pair.coin} Receive collateral weight message: {msg.collateral_weight}"
                )
            elif type(msg) is FtxLeverageMessage:
                self.leverage_info = msg.leverage
                self.logger.debug(
                    f"{self.hedge_pair.coin} Receive leverage message: {msg.leverage}"
                )
            elif type(msg) is FtxFundResponseMessage:
                self._fund_manager_response_messages[msg.id] = msg
                if not self._fund_manager_response_events.get(msg.id):
                    self._fund_manager_response_events[msg.id] = asyncio.Event()
                self._fund_manager_response_events[msg.id].set()
            elif type(msg) is FtxOrderMessage:
                self.logger.debug(
                    f"{self.hedge_pair.coin} Receive ws order message: {msg}"
                )
                order_id = msg.id
                self._ws_orders[order_id] = msg
                if not self._ws_orders_events.get(order_id):
                    self._ws_orders_events[order_id] = asyncio.Event()
                if msg.status == FtxOrderStatus.CLOSED:
                    self._ws_orders_events[order_id].set()
            elif type(msg) is FtxEntryPriceRequestMessage:
                market = msg.market
                if market == self.hedge_pair.spot:
                    entry_price = self.spot_entry_price
                elif market == self.hedge_pair.future:
                    entry_price = self.future_entry_price
                else:
                    entry_price = None
                self.conn.send(FtxEntryPriceResponseMessage(market, entry_price))
            else:
                self.logger.warning(
                    f"{self.hedge_pair.coin} receive unknown message: {msg}"
                )
            self._main_process_notify_event.clear()

    async def _indicator_polling_loop(self):
        while True:
            try:
                if not self.indicator.ready:
                    await self.indicator.update_indicator_info()
                    up = self.indicator.upper_threshold
                    low = self.indicator.lower_threshold
                    self.logger.info(
                        f"{self.hedge_pair.coin} Indicator is updated successfully. UP: {up}, LOW: {low}"
                    )
                now_ts = time.time()
                resolution = self.indicator.kline_resolution.value
                next_run_ts = (
                    self.indicator.last_kline_start_timestamp + 2 * resolution + 1
                )
                wait_time = max(
                    0, next_run_ts - now_ts
                )  # should be none negative wait time
                await asyncio.sleep(wait_time)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.error(
                    f"{self.hedge_pair.coin} Error while update indicator.",
                    exc_info=True,
                )
                await asyncio.sleep(10)

    async def wait_spot_ticker_notify(self):
        spot_cond = self.exchange.ticker_notify_conds.get(self.hedge_pair.spot)
        async with spot_cond:
            await spot_cond.wait()

    async def wait_future_ticker_notify(self):
        future_cond = self.exchange.ticker_notify_conds.get(self.hedge_pair.future)
        async with future_cond:
            await future_cond.wait()

    async def wait_either_one_ticker_condition_notify(self) -> TickerNotifyType:
        spot_task = asyncio.create_task(self.wait_spot_ticker_notify(), name="spot")
        future_task = asyncio.create_task(
            self.wait_future_ticker_notify(), name="future"
        )
        done, _ = await asyncio.wait(
            {spot_task, future_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if spot_task in done:
            return TickerNotifyType.SPOT
        elif future_task in done:
            return TickerNotifyType.FUTURE

    async def open_position(self):
        if self.ready:
            # if current leverage is too high, openning new position is disabled
            if self.leverage_info.current_leverage > self.config.leverage_limit:
                return

            # rate limit
            if not self.rate_limiter.ok:
                return

            # wait ticker notify
            ticker_notify_type = await self.wait_either_one_ticker_condition_notify()
            self.logger.debug(f"Get {ticker_notify_type.value} ticker notification")

            spot_ticker = self.exchange.tickers[self.hedge_pair.spot]
            future_ticker = self.exchange.tickers[self.hedge_pair.future]

            if ticker_notify_type is TickerNotifyType.SPOT:
                if spot_ticker.is_delay(self.config.ticker_delay_threshold):
                    self.logger.debug(f"{spot_ticker.symbol} ticker is delay")
                    return
            else:
                if future_ticker.is_delay(self.config.ticker_delay_threshold):
                    self.logger.debug(f"{future_ticker.symbol} ticker is delay")
                    return

            # get expected profit, cost, fee, etc...
            future_price = future_ticker.bid
            if future_price is None:
                return
            spot_price = spot_ticker.ask
            if spot_price is None:
                return
            basis = future_price - spot_price
            future_collateral_needed = future_price / self.leverage_info.max_leverage
            spot_collateral_supplied = spot_price * self.collateral_weight.weight
            fee = (spot_price + future_price) * self.fee_rate.taker_fee_rate
            cost = (
                spot_price + future_collateral_needed - spot_collateral_supplied + fee
            )
            profit = basis - 2 * fee

            if basis < self.config.open_fee_coverage_multiplier * fee:
                return

            # get apr
            pnl_rate = profit / cost
            seconds_to_expiry = max(0, self.future_expiry_ts - time.time())
            days_to_expiry = Decimal(str(seconds_to_expiry / 86400))
            apr = pnl_rate * Decimal("365") / days_to_expiry

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
            self.conn.send(
                FtxFundRequestMessage(request_id, fund_needed, spot_notional_value)
            )

            # await fund response
            if not self._fund_manager_response_events.get(request_id):
                self._fund_manager_response_events[request_id] = asyncio.Event()
            try:
                await asyncio.wait_for(
                    self._fund_manager_response_events[request_id].wait(),
                    timeout=self.config.ticker_delay_threshold,
                )
            except asyncio.TimeoutError:
                self.logger.warning(
                    f"{self.hedge_pair.coin} request fund for open position timeout."
                )
                return
            msg = self._fund_manager_response_messages[request_id]
            if not msg.approve:
                return

            # calculate order size if fund_supply is smaller than fund_needed
            if msg.fund_supply < fund_needed:
                order_size = self._get_open_order_size_with_fund_supply(
                    msg.fund_supply, cost
                )
                if order_size <= 0:
                    return

            # if borrow, calculate whether margin open is profitable or not
            if msg.borrow > 0:
                hours_to_expiry = days_to_expiry * Decimal("24")
                borrow_pnl = (
                    basis * order_size
                    - 2 * (future_price + spot_price) * self.fee_rate.taker_fee_rate
                    - msg.borrow * self.ewma_interest_rate.hourly_rate * hours_to_expiry
                )
                if borrow_pnl <= 0:
                    return

            # place order
            spot_place_order = self._place_market_order_with_retry(
                self.hedge_pair.spot, Side.BUY, order_size
            )
            future_place_order = self._place_market_order_with_retry(
                self.hedge_pair.future, Side.SELL, order_size
            )
            spot_order_result, future_order_result = await asyncio.gather(
                spot_place_order, future_place_order, return_exceptions=True
            )
            # call rate_limiter 2 times because we place 2 orders
            self.rate_limiter.add_record()
            self.rate_limiter.add_record()

            if isinstance(spot_order_result, Exception):
                self.logger.error(
                    f"Unexpected error while place {self.hedge_pair.spot} market order. Error message: {spot_order_result.with_traceback(None)}",
                    slack=self.config.slack_config.enable,
                )
                spot_result_ok = False
            else:
                spot_order_id = str(spot_order_result["id"])
                if not self._ws_orders_events.get(spot_order_id):
                    self._ws_orders_events[spot_order_id] = asyncio.Event()
                spot_order_msg = await self._wait_order(spot_order_id)
                spot_result_ok = True
            if isinstance(future_order_result, Exception):
                self.logger.error(
                    f"Unexpected error while place {self.hedge_pair.future} market order. Error message: {future_order_result.with_traceback(None)}",
                    slack=self.config.slack_config.enable,
                )
                future_result_ok = False
            else:
                future_order_id = str(future_order_result["id"])
                if not self._ws_orders_events.get(future_order_id):
                    self._ws_orders_events[future_order_id] = asyncio.Event()
                future_order_msg = await self._wait_order(future_order_id)
                future_result_ok = True

            if spot_result_ok and future_result_ok:
                if spot_order_msg and future_order_msg:
                    # check filled size
                    if spot_order_msg.filled_size != future_order_msg.filled_size:
                        self.logger.warning(
                            f"Filled size is not matched, {spot_order_msg.market}: {spot_order_msg.filled_size}, {future_order_msg.market}: {future_order_msg.filled_size}",
                            slack=self.config.slack_config.enable,
                        )
                    else:
                        # log open pnl rate, apr
                        real_basis = (
                            future_order_msg.avg_fill_price
                            - spot_order_msg.avg_fill_price
                        )
                        real_future_collateral_needed = (
                            future_order_msg.avg_fill_price
                            / self.leverage_info.max_leverage
                        )
                        real_spot_collateral_supplied = (
                            spot_order_msg.avg_fill_price
                            * self.collateral_weight.weight
                        )
                        real_fee = (
                            future_order_msg.avg_fill_price
                            + spot_order_msg.avg_fill_price
                        ) * self.fee_rate.taker_fee_rate
                        real_profit = real_basis - 2 * real_fee
                        real_cost = (
                            spot_order_msg.avg_fill_price
                            + real_future_collateral_needed
                            - real_spot_collateral_supplied
                            + real_fee
                        )
                        real_pnl_rate = real_profit / real_cost
                        real_apr = real_pnl_rate * Decimal("365") / days_to_expiry
                        self.logger.info(
                            f"{self.hedge_pair.future} Open APR: {apr:.2%}, Basis: {basis}, Indicator up: {self.indicator.upper_threshold}, size: {min(spot_size, future_size)}, filled APR: {real_apr:.2%}, Basis: {real_basis}, size: {spot_order_msg.filled_size}"
                        )
                        fund_used = real_cost * spot_order_msg.filled_size
                        real_spot_notional_value = (
                            spot_order_msg.avg_fill_price * spot_order_msg.filled_size
                        )
                        self.conn.send(
                            FtxFundOpenFilledMessage(
                                request_id, fund_used, real_spot_notional_value
                            )
                        )

            if spot_result_ok:
                if spot_order_msg is None:
                    self.logger.warning(
                        f"{self.hedge_pair.spot} order {spot_order_id} not found or not closed",
                        slack=self.config.slack_config.enable,
                    )
                else:
                    # update spot position size, entry price
                    spot_new_size = self.spot_position_size + spot_order_msg.filled_size
                    if self.spot_entry_price is None:
                        self.spot_entry_price = spot_order_msg.avg_fill_price
                    else:
                        self.spot_entry_price = (
                            self.spot_entry_price * self.spot_position_size
                            + spot_order_msg.avg_fill_price * spot_order_msg.filled_size
                        ) / spot_new_size
                        self.spot_position_size = spot_new_size

            if future_result_ok:
                if future_order_msg is None:
                    self.logger.warning(
                        f"{self.hedge_pair.future} order {future_order_id} not found or not closed",
                        slack=self.config.slack_config.enable,
                    )
                else:
                    # update future position size, entry price
                    future_new_size = (
                        self.future_position_size - future_order_msg.filled_size
                    )
                    if self.future_entry_price is None:
                        self.future_entry_price = future_order_msg.avg_fill_price
                    else:
                        self.future_entry_price = (
                            self.future_entry_price * self.future_position_size
                            + future_order_msg.avg_fill_price
                            * future_order_msg.filled_size
                        ) / future_new_size
                        self.future_position_size = future_new_size

    async def _wait_order(self, order_id: str, timeout: float = 3.0) -> FtxOrderMessage:
        event = self._ws_orders_events.get(order_id)
        if event is not None:
            try:
                await asyncio.wait_for(event.wait(), timeout)
            except asyncio.TimeoutError:
                self.logger.warning(
                    f"Wait order {order_id} event timeout: {timeout}s, try rest api get order"
                )
            else:
                order_msg = self._ws_orders[order_id]
                return order_msg
        # try rest api
        data = await self.exchange.get_order(order_id)
        order_msg = FtxOrderMessage(
            id=str(data["id"]),
            market=data["market"],
            type=FtxOrderType.LIMIT if data["type"] == "limit" else FtxOrderType.MARKET,
            side=Side.BUY if data["side"] == "buy" else Side.SELL,
            size=Decimal(str(data["size"])),
            price=Decimal(str(data["price"]))
            if isinstance(data["price"], (float, int))
            else data["price"],
            status=FtxOrderStatus.str_entry(data["status"]),
            filled_size=Decimal(str(data["filledSize"])),
            avg_fill_price=Decimal(str(data["avgFillPrice"]))
            if data["avgFillPrice"]
            else None,
            create_timestamp=dateutil.parser.parse(data["createdAt"]).timestamp(),
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

        return (
            order_size
            // self.combined_trading_rule.min_order_size
            * self.combined_trading_rule.min_order_size
        )

    def _get_close_order_size(
        self, future_size: Decimal, spot_size: Decimal
    ) -> Decimal:
        if self.config.min_order_size_mode:
            return self.combined_trading_rule.min_order_size

        available_size = min(future_size, spot_size)
        required_size = available_size * self.config.close_order_size_multiplier
        order_size = max(required_size, self.combined_trading_rule.min_order_size)

        return (
            order_size
            // self.combined_trading_rule.min_order_size
            * self.combined_trading_rule.min_order_size
        )

    def _get_open_order_size_with_fund_supply(
        self, fund_supply: Decimal, cost: Decimal
    ) -> Decimal:
        max_order_size = fund_supply / cost
        return (
            max_order_size
            // self.combined_trading_rule.min_order_size
            * self.combined_trading_rule.min_order_size
        )

    async def _place_market_order_with_retry(
        self,
        market: str,
        side: Side,
        size: Decimal,
        reduce_only: bool = False,
        attempts: int = 5,
        sleep: float = 0.2,
        sleep_delay: float = 0.05,
    ):
        attempt = 0
        ret_error = None
        while attempt < attempts:
            attempt += 1
            try:
                ret = await self.exchange.place_market_order(
                    market, side, size, reduce_only=reduce_only
                )
            except RateLimitExceeded as error:
                self.logger.warning(
                    f"Fail to place {market} market {side.value} order with size: {size}, attempt: {attempt}"
                )
                if attempt == attempts:
                    ret_error = error
                    break
                await asyncio.sleep(sleep)
                sleep += sleep_delay
            except ExchangeError as error:
                self.logger.error(
                    f"Fail to place {market} market {side.value} order with size: {size}, error: {error}"
                )
                ret_error = error
                break
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

        # rate limit
        if not self.rate_limiter.ok:
            return

        position_size = min(self.spot_position_size, -self.future_position_size)

        # wait ticker notify
        ticker_notify_type = await self.wait_either_one_ticker_condition_notify()
        self.logger.debug(f"Get {ticker_notify_type.value} ticker notification")

        spot_ticker = self.exchange.tickers[self.hedge_pair.spot]
        future_ticker = self.exchange.tickers[self.hedge_pair.future]

        if ticker_notify_type is TickerNotifyType.SPOT:
            if spot_ticker.is_delay(self.config.ticker_delay_threshold):
                self.logger.debug(f"{spot_ticker.symbol} ticker is delay")
                return
        else:
            if future_ticker.is_delay(self.config.ticker_delay_threshold):
                self.logger.debug(f"{future_ticker.symbol} ticker is delay")
                return

        spot_price = spot_ticker.bid
        spot_size = spot_ticker.bid_size
        if spot_price is None:
            return

        future_price = future_ticker.ask
        future_size = future_ticker.ask_size
        if future_price is None:
            return

        close_basis = future_price - spot_price

        to_close = False
        if close_basis <= 0:
            to_close = True

        open_basis = self.future_entry_price - self.spot_entry_price
        open_fee = (
            self.future_entry_price + self.spot_entry_price
        ) * self.fee_rate.taker_fee_rate
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
        spot_place_order = self._place_market_order_with_retry(
            self.hedge_pair.spot, Side.SELL, order_size, reduce_only=True
        )
        future_place_order = self._place_market_order_with_retry(
            self.hedge_pair.future, Side.BUY, order_size, reduce_only=True
        )
        spot_order_result, future_order_result = await asyncio.gather(
            spot_place_order, future_place_order, return_exceptions=True
        )
        # call rate_limiter 2 times because we place 2 orders
        self.rate_limiter.add_record()
        self.rate_limiter.add_record()

        if isinstance(spot_order_result, Exception):
            self.logger.error(
                f"Unexpected error while place {self.hedge_pair.spot} market order. Error message: {spot_order_result.with_traceback(None)}",
                slack=self.config.slack_config.enable,
            )
            spot_result_ok = False
        else:
            spot_order_id = str(spot_order_result["id"])
            if not self._ws_orders_events.get(spot_order_id):
                self._ws_orders_events[spot_order_id] = asyncio.Event()
            spot_order_msg = await self._wait_order(spot_order_id)
            spot_result_ok = True
        if isinstance(future_order_result, Exception):
            self.logger.error(
                f"Unexpected error while place {self.hedge_pair.future} market order. Error message: {future_order_result.with_traceback(None)}",
                slack=self.config.slack_config.enable,
            )
            future_result_ok = False
        else:
            future_order_id = str(future_order_result["id"])
            if not self._ws_orders_events.get(future_order_id):
                self._ws_orders_events[future_order_id] = asyncio.Event()
            future_order_msg = await self._wait_order(future_order_id)
            future_result_ok = True

        if spot_result_ok and future_result_ok:
            if spot_order_msg and future_order_msg:
                # check filled size
                if spot_order_msg.filled_size != future_order_msg.filled_size:
                    self.logger.warning(
                        f"Filled size is not matched, {spot_order_msg.market}: {spot_order_msg.filled_size}, {future_order_msg.market}: {future_order_msg.filled_size}",
                        slack=self.config.slack_config.enable,
                    )
                else:
                    # log close pnl rate, apr
                    future_collateral_needed = (
                        self.future_entry_price / self.leverage_info.max_leverage
                    )
                    spot_collateral_supplied = (
                        self.spot_entry_price * self.collateral_weight.weight
                    )
                    cost = (
                        self.spot_entry_price
                        + future_collateral_needed
                        - spot_collateral_supplied
                        + open_fee
                    )
                    pnl_rate = profit / cost
                    days_to_expiry = Decimal(
                        str(self.future_expiry_ts - time.time() / 86400)
                    )
                    apr = pnl_rate * Decimal("365") / days_to_expiry
                    real_basis = (
                        future_order_msg.avg_fill_price - spot_order_msg.avg_fill_price
                    )
                    real_close_fee = (
                        future_order_msg.avg_fill_price + spot_order_msg.avg_fill_price
                    ) * self.fee_rate.taker_fee_rate
                    real_profit = open_basis - real_basis - open_fee - real_close_fee
                    real_pnl_rate = real_profit / cost
                    real_apr = real_pnl_rate * Decimal("365") / days_to_expiry
                    self.logger.info(
                        f"{self.hedge_pair.future} Close APR: {apr:.2%}, Basis: {close_basis}, Indicator low: {self.indicator.lower_threshold}, size: {min(spot_size, future_size)}, filled APR: {real_apr:.2%}, Basis: {real_basis}, size: {spot_order_msg.filled_size}"
                    )

            # TODO: inform main process fund release

        if spot_result_ok:
            if spot_order_msg is None:
                self.logger.warning(
                    f"{self.hedge_pair.spot} order {spot_order_id} not found or not closed",
                    slack=self.config.slack_config.enable,
                )
            else:
                # update spot position size, entry price
                spot_new_size = self.spot_position_size - spot_order_msg.filled_size
                if spot_new_size == 0:
                    self.spot_entry_price = None
                self.spot_position_size = spot_new_size

        if future_result_ok:
            if future_order_msg is None:
                self.logger.warning(
                    f"{self.hedge_pair.future} order {future_order_id} not found or not closed",
                    slack=self.config.slack_config.enable,
                )
            else:
                # update future position size, entry price
                future_new_size = (
                    self.future_position_size + future_order_msg.filled_size
                )
                if future_new_size == 0:
                    self.future_entry_price = None
                self.future_position_size = future_new_size

    async def open_position_loop(self):
        if not self.hedge_pair.can_open:
            self.logger.info(
                f"Detect {self.hedge_pair.coin} trade type is {self.hedge_pair.trade_type}, which is not allowed to open position. Return"
            )
            return
        if self.config.release_mode:
            return
        await self._future_expiry_ts_update_event.wait()
        while (
            self.future_expiry_ts - time.time()
            > self.config.seconds_before_expiry_to_stop_open_position
        ):
            try:
                async with self._state_update_lock:
                    await self.open_position()
                await asyncio.sleep(0.2)
            except Exception:
                self.logger.error(
                    f"Unexpected error while open {self.hedge_pair.coin} position.",
                    exc_info=True,
                    slack=self.config.slack_config.enable,
                )
                await asyncio.sleep(5)

    async def close_position_loop(self):
        if not self.hedge_pair.can_close:
            self.logger.info(
                f"Detect {self.hedge_pair.coin} trade type is {self.hedge_pair.trade_type}, which is not allow to close position. Return"
            )
            return
        await self._future_expiry_ts_update_event.wait()
        await self._position_size_update_event.wait()
        while (
            self.future_expiry_ts - time.time()
            > self.config.seconds_before_expiry_to_stop_close_position
        ):
            if self.hedge_pair.trade_type is TradeType.CLOSE_ONLY and (
                self.spot_position_size < self.combined_trading_rule.min_order_size
                or self.future_position_size
                > -self.combined_trading_rule.min_order_size
            ):
                break
            try:
                async with self._state_update_lock:
                    await self.close_position()
                await asyncio.sleep(0.2)
            except Exception:
                self.logger.error(
                    f"Unexpected error while close {self.hedge_pair.coin} position.",
                    exc_info=True,
                    slack=self.config.slack_config.enable,
                )
                await asyncio.sleep(5)

    async def run(self):
        try:
            self.start_network()
            open_task = self.open_position_loop()
            close_task = self.close_position_loop()
            await asyncio.gather(open_task, close_task)
        except KeyboardInterrupt:
            await self.exchange.close()

    def signal_terminate(self, signum, frame):
        self.logger.info(
            f"Receive SIGTERM signal, try to exit {self.hedge_pair.coin} sub process"
        )
        self.stop_network()
        time.sleep(2)  # wait all tasks cancel
        sys.exit(0)


def run_sub_process(hedge_pair: FtxHedgePair, config: Config, conn: Connection, rate_limiter: RateLimiter):
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    sub_process = SubProcess(hedge_pair, config, conn, rate_limiter)
    sub_process.logger.info(f"start to run {hedge_pair.coin} process")
    signal(SIGTERM, sub_process.signal_terminate)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(sub_process.run())
