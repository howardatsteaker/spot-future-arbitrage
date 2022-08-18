import re
import logging
import pathlib
import time
import asyncio
from typing import Dict, Tuple
from decimal import Decimal
from concurrent.futures import ProcessPoolExecutor, Future
import multiprocessing as mp
from multiprocessing.connection import Connection
import dateutil.parser
from src.script.fund_manager import FundManager
from src.common import Config, Exchange
from src.exchange.ftx.ftx_client import FtxExchange
from src.exchange.ftx.ftx_data_type import (
    FtxCollateralWeight,
    Ftx_EWMA_InterestRate,
    FtxCollateralWeightMessage,
    FtxFeeRate,
    FtxFeeRateMessage,
    FtxFundOpenFilledMessage,
    FtxFundRequestMessage,
    FtxInterestRateMessage,
    FtxLeverageInfo,
    FtxLeverageMessage,
    FtxOrderMessage,
    FtxOrderStatus,
    FtxOrderType,
    FtxTradingRule, FtxHedgePair,
    FtxTradingRuleMessage,
    Side)
from src.script.sub_process import run_sub_process


class MainProcess:
    MARKET_STATUS_POLLING_INTERVAL = 300
    INTEREST_RATE_POLLING_INTERVAL = 3600
    FEE_RATE_POLLING_INTERVAL = 300
    COLLATERAL_WEIGHT_POLLING_INTERVAL = 300
    ACCOUNT_INFO_POLLING_INTERVAL = 3600
    FUND_MANAGER_POLLING_INTERVAL = 3

    def __init__(self, config: Config):
        self.config: Config = config
        self.logger = self._init_get_logger()
        if config.exchange == Exchange.FTX:
            self.exchange = FtxExchange(config.api_key, config.api_secret, config.subaccount_name)
            self.trading_rules: Dict[str, FtxTradingRule] = {}
            self.hedge_pairs: Dict[str, FtxHedgePair] = {}
            self.ewma_interest_rate = Ftx_EWMA_InterestRate(config.interest_rate_lookback_days)
            self.fee_rate = FtxFeeRate()
            self.collateral_weights: Dict[str, FtxCollateralWeight] = {}
            self.leverage_info = FtxLeverageInfo()

            # params initializer, to notify sub process all params are ready
            self._trading_rules_ready_event = asyncio.Event()
            self._interest_rate_ready_event = asyncio.Event()
            self._fee_rate_ready_event = asyncio.Event()
            self._collateral_weights_ready_event = asyncio.Event()
            self._account_info_ready_event = asyncio.Event()

            # Sub processes
            self._hedge_pair_initialized_cond = asyncio.Condition()
            self._loop = asyncio.get_event_loop()
            self._executor = ProcessPoolExecutor()
            self._connections: Dict[str, Tuple[Connection, Connection]] = {}
            self._sub_process_futures: Dict[str, Future] = {}
            self._sub_process_notify_events: Dict[str, asyncio.Event] = {}

            # tasks
            self._market_status_polling_task: asyncio.Task = None
            self._interest_rate_polling_task: asyncio.Task = None
            self._fee_rate_polling_task: asyncio.Task = None
            self._collateral_weight_polling_task: asyncio.Task = None
            self._spawn_sub_processes_task: asyncio.Task = None
            self._sub_process_listen_tasks: Dict[str, asyncio.Task] = {}
            self._start_ws_task: asyncio.Task = None
            self._listen_ws_orders_task: asyncio.Task = None
            self._account_info_polling_task: asyncio.Task = None
            self._fund_manager_polling_task: asyncio.Task = None

            # websocket
            self.exchange.ws_register_order_channel()

            # fund manager
            self.fund_manager = FundManager(leverage_limit=config.leverage_limit)

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
        return logger

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
        if self._spawn_sub_processes_task is None:
            self._spawn_sub_processes_task = asyncio.create_task(self._spawn_sub_processes())
        if self._start_ws_task is None:
            self._start_ws_task = asyncio.create_task(self.exchange.ws_start_network())
        if self._account_info_polling_task is None:
            self._account_info_polling_task = asyncio.create_task(self._account_info_polling_loop())
        if self._fund_manager_polling_task is None:
            self._fund_manager_polling_task = asyncio.create_task(self._fund_manager_polling_loop())
        if self._listen_ws_orders_task is None:
            self._listen_ws_orders_task = asyncio.create_task(self._listen_ws_orders())

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
        if self._spawn_sub_processes_task is not None:
            self._spawn_sub_processes_task.cancel()
            self._spawn_sub_processes_task = None
        if self._start_ws_task is not None:
            self._start_ws_task.cancel()
            self._start_ws_task = None
        if self._account_info_polling_task is not None:
            self._account_info_polling_task.cancel()
            self._account_info_polling_task = None
        if self._fund_manager_polling_task is not None:
            self._fund_manager_polling_task.cancel()
            self._fund_manager_polling_task = None
        if self._listen_ws_orders_task is not None:
            self._listen_ws_orders_task.cancel()
            self._listen_ws_orders_task = None
        self._stop_all_sub_process_listen_tasks()
        self._stop_all_sub_processes()

    async def _market_status_polling_loop(self):
        """Handle the market infomations. Combined the bollowing tasks to make only one request.
        1. update TradingRule
        2. uddate HedgePair
        """
        while True:
            try:
                markets = await self.exchange.get_markets()
                self._update_trading_rule(markets)
                await self._update_hedge_pair(markets)
                await asyncio.sleep(self.MARKET_STATUS_POLLING_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.error("Unexpected error while fetching market status.", exc_info=True)
                await asyncio.sleep(1)

    def _update_trading_rule(self, market_infos: dict):
        trading_rules = {}
        for market in market_infos:
            symbol = market['name']
            min_order_size = Decimal(str(market['sizeIncrement']))
            trading_rules[symbol] = FtxTradingRule(symbol, min_order_size)
        self.trading_rules.update(trading_rules)
        self._trading_rules_ready_event.set()
        for coin, (conn, _) in self._connections.items():
            spot = FtxHedgePair.coin_to_spot(coin)
            future = FtxHedgePair.coin_to_future(coin, self.config.season)
            if self.trading_rules.get(spot):
                conn.send(FtxTradingRuleMessage(self.trading_rules[spot]))
            if self.trading_rules.get(future):
                conn.send(FtxTradingRuleMessage(self.trading_rules[future]))

    async def _update_hedge_pair(self, market_infos: dict):
        # # For test
        # self.hedge_pairs['BTC'] = FtxHedgePair(
        #     coin='BTC',
        #     spot='BTC/USD',
        #     future=f'BTC-{self.config.season}'
        # )
        # self.hedge_pairs['ETH'] = FtxHedgePair(
        #     coin='ETH',
        #     spot='ETH/USD',
        #     future=f'ETH-{self.config.season}'
        # )
        # self.hedge_pairs['AMD'] = FtxHedgePair(
        #     coin='AMD',
        #     spot='AMD/USD',
        #     future=f'AMD-{self.config.season}'
        # )
        # self.hedge_pairs['DOGE'] = FtxHedgePair(
        #     coin='DOGE',
        #     spot='DOGE/USD',
        #     future=f'DOGE-{self.config.season}'
        # )
        # self.hedge_pairs['AMC'] = FtxHedgePair(
        #     coin='AMC',
        #     spot='AMC/USD',
        #     future=f'AMC-{self.config.season}'
        # )
        symbol_set = set([info['name'] for info in market_infos if info['enabled']])
        regex = re.compile(f"[0-9A-Z]+-{self.config.season}")
        hedge_pairs = {}
        for symbol in symbol_set:
            if regex.match(symbol) and FtxHedgePair.future_to_spot(symbol) in symbol_set:
                coin = FtxHedgePair.future_to_coin(symbol)
                spot = FtxHedgePair.coin_to_spot(coin)
                hedge_pairs[coin] = FtxHedgePair(
                    coin=coin,
                    spot=spot,
                    future=symbol
                )
        self.hedge_pairs.update(hedge_pairs)

        async with self._hedge_pair_initialized_cond:
            self._hedge_pair_initialized_cond.notify_all()

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
                self.ewma_interest_rate.last_timestamp = dateutil.parser.parse(rate_info[-1]['time']).timestamp()
                self._interest_rate_ready_event.set()
                for (conn, _) in self._connections.values():
                    conn.send(FtxInterestRateMessage(self.ewma_interest_rate))
                await asyncio.sleep(self.INTEREST_RATE_POLLING_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.error("Unexpected error while fetching USD interest rate.", exc_info=True)
                await asyncio.sleep(5)

    async def _fee_rate_polling_loop(self):
        while True:
            try:
                account = await self.exchange.get_account()
                self.fee_rate.maker_fee_rate = Decimal(str(account['makerFee']))
                self.fee_rate.taker_fee_rate = Decimal(str(account['takerFee']))
                self._fee_rate_ready_event.set()
                for (conn, _) in self._connections.values():
                    conn.send(FtxFeeRateMessage(self.fee_rate))
                await asyncio.sleep(self.FEE_RATE_POLLING_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.error("Unexpected error while fetching account fee rate.", exc_info=True)
                await asyncio.sleep(5)

    async def _collateral_weight_polling_loop(self):
         while True:
            try:
                coin_infos = await self.exchange.get_coins()
                for info in coin_infos:
                    coin = info['id']
                    weight = Decimal(str(info['collateralWeight']))
                    self.collateral_weights[coin] = FtxCollateralWeight(
                        coin=coin,
                        weight=weight)
                    if self._connections.get(coin):
                        self._connections[coin][0].send(
                            FtxCollateralWeightMessage(self.collateral_weights[coin]))
                self._collateral_weights_ready_event.set()
                await asyncio.sleep(self.COLLATERAL_WEIGHT_POLLING_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.error("Unexpected error while fetching account fee rate.", exc_info=True)
                await asyncio.sleep(5)

    async def _account_info_polling_loop(self):
        while True:
            try:
                account_info = await self.exchange.get_account()
                account_value = Decimal(str(account_info['totalAccountValue']))
                position_value = Decimal(str(account_info['totalPositionSize']))
                current_leverage = position_value / account_value
                self.leverage_info = FtxLeverageInfo(
                    max_leverage=Decimal(str(account_info['leverage'])),
                    account_value=account_value,
                    position_value=position_value,
                    current_leverage=current_leverage)
                self._account_info_ready_event.set()
                for (conn, _) in self._connections.values():
                    conn.send(FtxLeverageMessage(self.leverage_info))
                await asyncio.sleep(self.ACCOUNT_INFO_POLLING_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.error("Unexpected error while fetching account info.", exc_info=True)
                await asyncio.sleep(5)

    async def _fund_manager_polling_loop(self):
        while True:
            try:
                account_info = await self.exchange.get_account()
                await self.fund_manager.update_account_state(account_info)
                balances = await self.exchange.get_balances()
                usd_info = next(b for b in balances if b['coin'] == 'USD')
                await self.fund_manager.update_usd_state(Decimal(str(usd_info['free'])), Decimal(str(usd_info['spotBorrow'])))
                await asyncio.sleep(self.FUND_MANAGER_POLLING_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.error("Unexpected error while fetching account info.", exc_info=True)
                await asyncio.sleep(5)

    async def _spawn_sub_processes(self):
        while True:
            try:
                async with self._hedge_pair_initialized_cond:
                    await self._hedge_pair_initialized_cond.wait()
                await self._trading_rules_ready_event.wait()
                self.logger.debug("trading rules ready!")
                await self._interest_rate_ready_event.wait()
                self.logger.debug("interest rate ready!")
                await self._fee_rate_ready_event.wait()
                self.logger.debug("fee rate ready!")
                await self._collateral_weights_ready_event.wait()
                self.logger.debug("collateral weights ready!")
                await self._account_info_ready_event.wait()
                self.logger.debug("account info ready!")

                for coin, hedge_pair in self.hedge_pairs.items():
                    if self._sub_process_futures.get(coin) is None:
                        # build pipe connection, future, and sub process listener
                        conn1, conn2 = mp.Pipe(duplex=True)
                        self._connections[hedge_pair.coin] = (conn1, conn2)
                        sub_process_future = self._loop.run_in_executor(self._executor, run_sub_process, hedge_pair, self.config, conn2)
                        self._sub_process_futures[coin] = sub_process_future
                        self._sub_process_listen_tasks[coin] = asyncio.create_task(self._listen_sub_process_msg(coin))

                        # notify params
                        if self.trading_rules.get(hedge_pair.spot):
                            conn1.send(FtxTradingRuleMessage(self.trading_rules[hedge_pair.spot]))
                        if self.trading_rules.get(hedge_pair.future):
                            conn1.send(FtxTradingRuleMessage(self.trading_rules[hedge_pair.future]))
                        conn1.send(FtxInterestRateMessage(ewma_interest_rate=self.ewma_interest_rate))
                        conn1.send(FtxFeeRateMessage(fee_rate=self.fee_rate))
                        if self.collateral_weights.get(coin):
                            conn1.send(FtxCollateralWeightMessage(collateral_weight=self.collateral_weights[coin]))
                        conn1.send(FtxLeverageMessage(leverage=self.leverage_info))

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger.error(f"Unexpected error while spawn new sub process. {e}", exc_info=True)

    def _stop_all_sub_processes(self):
        for coin, task in self._sub_process_futures.items():
            task.cancel()
            del self._sub_process_futures[coin]
            self._connections[coin][0].close()
            self._connections[coin][1].close()
            del self._connections[coin]
            self.logger.debug(f"Close sub process. Coin: {coin}")
        self._executor.shutdown()

    async def _listen_sub_process_msg(self, coin: str):
        conn = self._connections[coin][0]
        self._loop.add_reader(conn.fileno(), self._sub_process_notify_events[coin].set)
        while True:
            try:
                if not conn.poll():
                    await self._sub_process_notify_events[coin].wait()
                msg = conn.recv()
                self.logger.debug(f"Get msg from {coin} child process: {msg}")
                if type(msg) is FtxFundRequestMessage:
                    response = await self.fund_manager.request_for_open(msg)
                    conn.send(response)
                elif type(msg) is FtxFundOpenFilledMessage:
                    await self.fund_manager.handle_open_order_filled(msg)
                self._sub_process_notify_events[coin].clear()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.error("Unexpected error while listen to sub process message.", exc_info=True)
                await asyncio.sleep(5)

    async def _stop_all_sub_process_listen_tasks(self):
        for task in self._sub_process_listen_tasks.values():
            task.cancel()

    async def _listen_ws_orders(self):
        while True:
            try:
                data = await self.exchange.orders.get()
                order_msg = FtxOrderMessage(
                    id=str(data['id']),
                    market=data['market'],
                    type=FtxOrderType.LIMIT if data['type'] == 'limit' else FtxOrderType.MARKET,
                    side=Side.BUY if data['side'] == 'buy' else Side.SELL,
                    size=Decimal(str(data['size'])),
                    price=Decimal(str(data['price'])),
                    status=FtxOrderStatus.str_entry(data['status']),
                    filled_size=Decimal(str(data['filledSize'])),
                    avg_fill_price=Decimal(str(data['avgFillPrice'])) if data['avgFillPrice'] else None,
                    create_timestamp=dateutil.parser.parse(data['createdAt']).timestamp(),
                )
                if FtxHedgePair.is_spot(order_msg.market):
                    coin = FtxHedgePair.spot_to_coin(order_msg.market)
                    if self._connections.get(coin):
                        conn = self._connections[coin][0]
                        conn.send(order_msg)
                elif FtxHedgePair.is_future(order_msg.market, self.config.season):
                    coin = FtxHedgePair.future_to_coin(order_msg.market)
                    if self._connections.get(coin):
                        conn = self._connections[coin][0]
                        conn.send(order_msg)
                else:
                    self.logger.warning(f"Get unknown order msg: {order_msg}")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.error("Unexpected error while listen to ws orders.", exc_info=True)
                await asyncio.sleep(5)

    async def run(self):
        self.start_network()
        try:
            while True:
                # if self.ready:
                #     pass
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            self.stop_network()
            await self.exchange.close()
