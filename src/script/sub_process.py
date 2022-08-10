import asyncio
import logging
import pathlib
from multiprocessing.connection import Connection
from src.common import Config
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


class SubProcess:

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

    async def consume_main_process_msg(self):
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


def run_sub_process(hedge_pair: FtxHedgePair, config: Config, conn: Connection):
    sub_process = SubProcess(hedge_pair, config, conn)
    sub_process.logger.debug(f'start to run {hedge_pair.coin} process')
    asyncio.run(sub_process.consume_main_process_msg())
