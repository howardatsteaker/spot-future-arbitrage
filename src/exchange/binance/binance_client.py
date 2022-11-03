from datetime import datetime
from decimal import Decimal
from typing import List

import aiohttp

import src.exchange.binance.binance_spot_constant as SPOT_CONSTANTS
import src.exchange.binance.binance_usd_margin_futures_constant as FUTURES_CONSTANTS
from src.exchange.api_throttler.async_throttler import AsyncThrottler
from src.exchange.binance.binance_data_type import BinanceCandleResolution
from src.exchange.exchange_data_type import ExchangeBase, Kline, Side, Trade


class BinanceSpotExchange(ExchangeBase):
    URL = "https://api.binance.com"

    def __init__(self, api_key: str, api_secret: str):
        self._api_key: str = api_key
        self._api_secret: str = api_secret
        self._rest_client: aiohttp.ClientSession = None
        self._rate_limiter = AsyncThrottler(SPOT_CONSTANTS.RATE_LIMITS)

    @property
    def name(self) -> str:
        return "Binance_Spot"

    async def close(self):
        if self._rest_client is not None:
            await self._rest_client.close()
            self._rest_client = None

    def _get_rest_client(self):
        if self._rest_client is None:
            self._rest_client = aiohttp.ClientSession()
        return self._rest_client

    async def get_candles(
        self,
        symbol: str,
        resolution: BinanceCandleResolution,
        start_time: int,
        end_time: int,
    ) -> List[Kline]:
        client = self._get_rest_client()
        url = self.URL + SPOT_CONSTANTS.KLINES_URL
        params = {
            "symbol": symbol,
            "interval": resolution.to_binance_rest_api_request_str(),
            "startTime": start_time * 1000,
            "endTime": end_time * 1000,
            "limit": 1000,
        }
        async with self._rate_limiter.execute_task(SPOT_CONSTANTS.KLINES_URL):
            async with client.get(url, params=params) as resp:
                res_json = await resp.json()

        ret = []
        for res in res_json:
            item = Kline(
                start_time=datetime.fromtimestamp(res[0] / 1000),
                open=Decimal(res[1]),
                high=Decimal(res[2]),
                low=Decimal(res[3]),
                close=Decimal(res[4]),
                base_volume=Decimal(res[5]),
                quote_volume=Decimal(res[7]),
            )
            ret.append(item)

        return ret

    def map_trade(self, binance_raw_trade: dict) -> Trade:
        return Trade(
            id=binance_raw_trade["a"],
            price=Decimal(binance_raw_trade["p"]),
            size=Decimal(binance_raw_trade["q"]),
            timestamp=binance_raw_trade["T"] / 1e3,
            taker_side=Side.SELL if binance_raw_trade["m"] else Side.BUY,
        )

    async def get_one_hour_trades(
        self, symbol: str, start_time_ms: int, end_time_ms: int,
    ) -> List[Trade]:
        client = self._get_rest_client()
        url = self.URL + SPOT_CONSTANTS.AGG_TRADES_URL
        all_trades = []
        id_set = set()
        while True:
            params = {
                "symbol": symbol,
                "startTime": start_time_ms,
                "endTime": end_time_ms,
                "limit": 1000,
            }
            async with self._rate_limiter.execute_task(SPOT_CONSTANTS.AGG_TRADES_URL):
                async with client.get(url, params=params) as resp:
                    trades = await resp.json()
            if len(trades) == 0:
                break
            dedupted_trades = [trade for trade in trades if trade["a"] not in id_set]
            if len(dedupted_trades) == 0:
                break
            all_trades.extend(dedupted_trades)
            id_set |= set([trade["a"] for trade in trades])
            start_time_ms = max([trade["T"] for trade in trades])

        all_trades = list(map(self.map_trade, all_trades))
        return sorted(all_trades, key=lambda trade: trade["id"])

    async def get_trades(
        self, symbol: str, start_time: float, end_time: float,
    ) -> List[Trade]:
        all_trades = []
        id_set = set()
        start_time_ms = int(start_time * 1e3)
        end_time_ms = int(end_time * 1e3)
        one_hour_ms = 3600000
        while start_time_ms < end_time_ms:
            one_hour_trades = await self.get_one_hour_trades(
                symbol=symbol,
                start_time_ms=start_time_ms,
                end_time_ms=min(end_time_ms, start_time_ms + one_hour_ms),
            )
            if len(one_hour_trades) == 0:
                continue
            dedupted_trades = [
                trade for trade in one_hour_trades if trade["id"] not in id_set
            ]
            if len(dedupted_trades) == 0:
                continue
            all_trades.extend(dedupted_trades)
            id_set |= set([trade["id"] for trade in one_hour_trades])
            start_time_ms += one_hour_ms

        return all_trades


class BinanceUSDMarginFuturesExchange(ExchangeBase):
    URL = "https://fapi.binance.com"

    def __init__(self, api_key: str, api_secret: str):
        self._api_key: str = api_key
        self._api_secret: str = api_secret
        self._rest_client: aiohttp.ClientSession = None
        self._rate_limiter = AsyncThrottler(FUTURES_CONSTANTS.RATE_LIMITS)

    @property
    def name(self) -> str:
        return "Binance_USD_Margin_Futures"

    async def close(self):
        if self._rest_client is not None:
            await self._rest_client.close()
            self._rest_client = None

    def _get_rest_client(self):
        if self._rest_client is None:
            self._rest_client = aiohttp.ClientSession()
        return self._rest_client

    async def get_candles(
        self,
        symbol: str,
        resolution: BinanceCandleResolution,
        start_time: int,
        end_time: int,
    ) -> List[Kline]:
        client = self._get_rest_client()
        url = self.URL + FUTURES_CONSTANTS.KLINES_URL
        params = {
            "symbol": symbol,
            "interval": resolution.to_binance_rest_api_request_str(),
            "startTime": start_time * 1000,
            "endTime": end_time * 1000,
            "limit": 1500,
        }
        async with self._rate_limiter.execute_task(FUTURES_CONSTANTS.KLINES_URL):
            async with client.get(url, params=params) as resp:
                res_json = await resp.json()

        ret = []
        for res in res_json:
            item = Kline(
                start_time=datetime.fromtimestamp(res[0] / 1000),
                open=Decimal(res[1]),
                high=Decimal(res[2]),
                low=Decimal(res[3]),
                close=Decimal(res[4]),
                base_volume=Decimal(res[5]),
                quote_volume=Decimal(res[7]),
            )
            ret.append(item)

        return ret

    def map_trade(self, binance_raw_trade: dict) -> Trade:
        return Trade(
            id=binance_raw_trade["a"],
            price=Decimal(binance_raw_trade["p"]),
            size=Decimal(binance_raw_trade["q"]),
            timestamp=binance_raw_trade["T"] / 1e3,
            taker_side=Side.SELL if binance_raw_trade["m"] else Side.BUY,
        )

    async def get_trades(
        self, symbol: str, start_time: float, end_time: float,
    ) -> List[Trade]:
        client = self._get_rest_client()
        url = self.URL + FUTURES_CONSTANTS.AGG_TRADES_URL
        all_trades = []
        id_set = set()
        start_time = int(start_time * 1e3)
        end_time = int(end_time * 1e3)
        while True:
            params = {
                "symbol": symbol,
                "startTime": start_time,
                "endTime": end_time,
                "limit": 1000,
            }
            async with self._rate_limiter.execute_task(
                FUTURES_CONSTANTS.AGG_TRADES_URL
            ):
                async with client.get(url, params=params) as resp:
                    trades = await resp.json()
            if len(trades) == 0:
                break
            dedupted_trades = [trade for trade in trades if trade["a"] not in id_set]
            if len(dedupted_trades) == 0:
                break
            all_trades.extend(dedupted_trades)
            id_set |= set([trade["a"] for trade in trades])
            start_time = max([trade["T"] for trade in trades])

        all_trades = list(map(self.map_trade, all_trades))
        return sorted(all_trades, key=lambda trade: trade["id"])
