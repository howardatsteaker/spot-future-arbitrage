from datetime import datetime
from decimal import Decimal
from typing import List

import aiohttp

from src.exchange.binance.binance_data_type import BinanceCandleResolution
from src.exchange.exchange_data_type import ExchangeBase, Kline


class BinanceSpotExchange(ExchangeBase):
    URL = "https://api.binance.com"

    def __init__(self, api_key: str, api_secret: str):
        self._api_key: str = api_key
        self._api_secret: str = api_secret
        self._rest_client: aiohttp.ClientSession = None

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
        url = self.URL + "/api/v3/klines"
        params = {
            "symbol": symbol,
            "interval": resolution.to_binance_rest_api_request_str(),
            "startTime": start_time * 1000,
            "endTime": end_time * 1000,
            "limit": 1000,
        }
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


class BinanceUSDMarginFuturesExchange(ExchangeBase):
    URL = "https://fapi.binance.com"

    def __init__(self, api_key: str, api_secret: str):
        self._api_key: str = api_key
        self._api_secret: str = api_secret
        self._rest_client: aiohttp.ClientSession = None

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
        url = self.URL + "/fapi/v1/klines"
        params = {
            "symbol": symbol,
            "interval": resolution.to_binance_rest_api_request_str(),
            "startTime": start_time * 1000,
            "endTime": end_time * 1000,
            "limit": 1500,
        }
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
