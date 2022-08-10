import logging
import time
import hmac
import json
from requests import Request
from typing import List
import aiohttp
import dateutil.parser
from src.exchange.ftx.ftx_data_type import FtxCandleResolution


class FtxExchange:
    REST_URL = "https://ftx.com/api"
    WS_URL = "wss://ftx.com/ws"

    _logger = None

    @classmethod
    def logger(self):
        if self._logger is None:
            self._logger = logging.getLogger(__name__)
        return self._logger

    def __init__(self, api_key: str, api_secret: str, subaccount_name: str = None) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._subaccount_name = subaccount_name
        self._rest_client = aiohttp.ClientSession()

    async def close(self):
        await self._rest_client.close()

    def _gen_auth_header(self, http_method: str, url: str, body: dict = None) -> dict:
        if http_method == "POST":
            request = Request(http_method, url, json=body)
            prepared = request.prepare()
            ts = int(time.time() * 1000)
            content_to_sign = f'{ts}{prepared.method}{prepared.path_url}'.encode()
            content_to_sign += prepared.body
        else:
            request = Request(http_method, url)
            prepared = request.prepare()
            ts = int(time.time() * 1000)
            content_to_sign = f'{ts}{prepared.method}{prepared.path_url}'.encode()

        signature = hmac.new(self._api_secret.encode(), content_to_sign, 'sha256').hexdigest()

        headers = {
            "FTX-KEY": self._api_key,
            "FTX-SIGN": signature,
            "FTX-TS": str(ts)
        }
        if self._subaccount_name is not None and self._subaccount_name != "":
            headers["FTX-SUBACCOUNT"] = self._subaccount_name

        return headers

    async def get_markets(self) -> List[str]:
        self.logger().debug('Send request get /markets')
        url = self.REST_URL + "/markets"
        async with self._rest_client.get(url) as res:
            json_res = await res.json()
        return json_res['result']

    async def get_candles(self, symbol: str, resolution: FtxCandleResolution, start_time: int, end_time: int):
        url = self.REST_URL + f"/markets/{symbol}/candles?resolution={resolution.value}&start_time={start_time}&end_time={end_time}"
        pass

    async def get_fills(self, symbol: str, start_time: float, end_time: float):
        all_fills = []
        id_set = set()
        while True:
            url = self.REST_URL + f"/fills?market={symbol}&start_time={start_time}&end_time={end_time}"
            headers = self._gen_auth_header('GET', url)
            async with self._rest_client.get(url, headers=headers) as res:
                json_res = await res.json()
            fills = json_res['result']
            if len(fills) == 0:
                break
            all_fills.extend([fill for fill in fills if fill['id'] not in id_set])
            id_set |= set([fill['id'] for fill in fills])
            end_time = min([dateutil.parser.parse(fill['time']).timestamp() for fill in fills]) - 0.000001

        return sorted(all_fills, key=lambda fill: dateutil.parser.parse(fill['time']))

    async def get_account(self) -> dict:
        url = self.REST_URL + "/account"
        headers = self._gen_auth_header('GET', url)
        async with self._rest_client.get(url, headers=headers) as res:
            json_res = await res.json()
        return json_res['result']

    async def set_leverage(self, leverage: int):
        url = self.REST_URL + "/account/leverage"
        data = {'leverage': leverage}
        headers = self._gen_auth_header('POST', url, body=data)
        async with self._rest_client.post(url, headers=headers, json=data) as res:
            json_res = await res.json()
            assert json_res['success'], "ftx set_leverage was not success"

    async def get_spot_margin_history(self, start_time: float = None, end_time: float = None) -> List[dict]:
        url = self.REST_URL + "/spot_margin/history"
        data = {}
        if start_time:
            data['start_time'] = start_time
        if end_time:
            data['end_time'] = end_time
        headers = self._gen_auth_header('GET', url)
        async with self._rest_client.get(url, headers=headers, params=data) as res:
            json_res = await res.json()
        return json_res['result']

    async def get_full_spot_margin_history(self, start_time: float, end_time: float) -> List[dict]:
        results = []
        while True:
            result = await self.get_spot_margin_history(start_time, end_time)
            usd_result = [r for r in result if r['coin'] == 'USD' and r not in results]
            if len(usd_result) == 0:
                break
            results.extend(usd_result)
            end_time = min(dateutil.parser.parse(r['time']).timestamp() for r in usd_result)
        return sorted(results, key=lambda r: dateutil.parser.parse(r['time']))

    async def get_coins(self) -> List[dict]:
        url = self.REST_URL + "/wallet/coins"
        headers = self._gen_auth_header('GET', url)
        async with self._rest_client.get(url, headers=headers) as res:
            json_res = await res.json()
        return json_res['result']
