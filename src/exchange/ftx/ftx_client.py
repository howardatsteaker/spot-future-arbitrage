import asyncio
import logging
import time
import hmac
from requests import Request
from typing import List, Dict
import aiohttp
import dateutil.parser
from src.exchange.ftx.ftx_data_type import FtxCandleResolution, FtxTicker


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
        self._ws_client = None

        # web socket
        self._to_subscribe_order_channel = False
        self._to_subscribe_ticker_channel = False
        self._ticker_symbols: list = []
        self.tickers: Dict[str, FtxTicker] = {}
        self.ticker_notify_conds: Dict[str, asyncio.Condition] = {}

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

    def ws_register_order_channel(self):
        self._to_subscribe_order_channel = True

    def ws_register_ticker_channel(self, symbols: List[str]):
        self._to_subscribe_ticker_channel = True
        self._ticker_symbols = symbols

    async def ws_start_network(self):
        if not (self._to_subscribe_order_channel or self._to_subscribe_ticker_channel):
            return
        ping_task: asyncio.Task = None
        while True:
            try:
                ws_client = self._get_ws_client()
                async with ws_client.ws_connect(self.WS_URL) as ws:
                    ping_task = asyncio.create_task(self._ping(ws))
                    if self._to_subscribe_order_channel:
                        ts = int(time.time() * 1e3)
                        sign_str = f"{ts}websocket_login".encode()
                        sign = hmac.new(self._api_secret.encode(), sign_str, 'sha256').hexdigest()
                        payload = {
                            'args': {
                                "key": self._api_key,
                                "sign": sign,
                                "time": ts,},
                            'op': 'login'}
                        if self._subaccount_name:
                            payload['args']['subaccount'] = self._subaccount_name
                        await ws.send_json(payload)
                        await ws.send_json({'op': 'subscribe', 'channel': 'orders'})
                    if self._to_subscribe_ticker_channel:
                        for symbol in self._ticker_symbols:
                            await ws.send_json({'op': 'subscribe', 'channel': 'ticker', 'market': symbol})
                    async for msg in ws:
                        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                        msg_json: dict = msg.json()
                        if msg_json.get('type') == 'update' and msg_json.get('channel') == 'ticker':
                            data = msg_json['data']
                            market = msg_json['market']
                            await self._ticker_notify_all(market)
                            self.tickers[market] = FtxTicker.ws_entry(market, data)
                        elif msg_json.get('type') == 'update' and msg_json.get('channel') == 'orders':
                            data = msg_json['data']
                            self.logger().debug(f'Receive orders data: {data}')
                        elif msg_json.get('type') == 'pong':
                            self.logger().debug('pong')
                        elif msg_json.get('type') == 'subscribed' and msg_json.get('channel') == 'ticker':
                            market = msg_json['market']
                            self.logger().debug(f'Subscribed {market} ticker channel')
                        elif msg_json.get('type') == 'subscribed' and msg_json.get('channel') == 'orders':
                            self.logger().debug('Subscribed orders channel')
                        else:
                            self.logger().debug(f'Receive unknown msg: {msg_json}')
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Error while listen ws network.", exc_info=True)
            finally:
                if ping_task:
                    ping_task.cancel()
                    ping_task = None
                if ws_client:
                    await ws_client.close()

    async def _ping(self, ws: aiohttp.ClientWebSocketResponse):
        while True:
            try:
                await ws.send_json({'op': 'ping'})
                await asyncio.sleep(15)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Error while send ping", exc_info=True)

    def _get_ws_client(self):
        if self._ws_client is None:
            self._ws_client = aiohttp.ClientSession()
        return self._ws_client

    async def _ticker_notify_all(self, symbol: str):
        if self.ticker_notify_conds.get(symbol) is None:
            self.ticker_notify_conds[symbol] = asyncio.Condition()
        async with self.ticker_notify_conds[symbol]:
            self.ticker_notify_conds[symbol].notify_all()
