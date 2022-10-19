import asyncio
import hmac
import logging
import time
from decimal import Decimal
from typing import Dict, List
import pytz

import aiohttp
import dateutil.parser
from requests import Request

from src.exchange.exchange_data_type import ExchangeBase, Trade, Kline
from src.exchange.ftx.ftx_data_type import (FtxCandleResolution, FtxOrderType,
                                            FtxTicker, Side)
from src.exchange.ftx.ftx_error import ftx_throw_exception


class FtxExchange(ExchangeBase):
    REST_URL = "https://ftx.com/api"
    WS_URL = "wss://ftx.com/ws"

    _logger = None

    @classmethod
    def logger(self):
        if self._logger is None:
            self._logger = logging.getLogger(__name__)
        return self._logger

    def __init__(
        self, api_key: str, api_secret: str, subaccount_name: str = None
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._subaccount_name = subaccount_name
        self._rest_client = None

        # web socket
        self._to_subscribe_order_channel = False
        self._to_subscribe_ticker_channel = False
        self._ticker_symbols: list = []
        self.tickers: Dict[str, FtxTicker] = {}
        self.ticker_notify_conds: Dict[str, asyncio.Condition] = {}
        self.orders = asyncio.Queue()

    @property
    def name(self) -> str:
        return "Ftx"

    async def close(self):
        if self._rest_client is not None:
            await self._rest_client.close()

    def _get_rest_client(self):
        if self._rest_client is None:
            self._rest_client = aiohttp.ClientSession()
        return self._rest_client

    def _gen_auth_header(
        self, http_method: str, url: str, params: dict = None, body: dict = None
    ) -> dict:
        if http_method == "POST":
            request = Request(http_method, url, json=body)
            prepared = request.prepare()
            ts = int(time.time() * 1000)
            content_to_sign = f"{ts}{prepared.method}{prepared.path_url}".encode()
            if prepared.body:
                content_to_sign += prepared.body
        else:
            request = Request(http_method, url, params=params)
            prepared = request.prepare()
            ts = int(time.time() * 1000)
            content_to_sign = f"{ts}{prepared.method}{prepared.path_url}".encode()

        signature = hmac.new(
            self._api_secret.encode(), content_to_sign, "sha256"
        ).hexdigest()

        headers = {"FTX-KEY": self._api_key, "FTX-SIGN": signature, "FTX-TS": str(ts)}
        if self._subaccount_name is not None and self._subaccount_name != "":
            headers["FTX-SUBACCOUNT"] = self._subaccount_name

        return headers

    async def get_markets(self) -> List[dict]:
        client = self._get_rest_client()
        url = self.REST_URL + "/markets"
        async with client.get(url) as res:
            json_res = await res.json()
            if json_res["success"]:
                return json_res["result"]
            else:
                error_msg = json_res["error"]
                ftx_throw_exception(error_msg)

    def map_kline(self, raw_kline: dict) -> Kline:
        return Kline(
            start_time=dateutil.parser.parse(raw_kline["startTime"]).replace(tzinfo=pytz.utc),
            open=Decimal(str(raw_kline["open"])),
            high=Decimal(str(raw_kline["high"])),
            low=Decimal(str(raw_kline["low"])),
            close=Decimal(str(raw_kline["close"])),
            quote_volume=Decimal(str(raw_kline["volume"])),
        )

    async def get_candles(
        self,
        symbol: str,
        resolution: FtxCandleResolution,
        start_time: int,
        end_time: int,
    ) -> List[Kline]:
        if start_time is None:
            start_time = 0
        if end_time is None:
            end_time = time.time()
        time_set = set()
        all_candles = []
        client = self._get_rest_client()
        while True:
            url = (
                self.REST_URL
                + f"/markets/{symbol}/candles?resolution={resolution.value}&start_time={start_time}&end_time={end_time}"
            )
            async with client.get(url) as res:
                res_json = await res.json()
                if res_json["success"]:
                    candles = res_json["result"]
                else:
                    error_msg = res_json["error"]
                    ftx_throw_exception(error_msg)
            if len(candles) == 0:
                break
            dedupted_candles = [c for c in candles if c["startTime"] not in time_set]
            if len(dedupted_candles) == 0:
                break
            all_candles.extend(dedupted_candles)
            time_set |= {c["startTime"] for c in dedupted_candles}
            end_time = (
                min(
                    [dateutil.parser.parse(c["startTime"]) for c in candles]
                ).timestamp()
                - 1
            )
            if end_time < start_time:
                break
        all_candles = sorted(all_candles, key=lambda x: dateutil.parser.parse(x["startTime"]))

        return list(map(self.map_kline, all_candles))

    async def get_fills(self, start_time: float, end_time: float, symbol: str = None):
        client = self._get_rest_client()
        all_fills = []
        id_set = set()
        while True:
            url = self.REST_URL + f"/fills?start_time={start_time}&end_time={end_time}"
            if symbol:
                url += f"&market={symbol}"
            headers = self._gen_auth_header("GET", url)
            async with client.get(url, headers=headers) as res:
                json_res = await res.json()
            if json_res["success"]:
                fills = json_res["result"]
            else:
                error_msg = json_res["error"]
                ftx_throw_exception(error_msg)
            if len(fills) == 0:
                break
            all_fills.extend([fill for fill in fills if fill["id"] not in id_set])
            id_set |= set([fill["id"] for fill in fills])
            end_time = (
                min([dateutil.parser.parse(fill["time"]).timestamp() for fill in fills])
                - 0.000001
            )

        return sorted(all_fills, key=lambda fill: dateutil.parser.parse(fill["time"]))

    async def get_account(self) -> dict:
        client = self._get_rest_client()
        url = self.REST_URL + "/account"
        headers = self._gen_auth_header("GET", url)
        async with client.get(url, headers=headers) as res:
            json_res = await res.json()
            if json_res["success"]:
                return json_res["result"]
            else:
                error_msg = json_res["error"]
                ftx_throw_exception(error_msg)

    async def set_leverage(self, leverage: int):
        client = self._get_rest_client()
        url = self.REST_URL + "/account/leverage"
        data = {"leverage": leverage}
        headers = self._gen_auth_header("POST", url, body=data)
        async with client.post(url, headers=headers, json=data) as res:
            json_res = await res.json()
            if not json_res["success"]:
                error_msg = json_res["error"]
                ftx_throw_exception(error_msg)

    async def get_spot_margin_history(
        self, start_time: float = None, end_time: float = None
    ) -> List[dict]:
        client = self._get_rest_client()
        url = self.REST_URL + "/spot_margin/history"
        data = {}
        if start_time:
            data["start_time"] = start_time
        if end_time:
            data["end_time"] = end_time
        headers = self._gen_auth_header("GET", url)
        async with client.get(url, headers=headers, params=data) as res:
            json_res = await res.json()
            if json_res["success"]:
                return json_res["result"]
            else:
                error_msg = json_res["error"]
                ftx_throw_exception(error_msg)

    async def get_full_spot_margin_history(
        self, start_time: float, end_time: float
    ) -> List[dict]:
        results = []
        while True:
            result = await self.get_spot_margin_history(start_time, end_time)
            usd_result = [r for r in result if r["coin"] == "USD" and r not in results]
            if len(usd_result) == 0:
                break
            results.extend(usd_result)
            end_time = min(
                dateutil.parser.parse(r["time"]).timestamp() for r in usd_result
            )
        return sorted(results, key=lambda r: dateutil.parser.parse(r["time"]))

    async def get_coins(self) -> List[dict]:
        client = self._get_rest_client()
        url = self.REST_URL + "/wallet/coins"
        headers = self._gen_auth_header("GET", url)
        async with client.get(url, headers=headers) as res:
            json_res = await res.json()
            if json_res["success"]:
                return json_res["result"]
            else:
                error_msg = json_res["error"]
                ftx_throw_exception(error_msg)

    async def place_order(
        self,
        market: str,
        side: Side,
        type: FtxOrderType,
        size: Decimal,
        price: Decimal = None,
        reduce_only: bool = False,
        ioc: bool = False,
        post_only: bool = False,
        client_id: str = None,
    ) -> dict:
        if type == FtxOrderType.LIMIT:
            assert price is not None, "price cannot be None when placing limit order"
        else:
            assert not post_only, "post_only cannot be used with market order"
        client = self._get_rest_client()
        url = self.REST_URL + "/orders"
        data = {
            "market": market,
            "side": side.value,
            "type": type.value,
            "size": str(size),
        }
        if price:
            data["price"] = str(price)
        else:
            data["price"] = None
        if reduce_only:
            data["reduceOnly"] = reduce_only
        if ioc:
            data["ioc"] = ioc
        if post_only:
            data["post_only"] = post_only
        if client_id:
            data["clientId"] = client_id
        headers = self._gen_auth_header("POST", url, body=data)
        async with client.post(url, headers=headers, json=data) as res:
            res_json = await res.json()
            if res_json["success"]:
                # TODO ftx response order id type is int, should cast to str
                result = res_json["result"]
                if type == FtxOrderType.LIMIT:
                    self.logger().info(
                        f"Ftx place {market} {type.value} {side.value} order (p: {price}, s: {size}, id: {result['id']})"
                    )
                else:
                    self.logger().info(
                        f"Ftx place {market} {type.value} {side.value} order (s: {size}, id: {result['id']})"
                    )
                return result
            else:
                error_msg = res_json.get("error")
                ftx_throw_exception(error_msg)

    async def place_market_order(
        self, market: str, side: Side, size: Decimal, reduce_only: bool = False
    ) -> dict:
        res_json = await self.place_order(
            market, side, FtxOrderType.MARKET, size, reduce_only=reduce_only
        )
        return res_json

    async def place_ioc_order(
        self,
        market: str,
        side: Side,
        size: Decimal,
        price: Decimal,
        reduce_only: bool = False,
    ) -> dict:
        res_json = await self.place_order(
            market,
            side,
            FtxOrderType.LIMIT,
            size,
            price,
            ioc=True,
            reduce_only=reduce_only,
        )
        return res_json

    async def place_limit_order(
        self,
        market: str,
        side: Side,
        size: Decimal,
        price: Decimal,
        post_only: bool = False,
        reduce_only: bool = False,
    ) -> dict:
        res_json = await self.place_order(
            market,
            side,
            FtxOrderType.LIMIT,
            size,
            price,
            post_only=post_only,
            reduce_only=reduce_only,
        )
        return res_json

    def quantize_order_size(self, size: Decimal, size_tick: Decimal) -> Decimal:
        return size // size_tick * size_tick

    def quantize_order_price(self, price: Decimal, price_tick: Decimal) -> Decimal:
        return price // price_tick * price_tick

    async def cancel_order(self, order_id: str) -> bool:
        client = self._get_rest_client()
        url = self.REST_URL + f"/orders/{order_id}"
        headers = self._gen_auth_header("DELETE", url)
        async with client.delete(url, headers=headers) as res:
            res_json = await res.json()
            if res_json["success"]:
                self.logger().info(f"{res_json['result']}, order id: {order_id}")
                return True
            else:
                error_msg = res_json["error"]
                self.logger().error(
                    f"Fail to cancel order: {order_id}. Error msg: {error_msg}"
                )
                return False

    async def get_fills_since_last_flat(
        self, symbol: str, position: Decimal
    ) -> List[dict]:
        """Get all fills of a symbol since last last
        position > 0 means long position
        position < 0 means short position
        """
        if position == 0:
            return []
        else:
            client = self._get_rest_client()
            temp_position = position
            all_fills = []
            id_set = set()
            end_time = time.time()
            while True:
                url = (
                    self.REST_URL
                    + f"/fills?market={symbol}&start_time=0&end_time={end_time}"
                )
                headers = self._gen_auth_header("GET", url)
                async with client.get(url, headers=headers) as res:
                    json_res = await res.json()
                if json_res["success"]:
                    fills = json_res["result"]
                else:
                    error_msg = json_res["error"]
                    ftx_throw_exception(error_msg)
                dedup_fills = [f for f in fills if f["id"] not in id_set]
                if len(dedup_fills) == 0:
                    break
                for fill in dedup_fills:
                    size = Decimal(str(fill["size"]))
                    if position > 0:
                        if fill["side"] == "buy":
                            temp_position -= size
                            all_fills.append(fill)
                            id_set.add(fill["id"])
                            if temp_position <= 0:
                                break
                        else:
                            temp_position += size
                            all_fills.append(fill)
                            id_set.add(fill["id"])
                    elif position < 0:
                        if fill["side"] == "sell":
                            temp_position += size
                            all_fills.append(fill)
                            id_set.add(fill["id"])
                            if temp_position >= 0:
                                break
                        else:
                            temp_position -= size
                            all_fills.append(fill)
                            id_set.add(fill["id"])
                end_time = (
                    min(
                        [
                            dateutil.parser.parse(fill["time"]).timestamp()
                            for fill in all_fills
                        ]
                    )
                    - 0.000001
                )
            return sorted(
                all_fills, key=lambda fill: dateutil.parser.parse(fill["time"])
            )

    async def get_positions(self):
        client = self._get_rest_client()
        url = self.REST_URL + "/positions"
        headers = self._gen_auth_header("GET", url)
        async with client.get(url, headers=headers) as res:
            json_res = await res.json()
        if json_res["success"]:
            return json_res["result"]
        else:
            error_msg = json_res["error"]
            ftx_throw_exception(error_msg)

    async def get_future(self, symbol: str):
        client = self._get_rest_client()
        url = self.REST_URL + f"/futures/{symbol}"
        headers = self._gen_auth_header("GET", url)
        async with client.get(url, headers=headers) as res:
            json_res = await res.json()
        if json_res["success"]:
            return json_res["result"]
        else:
            error_msg = json_res["error"]
            ftx_throw_exception(error_msg)

    async def get_balances(self):
        client = self._get_rest_client()
        url = self.REST_URL + "/wallet/balances"
        headers = self._gen_auth_header("GET", url)
        async with client.get(url, headers=headers) as res:
            json_res = await res.json()
        if json_res["success"]:
            return json_res["result"]
        else:
            error_msg = json_res["error"]
            ftx_throw_exception(error_msg)

    async def get_order(self, order_id: str) -> dict:
        client = self._get_rest_client()
        url = self.REST_URL + f"/orders/{order_id}"
        headers = self._gen_auth_header("GET", url)
        async with client.get(url, headers=headers) as res:
            json_res = await res.json()
        if json_res["success"]:
            return json_res["result"]
        else:
            error_msg = json_res["error"]
            ftx_throw_exception(error_msg)

    def map_trade(self, ftx_raw_trade: dict) -> Trade:
        return Trade(
            id=ftx_raw_trade["id"],
            price=Decimal(str(ftx_raw_trade["price"])),
            size=Decimal(str(ftx_raw_trade["size"])),
            timestamp=dateutil.parser.parse(ftx_raw_trade["time"]).timestamp(),
            taker_side=Side.BUY if ftx_raw_trade["side"] == "buy" else Side.SELL,
        )

    async def get_trades(
        self, symbol: str, start_time: float, end_time: float
    ) -> List[Trade]:
        client = self._get_rest_client()
        all_trades = []
        id_set = set()
        while True:
            url = (
                self.REST_URL
                + f"/markets/{symbol}/trades?start_time={start_time}&end_time={end_time}"
            )

            async with client.get(url) as res:
                json_res = await res.json()
            if json_res["success"]:
                trades = json_res["result"]
            else:
                error_msg = json_res["error"]
                ftx_throw_exception(error_msg)
            if len(trades) == 0:
                break
            trades = list(map(self.map_trade, trades))
            all_trades.extend([trade for trade in trades if trade["id"] not in id_set])
            id_set |= set([trade["id"] for trade in trades])
            end_time = min([trade["timestamp"] for trade in trades]) - 0.000001

        return sorted(all_trades, key=lambda trade: trade["id"])

    async def request_quote(self, from_coin: str, to_coin: str, size: Decimal) -> str:
        """Request for OTC quote

        Returns quote id
        """
        client = self._get_rest_client()
        url = self.REST_URL + "/otc/quotes"
        data = {"fromCoin": from_coin, "toCoin": to_coin, "size": str(size)}
        headers = self._gen_auth_header("POST", url, body=data)
        async with client.post(url, headers=headers, json=data) as res:
            json_res = await res.json()
        if json_res["success"]:
            result = json_res["result"]
            quote_id = str(result["quoteId"])
            return quote_id
        else:
            error_msg = json_res["error"]
            ftx_throw_exception(error_msg)

    async def accepte_quote(self, quote_id: str):
        """Accept OTC quote"""
        client = self._get_rest_client()
        url = self.REST_URL + f"/otc/quotes/{quote_id}/accept"
        headers = self._gen_auth_header("POST", url)
        async with client.post(url, headers=headers) as res:
            json_res = await res.json()
        if json_res["success"]:
            return
        else:
            error_msg = json_res["error"]
            ftx_throw_exception(error_msg)

    async def get_quote_status(self, quote_id: str):
        """Accept OTC quote"""
        client = self._get_rest_client()
        url = self.REST_URL + f"/otc/quotes/{quote_id}"
        headers = self._gen_auth_header("GET", url)
        async with client.get(url, headers=headers) as res:
            json_res = await res.json()
        if json_res["success"]:
            return json_res["result"]
        else:
            error_msg = json_res["error"]
            ftx_throw_exception(error_msg)

    async def place_otc_sell_order(self, from_coin: str, size: Decimal) -> dict:
        assert from_coin != "USD", "Cannot sell USD for USD"
        quote_id = await self.request_quote(from_coin, "USD", size)
        await self.accepte_quote(quote_id)
        await asyncio.sleep(1)
        status = await self.get_quote_status(quote_id)
        return status

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
                async with aiohttp.ClientSession() as ws_client:
                    async with ws_client.ws_connect(self.WS_URL) as ws:
                        ping_task = asyncio.create_task(self._ping(ws))
                        if self._to_subscribe_order_channel:
                            ts = int(time.time() * 1e3)
                            sign_str = f"{ts}websocket_login".encode()
                            sign = hmac.new(
                                self._api_secret.encode(), sign_str, "sha256"
                            ).hexdigest()
                            payload = {
                                "args": {
                                    "key": self._api_key,
                                    "sign": sign,
                                    "time": ts,
                                },
                                "op": "login",
                            }
                            if self._subaccount_name:
                                payload["args"]["subaccount"] = self._subaccount_name
                            await ws.send_json(payload)
                            await ws.send_json({"op": "subscribe", "channel": "orders"})
                        if self._to_subscribe_ticker_channel:
                            for symbol in self._ticker_symbols:
                                await ws.send_json(
                                    {
                                        "op": "subscribe",
                                        "channel": "ticker",
                                        "market": symbol,
                                    }
                                )
                        async for msg in ws:
                            if msg.type in (
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.ERROR,
                            ):
                                break
                            msg_json: dict = msg.json()
                            if (
                                msg_json.get("type") == "update"
                                and msg_json.get("channel") == "ticker"
                            ):
                                data = msg_json["data"]
                                market = msg_json["market"]
                                self.tickers[market] = FtxTicker.ws_entry(market, data)
                                await self._ticker_notify_all(market)
                            elif (
                                msg_json.get("type") == "update"
                                and msg_json.get("channel") == "orders"
                            ):
                                data = msg_json["data"]
                                self.orders.put_nowait(data)
                                self.logger().debug(f"Receive orders data: {data}")
                            elif msg_json.get("type") == "pong":
                                self.logger().debug("pong")
                            elif (
                                msg_json.get("type") == "subscribed"
                                and msg_json.get("channel") == "ticker"
                            ):
                                market = msg_json["market"]
                                self.logger().debug(
                                    f"Subscribed {market} ticker channel"
                                )
                            elif (
                                msg_json.get("type") == "subscribed"
                                and msg_json.get("channel") == "orders"
                            ):
                                self.logger().debug("Subscribed orders channel")
                            else:
                                self.logger().debug(f"Receive unknown msg: {msg_json}")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Error while listen ws network.", exc_info=True)
            finally:
                if ping_task:
                    ping_task.cancel()
                    ping_task = None

    async def _ping(self, ws: aiohttp.ClientWebSocketResponse):
        while True:
            try:
                await ws.send_json({"op": "ping"})
                await asyncio.sleep(15)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Error while send ping", exc_info=True)

    async def _ticker_notify_all(self, symbol: str):
        if self.ticker_notify_conds.get(symbol) is None:
            self.ticker_notify_conds[symbol] = asyncio.Condition()
        async with self.ticker_notify_conds[symbol]:
            self.ticker_notify_conds[symbol].notify_all()
