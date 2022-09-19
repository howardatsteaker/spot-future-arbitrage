import asyncio
import logging
from dataclasses import dataclass
from typing import Mapping

import aiohttp
import requests


@dataclass
class SlackConfig:
    enable: bool
    auth_token: str
    summary_channel: str
    alert_channel: str


class LatestMessageStoreHandler(logging.Handler):
    def __init__(self):
        self.latest_message = None
        super().__init__()

    def emit(self, record):
        self.latest_message = self.format(record)


class SlackWrappedLogger(logging.LoggerAdapter):
    URL = "https://slack.com/api/chat.postMessage"

    def __init__(self, logger, extra: Mapping[str, object]) -> None:
        super().__init__(logger, extra)
        self.latest_message_store_handler = LatestMessageStoreHandler()
        parent_formatter = self.logger.handlers[0].formatter
        self.latest_message_store_handler.setFormatter(parent_formatter)
        self.logger.addHandler(self.latest_message_store_handler)

    def debug(self, msg, *args, **kwargs):
        slack: bool = kwargs.pop("slack", False)
        use_async: bool = kwargs.pop("use_async", True)
        msg, kwargs = self.process(msg, kwargs)
        self.logger.debug(msg, *args, stacklevel=2, **kwargs)

        if slack and self.isEnabledFor(logging.DEBUG):
            auth_token = self.extra["auth_token"]
            channel = self.extra["info_channel"]
            if use_async:
                asyncio.create_task(
                    self.a_slack_send_message(
                        auth_token,
                        channel,
                        self.latest_message_store_handler.latest_message,
                    )
                )
            else:
                self.slack_send_message(
                    auth_token,
                    channel,
                    self.latest_message_store_handler.latest_message,
                )

    def info(self, msg, *args, **kwargs):
        slack: bool = kwargs.pop("slack", False)
        use_async: bool = kwargs.pop("use_async", True)
        msg, kwargs = self.process(msg, kwargs)
        self.logger.info(msg, *args, stacklevel=2, **kwargs)

        if slack and self.isEnabledFor(logging.INFO):
            auth_token = self.extra["auth_token"]
            channel = self.extra["info_channel"]
            if use_async:
                asyncio.create_task(
                    self.a_slack_send_message(
                        auth_token,
                        channel,
                        self.latest_message_store_handler.latest_message,
                    )
                )
            else:
                self.slack_send_message(
                    auth_token,
                    channel,
                    self.latest_message_store_handler.latest_message,
                )

    def warning(self, msg, *args, **kwargs):
        slack: bool = kwargs.pop("slack", False)
        use_async: bool = kwargs.pop("use_async", True)
        msg, kwargs = self.process(msg, kwargs)
        self.logger.warning(msg, *args, stacklevel=2, **kwargs)

        if slack and self.isEnabledFor(logging.WARNING):
            auth_token = self.extra["auth_token"]
            channel = self.extra["alert_channel"]
            if use_async:
                asyncio.create_task(
                    self.a_slack_send_message(
                        auth_token,
                        channel,
                        self.latest_message_store_handler.latest_message,
                    )
                )
            else:
                self.slack_send_message(
                    auth_token,
                    channel,
                    self.latest_message_store_handler.latest_message,
                )

    def error(self, msg, *args, **kwargs):
        slack: bool = kwargs.pop("slack", False)
        use_async: bool = kwargs.pop("use_async", True)
        msg, kwargs = self.process(msg, kwargs)
        self.logger.error(msg, *args, stacklevel=2, **kwargs)

        if slack and self.isEnabledFor(logging.ERROR):
            auth_token = self.extra["auth_token"]
            channel = self.extra["alert_channel"]
            if use_async:
                asyncio.create_task(
                    self.a_slack_send_message(
                        auth_token,
                        channel,
                        self.latest_message_store_handler.latest_message,
                    )
                )
            else:
                self.slack_send_message(
                    auth_token,
                    channel,
                    self.latest_message_store_handler.latest_message,
                )

    async def a_slack_send_message(self, auth_token: str, channel: str, text: str):
        if not auth_token or not channel:
            return
        headers = {"Authorization": "Bearer " + auth_token}
        data = {
            "channel": channel,
            "text": text,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(self.URL, data=data, headers=headers) as res:
                res_json = await res.json()
                if not res_json["ok"]:
                    error_msg = res_json["error"]
                    self.error(f"Slack error message: {error_msg}")

    def slack_send_message(self, auth_token: str, channel: str, text: str):
        if not auth_token or not channel:
            return
        headers = {"Authorization": "Bearer " + auth_token}
        data = {
            "channel": channel,
            "text": text,
        }
        res = requests.post(self.URL, data=data, headers=headers)
        res_json = res.json()
        if not res_json["ok"]:
            error_msg = res_json["error"]
            self.error(f"Slack error message: {error_msg}")
