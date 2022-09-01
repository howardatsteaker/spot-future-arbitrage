import asyncio
import logging
from dataclasses import dataclass

import aiohttp


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


class SlackWrappedLogger(logging.Logger):
    URL = "https://slack.com/api/chat.postMessage"

    def __init__(self, name="", level=logging.NOTSET, auth_token=None):
        super().__init__(name, level)
        self.auth_token = auth_token

        self.lastest_msg_store_handler = LatestMessageStoreHandler()
        self.addHandler(self.lastest_msg_store_handler)

    def addHandler(self, hdlr: logging.Handler) -> None:
        self.lastest_msg_store_handler.setFormatter(hdlr.formatter)
        return super().addHandler(hdlr)

    def debug(self, msg, *args, **kwargs):
        slack: bool = kwargs.pop("slack", False)
        channel: str = kwargs.pop("channel", None)
        super().debug(msg, *args, **kwargs)

        if slack and self.isEnabledFor(logging.DEBUG):
            assert (
                channel is not None
            ), "If slack is True, argument 'channel' should be given"
            asyncio.create_task(
                self.slack_send_message(
                    channel, self.lastest_msg_store_handler.latest_message
                )
            )

    def info(self, msg, *args, **kwargs):
        slack: bool = kwargs.pop("slack", False)
        channel: str = kwargs.pop("channel", None)
        super().info(msg, *args, **kwargs)

        if slack and self.isEnabledFor(logging.INFO):
            assert (
                channel is not None
            ), "If slack is True, argument 'channel' should be given"
            asyncio.create_task(
                self.slack_send_message(
                    channel, self.lastest_msg_store_handler.latest_message
                )
            )

    def warning(self, msg, *args, **kwargs):
        slack: bool = kwargs.pop("slack", False)
        channel: str = kwargs.pop("channel", None)
        super().warning(msg, *args, **kwargs)

        if slack and self.isEnabledFor(logging.WARNING):
            assert (
                channel is not None
            ), "If slack is True, argument 'channel' should be given"
            asyncio.create_task(
                self.slack_send_message(
                    channel, self.lastest_msg_store_handler.latest_message
                )
            )

    def error(self, msg, *args, **kwargs):
        slack: bool = kwargs.pop("slack", False)
        channel: str = kwargs.pop("channel", None)
        super().error(msg, *args, **kwargs)

        if slack and self.isEnabledFor(logging.ERROR):
            assert (
                channel is not None
            ), "If slack is True, argument 'channel' should be given"
            asyncio.create_task(
                self.slack_send_message(
                    channel, self.lastest_msg_store_handler.latest_message
                )
            )

    async def slack_send_message(self, channel: str, text: str):
        if not self.auth_token or not channel:
            return
        headers = {"Authorization": "Bearer " + self.auth_token}
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
