import asyncio


class ExchangeError(Exception):
    __module__ = Exception.__module__

    def __init__(self, *args: object) -> None:
        super().__init__(*args)
