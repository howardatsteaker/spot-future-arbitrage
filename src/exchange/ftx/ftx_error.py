class ExchangeError(Exception):
    __module__ = Exception.__module__

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class RateLimitExceeded(ExchangeError):
    pass


class InsufficientFunds(ExchangeError):
    pass


class InvalidOrder(ExchangeError):
    pass


ftx_error_message_map = {
    'Not enough balances': InsufficientFunds,
    'Account does not have enough margin for order': InsufficientFunds,
    'Please slow down': RateLimitExceeded,
    'Do not send more than': RateLimitExceeded,
    'Invalid reduce-only order': InvalidOrder,
}


def ftx_throw_exception(message: str) -> ExchangeError:
    for key in ftx_error_message_map:
        if key in message:
            raise ftx_error_message_map[key](message)
    raise ExchangeError(message)
