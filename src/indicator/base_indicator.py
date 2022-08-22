from decimal import Decimal


class BaseIndicator:

    @property
    def upper_threshold(self) -> Decimal:
        raise NotImplementedError

    @property
    def lower_threshold(self) -> Decimal:
        raise NotImplementedError

    async def update_indicator_info(self):
        raise NotImplementedError
