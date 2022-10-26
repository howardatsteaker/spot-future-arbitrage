from typing import List
import time


class OrderFailManager:
    """If a strategy failed to place order 'limit' times during some 'interval',
    then strategy is not allow to trade.
    """

    def __init__(self, limit: int, interval: float):
        self.limit = limit
        self.interval = interval
        self.fail_timestamp_list: List[float] = []

    def add_fail_record(self):
        self.fail_timestamp_list.append(time.time())

    @property
    def is_okay_to_trade(self) -> bool:
        if len(self.fail_timestamp_list) == 0:
            return True
        now: float = time.time()
        self.fail_timestamp_list = [fail_ts for fail_ts in self.fail_timestamp_list if now - fail_ts < self.interval]
        return len(self.fail_timestamp_list) < self.limit
