import time
from dataclasses import dataclass
from multiprocessing.managers import SyncManager


@dataclass
class RateLimitConfig:
    interval: float
    limit: int


class RateLimiter:
    """Rate limiter shared memory among processes by utilizing multiprocessing.Manager()

    Example:
    import multiprocessing as mp

    # initialization
    manager = mp.Manager()
    interval = 0.2  # seconds
    limit = 7
    rate_limiter = RateLimiter(manager, interval, limit)

    # check whether it is ok to process
    if rate_limiter.ok:
        ...
    else:
        ...

    # add record to timestamps list
    your_own_fn_that_consume_rate_limit(...)
    rate_limiter.add_record()
    """

    def __init__(self, manager: SyncManager, interval: float, limit: int):
        """
        :param manager: A multiprocessing.Manager() that create shared list and Lock()
        :param limit: A total number of calls permitted within interval period
        :param interval: The time interval in seconds
        """
        self.interval = interval
        self.limit = limit
        self.ts_list = manager.list()
        self.lock = manager.Lock()

    def drop_expired(self):
        """Drop timestamps in self.ts_list that are expired"""
        now = time.time()
        for ts in list(self.ts_list):
            if now - ts > self.interval:
                self.ts_list.remove(ts)

    def add_record(self, new_ts: float = None):
        if new_ts is None:
            new_ts = time.time()
        with self.lock:
            self.drop_expired()
            self.ts_list.append(new_ts)

    @property
    def ok(self) -> bool:
        with self.lock:
            self.drop_expired()
        return len(self.ts_list) < self.limit
