from src.exchange.api_throttler.data_types import (LinkedLimitWeightPair,
                                                   RateLimit)

# public
AGG_TRADES_URL = "/api/v3/aggTrades"
KLINES_URL = "/api/v3/klines"

# private

# Rate Limit Type
REQUEST_WEIGHT = "REQUEST_WEIGHT"
ORDERS = "ORDERS"
ORDERS_24HR = "ORDERS_24HR"

# Rate Limit time intervals
ONE_MINUTE = 60
ONE_SECOND = 1
ONE_DAY = 86400

MAX_REQUEST = 5000

RATE_LIMITS = [
    # Pools
    RateLimit(limit_id=REQUEST_WEIGHT, limit=1200, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDERS, limit=10, time_interval=ONE_SECOND),
    RateLimit(limit_id=ORDERS_24HR, limit=100000, time_interval=ONE_DAY),
    # Weighted Limits
    RateLimit(
        limit_id=AGG_TRADES_URL,
        limit=MAX_REQUEST,
        time_interval=ONE_MINUTE,
        linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1)],
    ),
    RateLimit(
        limit_id=KLINES_URL,
        limit=MAX_REQUEST,
        time_interval=ONE_MINUTE,
        linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1)],
    ),
    # RateLimit(limit_id=ORDER_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
    #           linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
    #                          LinkedLimitWeightPair(ORDERS, 1),
    #                          LinkedLimitWeightPair(ORDERS_24HR, 1)]),
]
