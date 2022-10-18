from src.exchange.api_throttler.data_types import RateLimit, LinkedLimitWeightPair

# public
KLINES_URL = "/fapi/v1/klines"
AGG_TRADES_URL = "/fapi/v1/aggTrades"

# private

# Rate Limit Type
REQUEST_WEIGHT = "REQUEST_WEIGHT"
ORDERS_1MIN = "ORDERS_1MIN"
ORDERS_1SEC = "ORDERS_1SEC"

# Rate Limit time intervals
ONE_HOUR = 3600
ONE_MINUTE = 60
ONE_SECOND = 1
ONE_DAY = 86400

MAX_REQUEST = 2400

RATE_LIMITS = [
    # Pool Limits
    RateLimit(limit_id=REQUEST_WEIGHT, limit=2400, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDERS_1MIN, limit=1200, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDERS_1SEC, limit=300, time_interval=10),
    # Weight Limits for individual endpoints
    RateLimit(limit_id=KLINES_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=10)]),
    RateLimit(limit_id=AGG_TRADES_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=20)]),
    
]