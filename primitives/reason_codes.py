from enum import IntEnum


class ReasonCode(IntEnum):
    UNKNOWN = 0
    NOT_PROFITABLE = 1
    MARKET_REJECTED = 2
    POST_ERROR = 3
    PARSE_ERROR = 4

    def __bool__(self):
        return False
