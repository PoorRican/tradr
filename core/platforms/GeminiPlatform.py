from core.proto import GeminiProto
from core.markets import GeminiMarket
from core.exchanges import GeminiExchange
from models.data import DATA_ROOT


class GeminiPlatform(GeminiMarket, GeminiExchange, GeminiProto):
    """ Combines the functionality of interacting with market data and encapsulates trading.

    See Also:
        GeminiMarket, GeminiExchange, GeminiProto
    """
    def __init__(self, api_key, api_secret, freq='15m', root=DATA_ROOT):
        super(GeminiProto, self).__init__(api_key, api_secret)
        super(GeminiMarket, self).__init__(api_key, api_secret, freq=freq, root=root)
        super(GeminiExchange, self).__init__(api_key, api_secret)
