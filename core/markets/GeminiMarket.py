from os import path
import pandas as pd

from core.proto.gemini import GeminiProto
from models.data import DATA_ROOT
from core.market import Market
from models.ticker import Ticker


class GeminiMarket(GeminiProto, Market):
    """ Abstracts getting ticker and orderbook data from the Gemini exchange. """
    def __init__(self, api_key, api_secret, freq='1m', root=DATA_ROOT):
        """
        Args:
            api_key: Gemini API key
            api_secret: Gemini API secret
            freq: ticker frequency
            root: root directory to store ticker data
        """
        super().__init__(api_key, api_secret)
        super(Market, self).__init__(freq=freq)

        self.root = root

        self.load()
        self.update()
        self.save()

    @classmethod
    def get_candles(cls, freq: str = None) -> pd.DataFrame:
        """ Get given candles from Gemini Market (1m, 5m, 15m, 30m, 1hr)"""
        assert freq in cls.valid_freqs

        response = cls.get("/v2/candles/btcusd/" + freq)
        btc_candle_data = response.json()
        data = Ticker.from_json(btc_candle_data)

        # the Gemini API sends data in a reverse direction for some reason (lower index are later times)
        # TODO: check that GeminiAPI has correct order of data in unittests
        data = data.iloc[::-1]
        assert data.iloc[0].name < data.iloc[-1].name

        # set flag/metadata on `DataFrame`
        data.attrs['freq'] = freq
        return data

    @property
    def filename(self):
        """ Return """
        return path.join(self.root, self.name + '_' + self.freq + ".pkl")

    @classmethod
    def get_orderbook(cls) -> dict:
        response = cls.get("/v1/book/btcusd")
        return response.json()

    @classmethod
    def convert_freq(cls, freq: str):
        """ Converts Gemini ticker interval to a value pandas can use.

        Examples:
            Gemini uses the string `15m` to denote an interval of 15 minutes, but
            pandas uses `15min` for the same.

        Args:
            freq: Gemini ticker interval

        Returns:

        """
        assert freq in cls.valid_freqs

        if 'm' in freq:
            return freq + 'in'
        elif 'hr' in freq:
            return freq[:-2] + 'H'
        else:
            return '1D'
