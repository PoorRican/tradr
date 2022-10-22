from abc import abstractmethod, ABC
import pandas as pd
from typing import Iterable

from models.ticker import Ticker


class Market(ABC):
    """ Core infrastructure which abstracts interaction with ticker data.

    Attributes:
        data: available ticker data
    """

    def __init__(self, freq: str):
        if hasattr(self, 'valid_freqs'):
            assert freq in getattr(self, 'valid_freqs')
        self.freq = freq
        self.data = Ticker.container()

    def update(self) -> None:
        """ Updates `data` with recent candle data """
        # self.data = combine_data(self.data, self.get_candles())
        self.data = self.get_candles(self.freq)

    @classmethod
    @abstractmethod
    def get_candles(cls, freq: str) -> pd.DataFrame:
        pass

    def load(self):
        try:
            self.data = pd.read_pickle(self.filename)
        except FileNotFoundError:
            pass

    def save(self):
        self.data.to_pickle(self.filename)

    @property
    @abstractmethod
    def filename(self) -> str:
        pass

    @abstractmethod
    def convert_freq(self, freq):
        """ Convert given market string interval into valid Pandas `DateOffset` value

        References:
            View Pandas documentation for a list of valid values
            https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timeseries-offset-aliases
        """
        pass
