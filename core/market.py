from abc import abstractmethod, ABC
from typing import Union, Tuple

import pandas as pd

from models.trades import Trade, SuccessfulTrade


class Market(ABC):
    """ Core infrastructure which abstracts communication with exchange.

    Holds minimal market data.

    Todo:
        - Add a layer of risk management:
            - "runaway" trade decisions or unfavorable outcomes
    """

    __name__ = 'Base'
    valid_freqs: Tuple[str, ...]
    asset_pairs: Tuple[str, ...]

    def __init__(self, symbol: str = None, freq: str = None):
        self.data = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
        """DataFrame: container for candle data.
        
        Container gets populated by `get_candles` and should otherwise be read-only.
        
        Notes:
            Should have `source` and `freq` set via the `DataFrame.attrs` convention.
        """
        if symbol and hasattr(self, 'asset_pairs'):
            assert symbol in self.asset_pairs
        if freq and hasattr(self, 'valid_freqs'):
            assert freq in self.valid_freqs

        self.symbol = symbol
        self.freq = freq

    @property
    def id(self) -> str:
        return f"{self.__name__}_{self.symbol}_{self.freq}"

    @abstractmethod
    def update(self):
        """ Update ticker data """
        pass

    def load(self, ignore: bool = True):
        try:
            self.data = pd.read_pickle(self.filename)
        except FileNotFoundError as e:
            if not ignore:
                raise e
            pass

    def save(self):
        self.data.to_pickle(self.filename)

    @property
    @abstractmethod
    def filename(self) -> str:
        pass

    @abstractmethod
    def translate_period(self, freq):
        """ Convert given market string interval into valid Pandas `DateOffset` value

        References:
            View Pandas documentation for a list of valid values
            https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timeseries-offset-aliases
        """
        pass

