from abc import abstractmethod, ABC
from pathlib import Path

import pandas as pd
from typing import Tuple, NoReturn

from primitives import StoredObject


class Market(StoredObject, ABC):
    """ Core infrastructure which abstracts communication with exchange.

    Holds minimal market data.

    Todo:
        - Add a layer of risk management:
            - "runaway" trade decisions or unfavorable outcomes
    """

    __name__ = 'Base'
    valid_freqs: Tuple[str, ...]
    asset_pairs: Tuple[str, ...]
    columns = ('open', 'high', 'low', 'close', 'volume')

    def __init__(self, symbol: str = None, **kwargs):
        """
        Args:
            symbol:
                Asset pair symbol to use for trading for this instance.
        """
        super().__init__(exclude=('_data',), **kwargs)
        self._data = pd.DataFrame(columns=list(self.columns))
        """DataFrame: container for candle data.
        
        Container gets populated by `get_candles` and should otherwise be read-only.
        
        Notes:
            Should have `source` and `freq` set via the `DataFrame.attrs` convention.
        """

        self._check_symbol(symbol)
        self.symbol = symbol

    @classmethod
    def _check_symbol(cls, symbol: str) -> NoReturn:
        if symbol:
            assert symbol in cls.asset_pairs

    @property
    def id(self) -> str:
        return f"{self.__name__}_{self.symbol}"

    @property
    def most_recent_timestamp(self) -> pd.Timestamp:
        return self._data.iloc[-1].name

    @abstractmethod
    def candles(self, freq: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def update(self):
        """ Update ticker data """
        pass

    @property
    def _instance_dir(self) -> Path:
        return Path(self.root, self.id)

    @abstractmethod
    def translate_period(self, freq):
        """ Convert given market string interval into valid Pandas `DateOffset` value

        References:
            View Pandas documentation for a list of valid values
            https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timeseries-offset-aliases
        """
        pass

    def assert_monotonic_increasing(self):
        """ Asserts that all candle data has monotonically increasing indexes """
        for freq in self.valid_freqs:
            assert self.candles(freq).index.is_monotonic_increasing
