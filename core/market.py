from abc import abstractmethod, ABC
from pathlib import Path

import pandas as pd
from typing import Tuple, NoReturn, Dict, Union

from primitives import StoredObject


class Market(StoredObject, ABC):
    """ Core infrastructure which abstracts communication with exchange.

    Holds minimal market data.

    Attributes:
        _data : Dict[str, pd.DataFrame]
            A dictionary containing the OHLC data for different frequencies.
            Use the `candles(freq, value=None)` method to get or set values in this attribute.
            The keys of the dictionary are the valid frequency values
            and the values are the corresponding dataframe.


    Todo:
        - Add a layer of risk management:
            - "runaway" trade decisions or unfavorable outcomes
    """

    __name__ = 'Base'
    valid_freqs: Tuple[str, ...]
    asset_pairs: Tuple[str, ...]
    columns = ('open', 'high', 'low', 'close', 'volume')

    def __init__(self, symbol: str, **kwargs):
        """
        Args:
            symbol:
                Asset pair symbol to use for trading for this instance.
            kwargs:
                Keyword Args that are passed to `StoredObject.__init__()`
        """
        super().__init__(exclude=('_data',), **kwargs)
        self._data: Dict['str', 'pd.DataFrame'] = {}
        """ Dict[str, pd.DataFrame]
        A dictionary containing the OHLC data for different frequencies.
        Use the `candles(freq, value=None)` method to get or set values in this attribute.
        The keys of the dictionary are the valid frequency values
        and the values are the corresponding dataframe.
        """

        assert symbol in self.asset_pairs
        self.symbol = symbol

    @property
    def empty(self) -> bool:
        """ Return `True` if *any* value in `_data` is empty.

        Technically, all values should either be empty or populated at any given time. There really
        shouldn't be a case where some values for a given frequency are populated while others are not.
        """
        empty_columns = [candles.empty for candles in self._data.values()]
        no_columns = not empty_columns
        return no_columns or any(empty_columns)

    @property
    def id(self) -> str:
        return f"{self.__name__}_{self.symbol}"

    @property
    def most_recent_timestamp(self) -> pd.Timestamp:
        most_recent: Union['pd.Timestamp', None] = None
        for candles in self._data.values():
            last = candles.iloc[-1].name
            if most_recent is None or last > most_recent:
                most_recent = last
        return most_recent

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
