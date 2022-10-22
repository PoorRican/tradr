from abc import abstractmethod, ABC
from typing import Union

import pandas as pd

from models.trades import Trade, SuccessfulTrade


class Market(ABC):
    """ Core infrastructure which abstracts communication with exchange.

    Posts sell and buy orders, records historical candle data.

    Todo:
        - Add a layer of risk management:
            - "runaway" trade decisions or unfavorable outcomes
    """
    def __init__(self):
        self.data = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
        """DataFrame: container for candle data.
        
        Container gets populated by `get_candles` and should otherwise be read-only.
        
        Notes:
            Should have `source` and `freq` set via the `DataFrame.attrs` convention.
        """

    @abstractmethod
    def update(self):
        """ Update ticker data """
        pass

    def load(self):
        """ Load pickled data """
        pass

    def save(self):
        """ Save pickled data """
        pass

    @abstractmethod
    def _convert(self, trade: Trade, response: dict) -> 'SuccessfulTrade':
        """ Generate `SuccessfulTrade` """
        pass

    @abstractmethod
    def place_order(self, trade: Trade) -> Union['SuccessfulTrade', bool]:
        """ Post order to market.

        Args:
            trade:
                Potential trade data

        Returns:
            If the market accepted trade and the order was executed, `SuccessfulTrade` is returned. This is
            necessary because the `rate` of trade might be better than requested.
        """
        pass

    @abstractmethod
    def get_fee(self, *args, **kwargs) -> float:
        """ Calculate cost of a transaction

        """
        pass

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
