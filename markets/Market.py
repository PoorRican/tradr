from abc import abstractmethod, ABC
from typing import Union

import pandas as pd

from models.trades import Trade, SuccessfulTrade


class Market(ABC):
    """ Core infrastructure which abstracts communication with exchange.

    Posts sell and buy orders, records historical ticker data.

    Todo:
        - Add a layer of risk management:
            - "runaway" trade decisions or unfavorable outcomes

    Attributes:
        data: available ticker data
    """
    def __init__(self):
        self.data = pd.DataFrame(columns=('open', 'high', 'low', 'close', 'volume'))

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
            trade: Potential trade data

        Returns:
            If the market accepted trade and the order was executed, `SuccessfulTrade` is returned. This is
            necessary because the `rate` of trade might be better than requested.
        """
        pass

    @abstractmethod
    def calc_fee(self, *args, **kwargs):
        """ Calculate cost of a transaction

        Returns:

        """
        pass

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
