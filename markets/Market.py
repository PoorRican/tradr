from abc import abstractmethod, ABC
from typing import Union

import pandas as pd


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
    def place_order(self, amount: float, rate: float, side: str) -> Union[dict, bool]:
        """ Post order to market.
        Args:
            amount: amount of asset to trade
            rate: exchange rate between asset and fiat
            side: type of trade ('buy'/'sell')

        Returns:
            If the market accepted trade and the order was executed, details of trade are returned. This is
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
