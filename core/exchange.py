from abc import ABC, abstractmethod
from typing import Union

from models.trades import SuccessfulTrade, Trade


class Exchange(ABC):
    """ Core infrastructure encapsulate exchange functionality.

    Abstracts the ability to interact with the market by placing and querying orders.

    Notes:
        The functionality to place orders should be separate from `Market` functionality because while input data might
        come from multiple `Market` sources, trades should be executed on a single `Exchange`.

    Todo:
        - Add a layer of risk management:
            - "runaway" trade decisions or unfavorable outcomes
    """
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
    def calc_fee(self, *args, **kwargs) -> float:
        """ Calculate cost of a transaction

        Returns:
            Fee per transaction
        """
        pass
