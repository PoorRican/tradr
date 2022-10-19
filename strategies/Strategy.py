import pandas as pd
from data import DATA_ROOT
from os import path
from datetime import datetime
from markets.Market import Market
from typing import Tuple, Union
from abc import ABC, abstractmethod
import logging


def truncate(f, n) -> float:
    """Truncates/pads a float f to n decimal places without rounding"""
    s = '{}'.format(f)
    if 'e' in s or 'E' in s:
        return float('{0:.{1}f}'.format(f, n))
    i, p, d = s.partition('.')
    return float('.'.join([i, (d+'0'*n)[:n]]))


class Strategy(ABC):
    """ Abstracted a trading strategy.

    Performs computation necessary to determine when and how much to trade. Inherited instances should
    define a profitable trade, when and how many trades of a certain type should be executed,
    and the amount of an asset to trade at a single time. Strategies should also determine a minimum amount of
    an asset to hold.

    To determine the fitness and performance of the trading strategy, reporting functions show the total amount of
    assets and fiat accrued. This can be used in active implementations as well as during backtesting.

    Attributes
        orders: history of orders performed by this strategy. Timestamps of extrema are used as indexes.
        starting: starting amount of fiat currency
        market: reference to `Market` object
    """
    name = 'base'

    def __init__(self, starting_fiat: float, market: Market):
        self.orders = pd.DataFrame(columns=('amt', 'rate', 'cost', 'side', 'id'))
        self.failed_orders = pd.DataFrame(columns=('amt', 'rate', 'cost', 'side'))

        self.starting = float(starting_fiat)

        self.market = market

        self.load()

    @property
    def filename(self):
        """ Filename for order data """
        return path.join(DATA_ROOT, '%s_orders.pkl' % self.name)

    def load(self):
        """ Load order data """
        try:
            self.orders = pd.read_pickle(self.filename)
        except FileNotFoundError:
            pass

    def save(self):
        """ Store order data """
        self.orders.to_pickle(self.filename)

    def _calc_profit(self, amount: float, rate: float, side: str) -> float:
        """ Calculates profit of a sale.
        """
        last_trade = self.orders.iloc[-1]
        assert last_trade['side'] != side

        gain = truncate(amount * rate, 2) - truncate(last_trade['amt'] * last_trade['rate'], 2)
        return gain - self.market.calc_fee()

    def _add_order(self, extrema: pd.Timestamp, amount: float, rate: float, cost: float, side: str) -> bool:
        """ Send order to market, and store in history.

        The assumption is that not all orders will post, so only orders that are executed (accepted by the
        market) are stored. However, for the purposes of debugging, failed orders are stored.

        Args:
            extrema: timestamp at which an extrema occurred.
            amount: the amount of asset which was traded.
            rate: rate at which asset cost.
            cost: total cost of trade in terms of fiat.
            side: type of order: 'buy' or 'sell'

        Returns:
            `true` if order was correctly stored.
            `false` if there was an error storing. Response and contents of error are logged.

        Todo:
            - Store values from `response` in order history, not self-generated values.
        """
        response = self.market.place_order(abs(amount), rate, side)
        try:
            if response:
                rate = float(response['price'])       # rate that order was fulfilled at
                self.orders.loc[extrema] = [amount, rate, amount*rate, side, response['order_id']]
                return True
            else:
                self.failed_orders.loc[extrema] = [amount, rate, amount*rate, side]
                return False
        except KeyError as e:
            logging.error(response)
            logging.error(e)
            return False

    def process(self, point: pd.Timestamp = None) -> bool:
        """ Determine and execute position.

        This method is the main interface method.

        Args:
            point: Current position in time. Used during backtesting.

        Returns:
            If algorithm decided to place an order, the result of order execution is returned.
            Otherwise, `False` is returned by default
        """
        position = self._determine_position(point)

        if position:
            side, extrema = position
            assert side in ('buy', 'sell')
            try:
                self.orders[extrema]                # has an order been placed for given extrema?
            except KeyError:
                if side == 'buy':
                    return self._buy(extrema)
                else:
                    return self._sell(extrema)
        return False

    @abstractmethod
    def _calc_rate(self, extrema: pd.Timestamp, side: str) -> float:
        """ Calculate rate for trade.

        This method should return the same value for given parameters.

        Args:
            extrema: Index/timestamp which triggered trade.
            side: Type of trade. May be 'buy'/'sell'

        Returns:
            Rate to use when buying
        """
        pass

    @abstractmethod
    def _calc_amount(self, extrema: pd.Timestamp, side: str) -> float:
        """ Calculate amount for trade.

        This method should return the same value for given parameters.

         Args:
            extrema: Index/timestamp which triggered trade.
            side: Type of trade. May be 'buy'/'sell'

        Returns:
            Amount of asset to trade
        """
        pass

    def _buy(self, extrema: pd.Timestamp) -> bool:
        """
        Attempt to perform buy.

        Profitability must be determined *before* this function is called.

        Args:
            extrema:
                Timestamp at which extrema occurred. This prevents multiple orders being placed for the same extrema
                (local min or max).

        Returns:
            Outcome of order is returned:
                `true` if trade executed,
                `false` if it was not placed.
        """

        rate = self._calc_rate(extrema, 'buy')
        amount = self._calc_amount(extrema, 'buy')

        accepted = self._add_order(extrema, amount, rate, truncate(amount * rate, 2), 'buy')
        if accepted:
            logging.info(f"Buy order at {rate} was placed at {datetime.now()}")
        return accepted

    def _sell(self, extrema: pd.Timestamp) -> bool:
        """
        Attempt to perform sell.

        Profitability must be determined *before* this function is called.

        Price at which to perform buy is determined by market last market frame. The amount traded is static.

        Args:
            extrema:
                Timestamp at which extrema occurred. This prevents multiple orders being placed for the same extrema
                (local min or max).

        Returns:
            Outcome of order is returned:
                `true` if trade executed,
                `false` if it was not placed.
        """

        rate = self._calc_rate(extrema, 'sell')
        amount = self._calc_amount(extrema, 'sell')

        accepted = self._add_order(extrema, amount, rate, truncate(amount * rate, 2), 'sell')
        if accepted:
            logging.info(f"Sell order at {rate} was placed at {datetime.now()}")
        return accepted

    @abstractmethod
    def _is_profitable(self, amount: float, rate: float, side: str) -> bool:
        """
        Determine if the given trade is profitable or not.

        Args:
            amount: amount of asset to be traded
            rate: rate of exchange
            side: buy or sell

        Returns:
            Determination whether trade should be executed is binary. It is either profitable or not.
        """
        pass

    @abstractmethod
    def _develop_signals(self, point: pd.Timestamp) -> pd.DataFrame:
        """ Use available data to update indicators.

        Args:
            point: Used in backtesting to simulate time

        Returns:
            Indicator/signal data
        """
        pass

    @abstractmethod
    def _determine_position(self, point: pd.Timestamp = None) -> Union[Tuple[str, 'pd.Timestamp'], 'False']:
        """ Determine whether buy or sell order should be executed.

        Args:
            point: Used in backtesting to simulate time

        Returns:
            If a valid extrema is found, returns a tuple with decision ('buy'/'sell') and extrema.

            Otherwise, if no valid extrema is found, `False, False` is returned. Tuple is returned to prevent
            an `TypeError` from being raised when unpacking non-iterable bool.
        """
        pass

    def pnl(self) -> float:
        buy_orders = self.orders[self.orders['side'] == 'buy']
        sell_orders = self.orders[self.orders['side'] == 'sell']

        buy_cost = buy_orders['cost'].sum()
        sell_cost = sell_orders['cost'].sum()
        return (sell_cost - buy_cost) - self.starting
