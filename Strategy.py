import pandas as pd
from data import DATA_ROOT
from os import path
from datetime import datetime
from MarketAPI import Market
from typing import Tuple, Union
from abc import ABC, abstractmethod
import logging


def increment_minute(timestamp):
    return pd.Timestamp(timestamp) + pd.offsets.Minute(1)


def decrement_minute(timestamp):
    return pd.Timestamp(timestamp) - pd.offsets.Minute(1)


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

    def __init__(self, starting_fiat: float, min_crypto: float, market: Market):
        self.orders = pd.DataFrame(columns=('amt', 'rate', 'price', 'side', 'id'))

        self.starting = float(starting_fiat)

        self.market = market

        self.load()

    @property
    def filename(self):
        """ Filename for order data """
        return path.join(DATA_ROOT, 'orders.pkl')

    def load(self):
        """ Load order data """
        try:
            self.orders = pd.read_pickle(self.filename)
        except FileNotFoundError:
            pass

    def save(self):
        """ Store order data """
        self.orders.to_pickle(self.filename)

    def add_order(self, extrema: pd.Timestamp, amount: float, rate: float, cost: float, side: str):
        """ Perform order and store in history.

        Args:
            extrema: timestamp at which an extrema occurred.
            amount: the amount of asset which was traded.
            rate: rate at which asset cost.
            cost: total cost of trade in terms of fiat.
            side: type of order: 'buy' or 'sell'

        Returns:
            `true` if order was correctly stored.
            `false` if there was an error storing. Response and contents of error are logged.
        """
        response = self.market.place_order(abs(amount), rate, side)
        try:
            if response:
                rate = float(response['price'])       # rate that order was fulfilled at
                self.orders.loc[pd.to_datetime(extrema, infer_datetime_format=True)] = [amount, rate, amount*rate, side,
                                                                                        response['order_id']]
                return True
        except KeyError as e:
            logging.error(response)
            logging.error(e)
            return False

    def process(self, data: pd.DataFrame, point: Union[pd.Timestamp, None] = None):
        """
        Determine and execute position.

        Args:
            data: Available ticker data.
            point: Current position in time. Used during backtesting.

        Returns:
            If algorithm decided to execute an order, the extrema at which an order was executed is returned.
            Otherwise, `None` is returned.
        """
        decision, extrema = self.determine_position(data, point)
        if decision:
            try:
                self.orders[extrema]                # has an order been placed for given extrema?
            except KeyError:
                if decision == 'buy':
                    self.buy(extrema)
                elif decision == 'sell':
                    self.sell(extrema)
                else:
                    pass
                return extrema

    @abstractmethod
    def calc_rate(self, extrema: pd.Timestamp, side: str) -> float:
        """ Calculate rate for trade.

        Args:
            extrema: Index/timestamp which triggered trade.
            side: Type of trade. May be 'buy'/'sell'

        Returns:
            Rate to use when buying
        """
        return NotImplemented

    @abstractmethod
    def calc_amount(self, extrema: pd.Timestamp, side: str) -> float:
        """ Calculate amount for trade.
         Args:
            extrema: Index/timestamp which triggered trade.
            side: Type of trade. May be 'buy'/'sell'

        Returns:
            Amount of asset to trade
        """
        return NotImplemented

    def buy(self, extrema: pd.Timestamp) -> bool:
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

        rate = self.calc_rate(extrema, 'buy')
        amount = self.calc_amount(extrema, 'buy')

        accepted = self.add_order(extrema, amount, rate, -truncate(amount*rate, 2), 'buy')
        if accepted:
            logging.info(f"Buy order at {rate} was placed at {datetime.now()}")
        return accepted

    def sell(self, extrema: pd.Timestamp) -> bool:
        """
        Attempt to perform sell.

        Profitability must be determined *before* this function is called.

        Price at which to perform buy is determined by market last market frame. The amount traded is static.

        Args:
            extrema:
                Timestamp at which extrema occurred. This prevents multiple orders being placed for the same extrema
                (local min or max).
            data: Available ticker data
            point: Simulated point in time. Used during backtesting.

        Returns:
            Outcome of order is returned:
                `true` if trade executed,
                `false` if it was not placed.
        """

        rate = self.calc_rate(extrema, 'sell')
        amount = self.calc_amount(extrema, 'sell')

        accepted = self.add_order(extrema, amount, rate, truncate(amount*rate, 2), 'sell')
        if accepted:
            logging.info(f"Sell order at {rate} was placed at {datetime.now()}")
        return accepted

    def total_amt(self) -> float:
        """ Reporting function for total amount of asset currently held.

        Can be used for active/live instances to show performance,
        or to determine fitness during backtesting.

        Returns:
            A local estimated sum of order amounts.
        """
        if self.orders.empty:
            return self.amount
        else:
            return self.orders['amt'].sum() + self.amount

    def total_fiat(self) -> float:
        """ Reporting function for total fiat accrued/held.

        Can be used for active/live instances to show performance,
        or to determine fitness during backtesting.

        Returns:
            A local estimated sum of order prices.
        """
        if self.orders.empty:
            return self.starting
        else:
            return self.orders['price'].sum() + self.starting

    @abstractmethod
    def is_profitable(self, amount: float, rate: float, side: str) -> bool:
        """
        Determine if the given trade is profitable or not.

        Args:
            amount: amount of asset to be traded
            rate: rate of exchange
            side: buy or sell

        Returns:
            Determination whether trade should be executed is binary. It is either profitable or not.
        """
        return NotImplemented

    @staticmethod
    @abstractmethod
    def develop_signals(data: pd.DataFrame) -> pd.DataFrame:
        """ Use available data to update indicators.

        Args:
            data: available ticker data.

        Returns:
            Indicator data
        """
        return NotImplemented

    @staticmethod
    @abstractmethod
    def determine_position(data: pd.DataFrame, point: pd.Timestamp) -> Union[Tuple[str, str],
                                                                             Tuple[bool, bool]]:
        return NotImplemented
