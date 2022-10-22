import pandas as pd
from os import path
from datetime import datetime
from typing import Tuple, Union
from abc import ABC, abstractmethod
import logging

from models.trades import Trade, SuccessfulTrade, add_to_df, truncate
from markets.Market import Market
from models.data import DATA_ROOT


class Strategy(ABC):
    """ Abstracted a trading strategy.

    Performs computation necessary to determine when and how much to trade. Inherited instances should
    define a profitable trade, when and how many trades of a certain type should be executed,
    and the amount of an asset to trade at a single time. Strategies should also determine a minimum amount of
    an asset to hold.

    To determine the fitness and performance of the trading strategy, reporting functions show the total amount of
    assets and fiat accrued. This can be used in active implementations as well as during backtesting.

    The buy process happens in the following way:
        - `process()` calls `determine_position` which determines what action should be taken, called `position`
        - if `position` is "truthy", it contains values `side` (type of action to be taken), and `extrema` (the point
            in the time-series to which an action is being taken for).
        - If an action has already not been executed regarding the given `extrema`, either `_buy()` or `_sell()`
            are called, dependent on the value of `side`.
        - Both `_buy()` and `_sell()` call `_calc_rate`, `_calc_amount` and `add_order`.
        - `add_order()` calls `market.place_order()` , who communicates with market API

    Attributes
        orders: history of orders performed by this strategy. Timestamps of extrema are used as indexes.
        starting: starting amount of fiat currency
        market: reference to `Market` object
    """
    name = 'base'

    def __init__(self, starting_fiat: float, market: Market):
        self.orders = SuccessfulTrade.container()
        self.failed_orders = Trade.container()

        self.starting = starting_fiat

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

    def _add_order(self, extrema: pd.Timestamp, side: str) -> Union['SuccessfulTrade', 'False']:
        """ Create and send order to market, then store in history.

        The assumption is that not all orders will post, so only orders that are executed (accepted by the
        market) are stored. However, for the purposes of debugging, failed orders are stored.

        Args:
            extrema: timestamp at which an extrema occurred.
            side: type of trade to execute

        Returns:
            `SuccessfulTrade` (returned from `market.place_order()`) if market accepted trade
            `False` if trade was rejected by market there was an error storing
        """
        amount = self._calc_amount(extrema, side)
        rate = self._calc_rate(extrema, side)
        trade = Trade(amount, rate, side)

        response = self.market.place_order(trade)
        if response:
            add_to_df(self, 'orders', extrema, response)
            return response
        else:
            add_to_df(self, 'failed_orders', extrema, trade)
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

        Notes:
            Called by `process` and calls `_add_order()` which sends directly to `market`. Therefore, profitability must
            be determined *before* this function is called.

        Args:
            extrema:
                Timestamp at which extrema occurred. This prevents multiple orders being placed for the same extrema
                (local min or max).

        Returns:
            Outcome of order is returned:
                `true` if trade executed,
                `false` if it was not placed.
        """

        accepted = self._add_order(extrema, 'buy')
        if accepted:
            logging.info(f"Buy order at {accepted.rate} was placed at {datetime.now()}")
        return bool(accepted)

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

        accepted = self._add_order(extrema, 'sell')
        if accepted:
            logging.info(f"Sell order at {accepted.rate} was placed at {datetime.now()}")
        return bool(accepted)

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
        # TODO: `unpaired_buys` need to be reflected. Either buy including current price, or excluding and mentioning
        #       the number of unpaired orders and unrealized gain.
        buy_orders = self.orders[self.orders['side'] == 'buy']
        sell_orders = self.orders[self.orders['side'] == 'sell']

        buy_cost = buy_orders['cost'].sum()
        sell_cost = sell_orders['cost'].sum()
        return (sell_cost - buy_cost) - self.starting
