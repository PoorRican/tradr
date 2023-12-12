import pandas as pd
from os import path
from datetime import datetime
from typing import Union, List, Dict
from abc import ABC, abstractmethod
import logging
from warnings import warn

from core.MarketAPI import MarketAPI
from models import SuccessfulTrade, add_to_df, truncate, FailedTrade, FutureTrade, Trade
from primitives import Side, StoredObject, Signal


class Strategy(StoredObject, ABC):
    """ Abstract a trading strategy.

        Performs computation necessary to determine when and how much to trade. Inherited instances should
        define a profitable trade, when and how many trades of a certain type should be executed,
        and the amount of an asset to trade at a single time. Strategies should also determine a minimum amount of
        an asset to hold.

        The trading process is controlled in the following way:

            -   `process()` calls `_determine_position()` which determines what action should be taken, and either
                returns `False` or proposes a `FutureTrade`. A `FutureTrade` might already be flagged to not be executed
                and is False when cast as a boolean. No further action is taken, and it is stored in `failed_orders`.
            -   If `FutureTrade` is "truthy" when cast, and no other trade exists for a given point in time,
                either `_buy()` or `_sell()` are called, dependent on the value of `side`. The `FutureTrade` is
                passed to `_add_order()` regardless of `side`.
            -   `_add_order()` calls `market.place_order()` with a `Trade` as an argument to interact with the market.
                If order is accepted by the market, it is converted to a `SuccessfulTrade`, otherwise becomes
                `FailedTrade` and the result is passed up. Note that `process()` returns a boolean regardless of the
                outcome.

        To determine the fitness and performance of the trading strategy, reporting functions show the total amount of
        assets and fiat accrued. This can be used in active implementations as well as during backtesting.
    """
    __name__: str = 'base'
    """ Name of strategy. """

    def __init__(self, market: 'MarketAPI', freq: str, **kwargs):
        """

        Args:
            market:
                Platform to use for market data and trading.

                Instances of both `Strategy` and `Market` are interchangeable at any time and would only be hard-linked
                for when there are un-executed trades on-the-books. This leaves room for multithreading and concurrency
                between multiple instances of both `Strategy` and `Market`.

            freq:
                Operating frequency for strategy. Both trade decisions (signals derived from `indicator` and how often
                trading decisions are evaluated) will be derived from this frequency.
        """
        super().__init__(**kwargs)
        self.orders = SuccessfulTrade.container()
        """ History of attempt orders performed by this strategy. Timestamps of extrema are used as indexes.
        
        Timestamps shall be sorted in an ascending consecutive series. This is so that selecting and grouping by index
        is not impeded by unsorted indexes. Insertion shall only be accomplished by `models.trades._add_to_df()` for
        both rigidity and convenience.
        
        Notes:
            Columns and dtypes should be identical to those of `SuccessfulTrade`
        """

        self.failed_orders = FailedTrade.container()
        """ History of orders that were not accepted by the market.
        
        Could be used to:
            - Debug
            - Test performance of computational trading methods (ie: `_calc_rate`, and related)
            - Programmatically diagnose estimation (ie: trend, bull/bear power, etc)
        """

        self.market = market
        self.freq = freq

    @classmethod
    def factory(cls, market: 'MarketAPI', params: List[Dict]):
        instances = []
        # TODO: markets need to by dynamically loaded
        for i in params:
            instances.append(cls(market=market, **i))

        return instances

    @property
    def candles(self):
        return self.market.candles(self.freq)

    def _calc_profit(self, trade: Trade, last_trade: Union['pd.Series', 'pd.DataFrame'] = None) -> float:
        """ Calculates profit of a sale.

        Returned profit should not be biased in any way. Any biasing on profit should be handled by
        a higher-level method such as `is_profitable()`.
        """
        if last_trade is None:
            last_trade = self.orders.iloc[-1]

        gain = truncate(trade.cost, 2) - truncate(last_trade['cost'], 2)
        return gain - self.market.fee

    def _post_sale(self, extrema: pd.Timestamp, trade: SuccessfulTrade):
        """ Post sale processing of trade before adding to local container. """
        pass

    def _add_order(self, trade: FutureTrade) -> Union['SuccessfulTrade', 'FailedTrade']:
        """ Create and send order to market, then store in history.

        Not all orders will post, so only orders that are executed (accepted by the market) are stored.
        For the purposes of debugging, analysis, and investigation, failed orders are stored.

        Returns:
            `SuccessfulTrade` (returned from `market.place_order()`) if market accepted trade
            `False` if trade was rejected by market there was an error storing
        """
        _trade, extrema = trade.separate()

        result: Union[SuccessfulTrade, FailedTrade] = self.market.post_order(_trade)
        self._store_order(result, extrema)
        return result

    def _store_order(self, trade: Union['FutureTrade', 'FailedTrade', 'SuccessfulTrade'],
                     extrema: pd.Timestamp = None):
        """ Manage trade storage.

        Notes:
            This should be the only function that calls `add_to_df`.

        Args:
            trade:
                May be any derived class of `Trade`. However, if not `FutureTrade`, then `extrema`
                must be passed.
        """
        # `FutureTrade` is passed only when `trade` has been marked flagged to not attempt
        #   therefore it should be a failed trade
        if type(trade) is FutureTrade:
            _trade, extrema = trade.separate()
        else:
            assert extrema is not None
            _trade = trade

        if _trade:
            self._post_sale(extrema, _trade)
            add_to_df(self, 'orders', extrema, _trade)
        else:
            add_to_df(self, 'failed_orders', extrema, _trade)

    def process(self, point: pd.Timestamp = None) -> bool:
        """ Determine and execute position.

        Notes:
            This method is the main interface method for automated trading. This function called
            `_determine_position()`, which returns a `FutureTrade` if a trade has been initiated or otherwise
            returns False. If position is False, then function exits and no further action is attempted.

            If a trade has been initiated, it might have already been turned down within the function chain
            called within `_determine_position()`. Continuation of trade is determined by `FutureTrade.attempt`
            flag. If True and is not a duplicate (determined by extrema that trade is based), then trade is passed
            to `_buy()` or `_sell()` function to be executed on open market. If trade is then rejected, it is handled
            by internally by `_add_order()`. On the other hand, if `_determine_position()` returns a `FutureTrade`
            that has already been turned down, then trade is appended to `failed_orders` container. Ideally,
            `_add_order()` should return a `FutureTrade` and there should be a dedicated function which appends
            trades to `orders` or `failed_orders` containers.

        Args:
            point:
                Current position in time. Used during backtesting. During normal operation, `point` remains
                `None` and most recent candle data is used.

        Returns:
            If algorithm decided to place an order, the result of order execution is returned.
            Otherwise, `False` is returned by default.
        """
        result: Union['FutureTrade', 'False'] = self._determine_position(point)

        if result is False:
            return False
        else:
            if result:          # `FutureTrade` is True if trade is being attempted
                if result.point in self.orders.index:
                    msg = f"Attempted duplicate trade ({result.side}) for extrema {result.point}"
                    warn(msg)
                    logging.warning(msg)
                else:
                    if result.side == Side.BUY:
                        return self._buy(result)
                    else:
                        return self._sell(result)
            else:
                # `FutureTrade` is False if a trade has been initiated but will not be attempted.
                #   This occurs when trade is not profitable, so it will be logged.
                self._store_order(result.convert(), result.point)
        return False

    @abstractmethod
    def _calc_rate(self, extrema: pd.Timestamp, side: Side) -> float:
        """ Calculate rate for trade.

        This method should return the same value for given parameters, but should only be calculated once.

        Args:
            extrema: Index/timestamp which triggered trade.
            side: Type of trade. May be 'buy'/'sell'

        Returns:
            Rate to use when buying
        """
        pass

    @abstractmethod
    def _calc_amount(self, extrema: pd.Timestamp, side: Side, rate: float) -> float:
        """ Calculate amount for trade.

        This method should return the same value for given parameters, but should only be calculated once.

         Args:
            extrema:
                Index/timestamp which triggered trade.
            side:
                Type of trade. May be 'buy'/'sell'
            rate:
                Rate to use when calculating trade size

        Returns:
            Amount of asset to trade
        """
        pass

    def _buy(self, trade: FutureTrade) -> bool:
        """ Attempt to execute buy order.

        Notes:
            Called by `process` and calls `_add_order()` which sends directly to `market`. Therefore, profitability must
            be determined *before* this function is called.

        Args:
            trade:
                Proposed buy to be attempted.

        Returns:
            Outcome of order is returned:
                `true` if trade executed,
                `false` if it was not placed.
        """

        accepted: SuccessfulTrade = self._add_order(trade)
        if accepted:
            assert accepted.side == Side.BUY
            logging.info(f"Buy order at {accepted.rate} was placed at {datetime.now()}")
        return bool(accepted)

    def _sell(self, trade: FutureTrade) -> bool:
        """ Attempt to execute sell order.

        Notes:
            Called by `process` and calls `_add_order()` which sends directly to `market`. Therefore, profitability must
            be determined *before* this function is called.

        Args:
            trade:
                Proposed sale to be attempted.

        Returns:
            Outcome of order is returned:
                `true` if trade executed,
                `false` if it was not placed.
        """

        accepted: SuccessfulTrade = self._add_order(trade)
        if accepted:
            assert accepted.side == Side.SELL
            logging.info(f"Sell order at {accepted.rate} was placed at {datetime.now()}")
        return bool(accepted)

    @abstractmethod
    def _is_profitable(self, trade: Trade, extrema: Union[str, 'pd.Timestamp'] = None,
                       strength: float = None) -> bool:
        """ Determine if the given trade is profitable or not.

        This function is the final decision maker for whether an order should be attempted or not. A profitable
        sale is considered one where the total gain from a sale is higher or equal to a threshold; a profitable
        buy is considered one where the short-term buy signal is higher than 2.

        Args:
            trade:
                Proposed trade
            extrema:
                Point in time that is responsible for initiating trade
            strength:
                Perceived strength of signal (stored in `trade.side`)

        Returns:
            Determination whether trade should be executed is binary. It is either profitable or not.
        """
        pass

    def calculate_all(self):
        """ Perform all available calculations for given data at runtime.

        During backtesting or theoretical high-level features, CPU wait time is reduced by performing all recursive,
        CPU-heavy, or mathematical functions at runtime. These functions must be agnostic to each other and their order.
        Secondary functions - such as indicators - may be executed elsewhere since that data relies on the output of
        these functions. During backtesting, since this data is available at startup and remains static, there is no
        need to calculate graph data multiple times.
        """

        # Develop indicator/oscillator data
        if hasattr(self, 'indicators'):
            self.indicators.update()

        # Develop trend detector data
        if hasattr(self, 'detector'):
            self.detector.update()

    @abstractmethod
    def _determine_position(self, extrema: pd.Timestamp = None) -> Union['FutureTrade', 'False']:
        """ Determine whether buy or sell order should be executed.

        Args:
            extrema:
                Used in backtesting to simulate time. If not provided, the last available candle data is used.

        Returns:
            If a valid extrema is found, returns a `FutureTrade` which includes `side`. `FutureTrade`
            may be used in an `if/else` statement to determine if trade should be attempted or not.

            Otherwise, if no valid extrema is found, `False` is returned.
        """
        pass

    @property
    def _instance_dir(self) -> str:
        """ Returns directory to store instance specific data.

        All dataframes are individually stored in yaml format.
        """
        _dir = f"{self.__name__}_{self.market.__name__}_{self.market.symbol}"
        return path.join(self.root, _dir)

    def _propose_trade(self, signal: Signal, point: pd.Timestamp) -> Trade:
        """ Generate a potential trade from given signal and point in time.

        Notes:
            This is essentially a wrapper for both `_calc_rate()` and `_calc_amount()`. As a wrapper,
            a `Trade` ***will*** always be generated. Therefore, any logic determining if a trade should
            be executed or not should be called *beforehand*. Additionally, `point` should be calculated before
            this function. The designated function responsible is `_determine_position()`.

        Args:
            signal:
                Type of `Trade` to generate.

            point:
                Point in time. No default is provided because a definite extrema should be determined
                beforehand.
        """
        side: Side = Side(signal)
        del signal
        rate: float = self._calc_rate(point, side)
        amount: float = self._calc_amount(point, side, rate)

        return Trade(amount, rate, side)
