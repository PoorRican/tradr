import logging
import pandas as pd
from typing import NoReturn, Tuple, Union
from warnings import warn

from misc import TZ
from models import SuccessfulTrade, FailedTrade
from primitives import Side


class OrderHandler(object):
    """ Mixin for Strategy that encapsulates management of capital and assets held.

    Functionality mainly involves management of total amount of capital acquired/available and loose orchestration
    of trading parameters such as maximum open/unsold trades allowed, or the amount of capital to use for a single
    trade.

    Open/unsold buy orders are managed by the `unsold` container. Assets are considered unsold if assets
    acquired have not been sold yet. The number of open/unsold buy orders is limited by `order_limit`.

    Capitol and assets are tracked by `capital` and `assets` respectfully. The starting capital to use per trade is
    calculated by `starting` and its returned value shall be as the starting basis for trading in `_calc_amount()`.

    Methods:
        pnl:
            Calculate profit-and-loss statement.

            Has four modes:
                - total:
                    Includes `unsold`.
                - immediate:
                    excludes `unsold`. Might result in a negative number if there are open/unsold orders.
                - growth:
    TODO:
        -   Convert `capital` and `assets` to time-series that tracks balance over time, and why values change
            (eg: user-initiated deposit, trade id)
    """
    orders: pd.DataFrame
    """ History of attempt orders performed by this strategy. Timestamps of extrema are used as indexes.
    
    Timestamps shall be sorted in an ascending consecutive series. This is so that selecting and grouping by index
    is not impeded by unsorted indexes. Insertion shall only be accomplished by `models.trades._add_to_df()` for
    both rigidity and convenience.
    
    Notes:
        Columns and dtypes should be identical to those of `SuccessfulTrade`
    """

    failed_orders: pd.DataFrame
    """ History of orders that were not accepted by the market.

    Could be used to:
        - Debug
        - Test performance of computational trading methods (ie: `_calc_rate`, and related)
        - Programmatically diagnose estimation (ie: trend, bull/bear power, etc)
    """

    unsold: pd.DataFrame
    """ Store for unsold/open buy orders.

    `id` contains the original order ID as found in `orders` and is used when pivoting between the two tables.
    The `unpaired` method should be used when accessing rows from `orders`. Additionally, during trading,
    `_check_unpaired` is used to select unsold orders based off of rate.

    The `amt` column serves as a ledger to track how much of the asset has been sold (since trade amounts are
    dynamically calculated taking into account market trends). When assets acquired by an unsold order are sold,
    rows are removed from container by `_clean_incomplete()`, but when partially sold, amount of assets sold is
    deducted unsold amount (denoted in the `amt` column).
    """

    threshold: float
    """ Minimum total profit per trade in dollar amounts.

    Profit from all sell trades must be equal to or greater than this value.
    
    Since marginal changes in price are intended to be exploited for gains, this number should be set accordingly
    in proportion to the asset price and supplied capital. This will most likely be set to < 1.0.
    """

    _capital: pd.Series
    """ Simple total of available capital to use in buying assets.

    This number is used to determine how much fiat currency will be used to purchase assets, and the cost of any buy
    order may never exceed this sum.
    
    Reference `starting` to observe how available capital is involved in setting up buy orders.
    
    The value of `capital` is not used when determining profit-and-loss, as unused capital is not needed to be reflected
    in sums of order costs.
    """

    _assets: pd.Series
    """ Simple total of available assets to use when selling assets.

    Available assets represents a ceiling for amount of asset that can be sold.
    """

    order_limit: int
    """ Hard limit on maximum number of unsold/open orders allowed.

    A hard limit is set to reduce exposure to risk but originally intended to manage available amount of capital
    used per transaction (this is implemented via `starting`).
    """

    def __init__(self, threshold: float, capital: float = 0, assets: float = 0, order_limit: int = 4, candles: pd.DataFrame = None):
        self.unsold: pd.DataFrame = pd.DataFrame(columns=['amt', 'rate', 'id'])
        self.orders = SuccessfulTrade.container()
        self.failed_orders = FailedTrade.container()

        assert threshold > 0
        self.threshold = threshold

        # set starting date for timeseries containers
        if candles is not None and len(candles):
            start = candles.index[0]
        else:
            start = pd.Timestamp.now(tz=TZ)

        self._capital = pd.Series(dtype=float)
        self._capital[start] = capital

        self._assets = pd.Series(dtype=float)
        self._assets[start] = assets

        self.order_limit = order_limit

    @property
    def remaining_buy_orders(self) -> int:
        """ The number of open/unsold orders allowed.

        When this number is 0, no more buy orders are allowed.
        """
        return self.order_limit - len(self.unsold)

    def _timeseries_setter(self, value: Union[float, Tuple['pd.Timestamp', float]], attr: str):
        """ Append a value to a timeseries container.

        This is used to append data to `capital` and `assets` containers.

        Args:
            value:
                Value to append. If a tuple is given, the first element is used as the index and the second as the
                value. Otherwise, the current time is used as the index.
            attr:
                Attribute name of timeseries container to append to.
        """
        assert hasattr(self, attr)

        if hasattr(value, '__iter__'):
            point, val = value

        else:
            val = value
            point = pd.Timestamp.now(tz=TZ)

        getattr(self, attr)[point] = val

    @property
    def capital(self) -> float:
        try:
            return self._capital.iloc[-1]
        except IndexError:
            return 0

    @capital.setter
    def capital(self, value: Union[float, Tuple['pd.Timestamp', float]]):
        self._timeseries_setter(value, '_capital')

    @property
    def capital_ts(self) -> pd.Series:
        return self._capital

    @property
    def assets(self) -> float:
        try:
            return self._assets.iloc[-1]
        except IndexError:
            return 0

    @assets.setter
    def assets(self, value: Union[float, Tuple['pd.Timestamp', float]]):
        self._timeseries_setter(value, '_assets')

    @property
    def assets_ts(self) -> pd.Series:
        return self._assets

    @property
    def available_capital(self) -> float:
        """ Amount of capital available for a buying assets.

        Value is computed dynamically so that trades grow larger as the amount of available capital increases. A
        fraction of available capital is used instead of spending all available to protect against trading inactivity
        during a price drop.

        Notes:
            Since the returned value is used when buying assets, a warning is raised and logged when `_remaining` equals
            to 0.


        Returns:
                Amount of capital to use when beginning to calculate final trade amount.
        """
        if self.remaining_buy_orders == 0:
            msg = '`starting` accessed while buying is restricted (`_remaining` == 0)'
            warn(msg)
            logging.warning(msg)

        return self.capital / self.order_limit

    def pnl(self) -> float:
        buy_orders = self.orders[self.orders['side'] == Side.BUY]
        sell_orders = self.orders[self.orders['side'] == Side.SELL]

        buy_cost = buy_orders['cost'].sum()
        sell_cost = sell_orders['cost'].sum()

        realized_pnl = sell_cost - buy_cost
        unrealized = self.unrealized_gain()

        return realized_pnl + unrealized

    def adjust_capital(self, trade: SuccessfulTrade, extrema: pd.Timestamp = None) -> NoReturn:
        """ Increase available capital when assets are sold, and decrease when assets are bought.
        """
        assert trade.side in (Side.BUY, Side.SELL)
        if extrema is None:
            extrema = pd.Timestamp.now(tz=TZ)

        if trade.side is Side.BUY:
            _capital = self.capital - trade.cost

            if _capital < 0:
                msg = "Accumulated capital has been set to a negative value"
                warn(msg)
                logging.warning(msg)
        else:
            _capital = self.capital + trade.cost

        self._capital[extrema] = _capital

    def adjust_assets(self, trade: SuccessfulTrade, extrema: pd.Timestamp = None) -> NoReturn:
        """ Increase available assets when bought, and decrease when sold.
        """
        assert trade.side in (Side.BUY, Side.SELL)
        if extrema is None:
            extrema = pd.Timestamp.now(tz=TZ)

        if trade.side is Side.BUY:
            _assets = self.assets + trade.amt
        else:
            _assets = self.assets - trade.amt

            if _assets < 0:
                msg = "Assets amount has been set to a negative value"
                warn(msg)
                logging.warning(msg)

        self._assets[extrema] = _assets

    def unsold_buy_orders(self) -> pd.DataFrame:
        """ Select of unsold buy orders by cross-referencing `unsold`.

        This retrieves orders data whose assets have not been sold yet. Used during calculation of pnl, and during
        normal trading operation to attempt sale of unsold assets and to clear `unsold` when assets are sold.

        Returns
            Original dataframe row from `orders` of orders whose IDs are found in `unsold`
        """
        return self.orders[self.orders['id'].isin(self.unsold['id'].values)]

    def get_sellable_orders(self, rate: float, original: bool = True) -> pd.DataFrame:
        """ Get any unsold buy orders that can be sold at a profit.

        Args:
            rate:
                Select rows whose rate is <= given `rate`. Since buy orders should be sold at a rate lower than the
                buy price, rate is used to select unsold order rows when determining amount of available assets
                should be sold.
            original:
                Boolean flag used to return from `orders` or `unsold`. This is because if the amount of asset sold
                is less than the unsold order, `unsold` serves as a ledger ta track how much of the asset has
                yet to be sold.

        Returns:
            Dataframe row (from `orders` or `unsold` depending on `original` flag) whose rate falls below `rate`
        """
        if original:
            unpaired = self.unsold_buy_orders()
        else:
            unpaired = self.unsold
        return unpaired[unpaired['rate'] <= rate]

    def unrealized_gain(self) -> float:
        """ Calculate max potential gain if all unsold buy orders were sold at the highest rate.

        Returns:
            Value of unsold assets sold at most expensive price.
        """
        unpaired = self.unsold_buy_orders()

        if unpaired.empty:
            return 0

        # account for last buy order if it is not in `unpaired`
        _last_order = self.orders.iloc[-1]
        if _last_order['side'] == Side.BUY and not _last_order['id'] in unpaired['id']:
            unpaired = pd.concat([pd.DataFrame(_last_order).T, unpaired], axis='index')
        highest = max(unpaired['rate'])
        return unpaired['amt'].sum() * highest


    def handle_inactive(self, row: pd.Series) -> NoReturn:
        """ Add unsold order to `unsold` container.

        Inactivity is defined by `IndicatorStrategy.timeout` and is checked during
        `IndicatorStrategy._oscillation()`.

        Args:
            row:
                row containing "inactive" order. Must contain `id`, `rate`, and `amt` columns.
        """
        assert type(row) is pd.Series
        assert row['side'] == Side.BUY

        if row['id'] in self.unsold['id'].values:
            warn('Adding duplicate id found in `unsold`')

        _row = pd.DataFrame([[row['amt'], row['rate'], row['id']]], columns=['amt', 'rate', 'id'])
        self.unsold = pd.concat([self.unsold, _row],
                                ignore_index=True)

    def clean_incomplete(self, trade: SuccessfulTrade):
        """ Drop rows from `unsold` when assets are sold.

        The assumption is made during all sales that any orders returned by `get_sellable_orders()` are
        attempted to be sold. Trend-dependant manipulation reduces order size in which case rows are
        not dropped, but instead amount of assets sold are deducted by `_deduct_sold()`.

        Notes:
            This function should be the primary interface through which to remove from or manipulate
            the `unsold` container. If all assets are not sold by `trade`, `_deduct_sold()` is
            automatically called.
        """
        if trade.side == Side.SELL:
            unpaired = self.get_sellable_orders(trade.rate, original=False)
            if not unpaired.empty:
                # if all assets are sold, drop all rows
                if trade.amt >= unpaired['amt'].sum():
                    matching = self.unsold['id'].isin(unpaired['id'].values)
                    indices = self.unsold[matching].index
                    self.unsold.drop(index=indices, inplace=True)
                # otherwise deduct however much was sold
                else:
                    self._deduct_sold(trade, unpaired)

    def _deduct_sold(self, trade: SuccessfulTrade, unpaired: pd.DataFrame) -> NoReturn:
        """ Deduct the amount of asset sold from unsold order storage.

        The amount of asset acquired from the last buy order is accounted for and is not included in the deduction from
        `unsold`. When assets from unpaired orders have been completely sold, they are dropped from the dataframe.

        Notes:
            This function should not be used directly and should only be called within
            `_clean_incomplete()`.

            When trend is modulating asset trade amounts, not all assets are sold. For example, during a strong
            downtrend, a purchase of one unit does not result in the sale of one unit. The `unsold` store serves as
            a ledger of the amount of assets sold. In the same scenario, those assets which have been bought during a
            strong downtrend should be sold during a strong uptrend. Since marginal trades might not be possible during
            a strong downtrend, profit may be recuperated by this strong sale.

        Args:
            trade:
                Executed trade data

            unpaired:
                Rows from `orders` referring to unpaired buy orders.

        TODO:
            -   Track related orders
        """
        assert trade.side == Side.SELL

        # account for assets acquired from previous buy
        last_buy = self.orders[self.orders['side'] == Side.BUY].iloc[-1]
        excess = trade.amt - last_buy['amt']

        # deduct excess from unpaired orders
        _drop = []              # rows to drop
        for index, order in unpaired.iterrows():
            if excess >= order['amt']:
                _drop.append(index)
                excess -= order['amt']
            else:
                # update amount remaining.
                _remaining = order['amt'] - excess
                self.unsold.loc[self.unsold['id'] == order['id'], 'amt'] = _remaining

        self.unsold.drop(index=_drop, inplace=True)
