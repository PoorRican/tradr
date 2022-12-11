from abc import ABC
import logging
import matplotlib.pyplot as plt
from matplotlib.colors import to_rgba
import pandas as pd
from typing import NoReturn, Tuple, Union
from warnings import warn

from strategies.strategy import Strategy
from models.trades import Side, SuccessfulTrade


class FinancialsMixin(Strategy, ABC):
    """ Mixin for Strategy that encapsulates management of capital and assets held.

    Functionality mainly involves management of total amount of capital acquired/available and loose orchestration
    of trading parameters such as maximum open/incomplete trades allowed, or the amount of capital to use for a single
    trade.

    Open/incomplete buy orders are managed by the `incomplete` container. Assets are considered incomplete if assets
    acquired have not been sold yet. The number of open/incomplete buy orders is limited by `order_count`.

    Capitol and assets are tracked by `capital` and `assets` respectfully. The starting capital to use per trade is
    calculated by `starting` and its returned value shall be as the starting basis for trading in `_calc_amount()`.

    Methods:
        pnl:
            Calculate profit-and-loss statement.

            Has four modes:
                - total:
                    Includes `incomplete`.
                - immediate:
                    excludes `incomplete`. Might result in a negative number if there are open/incomplete orders.
                - growth:
    TODO:
        -   Convert `capital` and `assets` to time-series that tracks balance over time, and why values change
            (eg: user-initiated deposit, trade id)
    """
    def __init__(self, threshold: float = None, capital: float = 0, assets: float = 0, order_count: int = 4, **kwargs):
        super().__init__(**kwargs)

        self.incomplete: pd.DataFrame = pd.DataFrame(columns=['amt', 'rate', 'id'])
        """ Store for incomplete/open buy orders.
        
        `id` contains the original order ID as found in `orders` and is used when pivoting between the two tables.
        The `unpaired` method should be used when accessing rows from `orders`. Additionally, during trading,
        `_check_unpaired` is used to select incomplete orders based off of rate.
        
        The `amt` column serves as a ledger to track how much of the asset has been sold (since trade amounts are
        dynamically calculated taking into account market trends). When assets acquired by an incomplete order are sold,
        rows are removed from container by `_clean_incomplete()`, but when partially sold, amount of assets sold is
        deducted unsold amount (denoted in the `amt` column).
        """

        assert threshold > 0
        self.threshold = threshold
        """ Minimum total profit per trade in dollar amounts.
        
        Profit from all sell trades must be equal to or greater than this value.
        
        Most likely will be set to less-than 1 when capital is significantly less than exchange rate of one unit of
        asset. Since marginal changes (<5%) in price are intended to be exploited for gains, this number should be
        set accordingly dependant on asset price and supplied capital.
        """

        self._capital = pd.Series(dtype=float)
        if capital != 0:
            self._capital[pd.Timestamp.now()] = capital
        """ Simple total of available capital to use in buying assets.
        
        This number is used to determine how much fiat currency will be used to purchase assets, and the cost of any buy
        order may never exceed this sum. Reference `starting` to observe how available capital is involved in setting up
        buy orders. The value of `capital` is not used when determining profit-and-loss, as unused capital is not needed
        to be reflected in sums of order costs.
        """
        self._assets = pd.Series(dtype=float)
        if assets != 0:
            self._assets[pd.Timestamp.now()] = assets
        """ Simple total of available assets to use when selling assets.
        
        Available assets represents a ceiling for amount of asset that can be sold.
        """

        self.order_count = order_count
        """ Hard limit on maximum number of incomplete/open orders allowed.
        
        A hard limit is set to reduce exposure to risk but originally intended to manage available amount of capital
        used per transaction (this is implemented via `starting`).
        """

    @property
    def _remaining(self) -> int:
        """ Calculate the number of open/incomplete buy order slots

        No more buy orders will be authorized when the returned value becomes 0.
        """
        return self.order_count - len(self.incomplete)

    @property
    def capital(self) -> float:
        try:
            return self._capital.iloc[-1]
        except IndexError:
            return 0

    @capital.setter
    def capital(self, value: Union[float, Tuple['pd.Timestamp', float]]):
        if hasattr(value, '__iter__'):
            self._capital[value[0]] = value[1]
        else:
            self._capital[pd.Timestamp.now()] = value

    @property
    def assets(self) -> float:
        try:
            return self._assets.iloc[-1]
        except IndexError:
            return 0

    @assets.setter
    def assets(self, value: Union[float, Tuple['pd.Timestamp', float]]):
        if hasattr(value, '__iter__'):
            self._assets[value[0]] = value[1]
        else:
            self._assets[pd.Timestamp.now()] = value

    @property
    def starting(self) -> float:
        """ Amount of capital to use for a buying assets.

        Value is computed dynamically so that trades grow larger as the amount of available capital increases. A
        fraction of available capital is used instead of spending all available to protect against trading inactivity
        during a price drop.

        Notes:
            Since the returned value is used when buying assets, a warning is raised and logged when `_remaining` equals
            to 0.


        Returns:
                Amount of capital to use when beginning to calculate final trade amount.
        """
        if self._remaining == 0:
            msg = '`starting` accessed while buying is restricted (`_remaining` == 0)'
            warn(msg)
            logging.warning(msg)

        return self.capital / self.order_count

    def pnl(self) -> float:
        # TODO: `unpaired_buys` need to be reflected. Either buy including current price, or excluding and mentioning
        #       the number of unpaired orders and unrealized gain.
        buy_orders = self.orders[self.orders['side'] == Side.BUY]
        sell_orders = self.orders[self.orders['side'] == Side.SELL]

        buy_cost = buy_orders['cost'].sum()
        sell_cost = sell_orders['cost'].sum()
        return sell_cost - buy_cost

    def _adjust_capital(self, trade: SuccessfulTrade, extrema: pd.Timestamp = None) -> NoReturn:
        """ Increase available capital when assets are sold, and decrease when assets are bought.
        """
        assert trade.side in (Side.BUY, Side.SELL)
        if extrema is None:
            extrema = pd.Timestamp.now()

        if trade.side is Side.BUY:
            _capital = self.capital - trade.cost

            if _capital < 0:
                msg = "Accumulated capital has been set to a negative value"
                warn(msg)
                logging.warning(msg)
        else:
            _capital = self.capital + trade.cost

        self._capital[extrema] = _capital

    def _adjust_assets(self, trade: SuccessfulTrade, extrema: pd.Timestamp = None) -> NoReturn:
        """ Increase available assets when bought, and decrease when sold.
        """
        assert trade.side in (Side.BUY, Side.SELL)
        if extrema is None:
            extrema = pd.Timestamp.now()

        if trade.side is Side.BUY:
            _assets = self.assets + trade.amt
        else:
            _assets = self.assets - trade.amt

            if _assets < 0:
                msg = "Assets amount has been set to a negative value"
                warn(msg)
                logging.warning(msg)

        self._assets[extrema] = _assets

    def unpaired(self) -> pd.DataFrame:
        """ Select of unpaired orders by cross-referencing `incomplete`.

        This retrieves orders data whose assets have not been sold yet. Used during calculation of pnl, and during
        normal trading operation to attempt sale of unsold assets and to clear `incomplete` when assets are sold.

        Returns
            Original dataframe row from `orders` of orders whose IDs are found in `incomplete`
        """
        return self.orders[self.orders['id'].isin(self.incomplete['id'].values)]

    def _check_unpaired(self, rate: float, original: bool = True) -> pd.DataFrame:
        """ Get any unpaired orders that can be sold at a profit.

        Args:
            rate:
                Select rows whose rate is <= given `rate`. Since buy orders should be sold at a rate lower than the
                buy price, rate is used to select incomplete order rows when determining amount of available assets
                should be sold.
            original:
                Boolean flag used to return from `orders` or `incomplete`. This is because if the amount of asset sold
                is less than the incomplete order, `incomplete` serves as a ledger ta track how much of the asset has
                yet to be sold.

        Returns:
            Dataframe row (from `orders` or `incomplete` depending on `original` flag) whose rate falls below `rate`
        """
        if original:
            unpaired = self.unpaired()
        else:
            unpaired = self.incomplete
        return unpaired[unpaired['rate'] <= rate]

    def unrealized_gain(self) -> float:
        """ Calculate max potential gain if all unpaired orders were sold at the highest rate.

        Returns:
            Value of unsold assets sold at most expensive price.
        """
        unpaired = self.unpaired()

        if unpaired.empty:
            return 0

        _last_order = self.orders.tail(1)
        # NOTE: indexing `_last_order` does not return literals but instead returns rows and columns
        if Side.BUY in _last_order['side'] and not _last_order['id'].isin(unpaired['id']).max():
            unpaired = pd.concat([unpaired, _last_order], ignore_index=True)
        highest = max(unpaired['rate'])
        return unpaired['amt'].sum() * highest

    def _post_sale(self, extrema: pd.Timestamp, trade: SuccessfulTrade) -> NoReturn:
        """ Handle mundane accounting functions for when a sale completes.

        After a sale, sold assets are dropped or deducted from `incomplete` container, and then
        `capital` and `assets` values are adjusted.
        """
        self._clean_incomplete(trade)
        self._adjust_capital(trade, extrema)
        self._adjust_assets(trade, extrema)

    def _handle_inactive(self, row: pd.Series) -> NoReturn:
        """ Add incomplete order to `incomplete` container.

        Inactivity is defined by `OscillatingStrategy.timeout` and is checked during
        `OscillatingStrategy._oscillation()`.

        Args:
            row:
                row containing "inactive" order. Must contain `id`, `rate`, and `amt` columns.
        """
        assert type(row) is pd.Series
        assert row['side'] == Side.BUY

        if row['id'] in self.incomplete['id'].values:
            warn('Adding duplicate id found in `incomplete`')

        _row = pd.DataFrame([[row['amt'], row['rate'], row['id']]], columns=['amt', 'rate', 'id'])
        self.incomplete = pd.concat([self.incomplete, _row],
                                    ignore_index=True)

    def _clean_incomplete(self, trade: SuccessfulTrade):
        """ Drop rows from `incomplete` when assets are sold.

        The assumption is made during all sales that any orders returned by `_check_unpaired()` are
        attempted to be sold. Trend-dependant manipulation reduces order size in which case rows are
        not dropped, but instead amount of assets sold are deducted by `_deduct_sold()`.

        Notes:
            This function should be the primary interface through which to remove from or manipulate
            the `incomplete` container. If all assets are not sold by `trade`, `_deduct_sold()` is
            automatically called.
        """
        if trade.side == Side.SELL:
            unpaired = self._check_unpaired(trade.rate, original=False)
            if not unpaired.empty:
                # if all assets are sold, drop all rows
                if trade.amt >= unpaired['amt'].sum():
                    matching = self.incomplete['id'].isin(unpaired['id'].values)
                    indices = self.incomplete[matching].index
                    self.incomplete.drop(index=indices, inplace=True)
                # otherwise deduct however much was sold
                else:
                    self._deduct_sold(trade, unpaired)

    def _deduct_sold(self, trade: SuccessfulTrade, unpaired: pd.DataFrame) -> NoReturn:
        """ Deduct the amount of asset sold from incomplete order storage.

        The amount of asset acquired from the last buy order is accounted for and is not included in the deduction from
        `incomplete`. When assets from unpaired orders have been completely sold, they are dropped from the dataframe.

        Notes:
            This function should not be used directly and should only be called within
            `_clean_incomplete()`.

            When trend is modulating asset trade amounts, not all assets are sold. For example, during a strong
            downtrend, a purchase of one unit does not result in the sale of one unit. The `incomplete` store serves as
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
                self.incomplete.loc[self.incomplete['id'] == order['id'], 'amt'] = _remaining

        self.incomplete.drop(index=_drop, inplace=True)

    def plot(self, size: int = 5000):
        """ Plot trade enter and exit points as an overlay to market data.

        Args:
            size:
                Scalar multiplier for marker size. Since marker size corresponds to amount of assets traded, this should
                be larger for assets of larger values as for a fixed price asset value and amount are inversely
                proportional (and therefore may be less than one).

        TODO:
            - subgraph which shows assets/capital
            - dynamically calculate scalar for marker size
        """
        o = self.orders
        sells = o[o['side'] == -1]
        buys = o[o['side'] == 1]
        plt.figure(figsize=[50, 25], dpi=250)
        plt.plot(self.candles.index, self.candles['close'], color=to_rgba('red', 0.5))
        plt.scatter(buys.index, buys['rate'], buys['amt'] * size, marker="^", color=to_rgba('green', .8))
        plt.scatter(sells.index, sells['rate'], sells['amt'] * size, marker="v", color=to_rgba('blue', 1))
