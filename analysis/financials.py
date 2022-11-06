from abc import ABC
import pandas as pd
from typing import NoReturn

from strategies import Strategy
from models.trades import Side, SuccessfulTrade


class FinancialsMixin(Strategy, ABC):
    """ Mixin for Strategy that encapsulates management of capitol and assets held.

    Functionality mainly involves management of total amount of capitol acquired/available and loose orchestration
    of trading parameters such as maximum open/incomplete trades allowed, or the amount of capitol to use for a single
    trade.

    Open/incomplete buy orders are managed by the `incomplete` container. Assets are considered incomplete if assets
    acquired have not been sold yet. The number of open/incomplete buy orders is limited by `order_count`.

    Capitol and assets are tracked by `capitol` and `assets` respectfully. The starting capitol to use per trade is
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
        -   Convert `capitol` and `assets` to time-series that tracks balance over time, and why values change
            (eg: user-initiated deposit, trade id)
    """
    def __init__(self, *args, threshold: float, capitol: float, assets: float, order_count: int = 4, **kwargs):
        super().__init__(*args, **kwargs)

        self.incomplete: pd.Series = pd.Series(name='incomplete/open buy orders', dtype='int32')

        assert threshold > 0
        self.threshold = threshold

        self.capitol = capitol
        self.assets = assets

        self.order_count = order_count

    @property
    def _remaining(self) -> int:
        """ Calculate the number of open/incomplete buy order slots

        No more buy orders will be authorized when the returned value becomes 0.
        """
        return self.order_count - len(self.incomplete)

    @property
    def starting(self) -> float:
        """ Amount of capitol to use for a trade.

        This fraction is dynamically calculated due to the assumption that order might remain incomplete. Flexibility
        is desired when placing a new order.
        """
        return self.capitol / self._remaining

    def pnl(self) -> float:
        # TODO: `unpaired_buys` need to be reflected. Either buy including current price, or excluding and mentioning
        #       the number of unpaired orders and unrealized gain.
        buy_orders = self.orders[self.orders['side'] == Side.BUY]
        sell_orders = self.orders[self.orders['side'] == Side.SELL]

        buy_cost = buy_orders['cost'].sum()
        sell_cost = sell_orders['cost'].sum()
        return (sell_cost - buy_cost) - self.starting

    def _adjust_capitol(self, trade: SuccessfulTrade) -> NoReturn:
        """ Add or subtract from `capitol`.

        Available capitol increases when assets are sold, and decreases when assets are bought.
        """
        assert trade.side in (Side.BUY, Side.SELL)

        if trade.side is Side.BUY:
            self.capitol -= trade.cost
        else:
            self.capitol += trade.cost

    def _adjust_assets(self, trade: SuccessfulTrade) -> NoReturn:
        """ Add or subtract from `assets`

        Available assets increase when assets are bought, and decrease when assets are sold.
        """
        assert trade.side in (Side.BUY, Side.SELL)

        if trade.side is Side.BUY:
            self.assets += trade.amt
        else:
            self.assets -= trade.amt

    def get_unpaired_orders(self) -> pd.DataFrame:
        """ Select of unpaired orders by cross-referencing `unpaired_buys` """
        return self.orders[self.orders['id'].isin(self.incomplete.values)]

    def _check_unpaired(self, rate: float):
        """ Get any unpaired orders that can be sold at a profit. """
        unpaired = self.get_unpaired_orders()
        return unpaired[unpaired['rate'] <= rate]

    def unrealized_gain(self) -> float:
        """ Calculate max potential gain if all unpaired orders were sold at the highest rate.

        Returns:
            Value of unsold assets sold at most expensive price.
        """
        unpaired = self.get_unpaired_orders()
        highest = max(unpaired['rate'])
        return unpaired['amt'].sum() * highest

    def _post_sale(self, trade: SuccessfulTrade):
        """ Clean `incomplete` after successful sale.
        """
        if trade.side == Side.SELL:
            unpaired = self._check_unpaired(trade.rate)
            if not unpaired.empty:
                matching = self.incomplete.isin(unpaired['id'].values)
                indices = self.incomplete.loc[matching].index
                self.incomplete.drop(index=indices, inplace=True)

    def _add_incomplete(self, row: pd.DataFrame):
        """ Add incomplete order.

        Args:
            row:
                row containing duplicate order. Must contain `id` column.
        """
        assert len(row) == 1

        _row = pd.Series([row['id']])
        self.incomplete = pd.concat([self.incomplete, row],
                                    ignore_index=True)
