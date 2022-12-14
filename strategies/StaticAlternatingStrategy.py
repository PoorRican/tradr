import pandas as pd
from strategies.strategy import Strategy
from core import Market
from typing import Union, Tuple

from primitives import Side


class StaticAlternatingStrategy(Strategy):
    """ Strategy that oscillates trading the same amount of an asset.

    This strategy exists to be a baseline for judging strategy performance. Since this strategy does not employ
    any analytical calculations, comparing the `pnl()` of this strategy against another on the same dataset
    exposes algorithmic flaws or fatal errors in implementation.

    Profitability is defined as any change in price that exceeds a fixed value in fiat currency.
    The goal of this strategy is to make small trades with small, positive profit at a high frequency.
    Amount of asset to trade defined by `amount` and profit threshold is defined by `threshold`.

    Examples:
        If `amount` is 1, and `threshold` is 0.30, asset is sold when the trade of 1 unit of
        an asset will yield a profit of 0.30 units of the underlying currency.

    The only flaw in this strategy is that it does not track trends. For example, if the price is rising,
    this strategy will continue to trade while price travels uphill. If then a buy is made at the peak,
    it will not sell until the price exceeds the peak. Therefore, this strategy is not meant for
    deployment. This strategy must employ the use of signals to indicate when to buy.
    """

    name = 'static_alternating'

    def __init__(self, starting: float, amount: float, threshold: float, market: Market):
        super().__init__(market)

        self.starting = starting
        self.amount = amount
        self.threshold = threshold

    def _calc_rate(self, extrema: pd.Timestamp, side: Side) -> float:
        """
        Rate is calculated by open, close, and high or low price.

        When buying, the third value is market high, when selling, market low price is used
        as the third value for averaging.

        Args:
            extrema: index that triggered trade
            side: type of order: buy/sell

        Returns:
            rate to use for trade
        """
        if side is Side.BUY:
            third = 'high'
        elif side is Side.SELL:
            third = 'low'
        else:
            raise ValueError('Invalid side')

        return self.market.data.loc[extrema][['open', 'close', third]].mean()

    def _calc_amount(self, extrema: pd.Timestamp, side: Side) -> float:
        return self.amount

    def _is_profitable(self, amount: float, rate: float, side: Side, extrema: Union[pd.Timestamp, str] = None) -> bool:
        """ Profitability is defined as any trade where net price exceeds a threshold.

        Examples:
            Sell: for a threshold of 0.30, if the last buy trade cost 100, a profitable sell trade is any
            trade that exceeds 100.30.

            Buy: any trade that is lower than a decrease in price is executed.
            (eg: if mean of open/close goes from 100 to 99, a trade is executed if price is below 99)

        Oscillation of `side` is not enforced here. That should be accomplished by `determine_position`.

        Args:
            amount: amount of asset being traded
            rate: current price of asset
            side: type of trade: 'buy'/'sell'
            extrema: Used during backtesting
        """
        assert side in (Side.BUY, Side.SELL)
        if side == Side.SELL:
            return self._calc_profit(amount, rate) >= self.threshold
        else:
            if extrema:
                data = self.market.data.loc[:extrema]
            else:
                data = self.market.data

            # return False if there is not enough market data (this occurs during backtesting)
            if len(data) > 2:
                second, last = [data.iloc[i]['close'] for i in (-2, -1)]
                return second > last
            return False

    def _determine_position(self, extrema: pd.Timestamp = None) -> Union[Tuple[str, 'pd.Timestamp'],
                                                                         'False']:
        """ Use market data to decide to execute buy/sell.

        The algorithm attempts to see if the opposite order type is profitable.

        Args:
            extrema: Used to simulate time during backtesting.

        Returns:
            If trade should be made, returns a tuple with decision ('buy'/'sell') and extrema.

            Otherwise, `False` is returned.
        """
        if self.orders.empty:
            side = Side.BUY
        else:
            last_order = self.orders.iloc[-1]
            assert last_order.side in (Side.BUY, Side.SELL)

            if self.orders.iloc[-1].side == Side.SELL:
                side = Side.BUY
            else:
                side = Side.SELL

        if not extrema:
            extrema = self.market.data.iloc[-1].name

        rate = self._calc_rate(extrema, side)
        amount = self._calc_amount(extrema, side)
        if self._is_profitable(amount, rate, side, extrema):
            return side, extrema
        else:
            return False
