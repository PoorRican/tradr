import pandas as pd
from Strategy import Strategy
from MarketAPI import Market
from typing import Union, Tuple


class StaticAlternatingStrategy(Strategy):
    """ Strategy that oscillates trading the same amount of an asset.

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

    def __init__(self, starting_fiat: float, amount: float, threshold: float, market: Market):
        super().__init__(starting_fiat, market)

        self.amount = amount
        self.threshold = threshold

    def calc_rate(self, extrema: pd.Timestamp, side: str) -> float:
        """
        Rate is calculated by open, close, and high or low price.

        When buying, the third value is market low, when selling, market high price is used
        as the third value for averaging.

        Args:
            extrema: index that triggered trade
            side: type of order: buy/sell

        Returns:
            rate to use for trade
        """
        if side == 'buy':
            third = 'low'
        elif side == 'sell':
            third = 'high'
        else:
            raise ValueError('Invalid value for `side`')

        return self.market.data.iloc[extrema][['open', 'close', third]].mean()

    def calc_amount(self, extrema: pd.Timestamp, side: str) -> float:
        return self.amount

    def calc_profit(self, amount: float, rate: float, side: str) -> float:
        """ Calculates profit of a trade.

        Since trades consist of fixed asset amount, just compare cost.
        """
        last_trade = self.orders.iloc[-1]
        assert last_trade['side'] != side

        gain = abs(last_trade['price'] - (amount * rate))
        return gain - self.market.calc_fee()

    def is_profitable(self, amount: float, rate: float, side: str) -> bool:
        """ Profitability is defined as any trade where net price exceeds a threshold.

        Examples:
            Sell: for a threshold of 0.30, if the last buy trade cost 100, a profitable sell trade is any
            trade that exceeds 100.30.

            Buy: any trade that is lower than the last two sequential decreases in price is executed.
            (eg: if mean of open/close goes from 100 to 99, then 99 to 98,
             a trade is executed if price is below 98)

        Oscillation of `side` is not enforced here. That should be accomplished by `determine_position`.

        Args:
            amount: amount of asset being traded
            rate: current price of asset
            side: type of trade: 'buy'/'sell'
        """
        assert side in ('buy', 'sell')
        if side == 'sell':
            return self.calc_profit(amount, rate, side) >= self.threshold
        else:
            third, second, last = [self.market.data.iloc[i]['rate'] for i in (-3, -2, -1)]
            return third > second > last

    def develop_signals(self) -> pd.DataFrame:
        """ Placeholder function

        Since this strategy does not employ any signals, this is a dummy function
        to overwrite the underlying `abstractmethod`.
        """
        return NotImplemented

    def determine_position(self, point: pd.Timestamp) -> Union[Tuple[str, 'pd.Timestamp'],
                                                               False]:
        """ Use market data to decide to execute buy/sell.

        The algorithm attempts to see if the opposite order type is profitable.

        Args:
            point: Used to simulate time during backtesting.

        Returns:
            If trade should be made, returns a tuple with decision ('buy'/'sell') and extrema.

            Otherwise, `False` is returned.
        """
        if self.orders.empty:
            side = 'buy'
        else:
            assert self.orders[-1].side in ('buy', 'sell')

            if self.orders[-1].side == 'sell':
                side = 'buy'
            else:
                side = 'sell'

        extrema = self.market.data.iloc[-1].index
        rate = self.calc_rate(extrema, side)
        amount = self.calc_amount(extrema, side)
        if self.is_profitable(amount, rate, side):
            return side, extrema
        else:
            return False
