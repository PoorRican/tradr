import logging
from math import isnan
import pandas as pd
from typing import Union, List, ClassVar

from analysis.trend import STRONG_THRESHOLD
from models import Trade
from models.indicators import *
from strategies.OscillationMixin import OscillationMixin
from primitives import Side, TrendDirection, MarketTrend




class ThreeProngAlt(OscillationMixin):
    """ Alternating high-freq strategy that bases decisions on 3 indicators: StochRSI, BB, MACD.
    """
    __name__ = 'ThreeProngAlt'

    def __init__(self, **kwargs):
        """

        Args:
            threshold:
                Minimum threshold (in currency amount) that a sale cost must exceed to deem
                a trade as profitable. This threshold should be high enough to make a sale worth
                the effort, but low enough so that trades can be executed. This should not be
                reflective of any market fees; trade fees are already incorporated into calculation.
            *args: Positional arguments to pass to `Strategy.__init__`
            **kwargs: Keyword arguments to pass to `Strategy.__init__`
        """
        _indicators: List = [BBANDSRow(), MACDRow(), STOCHRSIRow()]
        super().__init__(indicators=_indicators, **kwargs)

    def _calc_rate(self, extrema: pd.Timestamp, side: Side) -> float:
        """
        Rate is calculated by open, close, and high or low price.

        When buying, the third value is market high, when selling, market low price is used
        as the third value for averaging. Averaging this third value should help trade execute
        by offering a competitive price against price movements. Since all trades must pass by
        `is_profitable`, a trade won't get posted unless a minimum gain is guaranteed.

        Args:
            extrema: index that triggered trade
            side: type of order: buy/sell

        Returns:
            rate to use for trade

        Todo:
            - Integrate weights, so that extreme values do not skew
            - Incorporate orderbook into average
        """
        if side == Side.BUY:
            third = 'high'
        else:
            third = 'low'

        return self.candles.loc[extrema][['open', 'close', third]].mean()

    def false_positive_sell(self, trade: Trade, last_order: Union['pd.DataFrame', 'pd.Series']) -> bool:
        """ Filter false-positive sell signals from incomplete buys.

        A false-positive occurs when a sell signal is generated, but the price of the last buy is higher than the price.
        This occurs when the sale of the previous buys compensates for the loss. In this case, no sale should be
        executed because it would be more profitable to wait until the last order becomes profitable to sell.

        Returns:
            True if a false positive has been detected;
            False is returned by default, meaning that `trade` has not been generated by a false-positive.
        """
        if last_order['side'] == Side.BUY and last_order['amt'] < trade.amt and \
                self._calc_profit(trade, last_order) < self.threshold:
            return True
        return False

    def _calc_amount(self, extrema: pd.Timestamp, side: Side, rate: float) -> float:
        """ Calculate amount based on characterization of market.

        Notes:
            The amount to be traded is based on the characterization of the market, as determined by `_trend`
            The TrendDirection can be either `UP`, `DOWN` or `CYCLE`.

            When the trend attribute of _trend is UP, the returned amount modified to sell more and buy less.
            On the other hand, when `_trend` is DOWN, the returned amount is modified to sell less and buy more.
            When `_trend` is CYCLE, the returned amount is not modified, since calculated future cannot be determined.

            When buying, unpaired trades are added accordingly. However, if the calculated total is more than the
            available assets, the function returns the available assets. Similarly, if the total calculated is more than
            the available capital, the function returns the maximum amount that can be bought with the available
            capital.

        Args:
            self : object
                instance of the class
            extrema (pd.Timestamp):
                the extrema, should be processed beforehand. Preferably inside `_determine_position()`
            side (enum Side):
                buy or sell
            rate (float):
                rate of the trade, should be calculated beforehand using _calc_rate().

        Returns:
            float : amount of the trade
        """

        if self.orders.empty:
            assert side == Side.BUY
            last_order = {'amt': 0, 'side': Side.SELL}
        else:
            last_order = self.orders.iloc[-1]

        if side == Side.SELL:
            incomplete = self._check_unpaired(rate)

            total = last_order['amt'] + incomplete['amt'].sum()

            if total > self.assets:
                return self.assets

        else:       # Side.BUY
            total = (self.available_capital / rate)

            if self.capital < (total * rate):
                return self.capital / rate
        return total

    @staticmethod
    def _incorrect_trade(trend: MarketTrend, side: Side) -> bool:
        """ Check trade alignment during strong trend.
        """
        return trend.scalar > STRONG_THRESHOLD and \
               ((trend.trend is TrendDirection.UP and side is Side.BUY) or
                (trend.trend is TrendDirection.DOWN and side is Side.SELL))

    def _is_profitable(self, trade: Trade, extrema: Union['pd.Timestamp', str] = None,
                       strength: float = None) -> bool:
        """ See if given sale is profitable by checking if gain meets or exceeds a minimum threshold.

        Incorrect trades are rejected during strong trends

        Always buy according to signals. However, when selling during a strong uptrend, threshold is exponentially
        multiplied by `MarketTrend.scalar`. This is because significantly greater profit is expected during a strong
        uptrend.
        """
        assert trade.side in (Side.BUY, Side.SELL)

        if isnan(trade.amt):
            return False

        # TODO: reimplement trend detector

        if trade.side == Side.BUY:
            return strength >= 1
        else:
            last_order = self.orders.iloc[-1]
            if self.false_positive_sell(trade, last_order):
                return False

            # handle sell
            return self._calc_profit(trade, last_order) >= self.threshold
