import logging
from math import isnan
import pandas as pd
from typing import Union, List

from analysis.trend import TrendDetector, STRONG_THRESHOLD
from models import Trade
from models.indicators import *
from strategies.OscillationMixin import OscillationMixin
from primitives import Side, TrendDirection


class ThreeProngAlt(OscillationMixin):
    """ Alternating high-freq strategy that bases decisions on 3 indicators: StochRSI, BB, MACD.

    Foresight from `TrendDetector` is incorporated into `_calc_amount()` and `_is_profitable()` is overwritten.

    A buy or sale is made based on lt/gt comparisons of all three signals, and theoretically
    seems adept at trading with an extremely volatile stock like Bitcoin. Each buy costs the same
    amount of the underlying currency (regardless of the amount of asset it purchases). When
    selling, whatever amount of asset was bought becomes sold. The intent is to maximize the
    underlying currency. This logic should defeat the issue that arises when trading a fixed amount
    of an asset, where a rapid price increase could cause the fixed amount of asset to exceed available
    capital. However, the strategy forces alternation between buying and selling of an asset.
    Using these indicators, any generated signal is sure to be profitable.

    Logic:

        ***Buys***: price must be below 50% of difference between middle and lower band,
        MACD must be negative, and StochRSI K and D must be below 20 with K < D.

        ***Sell***: price must be above 50% of difference between upper and middle band,
        MACD must be positive, and StochRSI K and D must be above 80 with K > D.

    Signal parameters (for TradingView) are all set to close price. For Bollinger Bands,
    length is 20, StdDev is 2 with 0 offset. MACD uses EMA for both averages and has a
    fast / slow length of 6 and 26 respectively, signal smoothing is 9. StochRSI uses
    K/D values of 3, with a Stochastic and RSI length of 14.
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

        self.detector = TrendDetector(self.market, threads=self.threads)
        """ Sensor-like class that detects market trends through the `characterize()` method.
        
        Calculation of indicator data should be performed by a threaded-routine managed by startup level scheduler.
        Therefore, functions like `characterize()` must be asynchronous and a lock-flag placed on container.
        """

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
        assert side in (Side.BUY, Side.SELL)
        if side == Side.BUY:
            third = 'high'
        else:
            third = 'low'

        return self.candles.loc[extrema][['open', 'close', third]].mean()

    def false_positive_sell(self, trade: Trade, last_order: Union['pd.DataFrame', 'pd.Series']) -> bool:
        """ Filter false-positive sell signals from incomplete buys.

        False-positive occurs when asset price falls lower than last buy, a sell signal is generated, and the sale
        of the incomplete buys compensates for the loss from sale of the last buy. No sale should be executed because
        it would be more profitable to wait until the last order becomes profitable to sell.

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
        _trend = self.detector.characterize(extrema)

        # default to a scalar of 1 during `CYCLE` since future cannot be determined.
        if isnan(_trend.scalar):
            _trend.scalar = 1
        _trend.scalar /= 10

        if self.orders.empty:
            assert side == Side.BUY
            last_order = {'amt': 0, 'side': Side.SELL}
        else:
            last_order = self.orders.iloc[-1]

        _more, _less = 1, 1         # these values become a percentage used to modulate returned amount.
        _more += _trend.scalar
        _less -= _trend.scalar

        if side == Side.SELL:
            incomplete = self._check_unpaired(rate)

            total = last_order['amt'] + incomplete['amt'].sum()

            # sell more during strong uptrend
            if _trend.trend is TrendDirection.UP:
                total *= _more
            # sell less during strong downtrend
            elif _trend.trend is TrendDirection.DOWN:
                total /= _less

            if total > self.assets:
                return self.assets

        # side == Side.BUY
        else:
            total = self.starting / rate

            # buy less during strong uptrend
            if _trend.trend is TrendDirection.UP:
                total /= _less
            # buy more during strong downtrend
            elif _trend.trend is TrendDirection.DOWN:
                total *= _more

            if self.capital < total * rate:
                return self.capital / rate
        return total

    @staticmethod
    def _incorrect_trade(trend, side) -> bool:
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

        _trend = self.detector.characterize(extrema)

        # prevent incorrect trades during strong trend
        if self._incorrect_trade(_trend, trade.side):
            logging.warning(f'Prevented unaligned trade during strong trend @ {extrema}')
            return False

        if trade.side == Side.BUY:
            return strength >= 1
        else:
            last_order = self.orders.iloc[-1]
            if self.false_positive_sell(trade, last_order):
                return False

            # handle sell
            if _trend.trend == TrendDirection.UP:
                _min_profit = self.threshold * _trend.scalar
            else:
                _min_profit = self.threshold
            return self._calc_profit(trade, last_order) >= _min_profit
