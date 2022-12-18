import logging
from math import ceil
import pandas as pd
from typing import Sequence, Union

from analysis.trend import TrendDetector
from models.indicators import *
from strategies.OscillatingStrategy import OscillatingStrategy
from primitives import Side, TrendMovement


class ThreeProngAlt(OscillatingStrategy):
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

    def __init__(self, threads: int = 4, **kwargs):
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
        _indicators: Sequence = [BBANDSRow, MACDRow, STOCHRSIRow]
        super().__init__(indicators=_indicators, threads=threads, **kwargs)

        self.threads = threads
        self.detector = TrendDetector(self.market, threads=threads)
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

    def _calc_amount(self, extrema: pd.Timestamp, side: Side) -> float:
        if self.orders.empty:
            assert side == Side.BUY
            last_order = {'amt': 0, 'side': Side.SELL}
        else:
            last_order = self.orders.iloc[-1]

        rate = self._calc_rate(extrema, side)
        _trend = self.detector.characterize(extrema)
        _more = 1 + _trend.scalar / 10
        _less = ceil(_trend.scalar / 2)
        if side == Side.SELL:
            incomplete = self._check_unpaired(rate)

            total = last_order['amt'] + incomplete['amt'].sum()

            # sell more during strong uptrend
            if _trend.trend is TrendMovement.UP:
                return total * _more
            # sell less during strong downtrend
            elif _trend.trend is TrendMovement.DOWN:
                return total / _less

            if total > self.assets:
                return self.assets
            return total

        if side == Side.BUY:
            amt = self.starting / rate

            # buy less during strong uptrend; buy more during strong downtrend
            if _trend.trend is TrendMovement.UP:
                return amt / _less
            elif _trend.trend is TrendMovement.DOWN:
                return amt * _more

            if self.capital < amt * rate:
                return self.capital / rate
            return amt

    def _is_profitable(self, amount: float, rate: float, side: Side,
                       extrema: Union['pd.Timestamp', str] = None) -> bool:
        """ See if given sale is profitable by checking if gain meets or exceeds a minimum threshold.

        Incorrect trades are rejected during strong trends

        Always buy according to signals. However, when selling during a strong uptrend, threshold is exponentially
        multiplied by `MarketTrend.scalar`. This is because significantly greater profit is expected during a strong
        uptrend.
        """
        assert side in (Side.BUY, Side.SELL)

        # prevent incorrect trades during strong trend
        _trend = self.detector.characterize(extrema)
        if _trend.scalar > 6 and \
           (_trend.trend is TrendMovement.UP and side is Side.BUY) or \
           (_trend.trend is TrendMovement.DOWN and side is Side.SELL):
            logging.warning('Prevented unaligned trade during strong trend')
            return False

        if side == Side.BUY:
            return True
        else:
            # prevent false positive from incomplete buys
            last_order = self.orders.iloc[-1]
            if last_order['side'] == Side.BUY and last_order['amt'] < amount and \
               self._calc_profit(last_order['amt'], rate) < self.threshold:
                return False

            # handle sell
            if _trend.trend == TrendMovement.UP:
                _min_profit = self.threshold * _trend.scalar
            else:
                _min_profit = self.threshold
            return self._calc_profit(amount, rate) >= _min_profit
