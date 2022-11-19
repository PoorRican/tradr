import pandas as pd
from typing import Sequence, Union
from warnings import warn

from analysis.trend import TrendDetector
from models.signals import MACDRow, BBANDSRow, STOCHRSIRow
from models.trades import Side
from models.trend import TrendMovement
from strategies.OscillatingStrategy import OscillatingStrategy


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
    name = 'ThreeProngAlt'

    def __init__(self, *args, **kwargs):
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
        super().__init__(indicators=_indicators, *args, **kwargs)

        self.detector = TrendDetector(self.market)
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

        return self.market.data.loc[extrema][['open', 'close', third]].mean()

    def _calc_amount(self, extrema: pd.Timestamp, side: Side) -> float:
        if self.orders.empty:
            assert side == Side.BUY
            last_order = {'amt': 0, 'side': Side.SELL}
        else:
            last_order = self.orders.iloc[-1]

        rate = self._calc_rate(extrema, side)
        if side == Side.SELL:
            incomplete = self._check_unpaired(rate)

            total = last_order['amt'] + incomplete['amt'].sum()

            # modulate amount based on market trend

            # TODO: add partially sold buys when orders post using this

            _trend = self.detector.characterize(extrema)
            # sell more during strong uptrend
            if _trend.trend is TrendMovement.UP:
                return total * _trend.scalar
            # sell less during strong downtrend
            elif _trend.trend is TrendMovement.DOWN:
                return total / _trend.scalar

            if total > self.assets:
                warn("Calculated total of assets to sell exceeds actual total")
                return self.assets
            return total

        if side == Side.BUY:
            amt = self.starting / rate

            # modulate amount based on market trend
            _trend = self.detector.characterize(extrema)

            # buy less during strong uptrend; buy more during strong downtrend
            if _trend.trend is TrendMovement.UP:
                return amt / _trend.scalar
            elif _trend.trend is TrendMovement.DOWN:
                return amt * _trend.scalar

            if self.capital < amt * rate:
                warn("Calculated amount of assets to buy exceeds amount of available capital")
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
        if _trend.scalar > 3 and \
           (_trend.trend is TrendMovement.UP and side is Side.BUY) or \
           (_trend.trend is TrendMovement.DOWN and side is Side.SELL):
            return False

        if side == Side.BUY:
            return True
        else:
            # handle sell
            if _trend.trend == TrendMovement.UP:
                _min_profit = self.threshold * _trend.scalar * _trend.scalar
            else:
                _min_profit = self.threshold
            return self._calc_profit(amount, rate) >= _min_profit
