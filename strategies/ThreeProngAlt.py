import pandas as pd
from typing import Sequence, Generic

from models.signals import Signal, MACDRow, BBANDSRow, STOCHRSIRow, INDICATOR
from models.trades import Side
from strategies.OscillatingStrategy import OscillatingStrategy


class ThreeProngAlt(OscillatingStrategy):
    """ Alternating high-freq strategy that bases decisions on 3 indicators: StochRSI, BB, MACD.

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

    def __init__(self, threshold: float, *args, **kwargs):
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
        _indicators: Sequence[INDICATOR, ...] = [BBANDSRow, MACDRow, STOCHRSIRow]
        super().__init__(indicators=_indicators, *args, **kwargs)

        self.threshold = threshold

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
            other = self._check_unpaired(rate)
            return last_order['amt'] + other['amt'].sum()
        if side == Side.BUY:
            # TODO: amount should increase as total gain exceeds 125% of `starting`
            # TODO: amount bought should not exceed amount of starting capital and should
            #   take into account unpaired buy trades.
            return self.starting / rate

    def _is_profitable(self, amount: float, rate: float, side: Side) -> bool:
        """ See if given sale is profitable by checking if gain meets or exceeds a minimum threshold.

        Always buy according to signals.
        """
        assert side in (Side.BUY, Side.SELL)
        if side == Side.BUY:
            return True
        else:
            return self._calc_profit(amount, rate, side) >= self.threshold
