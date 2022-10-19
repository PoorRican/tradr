import pandas as pd
from strategies.Strategy import Strategy
from typing import Union, Tuple
from talib import STOCHRSI, MACD, BBANDS


class ThreeProngAlt(Strategy):
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
        super().__init__(*args, **kwargs)

        self.threshold = threshold

        self.indicators = pd.DataFrame(columns=['upperband', 'middleband', 'lowerband',
                                                'macd', 'macdsignal', 'macdhist',
                                                'fastk', 'fastd'])

    def _calc_rate(self, extrema: pd.Timestamp, side: str) -> float:
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
        assert side in ('buy', 'sell')
        if side == 'buy':
            third = 'high'
        else:
            third = 'low'

        return self.market.data.loc[extrema][['open', 'close', third]].mean()

    def _calc_amount(self, extrema: pd.Timestamp, side: str) -> float:
        if self.orders.empty:
            assert side == 'buy'
            last_order = {'amt': 0, 'side': 'sell'}
        else:
            last_order = self.orders.iloc[-1]

        if side == 'sell':
            assert last_order['side'] == 'buy'
            return last_order['amt']
        if side == 'buy':
            assert last_order['side'] == 'sell'
            # TODO: amount should increase as total gain exceeds 125% of `starting`
            return self.starting / self._calc_rate(extrema, side)

    def _is_profitable(self, amount: float, rate: float, side: str) -> bool:
        """ See if given sale is profitable by checking if gain meets or exceeds a minimum threshold.

        Always buy according to signals.
        """
        assert side in ('buy', 'sell')
        if side == 'buy':
            return True
        else:
            return self._calc_profit(amount, rate, side) >= self.threshold

    def _check_bb(self, rate: float) -> Union[str, 'False']:
        """ Check that price is close to or beyond edge of Bollinger Bands

        Args:
            rate: Current ticker price

        Returns:
            `buy`/`sell`: Signal interpreted from Bollinger Bands.
        """

        frame = self.indicators.iloc[-1][['lowerband', 'middleband', 'upperband']]
        buy = frame['middleband'] - frame['lowerband']
        sell = frame['upperband'] - frame['middleband']

        buy *= .5
        sell *= .5

        buy += frame['lowerband']
        sell += frame['middleband']

        if rate <= buy:
            return 'buy'
        elif rate >= sell:
            return 'sell'
        else:
            return False

    def _check_macd(self) -> Union[str, 'False']:
        """ Get signal interpretation from MACD indicator

        Returns:
            `buy`/`sell`: Signal interpreted from Bollinger Bands.
         """
        frame = self.indicators.iloc[-1]['macdhist']
        if frame < 0:
            return 'buy'
        elif frame > 0:
            return 'sell'
        else:
            return False

    def _check_stochrsi(self) -> Union[str, 'False']:
        """ Get signal from Stochastic RSI.

        Returns:
            'buy': if K and D are below 20 with K < D
            'sell': if K and D are above 80 with K > D
            Otherwise, `False` is returned
        """
        frame = self.indicators.iloc[-1][['fastk', 'fastd']]
        if frame['fastk'] < 20 and 20 > frame['fastd'] > frame['fastk']:
            return 'buy'
        elif frame['fastk'] > 80 and 80 < frame['fastd'] < frame['fastk']:
            return 'sell'
        else:
            return False

    def _develop_signals(self, point: pd.Timestamp = None) -> pd.DataFrame:
        d = pd.DataFrame(columns=['upperband', 'middleband', 'lowerband',
                                  'macd', 'macdsignal', 'macdhist',
                                  'fastk', 'fastd'])
        if point:
            data = self.market.data['close'].loc[:point]
        else:
            data = self.market.data['close']

        d['upperband'], d['middleband'], d['lowerband'] = BBANDS(data, 20)
        d['macd'], d['macdsignal'], d['macdhist'] = MACD(data, 6, 26, 9)
        d['fastk'], d['fastd'] = STOCHRSI(data, 14, 3, 3)

        return d

    def _determine_position(self, point: pd.Timestamp = None) -> Union[Tuple[str, 'pd.Timestamp'], 'False']:
        """ Evaluate market and decide on trade.

        Forced alternation of trade types is executed here. Duplicate trade type is not returned if a new signal is
        generated.
        """
        self.indicators = self._develop_signals(point)

        if point:
            extrema = self.market.data.loc[point]
        else:
            extrema = self.market.data.iloc[-1]

        signals = (
            self._check_stochrsi(),
            self._check_bb(extrema['close']),
            self._check_macd(),
        )
        if self.orders.empty:
            # force buy for first trade
            last_order_side = 'sell'
        else:
            last_order_side = self.orders.iloc[-1]['side']

        if signals[0] and signals[0] == signals[1] == signals[2] != last_order_side:
            side = signals[0]

            rate = self._calc_rate(extrema.name, side)
            amount = self._calc_amount(extrema.name, side)
            if self._is_profitable(amount, rate, side):
                return side, extrema.name

        return False
