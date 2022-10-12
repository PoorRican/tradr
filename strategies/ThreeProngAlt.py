import pandas as pd
from Strategy import Strategy, truncate
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

    def __init__(self, buy_cost: float, threshold: float, *args, **kwargs):
        """

        Args:
            buy_cost:
                Amount of currency to spend in each buy transaction. This is used when determining the
                amount of an asset to buy for each trade. For example, given that `buy_cost` is set to
                `100` and `rate` is `200`, a buy trade will be attempted to purchase .5 units of the
                underlying asset.
            threshold:
                Minimum threshold (in currency amount) that a sale cost must exceed to deem
                a trade as profitable. This threshold should be high enough to make a sale worth
                the effort, but low enough so that trades can be executed. This should not be
                reflective of any market fees; trade fees are already incorporated into calculation.
            *args: Positional arguments to pass to `Strategy.__init__`
            **kwargs: Keyword arguments to pass to `Strategy.__init__`
        """
        super().__init__(*args, **kwargs)

        self.buy_cost = buy_cost
        self.threshold = threshold

        self.indicators = self._develop_signals()

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
        assert side in ('buy', 'side')
        if side == 'buy':
            third = 'high'
        else:
            third = 'low'

        return self.market.data.loc[extrema][['open', 'close', third]].mean()

    def _calc_amount(self, extrema: pd.Timestamp, side: str) -> float:
        last_order = self.orders.iloc[-1]
        if side is 'sell':
            assert last_order['side'] == 'buy'
            return last_order['amount']
        if side is 'buy':
            assert last_order['side'] == 'sell'
            return self.buy_cost / self._calc_rate(extrema, side)

    def _is_profitable(self, amount: float, rate: float, side: str) -> bool:
        """ See if given sale is profitable by checking if gain meets or exceeds a minimum threshold """
        assert side in ('buy', 'sell')
        if side is 'buy':
            return True
        else:
            last_buy = self.orders[self.orders['side'] == 'buy'].iloc[-1]
            fee = self.market.calc_fee()

            last_cost = last_buy['amount'] * last_buy['rate']
            next_sell = amount * rate
            return truncate(next_sell, 2) > (truncate(last_cost + fee, 2) + 0.1)

    def _check_bb(self, price: float) -> Union[str, False]:
        """ Check that price is close to or beyond edge of Bollinger Bands

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

        if price <= buy:
            return 'buy'
        elif price >= sell:
            return 'sell'
        else:
            return False

    def _check_macd(self) -> Union[str, False]:
        """ Get signal interpretation from MACD indicator

        Returns:
            `buy`/`sell`: Signal interpreted from Bollinger Bands.
         """
        frame = self.indicators.iloc[-1][['macdhist']]
        if frame < 0:
            return 'buy'
        elif frame > 0:
            return 'sell'
        else:
            return False

    def _check_stochrsi(self) -> Union[str, False]:
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

    def _develop_signals(self) -> pd.DataFrame:
        d = pd.DataFrame()

        d['upperband'], d['middleband'], d['upperband'] = BBANDS(self.market.data, 20)
        d['macd'], d['macdsignal'], d['macdhist'] = MACD(self.market.data, 6, 26, 9)
        d['fastk'], d['fastd'] = STOCHRSI(self.market.data, 14, 3)

        return d

    def _determine_position(self, point: pd.Timestamp = None) -> Union[Tuple[str, 'pd.Timestamp'], False]:
        """ Evaluate market and decide on trade.

        Forced alternation of trade types is executed here. Duplicate trade type is not returned if a new signal is
        generated.
        """
        self._develop_signals()

        if point:
            extrema = self.market.data.loc[point]
        else:
            extrema = self.market.data.iloc[-1]

        signals = (
            self._check_stochrsi(),
            self._check_bb(extrema['price']),
            self._check_macd(),
        )
        last_order_side = self.orders.iloc[-1]

        if signals[0] == signals[1] == signals[2] != last_order_side:
            side = signals[0]

            rate = self._calc_rate(extrema.index, side)
            amount = self._calc_amount(extrema.index, side)
            if self._is_profitable(amount, rate, side):
                return side, extrema.index

        return False
