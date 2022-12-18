from abc import ABC
from datetime import datetime
import pandas as pd
from pytz import timezone
from typing import Union, Tuple, Sequence

from analysis.financials import FinancialsMixin
from models import Indicator, IndicatorContainer
from primitives import Signal, Side


class OscillatingStrategy(FinancialsMixin, ABC):
    def __init__(self, indicators: Sequence[type(Indicator)], timeout: str = '6h',
                 threads: int = 4, **kwargs):
        """
        Args:
            indicators:
            timeout:
                Timeout frequency for sequential buy orders. Used by `_oscillation()` frequency.
            **kwargs:
                Keyword Args passed to `FinancialsMixin.__init__()`
        """
        super().__init__(**kwargs)

        self.timeout: str = timeout
        self.indicators: IndicatorContainer = IndicatorContainer(indicators, lookback=1)

    def _oscillation(self, signal: Signal, timeout=True, point: pd.Timestamp = None) -> bool:
        """ Ensure that order types oscillate between `sell` and `buy`.

        Both the first order and `timeout` are taken into account.

        Check that `signal` (which is generated by `_check_signals()`) is the opposite of the last order type. If
        timeout has been reached, then multiple buy orders are allowed. Multiple buy orders are tracked by
        `FinancialsMixin.incomplete` and are limited by `order_count`.

        Args:
            signal:
                decision generated by `_check_signals()`. May be `False`, 'sell' or 'buy'
            timeout:
                flag to check timeout period. Used during unit testing to circumvent timeout checking.
            point:
                Simulated point in time. Used during backtesting.

        Returns:
            `true` if `signal`  decision values.
        """
        if self.orders.empty:           # first trade must be "buy"
            # TODO: check if `assets` is 0
            return signal == Signal.BUY

        if signal:
            last_order = self.orders.iloc[-1]

            # prevent more buy orders when there are too many incomplete orders
            if self._remaining == 0 and signal == Signal.BUY:
                return False
            # Allow repeated buys on timeout
            elif last_order['side'] == signal == Signal.BUY and self._remaining and timeout:
                inactive = self._check_timeout(point)
                if inactive:
                    self._handle_inactive(last_order)
                return inactive
            return last_order.side != signal

        return False

    def _determine_position(self, point: pd.Timestamp = None) -> Union[Tuple[Side, 'pd.Timestamp'], 'False']:
        """ Determine trade execution and type.

        Oscillation of trade types is executed here. Duplicate trade type is not returned if a new signal is
        generated.

        Number of incomplete (outstanding) orders is limited here. If there are no remaining allowed orders
        (as defined by `_remaining`) then False is returned.

        Notes:
            `self.indicators.develop()` needs to be called beforehand.
        """
        if not point:
            point = self.market.most_recent_timestamp

        if self._remaining <= 1:
            pass

        signal: Signal = self.indicators.signal(self.candles, point)
        if self._oscillation(signal, point=point):
            signal: Side = Side(signal)
            rate = self._calc_rate(point, signal)
            amount = self._calc_amount(point, signal)
            if self._is_profitable(amount, rate, signal, point):
                return signal, point

        return False

    def _check_timeout(self, point: pd.Timestamp = None) -> bool:
        """ Checks if trading has been inactive

        Allows the consecutive orders of the same type if inactivity exceeds timeout. Timeout should be sufficiently
        long enough to protect against price drops, but short enough to not miss ideal price drops.

        Notes:
            Logic definitions for when timeout is valid (ie: repeating buys) is performed before.

        Returns:
            True if alternation needs to be reset; False if last order wasn't a buy, or if timeout hasn't been reached.

        Raises:
            `IndexError` if `orders` is empty.

        TODO:
            - Pass trend strength (if trend is bear market) and permit buys if trend is strong enough
        """
        last = self.orders.iloc[-1].name
        last = pd.Timestamp.fromtimestamp(last.timestamp(), tz=timezone('US/Pacific'))
        if point:
            now = point
        else:
            now = datetime.now(tz=timezone('US/Pacific'))
        diff = now - last
        period = pd.to_timedelta(self.timeout)

        return diff > period
