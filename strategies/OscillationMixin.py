from abc import ABC
from datetime import datetime
from typing import Union, List

import pandas as pd

from misc import TZ
from models import Indicator, IndicatorGroup, FutureTrade
from primitives import Signal, ReasonCode
from strategies.financials import OrderHandler


class OscillationMixin(OrderHandler, ABC):
    def __init__(self, indicators: List[Indicator], freq: str, timeout: str = '6h',
                 threads: int = 4, lookback: int = 2, **kwargs):
        """
        Args:
            indicators:
                Instances of indicators to use for determining market positions.
            timeout:
                Timeout frequency for sequential buy orders. Used by `_oscillation()` frequency.
            **kwargs:
                Keyword Args passed to `OrderHandler.__init__()`
        """
        super().__init__(freq=freq, **kwargs)

        self.threads = threads
        self.lookback = lookback
        self.timeout: str = timeout
        self.indicators: IndicatorGroup = IndicatorGroup(self.market, freq, indicators, unison=True)

    def _oscillation(self, signal: Signal, timeout=True, point: pd.Timestamp = None) -> bool:
        """ Allow for repeated buy orders if timeout has been reached.

        Multiple sell orders are always allowed.

        If timeout has been reached, then multiple buy orders are allowed. Multiple buy orders are tracked by
        `OrderHandler.incomplete` and are limited by `order_limit`.

        Args:
            signal:
                `Signal` value to check.
            timeout:
                flag to check timeout period. Used during unit testing to circumvent timeout checking.
            point:
                Simulated point in time. Used during backtesting.

        Returns:
            True if `signal` is a repeated buy order and timeout has been reached.
            False if `signal` is a repeated buy order and timeout has not been reached.
            True if `signal` is not a repeated buy order.
            False if `signal` is `HOLD`.
        """
        if self.orders.empty:           # first trade must be "buy"
            # TODO: check if `assets` is 0
            return signal == Signal.BUY

        if signal is not Signal.HOLD:

            # prevent more buy orders when there are too many incomplete orders
            if signal == Signal.BUY:

                last_order = self.orders.iloc[-1]

                if self._remaining == 0:
                    return False
                # Allow repeated buys on timeout
                elif last_order['side'] == Signal.BUY and self._remaining and timeout:
                    inactive = self._check_timeout(point)
                    if inactive:
                        self._handle_inactive(last_order)
                    return inactive

            return True

        return False

    def _determine_position(self, point: pd.Timestamp = None) -> Union['FutureTrade', 'False']:
        """ Determine trade execution and type.

        Oscillation of trade types is executed here. Duplicate trade type is not returned if a new signal is
        generated.

        Number of incomplete (outstanding) orders is limited here. If there are no remaining allowed orders
        (as defined by `_remaining`) then False is returned.

        Args:
            point:
                Point in time to examine. During backtesting, `point` must be quantized to the keys in `candles`

        Notes:
            `self.indicators.update()` needs to be called beforehand.

        Returns:
            `FutureTrade` if trade signals have initiated a trade. If trade is not profitable, then
            `attempt` is False and proper `ReasonCode` is tagged. A `FutureTrade` is returned because
            it contains data about when this trade was initiated and if this trade should be attempted or not,
            but does not indicate that the trade has been successfully executed. The `SuccessfulTrade` dataclass
            has been explicitly reserved for that function.

            Otherwise, if a trade has not been initiated, False is returned.
        """
        if not point:
            point = self.market.most_recent_timestamp

        signal: Signal = self.indicators.signal(point)
        strength: float = self.indicators.strength(signal, point)
        if signal is Signal.BUY and self._remaining < 1:
            pass
        elif self._oscillation(signal, point=point):
            trade = self._propose_trade(signal, point)
            _profitable: bool = self._is_profitable(trade, point, strength)
            trade = FutureTrade.factory(trade, _profitable, point)
            if not _profitable:
                trade.load = ReasonCode.NOT_PROFITABLE
            return trade

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
        last = pd.Timestamp.fromtimestamp(last.timestamp(), tz=TZ)
        if point:
            now = point
        else:
            now = datetime.now(tz=TZ)
        diff = now - last
        period = pd.to_timedelta(self.timeout)

        return diff > period
