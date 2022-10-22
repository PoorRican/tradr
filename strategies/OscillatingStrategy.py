from typing import Union, Tuple
from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd

from strategies.strategy import Strategy
from models.trades import SuccessfulTrade


class OscillatingStrategy(Strategy, ABC):
    def __init__(self, *args, timeout='6h', **kwargs):
        super().__init__(*args, **kwargs)

        self.unpaired_buys = pd.Series(name='unpaired buy id\'s', dtype='int32')

        self.timeout = timeout

    @abstractmethod
    def _calc_amount(self, extrema: pd.Timestamp, side: str) -> float:
        pass

    @abstractmethod
    def _is_profitable(self, amount: float, rate: float, side: str) -> bool:
        pass

    @abstractmethod
    def _develop_signals(self, point: pd.Timestamp) -> pd.DataFrame:
        pass

    def _oscillation(self, signal: Union['False', str]) -> bool:
        """ Ensure that order types oscillate between `sell` and `buy`.

        Both the first order and `timeout` are taken into account.

        Check that `signal` (which is generated by `_check_signals()`) is the opposite
        of the last order type. If timeout has been reached, add last 'buy' to `unpaired_buys`

        Args:
            signal: decision generated by `_check_signals()`. May be `False`, 'sell' or 'buy'

        Returns:
            `true` if `signal`  decision values.
        """
        if self.orders.empty:
            # force buy for first trade
            return signal == 'buy'

        if signal:
            last_order = self.orders.iloc[-1]
            if last_order['side'] == signal == 'buy':
                timeout = self._check_timeout()
                if timeout:
                    row = pd.Series([last_order['id']])
                    self.unpaired_buys = pd.concat([self.unpaired_buys, row],
                                                   ignore_index=True, names=['id\'s'])
                return timeout
            return last_order.side != signal

        return signal

    def _post_sale(self, trade: SuccessfulTrade):
        """ Clean `unpaired_buys` after successful sale.
        Assumption is that `trade.related` will be populated. """

        if trade.side == 'sell':
            unpaired = self._check_unpaired(trade.rate)
            if not unpaired.empty:
                matching = self.unpaired_buys.isin(unpaired['id'].values)
                indices = self.unpaired_buys.loc[matching].index
                self.unpaired_buys.drop(index=indices, inplace=True)

    def _determine_position(self, point: pd.Timestamp = None) -> Union[Tuple[str, 'pd.Timestamp'], 'False']:
        """ Determine trade execution and type.

        Oscillation of trade types is executed here. Duplicate trade type is not returned if a new signal is
        generated.
        """
        self.indicators = self._develop_signals(point)

        if point:
            extrema = self.market.data.loc[point]
        else:
            extrema = self.market.data.iloc[-1]

        signal = self._check_signals(extrema)
        if self._oscillation(signal):
            rate = self._calc_rate(extrema.name, signal)
            amount = self._calc_amount(extrema.name, signal)
            if self._is_profitable(amount, rate, signal):
                return signal, extrema.name

        return False

    @abstractmethod
    def _calc_rate(self, extrema: pd.Timestamp, side: str) -> float:
        pass

    @abstractmethod
    def _check_signals(self, extrema: pd.DataFrame) -> Union[str, 'False']:
        pass

    def get_unpaired_orders(self) -> pd.DataFrame:
        """ Select of unpaired orders by cross-referencing `unpaired_buys` """
        return self.orders[self.orders['id'].isin(self.unpaired_buys.values)]

    def _check_unpaired(self, rate: float):
        """ If any unpaired orders can be sold at a profit. """
        unpaired = self.get_unpaired_orders()
        return unpaired[unpaired['rate'] <= rate]

    def unrealized_gain(self) -> float:
        """ Calculate potential gain if all unpaired orders were sold at the highest rate.

        Returns:
            Value of unsold assets sold at most expensive price.
        """
        unpaired = self.get_unpaired_orders()
        highest = max(unpaired['rate'])
        return unpaired['amt'].sum() * highest

    def _check_timeout(self) -> bool:
        """ Reset current alternation based on inactivity timeout.

        Allows the consecutive `buy` orders to be attempted if inactivity exceeds timeout.

        This function assumes that it is not the very first order.

        Returns:
            True if alternation needs to be reset; False if last order wasn't a buy, or if timeout hasn't been reached.

        Raises:
            `IndexError` if `orders` is empty.
        """
        last_order = self.orders.iloc[-1]
        if last_order['side'] == 'buy':
            now = datetime.now()
            diff = now - pd.to_datetime(last_order.name)
            period = pd.to_timedelta(self.timeout)

            return diff > period
        return False

