import pandas as pd
from talib import MACD
from typing import Union

from models.indicator import Indicator, MAX_STRENGTH
from primitives import Signal, Normalizer


class MACDRow(Indicator):
    name = 'MACD'
    _function = MACD
    _parameters = {'fastperiod': 6, 'slowperiod': 26, 'signalperiod': 9}
    _source = 'close'

    columns = ('macd', 'macdsignal', 'macdhist')

    def __init__(self, *args, threshold: float = 0.01, **kwargs):
        super().__init__(*args, **kwargs)

        self._threshold = threshold
        # TODO: would this work as a class-level attribute?
        self._decision_normalizer = Normalizer(self, 'graph', ['macd', 'macdsignal'], diff=True, apply_abs=True)
        self._strength_normalizer = Normalizer(self, 'graph', ['macd'], apply_abs=True)

    def _row_decision(self, row: Union['pd.Series', 'pd.DataFrame'], candles: pd.DataFrame = None) -> Signal:
        """
        Notes:
            Buy signal is when MACD crosses and is greater than signal line; sell signal is when MACD crosses and is
            less than signal line. However, for buys, MACD should be less than 0, vice versa.
        """
        macd = row['macd']
        if hasattr(macd, '__iter__'):
            macd = macd[0]

        _point: pd.Timestamp = row.name
        _normals = self._decision_normalizer(_point)

        if _normals.iloc[-1] <= self._threshold:
            if macd <= 0:
                return Signal.BUY
            elif macd >= 0:
                return Signal.SELL
        return Signal.HOLD

    def _row_strength(self, row: Union['pd.Series', 'pd.DataFrame'], *args, **kwargs) -> float:
        if not hasattr(row, 'name'):
            point = row.index[0]
        else:
            point = row.name

        _normals = self._strength_normalizer(point)
        return _normals.iloc[-1] * MAX_STRENGTH
