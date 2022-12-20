from math import isnan, nan
import pandas as pd
from talib import MACD
from typing import Union, Iterable

from models.indicator import Indicator, MAX_STRENGTH
from primitives import Signal


class MACDRow(Indicator):
    name = 'MACD'
    _function = MACD
    _parameters = {'fastperiod': 6, 'slowperiod': 26, 'signalperiod': 9}
    _source = 'close'

    columns = ('macd', 'macdsignal', 'macdhist')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.scalar: float = nan
        self.last_scalar: float = 0

    def normalize(self, value: float, point: pd.Timestamp, column: Union[str, Iterable[str]]) -> float:
        """ Normalize `value`.

        This is used to convert `strength` into a usable scalar that falls between 1 and `MAX_STRENGTH`.

        Args:
            value:
                Value to normalize.
            point:
                Point in time to reference. During backtesting future data should not be used to generate
                scalar value.
            column:
                Column name (or iterable containing column names) to use as reference. Used for indexing.

        Returns:

        """
        _col = self.graph.loc[:point, column]
        _col = _col.dropna()
        _col = abs(_col)

        _min = min(_col.values)
        _max = max(_col.values)
        scalar = _max - _min

        if scalar == 0 or isnan(scalar):
            return 0

        normal = (value - _min) / scalar
        return normal * MAX_STRENGTH

    def _row_decision(self, row: Union['pd.Series', 'pd.DataFrame'], candles: pd.DataFrame = None) -> Signal:
        signal = row['macdsignal']
        macd = row['macd']

        if hasattr(signal, '__iter__'):
            signal = signal[0]
        if hasattr(macd, '__iter__'):
            macd = macd[0]

        if macd < signal < 0:
            return Signal.BUY
        elif macd > signal > 0:
            return Signal.SELL
        else:
            return Signal.HOLD

    def _row_strength(self, row: Union['pd.Series', 'pd.DataFrame'], *args, **kwargs) -> float:
        assert len(self.graph)

        signal = row['macdsignal']
        macd = row['macd']

        if hasattr(signal, '__iter__'):
            signal = signal[0]
        if hasattr(macd, '__iter__'):
            macd = macd[0]

        # throwout intermediate `HOLD` values
        if not (macd < signal < 0 or macd > signal > 0):
            return nan

        _col = 'macdhist'
        val = abs(row[_col])

        if isnan(val):
            return nan
        elif not hasattr(row, 'name'):
            point = row.index[0]
        else:
            point = row.name

        val = self.normalize(val, point, _col)

        return val
