from math import isnan, nan
import pandas as pd
from talib import MA
from typing import Union, Tuple

from models.indicator import Indicator
from primitives import Signal


class MARow(Indicator):
    name = 'MA'
    _function = MA
    _parameters = {'timeperiod': 50, 'matype': 0}
    _source = 'close'
    columns = ('ma',)


    def _extract_rate(self, row: Union['pd.Series', 'pd.DataFrame'], candles: pd.DataFrame) -> float:
        """ Extract `rate` from `row`.

        Notes:
            This function is necessary because indexing daily data returns a dataframe, due to indexing a
            `DatetimeIndex` with a `str` instead of a `Timestamp` object. Normally,
        """
        if not hasattr(row, 'name'):
            point = row.index[0]
        else:
            point = row.name
        return float(candles.loc[point, self._source])

    def _row_decision(self, row: Union['pd.Series', 'pd.DataFrame'], candles: pd.DataFrame = None) -> Signal:
        rate = self._extract_rate(row, candles)

        ma = row['ma']

        if rate < ma:
            return Signal.BUY
        elif rate > ma:
            return Signal.SELL
        else:
            return Signal.HOLD

    def _row_strength(self, row: Union['pd.Series', 'pd.DataFrame'], candles: pd.DataFrame = None) -> float:
        rate = self._extract_rate(row, candles)

        ma = row['ma']

        if rate <= ma:
            return 1
        elif rate >= ma:
            return 1
        else:
            return nan

    def plot(self, *args, **kwargs):
        primary_idx = 0

        # always attempt to add to primary sub-plot
        return super().plot(*args, **kwargs, index=primary_idx)
