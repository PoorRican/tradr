from math import fabs, isnan, ceil, nan
import pandas as pd
from talib import STOCHRSI
from typing import Union

from models.indicator import Indicator
from primitives import Signal, Normalizer


# noinspection PyUnusedLocal
class STOCHRSIRow(Indicator):
    name = 'STOCHRSI'
    _function = STOCHRSI
    _parameters = {'timeperiod': 14, 'fastk_period': 5, 'fastd_period': 3}
    _source = 'close'

    columns = ('fastk', 'fastd')

    def __init__(self, *args, overbought: float = 20, oversold: float = 80, threshold: float = 0.01, **kwargs):
        super().__init__(*args, **kwargs)
        self._overbought = overbought
        self._oversold = oversold
        self._threshold = threshold

    def oversold(self, d: float, k: float) -> bool:
        return self._oversold < d <= k

    def overbought(self, d: float, k: float) -> bool:
        return self._overbought > d >= k

    def _row_decision(self, row: Union['pd.Series', 'pd.DataFrame'], candles: pd.DataFrame = None) -> Signal:
        k = row['fastk']
        d = row['fastd']

        if hasattr(k, '__iter__'):
            k = k[0]
        if hasattr(d, '__iter__'):
            d = d[0]

        if self.overbought(d, k):
            return Signal.SELL
        elif self.oversold(d, k):
            return Signal.BUY
        return Signal.HOLD

    def _row_strength(self, row: Union['pd.Series', 'pd.DataFrame'], candles: pd.DataFrame) -> float:
        k = row['fastk']
        d = row['fastd']

        if hasattr(k, '__iter__'):
            k = k[0]
        if hasattr(d, '__iter__'):
            d = d[0]

        val = fabs(k - d)

        if isnan(val) or (not self.overbought(d, k) and not self.oversold(d, k)):
            return nan
        return ceil(val / 4) or 1
