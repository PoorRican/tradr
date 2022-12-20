from math import fabs, isnan, ceil
import pandas as pd
from talib import STOCHRSI
from typing import Union

from models.indicator import Indicator
from primitives import Signal


# noinspection PyUnusedLocal
class STOCHRSIRow(Indicator):
    name = 'STOCHRSI'
    _function = STOCHRSI
    _parameters = {'timeperiod': 14, 'fastk_period': 3, 'fastd_period': 3}
    _source = 'close'

    columns = ('fastk', 'fastd')

    def __init__(self, *args, overbought: float = 20, oversold: float = 80, **kwargs):
        super().__init__(*args, **kwargs)
        self._overbought = overbought
        self._oversold = oversold

    def oversold(self, d: float, k: float) -> bool:
        return self._oversold < d <= k

    def overbought(self, d: float, k: float) -> bool:
        return self._overbought > d >= k

    def _row_decision(self, row: Union['pd.Series', 'pd.DataFrame'], candles: pd.DataFrame = None) -> Signal:
        fastk = row['fastk']
        fastd = row['fastd']

        if hasattr(fastk, '__iter__'):
            fastk = fastk[0]
        if hasattr(fastd, '__iter__'):
            fastd = fastd[0]

        if self.overbought(fastd, fastk):
            return Signal.BUY
        elif self.oversold(fastd, fastk):
            return Signal.SELL
        else:
            return Signal.HOLD

    def _row_strength(self, row: Union['pd.Series', 'pd.DataFrame'], candles: pd.DataFrame) -> float:
        k = row['fastk']
        d = row['fastd']

        val = fabs(k - d)

        if isnan(val) or (not self.overbought(d, k) and not self.oversold(d, k)):
            return 0
        return ceil(val / 5)
