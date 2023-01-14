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
    _parameters = {'timeperiod': 14, 'fastk_period': 3, 'fastd_period': 3}
    _source = 'close'

    columns = ('fastk', 'fastd')

    def __init__(self, *args, overbought: float = 20, oversold: float = 80, threshold: float = 0.01, **kwargs):
        super().__init__(*args, **kwargs)
        self._overbought = overbought
        self._oversold = oversold
        self._threshold = threshold

        self._decision_normalizer = Normalizer(self, 'graph', ['fastk', 'fastd'], diff=True, apply_abs=True)

    def oversold(self, d: float, k: float) -> bool:
        return self._oversold < d <= k

    def overbought(self, d: float, k: float) -> bool:
        return self._overbought > d >= k

    def _row_decision(self, row: Union['pd.Series', 'pd.DataFrame'], candles: pd.DataFrame = None) -> Signal:
        fastk = row['fastk']

        if hasattr(fastk, '__iter__'):
            fastk = fastk[0]

        _point: pd.Timestamp = row.name
        _normals = self._decision_normalizer(_point)

        if _normals.iloc[-1] <= self._threshold:
            if self._oversold <= fastk:
                return Signal.SELL
            elif self._overbought >= fastk:
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
