from math import fabs, isnan, ceil
from typing import Union

import pandas as pd
from talib import MACD

from models.indicator import Indicator
from primitives import Signal


class MACDRow(Indicator):
    name = 'MACD'
    _function = MACD
    _parameters = {'fastperiod': 6, 'slowperiod': 26, 'signalperiod': 9}
    _source = 'close'

    columns = ('macd', 'macdsignal', 'macdhist')

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

        val = fabs((signal + macd) / 2)
        val *= 500
        if isnan(val) or not (macd < signal < 0 or macd > signal > 0):
            return 0
        return ceil(val)
