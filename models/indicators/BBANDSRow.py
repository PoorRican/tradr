from math import isnan
from typing import Union, Tuple

import pandas as pd
from talib import BBANDS

from models.indicator import Indicator
from primitives import Signal


class BBANDSRow(Indicator):
    name = 'BB'
    _function = BBANDS
    _parameters = {'timeperiod': 20}
    _source = 'close'
    columns = ('upperband', 'middleband', 'lowerband')

    def __init__(self, *args, threshold: float = 0.5, **kwargs):
        super().__init__(*args, **kwargs)
        self.threshold = threshold

    def _calculate_thresholds(self, row: Union['pd.Series', 'pd.DataFrame']) -> Tuple[float, float]:
        buy = row['middleband'] - row['lowerband']
        sell = row['upperband'] - row['middleband']

        buy *= self.threshold
        sell *= self.threshold

        buy += row['lowerband']
        sell += row['middleband']

        buy = float(buy)
        sell = float(sell)

        return buy, sell

    def _extract_rate(self, row: Union['pd.Series', 'pd.DataFrame'], candles: pd.DataFrame) -> float:
        if not hasattr(row, 'name'):
            point = row.index[0]
        else:
            point = row.name
        return float(candles.loc[point, self._source])

    def _row_decision(self, row: Union['pd.Series', 'pd.DataFrame'], candles: pd.DataFrame = None) -> Signal:
        # hack to unpack `point` from row
        rate = self._extract_rate(row, candles)

        buy, sell = self._calculate_thresholds(row)

        if rate <= buy:
            return Signal.BUY
        elif rate >= sell:
            return Signal.SELL
        else:
            return Signal.HOLD

    def _row_strength(self, row: Union['pd.Series', 'pd.DataFrame'], candles: pd.DataFrame = None) -> float:
        """ Determine strength of Trend """
        assert len(self.graph)

        rate = self._extract_rate(row, candles)

        buy, sell = self._calculate_thresholds(row)

        if isnan(buy) or isnan(sell):
            return 0
        elif rate <= buy:
            lower = row['lowerband']
            diff = buy - lower
            if rate > lower:
                return 1
            elif rate > lower - diff:
                return 2
            return 3
        elif rate >= sell:
            upper = row['upperband']
            diff = upper - sell
            if rate < upper:
                return 1
            if rate < upper + diff:
                return 2
            return 3
        return 0
