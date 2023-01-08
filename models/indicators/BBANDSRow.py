from math import isnan, nan
import pandas as pd
from talib import BBANDS
from typing import Union, Tuple

from models.indicator import Indicator
from primitives import Signal


class BBANDSRow(Indicator):
    name = 'BB'
    _function = BBANDS
    _parameters = {'timeperiod': 20}
    _source = 'close'
    columns = ('upperband', 'middleband', 'lowerband')

    def __init__(self, *args, threshold: float = 0.25, **kwargs):
        super().__init__(*args, **kwargs)

        self.threshold = threshold

    def _calculate_thresholds(self, row: Union['pd.Series', 'pd.DataFrame'],
                              threshold: float = None) -> Tuple[float, float]:
        """ Calculate and return threshold values for buy and sell signals.

        Buy signal is generated when price is lower than buy threshold;  sell signal is generated when
        price is higher than sell threshold.

        Args:
            threshold:
                Optional value to override default instance value.

                "Width" gets wider as the value of `threshold` increases.  A value of 1 would be equal to "upper" and
                "lower" bands.  A value of 0 would be equal to "middle" band.  Therefore, a higher value of threshold
                selects for more divergent values.
        """
        if threshold is None:
            threshold = self.threshold
        buy = row['middleband'] - row['lowerband']
        sell = row['upperband'] - row['middleband']

        buy *= 1 - threshold
        sell *= threshold

        buy += row['lowerband']
        sell += row['middleband']

        buy = float(buy)
        sell = float(sell)

        return buy, sell

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
        rate = self._extract_rate(row, candles)

        buy, sell = self._calculate_thresholds(row)
        buy2, sell2 = self._calculate_thresholds(row, .5)
        buy3, sell3 = self._calculate_thresholds(row, .75)

        if isnan(buy) or isnan(sell):
            return nan
        elif rate < buy:
            lower = row['lowerband']

            if rate < lower:
                return 4
            elif rate < buy3:
                return 3
            elif rate < buy2:
                return 2
            return 1
        elif rate > sell:
            upper = row['upperband']
            if rate > upper:
                return 4
            elif rate > sell3:
                return 3
            elif rate > sell2:
                return 2
            return 1
        return nan

    def plot(self, *args, **kwargs):
        primary_idx = 0
        # always attempt to add to primary sub-plot
        return super().plot(*args, **kwargs, index=primary_idx)
