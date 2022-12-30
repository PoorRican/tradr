from typing import Union

import pandas as pd

from models.indicator import Indicator
from primitives import Signal


class TestingIndicator(Indicator):

    name = 'TestingIndicator'
    _function = None
    """ indicator function that is passed a single column of candle data, and ambiguous keyword arguments. """

    columns = ('first', 'second')

    def _row_strength(self, row: Union['pd.Series', 'pd.DataFrame'], candles: pd.DataFrame) -> float:
        return 1 - row['second']

    def _row_decision(self, row: Union['pd.Series', 'pd.DataFrame'], candles: pd.DataFrame) -> Signal:
        if row['first'] % 10 == 0:
            return Signal.HOLD
        elif row['first'] % 2 == 0:
            return Signal.SELL
        return Signal.BUY
