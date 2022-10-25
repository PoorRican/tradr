from enum import IntEnum
from dataclasses import dataclass, fields
from typing import List, ClassVar, Callable, Dict, TypeVar, NoReturn, Optional, Tuple
import pandas as pd
from collections import UserList
from talib import BBANDS, STOCHRSI, MACD


class Signal(IntEnum):
    SELL = -1
    HOLD = 0
    BUY = 1


@dataclass
class Indicator:
    _function: ClassVar[Callable]
    _parameters: ClassVar[Dict]
    _source: ClassVar[Dict]

    @classmethod
    def columns(cls) -> List[str]:
        return [i.name for i in fields(cls)]

    @classmethod
    def process(cls, data: pd.DataFrame, **kwargs) -> Tuple:
        params = cls._parameters
        params.update(kwargs)

        return cls._function(data[cls._source], **params)


INDICATOR = TypeVar('INDICATOR', bound=Indicator)


@dataclass
class MACDRow(Indicator):
    _function = MACD
    _parameters = {'fastperiod': 6, 'slowperiod': 26, 'signalperiod': 9}
    _source = 'close'

    macd: float
    macdsignal: float
    macdhist: float

    @staticmethod
    def check(frame: pd.DataFrame, *args, **kwargs) -> Signal:
        _ = frame['macdhist']
        if _ < 0:
            return Signal.BUY
        elif _ > 0:
            return Signal.SELL
        else:
            return Signal.HOLD


@dataclass
class BBANDSRow(Indicator):
    _function = BBANDS
    _parameters = {'timeperiod': 20}
    _source = 'close'

    upperband: float
    middleband: float
    lowerband: float

    @staticmethod
    def check(frame: pd.DataFrame, rate: float, threshold: float = 0.5,
              *args, **kwargs) -> Signal:
        buy = frame['middleband'] - frame['lowerband']
        sell = frame['upperband'] - frame['middleband']

        buy *= threshold
        sell *= threshold

        buy += frame['lowerband']
        sell += frame['middleband']

        if rate <= buy:
            return Signal.BUY
        elif rate >= sell:
            return Signal.SELL
        else:
            return Signal.HOLD


@dataclass
class STOCHRSIRow(Indicator):
    _function = STOCHRSI
    _parameters = {'timeperiod': 14, 'fastk_period': 3, 'fastd_period': 3}
    _source = 'close'

    fastk: float
    fastd: float

    @staticmethod
    def check(frame: pd.DataFrame, overbought: float = 20, oversold: float = 80, *args, **kwargs) -> Signal:
        if frame['fastk'] < overbought and overbought > frame['fastd'] > frame['fastk']:
            return Signal.BUY
        elif frame['fastk'] > oversold and oversold < frame['fastd'] < frame['fastk']:
            return Signal.SELL
        else:
            return Signal.HOLD


class IndicatorContainer(UserList[INDICATOR]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.graph: pd.DataFrame = self.container()

    def container(self, index: Optional[pd.Index] = None) -> pd.DataFrame:
        """ Generate and return an empty `DataFrame` with indicator columns.

        DataFrame should be re-initialized by this function if indicators are ever added or removed.
        """
        cols = []
        for cls in self.data:
            cols.extend(cls.columns())
        return pd.DataFrame(columns=cols, index=index)

    def develop(self, data: pd.DataFrame) -> NoReturn:
        """ Generate indicator data for all available given candle data.

        `self.graph` is used to store all indicator data and should only be updated by this method.

        Args:
            data: Candle data. Should be shortened if speed is an issue.

        """
        df = self.container(data.index)
        for indicator in self.data:
            _graph = indicator.process(data)
            for name, _data in zip(indicator.columns(), _graph):
                df[name] = _data
        self.graph = df

    def check(self, data: pd.DataFrame, point: pd.Timestamp = None) -> Signal:
        # TODO: check that market data is not too ahead of computed indicators
        if point:
            frame = self.graph.loc[point]
        else:
            frame = self.graph.iloc[-1]

        kwargs = {'rate': data['close'].loc[frame.name]}
        signals = [indicator.check(frame, **kwargs) for indicator in self.data]
        if signals[0] == signals[1] == signals[2]:
            return signals[0]

        return Signal.HOLD
