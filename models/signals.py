from abc import ABC, abstractmethod
from enum import IntEnum
from dataclasses import dataclass, fields
from typing import List, ClassVar, Callable, Dict, TypeVar, NoReturn, Optional, Tuple
import pandas as pd
from collections import UserList
from talib import BBANDS, STOCHRSI, MACD


class Signal(IntEnum):
    """ Abstracts `Indicator` return output as a trinary decision.

    The available decisions are 'buy', 'hold', and 'sell'. In addition, 'buy'/'sell' decisions
    can be converted to a value of `Side`. Both discrete objects are needed to explicitly abstract
    indicator output and trade type. Boilerplate code is then reduced when checking signal value.

    Example:
        Convert value to `Side`:
            >>> from models.trades import Side
            >>> signal = Side(signal)

        Check signal value:
            >>> if Signal:
            >>>     pass    # handle buy/sell
            >>> else:
            >>>     pass    # handle hold
    """
    SELL = -1
    HOLD = 0
    BUY = 1


@dataclass
class Indicator(ABC):
    """ Abstracts statistical functions and encapsulates logic to derive discrete values.
    """
    _function: ClassVar[Callable]
    """ indicator function that is passed a single column of candle data, and ambiguous keyword arguments. """

    _parameters: ClassVar[Dict] = {}
    """ Ambiguous parameters for `_function` """

    _source: ClassVar[Dict] = 'close'
    """ Stores which column of input candle data to use.
    """

    @classmethod
    def columns(cls) -> List[str]:
        return [i.name for i in fields(cls)]

    @classmethod
    def process(cls, data: pd.DataFrame, **kwargs) -> Tuple:
        params = cls._parameters
        params.update(kwargs)

        return cls._function(data[cls._source], **params)

    @abstractmethod
    def check(self, *args, **kwargs):
        pass

    @staticmethod
    @abstractmethod
    def strength(*args, **kwargs) -> float:
        """ Determine strength of Trend """
        pass


INDICATOR = TypeVar('INDICATOR', bound=Indicator)


# noinspection PyUnusedLocal
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
        signal = frame['macdsignal']
        macd = frame['macd']

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

    @staticmethod
    def strength(frame: pd.DataFrame, *args, **kwargs) -> float:
        return 1


# noinspection PyUnusedLocal
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

        if hasattr(buy, '__iter__'):
            buy = buy[0]
        if hasattr(sell, '__iter__'):
            sell = sell[0]

        if rate <= buy:
            return Signal.BUY
        elif rate >= sell:
            return Signal.SELL
        else:
            return Signal.HOLD

    @staticmethod
    def strength(frame: pd.DataFrame, *args, **kwargs) -> float:
        return 1


# noinspection PyUnusedLocal
@dataclass
class STOCHRSIRow(Indicator):
    _function = STOCHRSI
    _parameters = {'timeperiod': 14, 'fastk_period': 3, 'fastd_period': 3}
    _source = 'close'

    fastk: float
    fastd: float

    @staticmethod
    def check(frame: pd.DataFrame, overbought: float = 20, oversold: float = 80, *args, **kwargs) -> Signal:
        fastk = frame['fastk']
        fastd = frame['fastd']

        if hasattr(fastk, '__iter__'):
            fastk = fastk[0]
        if hasattr(fastd, '__iter__'):
            fastd = fastd[0]

        if overbought > fastd > fastk:
            return Signal.BUY
        elif oversold < fastd < fastk:
            return Signal.SELL
        else:
            return Signal.HOLD

    @staticmethod
    def strength(frame: pd.DataFrame, *args, **kwargs) -> float:
        return 1


class IndicatorContainer(UserList[INDICATOR]):
    """ Container that abstracts concurrently using multiple indicators to derive a discrete decision.

    This can be used in `Strategy` to direct trade decisions, or can be used to indicate trends."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.graph: pd.DataFrame = self.container()
        """ Stores numeric indicator data for all bundled `Indicator` classes.
        
        Initially starts off as empty row with columns
        """

    def container(self, index: Optional[pd.Index] = None) -> pd.DataFrame:
        """ Generate and return an empty `DataFrame` with indicator columns.

        Notes:
            When indicators are ever added or removed, DataFrame shall be re-initialized by this function.

        Args:
            index:
                Valid pandas index sequence. Since pandas cannot expand `DataFrame` objects, this argument _must_
                be defined before any computation can be performed.
        """
        cols = []
        for cls in self.data:
            cols.extend(cls.columns())
        return pd.DataFrame(columns=cols, index=index)

    def develop(self, data: pd.DataFrame) -> NoReturn:
        """ Generate indicator data for all available given candle data.

        Used to update `self.graph` which is dedicated to store all indicator data and should only be updated
        by this method.

        Args:
            data: Candle data. Should be shortened (by not using older data) when speed becomes an issue
        """
        df = self.container(data.index)
        for indicator in self.data:
            _graph = indicator.process(data)
            for name, _data in zip(indicator.columns(), _graph):
                df[name] = _data
        self.graph = df

    def check(self, data: pd.DataFrame, point: pd.Timestamp = None, freq: str = False) -> Signal:
        """ Infer signals from indicators.

        Notes:
            Processing and computation of indicator data is handled by `self.develop()` and shall therefore
            not be called within this function.

            `point` needs to be manipulated before accessing intraday and daily candle data. When accessing Gemini
            6-hour market data, returned data is timestamped in intervals of 3, 9, 15, then 21. For daily data, returned
            data is timestamped at around 20:00 or 21:00. Timestamps might be tied to timezone differences, so
            data wrangling might need to be modified in the future. Maybe data wrangling could be avoided entirely by
            just using built-in resampling.

        Args:
            data:
                Market data. Passed for a reference for indicators to use.
            point:
                Point in time. Used during backtesting. Defaults to last frame in `self.graph`
            freq:
                pandas unit of frequency to select indicator data from greater time frequencies. Used by
                `TrendDetector` to prevent `KeyError` from being raised during simulation arising from larger timeframes
                of stored candle data (ie: 1day, 6hr, or 1hr)  and smaller timeframes used by strategy/backtesting
                functions (ie: 15min). If passed, `point` is rounded down to the largest frequency less than `point`
                (eg: 14:45 becomes 14:00). If not passed, `point` is untouched.

        Returns:
            Trade signal based on consensus from indicators.
        """
        # TODO: check that market data is not too ahead of computed indicators
        if point:

            # modify `point` to access correct timeframe
            if freq:
                if freq == '6H':
                    point = point.floor(freq)
                    if point.dst():
                        point -= pd.DateOffset(hours=3)
                    else:
                        point -= pd.DateOffset(hours=4)
                    point = point.floor('H', nonexistent='shift_backward')
                elif freq == '1D':
                    # TODO: faulty implementation
                    # Incorrect date is indexed immediately after daily candle data is released.
                    point = point.floor(freq) - pd.DateOffset(days=1)
                    point = point.strftime('%m/%d/%Y')          # generically select data
                else:
                    point = point.floor(freq, nonexistent='shift_backward')

                frame = self.graph.loc[point]

            else:
                frame = self.graph.loc[point]
        else:
            frame = self.graph.iloc[-1]
            point = frame.name

        # hack to get function working.
        # For some reason a sequence is getting passed to individual check functions instead of a primitive value
        _rate = data['close'].loc[point]
        if hasattr(_rate, '__iter__'):
            kwargs = {'rate': _rate[0]}
        else:
            kwargs = {'rate': _rate}

        signals = [indicator.check(frame, **kwargs) for indicator in self.data]
        # TODO: use dynamic number of array length
        if signals[0] == signals[1] == signals[2]:
            return signals[0]

        return Signal.HOLD

    def strength(self, point: pd.Timestamp = None):
        assert len(self.graph)
        if point:
            frame = self.graph.loc[point]
        else:
            frame = self.graph.iloc[-1]

        strengths = pd.Series([indicator.strength(frame) for indicator in self.data])
        return strengths.mean()
