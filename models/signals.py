from abc import ABC, abstractmethod
from enum import IntEnum
from typing import Sequence, ClassVar, Callable, Dict, NoReturn, Optional, Tuple
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

    columns = ClassVar[Tuple[str, ...]]

    def __init__(self, index: pd.Index = None):
        self.graph = self.container(index)

    @classmethod
    def container(cls, index: pd.Index = None, data: Tuple = None) -> pd.DataFrame:
        if data:
            _data = [i.values.T for i in data]
            _dict = {}
            for name, _col in zip(cls.columns, _data):
                _dict[name] = _col
            return pd.DataFrame(_dict, index=index, dtype=float)
        return pd.DataFrame(index=index, columns=list(cls.columns), dtype=float)

    def process(self, data: pd.DataFrame, **kwargs) -> NoReturn:
        """

        Args:
            data:
                New candle data to process.
            **kwargs:

        Returns:

        """
        # TODO: make async

        # new or empty rows get updated
        # TODO: raise an error if there is a gap
        _index = list(data.index.values)
        _index.extend(list(self.graph.values))
        if type(data.index) == pd.DatetimeIndex:
            _index = pd.DatetimeIndex(_index)
        else:
            _index = pd.Index(_index)
        _not_empty = self.graph.notna()
        updates = _not_empty.index.isin(_index)

        # setup and run indicator function
        params = self._parameters
        params.update(kwargs)

        _output = self.__class__._function(data[self._source], **params)
        buffer = self.container(data.index, _output)

        # TODO: atomically update graph
        # self.graph = pd.concat([self.graph, buffer.loc[updates.values]])
        self.graph = buffer

    @abstractmethod
    def check(self, point: pd.Timestamp, candles: pd.DataFrame) -> Signal:
        pass

    # @abstractmethod
    def strength(self, point: pd.Timestamp, *args, **kwargs) -> float:
        """ Determine strength of Trend """
        assert len(self.graph)

        # if point:
        #     frame = self.graph.loc[point]
        # else:
        #     frame = self.graph.iloc[-1]
        return 1


class MACDRow(Indicator):
    _function = MACD
    _parameters = {'fastperiod': 6, 'slowperiod': 26, 'signalperiod': 9}
    _source = 'close'

    columns = ('macd', 'macdsignal', 'macdhist')

    def check(self, point: pd.Timestamp, *args, **kwargs) -> Signal:
        frame = self.graph.loc[point]

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


# noinspection PyUnusedLocal
class BBANDSRow(Indicator):
    _function = BBANDS
    _parameters = {'timeperiod': 20}
    _source = 'close'
    columns = ('upperband', 'middleband', 'lowerband')

    def __init__(self, *args, threshold: float = 0.5, **kwargs):
        super().__init__(*args, **kwargs)
        self.threshold = threshold

    def check(self, point: pd.Timestamp, candles: pd.DataFrame) -> Signal:
        rate = float(candles.loc[point, self._source])
        frame = self.graph.loc[point]

        buy = frame['middleband'] - frame['lowerband']
        sell = frame['upperband'] - frame['middleband']

        buy *= self.threshold
        sell *= self.threshold

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


# noinspection PyUnusedLocal
class STOCHRSIRow(Indicator):
    _function = STOCHRSI
    _parameters = {'timeperiod': 14, 'fastk_period': 3, 'fastd_period': 3}
    _source = 'close'

    columns = ('fastk', 'fastd')

    def __init__(self, *args, overbought: float = 20, oversold: float = 80, **kwargs):
        super().__init__(*args, **kwargs)
        self.overbought = overbought
        self.oversold = oversold

    def check(self, point: pd.Timestamp, *args, **kwargs) -> Signal:
        frame = self.graph.loc[point]
        fastk = frame['fastk']
        fastd = frame['fastd']

        if hasattr(fastk, '__iter__'):
            fastk = fastk[0]
        if hasattr(fastd, '__iter__'):
            fastd = fastd[0]

        if self.overbought > fastd > fastk:
            return Signal.BUY
        elif self.oversold < fastd < fastk:
            return Signal.SELL
        else:
            return Signal.HOLD


class IndicatorContainer(object):
    """ Container that abstracts concurrently using multiple indicators to derive a discrete decision.

    Primary purpose is to wrap multiple `Indicator` instances and connect them with market data.

    This can be used in `Strategy` to direct trade decisions, or can be used to indicate trends."""
    def __init__(self, indicators: Sequence[type(Indicator)], index: Optional[pd.Index] = None):
        self.indicators = [i(index) for i in indicators]

    def develop(self, data: pd.DataFrame, buffer: bool = False) -> NoReturn:
        """ Generate indicator data for all available given candle data.

        Used to update `self.graph` which is dedicated to store all indicator data and should only be updated
        by this method.

        Args:
            data:
                Candle data. Should be shortened (by not using older data) when speed becomes an issue
            buffer:
                Flag that turns buffering on or off
        """
        _buffer_len = 50
        if len(data) < _buffer_len or not buffer:
            _buffer = data
        else:
            _buffer = data.iloc[_buffer_len - 1:]

        for indicator in self.indicators:
            indicator.process(_buffer)

    @property
    def graph(self) -> pd.DataFrame:
        return pd.concat([i.graph for i in self.indicators], axis='columns')

    def check(self, data: pd.DataFrame, point: pd.Timestamp = None) -> Signal:
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

        Returns:
            Trade signal based on consensus from indicators.
        """
        # TODO: check that market data is not too ahead of computed indicators

        # hack to get function working.
        # For some reason a sequence is getting passed to individual check functions instead of a primitive value

        signals = [indicator.check(point, data) for indicator in self.indicators]
        # TODO: use dynamic number of array length
        if signals[0] == signals[1] == signals[2]:
            return signals[0]

        return Signal.HOLD

    def strength(self, point: pd.Timestamp = None):
        strengths = pd.Series([indicator.strength(point) for indicator in self.indicators])
        return strengths.mean()
