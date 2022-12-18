from abc import ABC, abstractmethod
import concurrent.futures
from enum import IntEnum
from math import fabs, ceil, isnan, floor
import matplotlib.pyplot as plt
import numpy as np
from typing import Sequence, ClassVar, Callable, Dict, NoReturn, Optional, Tuple, Union
import pandas as pd
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

    Indicator function is defined by `_function` and is called in `process()` which updates `graph`. `graph` is
    an internal DataFrame that stores the output of the indicator function on a time series that matches candle
    data that was supplied to `process()`. The values of `graph` are qualified by `_row_decision()` and
    `_row_strength()`, both of which accept a row of `graph` and return a value of `Signal` or float respectively.

    During backtesting, `process()` should be called with all available market data; during a live implementation,
    `process()` should be called with buffered candle data and `graph` will be updated atomically. Likewise, during
    backtesting, computation of graph can be vectorized by calling `calculate_all()`, which uses `computed` to store
    results of `Signal` and strength. Otherwise, as `graph` is atomically updated during a live implementation,
    `signal()` and `strength()` can both be given a point to derive their respective values from `graph`.
    """
    name: ClassVar[str]
    _function: ClassVar[Callable]
    """ indicator function that is passed a single column of candle data, and ambiguous keyword arguments. """

    _parameters: ClassVar[Dict] = {}
    """ Ambiguous parameters for `_function` """

    _source: ClassVar[Dict] = 'close'
    """ Stores which column of input candle data to use.
    """

    columns = ClassVar[Tuple[str, ...]]

    def __init__(self, index: pd.Index = None, lookback: int = 0):
        self._lookback = lookback

        self.graph = self.container(index)
        self.computed = self.container(index, columns=('signal', 'strength'))

    @classmethod
    def container(cls, index: pd.Index = None, data: Tuple = None, columns: Sequence[str] = None) -> pd.DataFrame:
        if data:
            _data = [i.values.T for i in data]
            _dict = {}
            for name, _col in zip(cls.columns, _data):
                _dict[name] = _col
            return pd.DataFrame(_dict, index=index, dtype=float)

        if columns is None:
            columns = cls.columns

        return pd.DataFrame(index=index, columns=list(columns), dtype=float)

    def process(self, data: pd.DataFrame, **kwargs) -> NoReturn:
        """ Prepares internal `computed` and incoming data.

        In the future, data will be atomically updated with incoming data

        Args:
            data:
                New candle data to process.
            **kwargs:

        Returns:

        """
        # TODO: make async

        # new or empty rows get updated
        _index = list(data.index.values)
        _index.extend(list(self.graph.values))
        if type(data.index) == pd.DatetimeIndex:
            _index = pd.DatetimeIndex(_index)
        else:
            _index = pd.Index(_index)

        if len(_index.notna()) != len(_index):
            raise ValueError('Resulting index contains a date-gap')

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

    def calculate_all(self, candles: pd.DataFrame) -> NoReturn:
        assert len(self.graph)

        # TODO: vectorizing computation across columns should provide greater speed increase

        self.computed['signal'] = self.graph.apply(self._row_decision, axis='columns', candles=candles)
        self.computed['strength'] = self.graph.apply(self._row_strength, axis='columns', candles=candles)

    @abstractmethod
    def _row_decision(self, row: Union['pd.Series', 'pd.DataFrame'], candles: pd.DataFrame) -> Signal:
        pass

    def signal(self, point: pd.Timestamp, candles: pd.DataFrame) -> Signal:
        """ Return `Signal` from `point`.

        First, `computed` is checked to see if a value has been calculated. If not, decision is calculated
        from `graph`, added to `computed`, then returned.

        A look-back has been implemented which checks the previous row, to see both signals are equal. This is to
        prevent both false-positives and add a delay to "catch" optimal price movements.

        Args:
            point:
                Point in time to get `Signal` from.
            candles:
                Available market candle data. Is needed for certain instances of `_row_decision()`.

        Returns:
            `Signal` derived from `_function` at the given `point`.
        """
        if point in self.computed.index:
            signal = self.computed.loc[point, 'signal']
            if not isnan(signal):
                signal = int(signal)
                idx: Union[int, slice] = self.computed.index.get_loc(point)
                if isinstance(idx, slice):
                    start = idx.start
                    stop = idx.stop
                else:
                    start = idx
                    stop = idx + 1

                if start > 0:
                    lookback = self._lookback
                    if start < self._lookback:
                        lookback = idx
                    prev_points = slice(start - lookback, stop)
                    prev = self.computed[prev_points]
                    prev = prev['signal']
                    avg = int(prev.mean())
                    if floor(abs(avg)):
                        return Signal(avg)
                else:
                    return Signal(signal)
            return Signal.HOLD

        assert len(self.graph)

        row = self.graph.loc[point]
        decision = self._row_decision(row, candles)
        self.computed.loc[point, 'signal'] = decision
        return decision

    def strength(self, point: pd.Timestamp, candles: pd.DataFrame) -> float:
        """ Return strength from `point`.

        First, `computed` is checked to see if a value has been calculated. If not, strength is calculated
        from `graph`.

        Args:
            point:
                Point in time to get `Signal` strength from.
            candles:
                Available market candle data. Is needed for certain instances of `_row_strength()`.

        Returns:
            `Signal` strength derived from `_function` at the given `point`.
        """
        if point in self.computed.index:
            strength = self.computed.loc[point, 'strength']
            if not isnan(strength):
                return float(strength)

        assert len(self.graph)

        if point:
            row = self.graph.loc[point]
        else:
            row = self.graph.iloc[-1]

        return self._row_strength(row, candles)

    @abstractmethod
    def _row_strength(self, row: Union['pd.Series', 'pd.DataFrame'], candles: pd.DataFrame) -> float:
        pass


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


class IndicatorContainer(object):
    """ Container that abstracts concurrently using multiple indicators to derive a discrete decision.

    Primary purpose is to wrap multiple `Indicator` instances and connect them with market data.

    This can be used in `Strategy` to direct trade decisions, or can be used to indicate trends."""
    def __init__(self, indicators: Sequence[type(Indicator)], index: Optional[pd.Index] = None, lookback: int = 0,
                 threads: int = 4):
        self.indicators = [i(index, lookback) for i in indicators]
        self.threads = threads

    def develop_threads(self, executor, candles: pd.DataFrame) -> Sequence['concurrent.futures.Future']:
        fs = [executor.submit(indicator.process, candles) for indicator in self.indicators]
        return fs

    def develop(self, data: pd.DataFrame, buffer: bool = False,
                executor: concurrent.futures.Executor = None) -> NoReturn:
        """ Generate indicator data for all available given candle data.

        Used to update `self.graph` which is dedicated to store all indicator data and should only be updated
        by this method.

        Args:
            data:
                Candle data. Should be shortened (by not using older data) when speed becomes an issue
            buffer:
                Flag that turns buffering on or off
            executor:
                Optional argument when this function call is nested in a threaded call tree.
        """

        _buffer_len = 50
        if len(data) < _buffer_len or not buffer:
            _buffer = data
        else:
            _buffer = data.iloc[_buffer_len - 1:]

        if self.threads:

            if executor is not None:
                self.develop_threads(executor, _buffer)
                return None

            with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads) as executor:
                # Start the load operations and mark each future with its URL
                fs = self.develop_threads(executor, _buffer)
                concurrent.futures.wait(fs)
                # result would be accessible by `future.result for future in ...`
        else:
            [i.process(_buffer) for i in self.indicators]

    @property
    def graph(self) -> pd.DataFrame:
        return pd.concat([i.graph for i in self.indicators], axis='columns')

    @property
    def computed(self) -> pd.DataFrame:
        return pd.concat([i.computed for i in self.indicators], axis='columns',
                         keys=[i.name for i in self.indicators])

    def plot(self):
        assert self.computed.index.equals(self.graph.index)

        index = self.computed.index
        plt.figure(figsize=[50, 25], dpi=250)
        plt.hist(index, self.computed.xs('strength', axis=1, level=1).mean(axis='columns'))
        plt.bar(index, self.computed.xs('signal', axis=1, level=1).mean(axis='columns'))

    def signal_threads(self, executor: concurrent.futures.Executor, point: pd.Timestamp, data: pd.DataFrame) \
            -> Sequence[concurrent.futures.Future]:
        """ Add function calls to `Indicator.signal()` to `executor`. """
        return [executor.submit(indicator.signal, point, data) for indicator in self.indicators]

    def signal(self, data: pd.DataFrame, point: pd.Timestamp = None,
               executor: concurrent.futures.Executor = None) -> Union['Signal', None]:
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
            executor:
                Optional argument when this function call is nested in a threaded call tree.

        Returns:
            Trade signal based on consensus from indicators.
        """
        # TODO: check that market data is not too ahead of computed indicators

        # initialize executor or run on single thread
        if self.threads:

            # add to `executor` if not passed
            if executor is not None:
                self.signal_threads(executor, point, data)
                return None

            with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads) as executor:
                fs = self.signal_threads(executor, point, data)
                signals = pd.Series([future.result() for future in concurrent.futures.wait(fs)[0]])

        else:
            signals = pd.Series([i.signal(point, data) for i in self.indicators])

        if len(signals.unique()) == 1:
            return Signal(signals.mode()[0])

        return Signal.HOLD

    def strength_threads(self, executor: concurrent.futures.Executor, point: pd.Timestamp, data: pd.DataFrame) \
            -> Sequence[concurrent.futures.Future]:
        """ Add function calls to `Indicator.strength()` to `executor`. """
        return [executor.submit(indicator.strength, point, data) for indicator in self.indicators]

    def strength(self, data: pd.DataFrame, point: pd.Timestamp = None,
                 executor: concurrent.futures.Executor = None) -> Union[float, None]:
        # TODO: ensure that market data is not too ahead of computed indicators

        # initialize executor or run on single thread
        if self.threads:

            # add to `executor` if not passed
            if executor is not None:
                self.strength_threads(executor, point, data)
                return None

            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                fs = [executor.submit(indicator.strength, point, data) for indicator in self.indicators]
                # TODO: use dynamic number of array length
                strengths = pd.Series([future.result() for future in concurrent.futures.wait(fs)[0]])

        else:
            strengths = pd.Series([i.strength(point, data) for i in self.indicators])

        return strengths.mean()

    def calculate_all(self, candles: pd.DataFrame):
        for indicator in self.indicators:
            indicator.process(candles)
            indicator.calculate_all(candles)
