import concurrent.futures
from typing import Sequence, Optional, NoReturn, Union

import pandas as pd
from matplotlib import pyplot as plt

from models.indicator import Indicator
from primitives import Signal


class IndicatorContainer(object):
    """ Container that abstracts concurrently using multiple indicators to derive a discrete decision.

    Primary purpose is to wrap multiple `Indicator` instances and connect them with market data.

    This can be used in `Strategy` to direct trade decisions, or can be used to indicate trends."""
    def __init__(self, indicators: Sequence[type(Indicator)], index: Optional[pd.Index] = None, lookback: int = 0,
                 threads: int = 4, unison: bool = False):
        self.indicators: Sequence[Indicator] = [i(index, lookback) for i in indicators]
        self.threads = threads
        self.unison: bool = unison
        """ Optional flag used to require or disable that all `Indicator` objects return the same `Signal` """

    def find(self, cls: type(Indicator)) -> Union[int, 'False']:
        for i, _instance in enumerate(self.indicators):
            # noinspection PyTypeChecker
            if isinstance(_instance, cls):
                return i
        return False

    def isin(self, cls: type(Indicator)) -> bool:
        return type(self.find(cls)) == int

    def develop_threads(self, executor, candles: pd.DataFrame) -> Sequence['concurrent.futures.Future']:
        fs = [executor.submit(indicator.process, candles) for indicator in self.indicators]
        return fs

    def __getitem__(self, item: Union[int, type(Indicator)]) -> Indicator:
        if type(item) == int:
            return self.indicators[item]
        elif issubclass(item, Indicator):
            return self.indicators[self.find(item)]
        else:
            raise ValueError("Incorrect type for `item` argument")

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

    def func_threads(self, func: str, *args, executor: concurrent.futures.Executor = None, **kwargs) \
            -> Sequence[concurrent.futures.Future]:
        """ Add function calls to `executor`. """
        if executor is None:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads) as _executor:
                return self.func_threads(func, *args, executor=_executor, **kwargs)

        return [executor.submit(getattr(indicator, func), *args, **kwargs) for indicator in self.indicators]

    def signal(self, candles: pd.DataFrame, point: pd.Timestamp = None,
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
            candles:
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
            fs = self.func_threads('signal', executor=executor,
                                   point=point, candles=candles)
            signals = pd.Series([future.result() for future in concurrent.futures.wait(fs)[0]])

        else:
            signals = pd.Series([i.signal(point, candles) for i in self.indicators])

        unique = len(signals.unique())
        if (self.unison and unique == 1) or (not self.unison and unique <= len(self.indicators) - 1):
            return Signal(signals.mode()[0])

        return Signal.HOLD

    def _strength_threads(self, executor: concurrent.futures.Executor, point: pd.Timestamp, data: pd.DataFrame) \
            -> Sequence[concurrent.futures.Future]:
        """ Add function calls to `Indicator.strength()` to `executor`. """
        return [executor.submit(indicator.strength, point, data) for indicator in self.indicators]

    def strength(self, candles: pd.DataFrame, point: pd.Timestamp = None,
                 executor: concurrent.futures.Executor = None) -> Union[float, None]:
        # TODO: ensure that market data is not too ahead of computed indicators

        # initialize executor or run on single thread
        if self.threads:
            self.func_threads('strength', executor=executor,
                              point=point, candles=candles)
            return None

        else:
            strengths = pd.Series([i.strength(point, candles) for i in self.indicators])

        return strengths.mean()

    def calculate_all(self, candles: pd.DataFrame):
        for indicator in self.indicators:
            indicator.process(candles)
            indicator.calculate_all(candles)
