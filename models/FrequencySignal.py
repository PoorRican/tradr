import concurrent.futures
from math import nan
import pandas as pd
from typing import List, NoReturn, Union

from core import MarketAPI
from misc import TZ
from models import Indicator
from primitives import Signal


class FrequencySignal(object):
    """ Functor which derives a discrete trade signal from a given point of market data.

    This object serves to wrap several `Indicator` objects then handle their concurrent outputs, and connect them to
    a market object.

    Trade signal is derived from a combination of `Indicator` objects. Signal can be determined from
    a majority of returned outputs or in unison, controlled by the `unison` flag. In essence, `FrequencySignal`
    serves as a container for `Indicator` objects. This can be used in `Strategy` to direct trade decisions, or can be
    used to indicate trends.

    New incoming data is processed by `update()`.
    """
    market: 'MarketAPI'
    indicators: List[Indicator]
    threads: int
    executor: concurrent.futures.Executor
    last_update: Union['pd.Timestamp', None]

    def __init__(self, market: 'MarketAPI', freq: 'str', indicators: List[Indicator],
                 unison: bool = False, update: bool = True, lookback: int = 1,
                 executor: concurrent.futures.Executor = None, threads: int = 16):
        """ Set up container for `Indicator`

        Args:
            market:
                Platform market to operate on.
            freq:
                Frequency of candle data to operate on. This must be in `market.valid_freqs`.
            indicators:
                Instantiated `Indicator` objects
            unison: (optional)
                Flag to require a consensus among all indicators.
            update: (optional)
                Flag to auto-compute upon startup. Uses existing market data. Also, disables checking of stale data.
            lookback: (optional)
                Number of signal repetition to convert signal time-series data to `Signal` objects.
            executor: (optional)
                Should be passed if called alongside other `FrequencySignal` objects.
            threads:
                Total number of threads to use. `0` disables threading and is meant to be passed while debugging.
        """
        self.market = market
        self.threads = threads
        self.indicators = indicators
        self.unison = unison
        self.lookback = lookback
        self.executor = executor
        self.last_update = None
        self.freq = freq

        self._update: bool = update
        if update:
            self.update()

        self.timeout = pd.Timedelta(self.market.translate_period(self.freq))

    def __getitem__(self, item: Union[int, type(Indicator)]) -> Indicator:
        if type(item) == int:
            return self.indicators[item]
        elif issubclass(item, Indicator):
            return self.indicators[self.find(item)]
        else:
            raise ValueError("Incorrect type for `item` argument")

    @property
    def candles(self):
        return self.market.candles(self.freq)

    @property
    def graph(self) -> pd.DataFrame:
        """ Return aggregation of `graph` dataframes.

        Notes:
            This must only be used for analysis and must not be used for computation as this is
            computationally expensive.
        """
        return pd.concat([i.graph for i in self.indicators], axis='columns')

    @property
    def computed(self) -> pd.DataFrame:
        """ Return aggregation of `computed` dataframes.

        Notes:
            This must only be used for analysis and must not be used for computation as this is
            computationally expensive.
        """
        return pd.concat([i.computed for i in self.indicators], axis='columns',
                         keys=[i.name for i in self.indicators])

    def __call__(self, point: Union['pd.Timestamp', str]):
        """ Return signal and strength at given point """
        if type(point) == str:
            point = pd.Timestamp(point, tz=TZ)
        if self._update and self.timeout > point - self.last_update:
            self.update()
        signal = self.signal(point)
        _strengths = pd.Series([i.strength(point, self.candles) for i in self.indicators])
        signals = pd.Series([i.signal(point, self.candles) for i in self.indicators])
        if signal != Signal.HOLD:
            strength = _strengths[signals == signal].mean()
        else:
            strength = nan
        return signal, strength

    def update(self):
        """ Process new, incoming data.

        This serves as a wrapper for both `_process()` and `_compute()`, which both serve to populate the
        `graph` and `computed` DataFrame containers respectively. Therefore, both functions shouldn't be
        called outside.

        Buffering should be accomplished here since each instance directly accesses candle data and there
        shouldn't be any redundant access to specific frequency candle data outside of this functor.
        """
        self.last_update = self.candles.index[-1]
        self._process(self.candles)
        self._compute(self.candles)

    def _compute(self, data: pd.DataFrame, buffer: bool = False,
                 executor: concurrent.futures.Executor = None) -> NoReturn:
        """ Compute signals from indicator graph data

        Used to update `self.computed` which is dedicated to store all indicator data and should only be updated
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
        if not buffer or len(data) < _buffer_len:
            _buffer = data
        else:
            _buffer = data.iloc[_buffer_len - 1:]

        if self.threads:

            if executor is not None:
                self.func_threads('compute', executor=executor, candles=_buffer)

            with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads) as executor:
                # Start the load operations and mark each future with its URL
                fs = self.func_threads('compute', executor=executor, candles=_buffer)
                concurrent.futures.wait(fs)
                # result would be accessible by `future.result for future in ...`
        else:
            [i.compute(_buffer) for i in self.indicators]

    def _process(self, data: pd.DataFrame, buffer: bool = False,
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
        if not buffer or len(data) < _buffer_len:
            _buffer = data
        else:
            _buffer = data.iloc[_buffer_len - 1:]

        if self.threads:

            if executor is not None:
                self.func_threads('process', executor=executor, candles=_buffer)

            with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads) as executor:
                # Start the load operations and mark each future with its URL
                fs = self.func_threads('process', executor=executor, candles=_buffer)
                concurrent.futures.wait(fs)
                # result would be accessible by `future.result for future in ...`
        else:
            [i.process(_buffer) for i in self.indicators]

    def func_threads(self, func: str, *args, executor: concurrent.futures.Executor = None, **kwargs) \
            -> List[concurrent.futures.Future]:
        """ Add function calls to `executor`. """
        if executor is None:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads) as _executor:
                return self.func_threads(func, *args, executor=_executor, **kwargs)

        return [executor.submit(getattr(indicator, func), *args, **kwargs) for indicator in self.indicators]

    def signal(self, point: pd.Timestamp = None,
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
                                   point=point, candles=self.candles)
            signals = pd.Series([future.result() for future in concurrent.futures.wait(fs)[0]])

        else:
            signals = pd.Series([i.signal(point, self.candles) for i in self.indicators])

        unique = len(signals.unique())
        if (self.unison and unique == 1) or (not self.unison and unique <= len(self.indicators) - 1):
            return Signal(signals.mode()[0])

        return Signal.HOLD

    def strength(self, signal: Signal, point: pd.Timestamp = None,
                 executor: concurrent.futures.Executor = None) -> Union[float, None]:
        # TODO: ensure that market data is not too ahead of computed indicators

        # initialize executor or run on single thread
        if self.threads:
            self.func_threads('strength', executor=executor,
                              point=point, candles=self.candles)
            return None

        else:
            strengths = pd.Series([i.strength(point, self.candles) for i in self.indicators])
            signals = pd.Series([i.signal(point, self.candles) for i in self.indicators])
            return strengths[signals == signal].mean()

    def find(self, cls: type(Indicator)) -> Union[int, 'False']:
        for i, _instance in enumerate(self.indicators):
            # noinspection PyTypeChecker
            if isinstance(_instance, cls):
                return i
        return False

    def isin(self, cls: type(Indicator)) -> bool:
        return type(self.find(cls)) == int
