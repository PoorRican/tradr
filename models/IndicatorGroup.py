import concurrent.futures
from math import nan
import pandas as pd
from typing import List, NoReturn, Union

from core import MarketAPI
from misc import TZ
from models import Indicator
from primitives import Signal


class IndicatorGroup(object):
    """ A functor for interacting with multiple `Indicator` objects.

    This class is meant to be used as a wrapper for multiple `Indicator` objects. It wraps the ability to update
    indicator and signal data. It also provides functions for determining consensus among indicators.
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
                Should be passed if called alongside other `IndicatorGroup` objects.
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
        """ Return signal and strength at given point
        """
        if type(point) == str:
            point = pd.Timestamp(point, tz=TZ)
        if self._update and self.timeout > point - self.last_update:
            self.update()

        _point = self.market.process_point(point, freq=self.freq)
        signal = self.signal(_point)
        _strengths = pd.Series([i.strength(_point, self.candles) for i in self.indicators])
        signals = pd.Series([i.signal(_point, self.candles) for i in self.indicators])
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

        If `buffer` is `True`, then only the last 50 rows of `data` is used to compute indicator data. This is
        to prevent redundant computation of data that has already been computed.

        If `threads` is a positive integer, then computation is done in parallel. Otherwise, computation is done
        serially.

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
        """ Generate indicator data for given candle data.

        This is used to update `self.graph` for each indicator.

        Args:
            data:
                Candle data.
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

    @staticmethod
    def _conflicting_signals(signals: pd.Series) -> bool:
        """ Return `True` if `signals` contains both BUY AND SELL.

        Notes:
            It might be smart to define a signals as a discreet object which contains mundane instance
            methods such as these.
        """
        return Signal.BUY in signals.values and Signal.SELL in signals.values

    def _consensus(self, signals: pd.Series) -> bool:
        """ Determines if there is sufficient consensus from given `signals`.

        `unison` flag is taken into account.

        Returns:
            True if signals agree, otherwise False
        """
        unique = len(signals.unique())
        if self.unison and unique == 1:
            pass
        elif not self.unison and unique <= len(self.indicators) - 1 and \
                not self._conflicting_signals(signals):
            pass
        else:
            return False
        return True

    def signal(self, point: pd.Timestamp = None,
               executor: concurrent.futures.Executor = None) -> Union['Signal', None]:
        """ Infer signals from indicators.

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
        if self.threads and False:
            fs = self.func_threads('signal', executor=executor,
                                   point=point, candles=self.candles)
            signals = pd.Series([future.result() for future in concurrent.futures.wait(fs)[0]])

        else:
            signals = pd.Series([i.signal(point, self.candles) for i in self.indicators])

        if self._consensus(signals):
            return Signal(signals.mode()[0])

        return Signal.HOLD

    def strength(self, signal: Signal, point: pd.Timestamp = None,
                 executor: concurrent.futures.Executor = None) -> Union[float, None]:
        # TODO: ensure that market data is not too ahead of computed indicators
        if signal is Signal.HOLD:
            return nan

        # initialize executor or run on single thread
        if self.threads and False:
            self.func_threads('strength', executor=executor,
                              point=point, candles=self.candles)
            return None

        else:
            strengths = pd.Series([i.strength(point, self.candles) for i in self.indicators])
            signals = pd.Series([i.signal(point, self.candles) for i in self.indicators])

            _mean = strengths[signals == signal].mean()
            if _mean < 1:
                return 1
            return _mean

    def find(self, cls: type(Indicator)) -> Union[int, 'False']:
        for i, _instance in enumerate(self.indicators):
            # noinspection PyTypeChecker
            if isinstance(_instance, cls):
                return i
        return False

    def isin(self, cls: type(Indicator)) -> bool:
        return type(self.find(cls)) == int
