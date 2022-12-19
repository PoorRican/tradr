import concurrent.futures
from math import floor
import pandas as pd
from typing import Mapping, Optional, NoReturn, Union

from core import MarketAPI, TZ
from models.indicators import *
from models import IndicatorContainer
from primitives import TrendMovement, MarketTrend


STRONG_THRESHOLD = 3
""" This is a scalar value which determines whether a trend is strong or not. """


class TrendDetector(object):
    """ An independent object that provides foresight by encapsulating analysis of market trends.

    Uses multiple long-term (ie: 1H, 6H, 1 day, 1 week) candle data to detect if market is trending up, down or is
    remaining constant. This data is used to better characterize enter/exit points and modulate trading amount (eg:
    buy more/sell less during downtrend, buy less/say more during uptrend).

    Methods:
        - `develop()`:
            Calculate all indicators
        - `characterize()`:
            Determine `MarketTrend` for a given point
    """

    _frequencies = ('30m', '1hr', '6hr', '1day')
    """ Frequencies to use for fetching candle data.
    
    Shall be ordered from shortest-to-longest.
    """

    def __init__(self, market: MarketAPI, threads: int = 0, lookback: int = 1):
        super().__init__()

        self.market = market
        self.threads = threads
        self.lookback = lookback

        self._indicators: Mapping[str, 'IndicatorContainer'] = self._create_indicator_container()
        """ Store indicator data as a mapping where each key is a frequency.
        """

    def _create_indicator_container(self) -> Mapping[str, 'IndicatorContainer']:
        indicators = {}
        for freq in self._frequencies:
            indicators[freq] = IndicatorContainer([MACDRow, BBANDSRow, STOCHRSIRow],
                                                  lookback=self.lookback, threads=self.threads)
        return indicators

    def candles(self, frequency: str) -> pd.DataFrame:
        """ Get cleaned candle data for a given frequency.

        Reduces boilerplate for returning cleaned data.
        """
        assert frequency in self._frequencies

        # fetch via multi-index
        return self.market.candles(frequency)

    def develop(self) -> NoReturn:
        """ Compute all indicator functions across all frequencies using existing candle data.

        Since smaller frequencies will have values with `NaN`, `get_candles()` must be called to drop
        null values.

        TODO:
            - Asynchronous
            - Threading
        """
        assert len(self._indicators) != 0

        if self.threads:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads * len(self._frequencies)) as executor:
                fs = []
                [fs.extend(executor.submit(container.develop,
                                           self.candles(freq)
                                           ).result()) for freq, container in self._indicators.items()]
                concurrent.futures.wait(fs)
        else:
            [container.develop(self.candles(freq)) for freq, container in self._indicators.items()]

    def _fetch_trends(self, point: pd.Timestamp = None,
                      executor: concurrent.futures.Executor = None) -> TrendMovement:
        """

        Args:
            point:
            executor:

        Notes:

        Returns:

        """
        if self.threads:
            fs = []
            [fs.extend(future) for future in [
                container.func_threads('signal', executor=executor,
                                       point=self.market.process_point(point, freq),
                                       candles=self.candles(freq)
                                       ) for freq, container in self._indicators.items()]]
            signals = pd.Series([future.result() for future in concurrent.futures.wait(fs)[0]])
        else:
            values = [container.signal(self.candles(freq),
                                       self.market.process_point(point, freq),
                                       ) for freq, container in self._indicators.items()]
            signals = pd.Series(values)

        return TrendMovement(signals.mode()[0])

    def _determine_scalar(self, point: Optional[pd.Timestamp] = None,
                          executor: concurrent.futures.Executor = None) -> int:
        if self.threads:
            fs = []
            [fs.extend(future) for future in [
                container.func_threads('strength', executor=executor,
                                       point=self.market.process_point(point, freq),
                                       candles=self.candles(freq)
                                       ) for freq, container in self._indicators.items()]]
            results = pd.Series([future.result() for future in concurrent.futures.wait(fs)[0]])
        else:
            values = [container.strength(self.candles(freq),
                                         self.market.process_point(point, freq),
                                         ) for freq, container in self._indicators.items()]
            results = pd.Series(values)

        # TODO: implement scalar weight. Longer frequencies influence scalar more heavily
        # TODO: only incorporate `strength()` from outputs from consensus
        return int(floor(results.mean())) or 1

    def characterize(self, point: Optional[pd.Timestamp] = None) -> MarketTrend:
        """ Characterize trend magnitude (direction and strength of trend).

        Indicators must be calculated via `develop()`, beforehand.

        Accomplished by numerically comparing indicator/oscillator values over time. The returned value is
        the most common value

        Other computations (ie: b/b power indicator or Hilbert transform) may be added to children and called.
        However, `compute()` must be called beforehand to calculate indicator values.

        TODO:
            -   introduce `unison` binary flag where indicators are run individually and the mean is returned when
                `unison == False`, or where consensus of `self.indicators.check()` is used when `unison == True`
            -   introduce `strength` binary flag to return values based on most extreme scalar (`abs` of `strength()`)
        """
        if not point:
            point = self.market.most_recent_timestamp

        # TODO: is reusing the name going to affect original `point`?
        # remove `freq` value to prevent `KeyError`
        if hasattr(point, 'timestamp'):
            point = pd.Timestamp.fromtimestamp(point.timestamp(), tz=TZ)

        if self.threads:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads * len(self._frequencies)) as executor:
                _signals = executor.submit(self._fetch_trends, point, executor)
                _strengths = executor.submit(self._determine_scalar, point, executor)

                concurrent.futures.wait([_signals, _strengths])
                signal = _signals.result()
                scalar = _strengths.result()
        else:
            signal = self._fetch_trends(point)
            scalar = self._determine_scalar(point)

        return MarketTrend(signal, scalar=scalar)

    def calculate_all(self):
        for freq, container in self._indicators.items():
            container.calculate_all(self.candles(freq))
