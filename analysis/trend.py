import concurrent.futures
from math import floor, nan
import pandas as pd
from typing import Mapping, Optional, NoReturn, Union

from core import MarketAPI
from misc import TZ
from models.indicators import MACDRow, BBANDSRow, STOCHRSIRow
from models import IndicatorContainer
from primitives import TrendDirection, MarketTrend, Signal


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

    @property
    def graph(self):
        return pd.concat([i.graph for i in self._indicators.values()], keys=self._frequencies)

    @property
    def computed(self):
        return pd.concat([i.computed for i in self._indicators.values()], keys=self._frequencies)

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

    def _fetch_trend(self, point: pd.Timestamp,
                     executor: concurrent.futures.Executor = None,
                     raw: bool = False) -> Union['TrendDirection', 'pd.Series']:
        """

        Args:
            point:
                Point in time to evaluate.
            executor:
            raw (False):
                Option flag to return values of all indicators instead of a singular value. This would be used for
                masking or building a 3d-array of candles for given point. This defaults to false.

        Notes:

        Returns:
            If `raw` is True, a `pd.Series` of `Signal` values are returned. Otherwise, the mode of such a `Series` is
            returned.
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

        if raw:
            return signals

        return TrendDirection(signals.mode()[0])

    def _determine_scalar(self, trend: TrendDirection, point: Optional[pd.Timestamp],
                          executor: concurrent.futures.Executor = None) -> float:
        signal: Signal = Signal(trend)

        # no strength should be returned if there is no price movement
        if signal == Signal.HOLD:
            return nan

        if self.threads:
            fs = []
            [fs.extend(future) for future in [
                container.func_threads('strength', executor=executor,
                                       point=self.market.process_point(point, freq),
                                       candles=self.candles(freq),
                                       signal=signal
                                       ) for freq, container in self._indicators.items()]]
            results = pd.Series([future.result() for future in concurrent.futures.wait(fs)[0]])
        else:
            values = [container.strength(signal, self.candles(freq),
                                         self.market.process_point(point, freq)
                                         ) for freq, container in self._indicators.items()]
            results = pd.Series(values)

        _row = pd.DataFrame([container.computed.loc[self.market.process_point(point, freq)]
                             for freq, container in self._indicators.items()])
        _signals = _row.xs('signal', axis='columns', level=1)
        signals: pd.Series = _signals.mode(axis='columns')[0]
        assert signals.shape == results.shape
        signals.index = [i for i in range(len(results))]

        # TODO: implement scalar weight. Longer frequencies influence scalar more heavily
        return int(floor(results[signals == signal].dropna().mean()))

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

        # remove `freq` value to prevent `KeyError`
        if hasattr(point, 'timestamp'):
            point = pd.Timestamp.fromtimestamp(point.timestamp(), tz=TZ)

        if self.threads:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads * len(self._frequencies)) as executor:
                _trend = executor.submit(self._fetch_trend, point, executor)
                concurrent.futures.wait([_trend])
                trend = _trend.result()

                _strengths = executor.submit(self._determine_scalar, trend, point, executor)
                concurrent.futures.wait([_strengths])
                scalar = _strengths.result()
        else:
            trend = self._fetch_trend(point)
            scalar = self._determine_scalar(trend, point)

        return MarketTrend(trend, scalar=scalar)

    def calculate_all(self):
        for freq, container in self._indicators.items():
            container.calculate_all(self.candles(freq))
