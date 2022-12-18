import concurrent.futures
from math import floor
import pandas as pd
from typing import Mapping, Optional, NoReturn, Union

from core import MarketAPI, TZ
from models.indicators import *
from models import IndicatorContainer
from primitives import TrendMovement, MarketTrend


class TrendDetector(object):
    """ An independent object that provides foresight by encapsulating analysis of market trends.

    Candle data is stored internally in `_candles`

    Uses multiple long-term (ie: 1H, 6H, 1 day, 1 week) candle data to detect if market is trending up, down or is
    remaining constant. This data is used to better characterize enter/exit points and modulate trading amount (eg:
    buy more/sell less during downtrend, buy less/say more during uptrend).

    Methods:
        - `update_candles()`:
            Update internal candle data
        - `develop()`:
            Calculate all indicators
        - `characterize()`:
            Determine `MarketTrend` for a given point
    """

    _frequencies = ('1hr', '6hr', '1day')
    """ Frequencies to use for fetching candle data.
    
    Shall be ordered from shortest-to-longest.
    """

    def __init__(self, market: MarketAPI, threads: int = 0, **kwargs):
        super().__init__(**kwargs)

        self.market = market
        self.threads = threads

        self._indicators: Mapping[str, 'IndicatorContainer'] = self._create_indicator_container()
        """ Store indicator data as a mapping where each key is a frequency.
        """

    def _create_indicator_container(self) -> Mapping[str, 'IndicatorContainer']:
        indicators = {}
        for freq in self._frequencies:
            indicators[freq] = IndicatorContainer([MACDRow, BBANDSRow, STOCHRSIRow], lookback=1, threads=self.threads)
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

    def _process_point(self, point: pd.Timestamp, freq: str) -> Union['pd.Timestamp', str]:
        """ Quantize and shift timestamp `point` for parsing market specific data.

        Used for indexing higher level frequencies

        Args:
            point:
                timestamp to use to index `_indicators`
            freq:
                Unit of time to quantize to. Should be written
        """
        # modify `point` to access correct timeframe
        _freq = self.market.translate_period(freq)      # `DateOffset` conversion
        _point = point.floor(_freq, nonexistent='shift_backward')
        if _freq == '6H':
            if _point.dst():
                _point -= pd.DateOffset(hours=3)
            else:
                _point -= pd.DateOffset(hours=4)
            _point = _point.floor('H', nonexistent='shift_backward')
        elif _freq == '1D':
            # TODO: faulty implementation
            # Incorrect date is indexed immediately after daily candle data is released.
            _point -= pd.DateOffset(days=1)
            _point = point.strftime('%m/%d/%Y')          # generically select data
        return _point

    def _fetch_trends(self, point: pd.Timestamp = None,
                      executor: concurrent.futures.Executor = None) -> TrendMovement:
        """

        Args:
            point:
            executor:

        Notes:
            Frequency needs to be modified before accessing indicator graphs. A pandas unit of frequency to select
            indicator data from greater time frequencies needs to be passed as `point`. Used by
            `TrendDetector` to prevent `KeyError` from being raised during simulation arising from larger timeframes
            of stored candle data (ie: 1day, 6hr, or 1hr)  and smaller timeframes used by strategy/backtesting
            functions (ie: 15min). If passed, `point` is rounded down to the largest frequency less than `point`
            (eg: 14:45 becomes 14:00). If not passed, `point` is untouched.

        Returns:

        """
        if self.threads:
            fs = []
            [fs.extend(future) for future in [
                container.func_threads('signal', executor=executor,
                                       point=self._process_point(point, freq),
                                       candles=self.candles(freq)
                                       ) for freq, container in self._indicators.items()]]
            signals = pd.Series([future.result() for future in concurrent.futures.wait(fs)[0]])
        else:
            values = [container.signal(self.candles(freq),
                                       self._process_point(point, freq),
                                       ) for freq, container in self._indicators.items()]
            signals = pd.Series(values)

        return TrendMovement(signals.mode()[0])

    def _determine_scalar(self, point: Optional[pd.Timestamp] = None,
                          executor: concurrent.futures.Executor = None) -> int:
        if self.threads:
            fs = []
            [fs.extend(future) for future in [
                container.func_threads('strength', executor=executor,
                                       point=self._process_point(point, freq),
                                       candles=self.candles(freq)
                                       ) for freq, container in self._indicators.items()]]
            results = pd.Series([future.result() for future in concurrent.futures.wait(fs)[0]])
        else:
            values = [container.strength(self.candles(freq),
                                         self._process_point(point, freq),
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

        # remove `freq` value to prevent `KeyError`
        # TODO: is reusing the name going to affect original `point`?
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
