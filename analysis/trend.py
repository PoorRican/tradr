import concurrent.futures
from math import floor
import pandas as pd
from pytz import timezone
from typing import Mapping, Optional, NoReturn, Sequence

from core.MarketAPI import MarketAPI
from models.signals import IndicatorContainer, MACDRow, BBANDSRow, STOCHRSIRow, Signal
from models.trend import TrendMovement, MarketTrend


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

    def __init__(self, market: MarketAPI):
        super().__init__()

        self.market = market

        self._candles: pd.DataFrame = pd.DataFrame()
        """ 3-dim DataFrame of candle data incorporating multiple timeframes
        
        This should be updated by `update()`.
        
        Index timestamp. Columns frequencies. 3rd dim should be market value corresponding to index.
        
        Candles should be a multi-indexed `DataFrame` where primary index is the frequency (eg: 1h, 1d, etc).
        
        Interpolation (handled by pandas) can be used to account for missing timestamps when plotting longer timeframes.
        Normally, missing data shall be dropped before being passed.
        """

        self._indicators: Mapping[str, 'IndicatorContainer'] = self._create_indicator_container()
        """ Store indicator data as a mapping where each key is a frequency.
        """

    @classmethod
    def _create_indicator_container(cls) -> Mapping[str, 'IndicatorContainer']:
        indicators = {}
        for freq in cls._frequencies:
            indicators[freq] = IndicatorContainer([MACDRow, BBANDSRow, STOCHRSIRow])
        return indicators

    def candles(self, frequency: str) -> pd.DataFrame:
        """ Get cleaned candle data for a given frequency.

        Reduces boilerplate for returning cleaned data.
        """
        assert frequency in self._frequencies

        # fetch via multi-index
        return self._candles.loc[frequency]

    def develop(self) -> NoReturn:
        """ Compute all indicator functions across all frequencies using existing candle data.

        Since smaller frequencies will have values with `NaN`, `get_candles()` must be called to drop
        null values.

        TODO:
            - Asynchronous
            - Threading
        """
        assert len(self._candles) != 0
        assert len(self._indicators) != 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            fs = []
            [fs.extend(executor.submit(container.develop,
                                       self.candles(freq)).result()) for freq, container in self._indicators.items()]
            concurrent.futures.wait(fs)

    def _fetch(self) -> pd.DataFrame:
        """ Get all candle data then combine into multi-indexed `DataFrame`.

        TODO:
            - Use `pd.infer_freq`. Fill in missing gaps to create uniform dataframe
        """
        data = []

        # get candle data for all frequencies
        for freq in self._frequencies:
            fetched = pd.DataFrame(self.market.get_candles(freq), copy=True)
            data.append(fetched)

        combined = pd.concat(data, keys=self._frequencies)

        return combined

    def _process_point(self, point: pd.Timestamp, freq: str) -> pd.Timestamp:
        # modify `point` to access correct timeframe
        _freq = self.market.translate_period(freq)      # `DateOffset` conversion
        if _freq == '6H':
            point = point.floor(_freq)
            if point.dst():
                point -= pd.DateOffset(hours=3)
            else:
                point -= pd.DateOffset(hours=4)
            point = point.floor('H', nonexistent='shift_backward')
        elif _freq == '1D':
            # TODO: faulty implementation
            # Incorrect date is indexed immediately after daily candle data is released.
            point = point.floor(_freq) - pd.DateOffset(days=1)
            point = point.strftime('%m/%d/%Y')          # generically select data
        else:
            point = point.floor(_freq, nonexistent='shift_backward')
        return point

    def _fetch_trends(self, executor, point: pd.Timestamp = None) -> TrendMovement:
        """

        Args:
            point:

        Notes:
            Frequency needs to be modified before accessing indicator graphs. A pandas unit of frequency to select
            indicator data from greater time frequencies needs to be passed as `point`. Used by
            `TrendDetector` to prevent `KeyError` from being raised during simulation arising from larger timeframes
            of stored candle data (ie: 1day, 6hr, or 1hr)  and smaller timeframes used by strategy/backtesting
            functions (ie: 15min). If passed, `point` is rounded down to the largest frequency less than `point`
            (eg: 14:45 becomes 14:00). If not passed, `point` is untouched.

        Returns:

        """
        fs = []
        [fs.extend(future) for future in [
            container.signal_threads(executor, self._process_point(point, freq),
                                     self.candles(freq)
                                     ) for freq, container in self._indicators.items()]]
        signals = pd.Series([future.result() for future in concurrent.futures.wait(fs)[0]])
        return TrendMovement(signals.mode()[0])

    def update_candles(self) -> NoReturn:
        """ Update `candles` with current market data """
        self._candles = self._fetch()

    def _determine_scalar(self, executor, point: Optional[pd.Timestamp] = None) -> int:
        fs = []
        [fs.extend(future) for future in [
            container.strength_threads(executor, self._process_point(point, freq),
                                       self.candles(freq)
                                       ) for freq, container in self._indicators.items()]]
        results = pd.Series([future.result() for future in concurrent.futures.wait(fs)[0]])
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
            point = self._candles.iloc[-1].name

        # remove `freq` value to prevent `KeyError`
        # TODO: is reusing the name going to affect original `point`?
        if hasattr(point, 'timestamp'):
            point = pd.Timestamp.fromtimestamp(point.timestamp(), tz=timezone('US/Pacific'))

        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            _signals = executor.submit(self._fetch_trends, executor, point)
            _strengths = executor.submit(self._determine_scalar, executor, point)

            concurrent.futures.wait([_signals, _strengths])
            return MarketTrend(_signals.result(), scalar=_strengths.result())

    def calculate_all(self):
        for freq, container in self._indicators.items():
            container.calculate_all(self.candles(freq))
