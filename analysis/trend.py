import pandas as pd
from typing import Mapping, Optional, NoReturn, Sequence

from core.markets import GeminiMarket
from models.signals import IndicatorContainer, MACDRow, BBANDSRow, STOCHRSIRow, Signal
from models.trend import TrendMovement, MarketTrend


class TrendDetector(object):
    """ Encapsulates analysis of market trends.

    Uses multiple long-term (ie: 1H, 6H, 1 day, 1 week) candle data to detect if market is trending up, down or is
    remaining constant. This data is used to better characterize enter/exit points and modulate trading amount (eg:
    buy more/sell less during downtrend, buy less/say more during uptrend).
    """

    frequencies = ('1hr', '6hr', '1day')
    """ Frequencies to use for fetching candle data.
    
    Shall be ordered from shortest-to-longest.
    """

    def __init__(self, market: GeminiMarket):
        super().__init__()

        self.market = market

        self.candles: pd.DataFrame = pd.DataFrame()
        """ 3-dim DataFrame of candle data incorporating multiple timeframes
        
        This should be updated by `update()`.
        
        Index timestamp. Columns frequencies. 3rd dim should be market value corresponding to index.
        
        Candles should be a multi-indexed `DataFrame` where primary index is the frequency (eg: 1h, 1d, etc).
        
        Interpolation (handled by pandas) can be used to account for missing timestamps when plotting longer timeframes.
        Normally, missing data shall be dropped before being passed.
        """

        self.indicators: Mapping[str, 'IndicatorContainer'] = self._create_indicator_container()
        """ Store indicator data as a mapping where each key is a frequency.
        """

    @classmethod
    def _create_indicator_container(cls) -> Mapping[str, 'IndicatorContainer']:
        indicators = {}
        for freq in cls.frequencies:
            indicators[freq] = IndicatorContainer([MACDRow, BBANDSRow, STOCHRSIRow])
        return indicators

    def get_candles(self, frequency: str) -> pd.DataFrame:
        """ Get cleaned candle data for a given frequency.

        Reduces boilerplate for returning cleaned data.
        """
        assert frequency in self.frequencies
        assert frequency in self.candles.columns

        # fetch via multi-index
        return self.candles[frequency].dropna()

    def develop(self) -> NoReturn:
        """ Compute all indicator functions across all frequencies using existing candle data.

        Since smaller frequencies will have values with `NaN`, `get_candles()` must be called to drop
        null values.

        TODO:
            - Asynchronous
            - Threading
        """
        assert len(self.candles) is not 0
        assert len(self.indicators) is not 0

        for freq in self.frequencies:
            self.indicators[freq].develop(self.get_candles(freq))

    def _fetch(self) -> pd.DataFrame:
        """ Get all candle data then combine into multi-indexed `DataFrame`.

        TODO:
            - Use `pd.infer_freq`. Fill in missing gaps to create uniform dataframe
        """
        data = {}

        # get candle data for all frequencies
        for freq in self.frequencies:
            data[freq] = self.market.get_candles(freq)

            # add freq value to as multi-index column
            _freq_str = (freq,) * len(data[freq].columns)
            _columns = data[freq].columns
            data[freq].columns = pd.MultiIndex.from_tuples(zip(_freq_str, _columns))

        combined = pd.concat([i for i in data.values()], axis='columns')

        return combined

    def _fetch_trends(self, point: 'pd.Timestamp') -> Mapping[str, 'TrendMovement']:
        trends: Mapping['str': 'TrendMovement'] = {}
        for freq in self.frequencies:
            result: Signal = self.indicators[freq].check(self.get_candles(freq), point)
            trends[freq] = TrendMovement(result)
        return trends

    def update(self) -> NoReturn:
        """ Update `candles` with current market data """
        self.candles = self._fetch()

    def _determine_scalar(self, point: Optional[pd.Timestamp] = None) -> int:
        results = []
        for freq in self.frequencies:
            for indicator in self.indicators[freq]:
                results.append(indicator.strength())
        # TODO: determine integer scalar from signal strength
        return 1

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
        """
        if not point:
            point = self.candles.iloc[-1]

        results = self._fetch_trends(point)
        consensus = self._determine_consensus(list(results.values()))

        return MarketTrend(consensus, scalar=self._determine_scalar())

    @staticmethod
    def _determine_consensus(values: Sequence) -> TrendMovement:
        """ Return the most common value in a dict containing a list of returned results """
        counts = {}
        for v in TrendMovement.__members__.values():
            counts[v] = list(values).count(v)
        _max = max(counts.values())
        i = list(counts.values()).index(_max)
        winner = list(counts.keys())[i]
        return winner
