from math import nan
import pandas as pd
import unittest
from unittest.mock import patch, MagicMock

from analysis.trend import TrendDetector
from primitives import TrendDirection, MarketTrend
from misc import TZ


FREQUENCIES = ('15m', '1hr', '6h')


@patch('models.Indicator.__abstractmethods__', set())
class TrendDetectorTests(unittest.TestCase):
    def set_indicator_attr(self, attr, values):
        assert len(values) == len(FREQUENCIES)

        for i, val in zip(FREQUENCIES, values):
            setattr(self.detector._indicators[i], attr, MagicMock(return_value=val))

    def propagate_to_indicator_df(self, idx, attr, col, values):
        for freq, container, val in zip(self.detector._indicators.keys(),
                                        self.detector._indicators.values(), values):
            point = self.market.process_point(idx, freq)
            for i in self.detector._indicators[freq].indicators:
                setattr(i, attr, pd.DataFrame({col: val}, index=[point]))

    def setUp(self) -> None:
        with patch('core.markets.GeminiMarket') as cls:
            self.market = cls()

        TrendDetector._frequencies = FREQUENCIES
        self.index = pd.date_range(pd.Timestamp.now(), tz=TZ, freq='15m', periods=3)
        self.market.translate_period = MagicMock(return_value=FREQUENCIES[-1])
        self.detector = TrendDetector(self.market)

    def test_init_args(self):
        self.assertEqual(tuple(self.detector._indicators.keys()), TrendDetector._frequencies)

    def test_develop(self):
        # mock indicators.develop(). Return predetermined df depending on freq
        _candles = pd.DataFrame([0, 1, 2])
        self.detector.candles = MagicMock(return_value=_candles)
        for container in self.detector._indicators.values():
            container.update = MagicMock()

        # assert that develop was called n-times, where n == len(frequencies)
        self.detector.update()
        for container in self.detector._indicators.values():
            container.update.assert_called_with(_candles)

        # get assertion error when no indicators
        with self.assertRaises(AssertionError):
            self.detector._indicators = []
            self.detector.update()

    def test_determine_scalar(self):
        idx = self.index[-1]        # this is a placeholder since returned value is mock
        self.market.process_point = MagicMock(return_value=idx)

        # mocked up-trend
        values = [TrendDirection.UP, TrendDirection.CYCLE, TrendDirection.UP]
        self.propagate_to_indicator_df(idx, 'computed', 'signal', values)
        self.set_indicator_attr('strength', [2, nan, 4])

        # `nan` should be dropped, since `signal` value does not agree
        self.assertEqual(3, self.detector._determine_scalar(TrendDirection.UP, idx))

        # mock cycle
        values = [TrendDirection.CYCLE, TrendDirection.CYCLE, TrendDirection.UP]
        self.propagate_to_indicator_df(idx, 'computed', 'signal', values)
        self.set_indicator_attr('strength', [nan, nan, 4])

        self.assertTrue(self.detector._determine_scalar(TrendDirection.CYCLE, idx) is nan)

        # mock ambiguous
        values = [TrendDirection.DOWN, TrendDirection.CYCLE, TrendDirection.UP]
        self.propagate_to_indicator_df(idx, 'computed', 'signal', values)
        self.set_indicator_attr('strength', [1, nan, 4])

        self.assertTrue(self.detector._determine_scalar(TrendDirection.CYCLE, idx) is nan)

        # mock ambiguous
        values = [TrendDirection.DOWN, TrendDirection.DOWN, TrendDirection.UP]
        self.propagate_to_indicator_df(idx, 'computed', 'signal', values)
        self.set_indicator_attr('strength', [1, nan, 4])

        self.assertEqual(1, self.detector._determine_scalar(TrendDirection.DOWN, idx))

    def test_characterize(self):
        # setup mock functions
        self.detector._fetch_trend = MagicMock(return_value=TrendDirection.UP)
        self.detector._determine_scalar = MagicMock(return_value=1)

        trend = self.detector.characterize()

        self.detector._fetch_trend.assert_called_once()
        self.detector._determine_scalar.assert_called_once()
        self.assertIsInstance(trend, MarketTrend)
        self.assertEqual(trend.trend, TrendDirection.UP)
        self.assertEqual(trend.scalar, 1)

    def test_fetch_trend(self):
        idx = self.index[-1]        # this is a placeholder since returned value is mock
        self.set_indicator_attr('signal', [TrendDirection.UP, TrendDirection.CYCLE, TrendDirection.UP])

        trend = self.detector._fetch_trend(idx)
        self.assertIsInstance(trend, TrendDirection)

        # assert that highest occurring value is returned
        self.assertEqual(trend, TrendDirection.UP)


if __name__ == '__main__':
    unittest.main()
