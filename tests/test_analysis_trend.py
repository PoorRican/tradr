import pandas as pd
import unittest
from unittest.mock import patch, MagicMock

from analysis.trend import TrendMovement, TrendDetector, MarketTrend


FREQUENCIES = ('freq1', 'freq2')


class TrendDetectorTests(unittest.TestCase):
    def setUp(self) -> None:
        with patch('core.markets.GeminiMarket') as cls:
            self.market = cls()

        TrendDetector._frequencies = FREQUENCIES
        self.detector = TrendDetector(self.market)

        # setup detector
        _index = pd.MultiIndex.from_tuples(zip(FREQUENCIES, ('a', 'b')))
        df = pd.DataFrame([(1, 2), (pd.NA, 4)], index=_index)
        self.detector._candles = df

    def test_init_args(self):
        self.assertTrue(type(self.detector._candles) == pd.DataFrame)
        self.assertEqual(tuple(self.detector._indicators.keys()), TrendDetector._frequencies)

    def test_candles(self):
        # assert func excludes missing values
        self.assertEqual(len(self.detector.candles('freq1')), 1)
        self.assertEqual(len(self.detector.candles('freq2')), 0)

    def test_develop(self):
        # mock indicators.develop(). Return predetermined df depending on freq
        self.detector.candles = MagicMock(return_value='get_candles')
        for container in self.detector._indicators.values():
            container.develop = MagicMock()

        # assert that develop was called n-times, where n == len(frequencies)
        self.detector.develop()
        for container in self.detector._indicators.values():
            container.develop.assert_called_with('get_candles')

        # get assertion error when no candles
        with self.assertRaises(AssertionError):
            _candles = self.detector._candles
            self.detector._candles = []
            self.detector.develop()
            self.detector._candles = _candles
            del _candles

        # get assertion error when no indicators
        with self.assertRaises(AssertionError):
            _indicators = self.detector._indicators
            self.detector._indicators = []
            self.detector.develop()
            self.detector._indicators = _indicators
            del _indicators

    def test_fetch(self):
        # mock get_candles
        _candles = pd.DataFrame({'c': [1, 'test'], 'd': [3, 4]})
        self.detector.market.get_candles = MagicMock(return_value=_candles)

        fetched = self.detector._fetch()

        # assert proper multi-index columns
        for freq in FREQUENCIES:
            self.assertIn(freq, fetched.index)

        # assert returned values
        for freq in FREQUENCIES:
            self.assertEqual(fetched.loc[freq, 'c'].iloc[1], 'test')

    def test_update(self):
        self.detector._fetch = MagicMock(return_value='fetched')
        # assert `_fetch()` is called
        self.detector.update_candles()
        self.detector._fetch.assert_called_once()
        self.assertEqual(self.detector._candles, 'fetched')

    def test_determine_scalar(self):
        # mock indicator.strength()
        # input mocked results
        self.skipTest('Not Implemented Yet')

    def test_characterize(self):
        # setup mock functions
        self.detector._fetch_trends = MagicMock()
        self.detector._determine_consensus = MagicMock(return_value=TrendMovement.UP)
        self.detector._determine_scalar = MagicMock(return_value=1)

        trend = self.detector.characterize()

        self.assertIsInstance(trend, MarketTrend)
        self.assertEqual(trend.trend, TrendMovement.UP)
        self.assertEqual(trend.scalar, 1)

    def test_determine_consensus(self):
        _results = (TrendMovement.UP, TrendMovement.UP, TrendMovement.CYCLE)

        trend = self.detector._determine_consensus(_results)
        self.assertIsInstance(trend, TrendMovement)

        # assert that highest occurring value is returned
        self.assertEqual(trend, TrendMovement.UP)


if __name__ == '__main__':
    unittest.main()
