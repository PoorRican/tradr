import datetime as dt
from os import mkdir, path
from typing import Union

import numpy as np
import pandas as pd
from pytz import timezone
from shutil import rmtree
import unittest
from unittest.mock import patch, MagicMock, PropertyMock
from yaml import safe_dump, safe_load

from core import MarketAPI


class BaseMarketAPITests(unittest.TestCase):
    valid_freqs = ['D', '1h', '6h', '1d', '4h']

    @patch("core.MarketAPI.__abstractmethods__", set())
    def setUp(self) -> None:
        self.args = {'api_key': 'key', 'api_secret': 'secret', 'update': False, 'load': False,
                     'auto_update': False}
        self.market = MarketAPI(**self.args)
        self.market.__class__._SECRET_FN = 'mock_secret.yml'
        self.market.__class__._INSTANCES_FN = 'mock_instances.yml'
        self.market.__class__.valid_freqs = self.valid_freqs

    def _create_multiindex(self, start: Union['pd.Timestamp', str] = None, end: Union['pd.Timestamp', str] = None,
                           periods: int = 10, tz: timezone = None):
        if start is None:
            start = dt.datetime.now(tz=tz)
        else:
            if end:
                periods = None

                if type(end) is str:
                    end = pd.Timestamp(end)

            if type(start) is str:
                start = pd.Timestamp(start)

        dates = []
        for freq in self.valid_freqs:
            dates.append(pd.date_range(start=start, end=end, periods=periods, freq=freq))

        index = pd.concat([pd.DataFrame(index=i) for i in dates],
                          keys=self.valid_freqs).index
        return index


class CheckTZTests(BaseMarketAPITests):
    def test_basic(self):
        self.market.valid_freqs = self.valid_freqs
        tz = timezone('US/Pacific')
        index = self._create_multiindex(tz=tz)
        self.market._data = pd.DataFrame(index=index)

        # assert that index has correct tz
        for freq in self.valid_freqs:
            self.assertEqual(self.market.candles(freq).index.tz, tz)

        # assert that global timezone can be overwritten
        new_tz_n = 'MST'
        new_tz = timezone(new_tz_n)
        with patch('core.MarketAPI._global_tz',
                   new_callable=PropertyMock(return_value=new_tz_n)):
            self.market._check_tz()
            self.assertEqual(self.market.tz, new_tz)

            for freq in self.valid_freqs:
                self.assertIsNot(self.market.candles(freq).index.tz, tz)
                self.assertIs(self.market.candles(freq).index.tz, new_tz)

    def test_data_matches(self):
        # test that data matches corresponding index
        index = self._create_multiindex(start="12/12/2012", tz=timezone('US/Pacific'))
        columns = [0, 1, 2]
        _data = pd.DataFrame(columns=columns, index=index)
        for col in columns:
            _data[col] = [i for i in range(len(_data[col]))]

        self.market._data = _data.copy()

        new_tz_n = 'MST'
        with patch('core.MarketAPI._global_tz',
                   new_callable=PropertyMock(return_value=new_tz_n)):
            self.market._check_tz()

        self.assertTrue(np.array_equal(_data.values, self.market._data.values))


class CombineCandlesTests(BaseMarketAPITests):
    def setUp(self):
        super().setUp()
        self.index1 = self._create_multiindex("1/1/2020", "6/1/2021")
        self.index2 = self._create_multiindex("1/1/2021", "1/1/2022")
        self.expected = self._create_multiindex("1/1/2020", "1/1/2022")

        self.market._data = pd.DataFrame(index=self.index1)

    def test_dt_index(self):
        _df = pd.DataFrame(index=self.index2)
        combined = self.market._combine_candles(_df)

        # compare indexes. `equal()` function doesn't work
        self.assertEqual(0, len(combined.index.get_level_values(0).difference(self.expected.get_level_values(0))))
        self.assertEqual(0, len(self.expected.get_level_values(0).difference(combined.index.get_level_values(0))))

        self.assertEqual(0, len(combined.index.get_level_values(1).difference(self.expected.get_level_values(1))))
        self.assertEqual(0, len(self.expected.get_level_values(1).difference(combined.index.get_level_values(1))))

        self.assertFalse(False in [i in self.expected for i in combined.index])
        self.assertFalse(False in [i in combined.index for i in self.expected])

    def test_overlap(self):
        """ Assert that overlapped timeframe preserved """
        # use DatetimeIndex
        self.market._data = pd.DataFrame(columns=self.valid_freqs, index=self.index1)
        _df = pd.DataFrame(index=self.index2)
        combined = self.market._combine_candles(_df)

        # assert that first values from overlap are kept
        overlap = pd.date_range("1/1/2021", "6/1/2021")
        self.assertTrue(overlap.isin(combined.index.get_level_values(1)).all())

        # assert contiguous sequence
        self.assertTrue(combined.index.is_monotonic_increasing)

    def test_return_multiindex(self):
        # assert that a multiindex is returned
        _df = pd.DataFrame(index=self.index2)
        combined = self.market._combine_candles(_df)

        self.assertIsInstance(combined.index, pd.MultiIndex)

    def test_data_preserved(self):
        columns = [0, 1, 2]
        _data = pd.DataFrame(columns=columns, index=self.index1)
        _df = pd.DataFrame(columns=columns, index=self.index2)
        for col in columns:
            _data[col] = [i for i in range(len(_data[col]))]
            _df[col] = [i + 10 for i in range(len(_df[col]))]

        self.market._data = _data.copy(deep=True)
        combined = self.market._combine_candles(_df)

        _start = self.index1.get_level_values(1)[0]
        _end = self.index1.get_level_values(1)[-1]
        for freq in self.valid_freqs:
            _combined = combined.loc[freq][_start:_end]
            _orig = _data.loc[freq]
            self.assertTrue(_combined.equals(_orig))

    def test_return_dt_index(self):
        # assert that first level is datetime index and not index
        _df = pd.DataFrame(index=self.index2)
        combined = self.market._combine_candles(_df)

        self.assertIsInstance(combined.index.get_level_values(1), pd.DatetimeIndex)

    def test_assert_same_tz(self):
        # assert that AssertionError is raised if two different timezones are passed
        idx2 = self.index2.set_levels(self.index2.get_level_values(1).tz_localize('UTC'), level=1,
                                      verify_integrity=False)
        _df = pd.DataFrame(index=idx2)

        # test when one is tz-naive
        with self.assertRaises(AssertionError):
            self.market._combine_candles(_df)

        idx1 = self.index1.set_levels(self.index1.get_level_values(1).tz_localize('MST'), level=1,
                                      verify_integrity=False)
        self.market._data.index = idx1

        # test when both are tz-aware but not equal
        with self.assertRaises(AssertionError):
            self.market._combine_candles(_df)


class RepairCandlesTests(BaseMarketAPITests):
    def test_basic(self):
        # basic test of repair candles
        _freq = 'D'
        self.market.translate_period = MagicMock(return_value=_freq)
        _start = pd.Timestamp("1/1/2023")
        _end = pd.Timestamp("1/3/2023")
        _expected = pd.Timestamp("1/2/2023")
        idx = pd.Index([_start, _end])
        df = pd.DataFrame([1, 3], index=idx)
        result = self.market._repair_candles(df, _freq)
        isin = result.index.isin([_expected])
        self.assertTrue(bool(isin.any()))
        self.assertEqual(2, result.iloc[1].values)

    def test_duplicates(self):
        # assert that duplicates are dropped
        self.skipTest('')

    def test_date_range(self):
        # assert that len of date range is the same
        _freq = '1h'
        self.market.translate_period = MagicMock(return_value=_freq)
        _start = pd.Timestamp("1/1/2023")
        _end = pd.Timestamp("1/2/2023")
        idx = pd.date_range(_start, _end, freq=_freq)
        df = pd.DataFrame([i for i in range(len(idx))], index=idx)
        result = self.market._repair_candles(df, _freq)
        self.assertEqual(len(idx), len(result.index))

    def test_interpolated(self):
        # test that na values are replaced with interpolated ones
        self.skipTest('')


class InstanceTests(BaseMarketAPITests):
    def setUp(self) -> None:
        super().setUp()

        self._class = self.market.__class__
        self.root = f"/tmp/root_{dt.datetime.now()}"
        mkdir(self.root)

        self._class.root = self.root

    def tearDown(self):
        rmtree(self.root)

    def test_init(self):
        self.assertEqual(len(self.market.instances), 1)
        self.assertIn('_data', self.market.exclude)

    @patch("core.MarketAPI.__abstractmethods__", set())
    def test_restore(self):
        params = [{'symbol': 'btcusd'},
                  {'symbol': 'ethusd'},
                  {'symbol': 'dogeusd'}]
        _secrets = {'secret': 'blah', 'key': 'key'}

        _dir = path.join(self.root, self.market._SECRET_FN)
        with open(_dir, 'w') as f:
            safe_dump(_secrets, f)
        _dir = path.join(self.root, self.market._INSTANCES_FN)
        with open(_dir, 'w') as f:
            safe_dump(params, f)

        with patch.object(self.market.__class__, 'load') as _mock_load:
            self.market.restore()
            _mock_load.assert_called()

        for param in params:
            symbol = param['symbol']
            s = f"MarketAPI_{symbol}"
            self.assertIn(s, self.market.instances.keys())
            instance = self.market.instances[s]
            self.assertEqual(getattr(instance, 'symbol'), param['symbol'])

    @patch("core.MarketAPI.__abstractmethods__", set())
    def test_snapshot(self):
        instances = [self._class(symbol='btcusd', **self.args),
                     self._class(symbol='ethusd', **self.args),
                     self._class(symbol='dogeusd', **self.args)]
        self.market.__class__.instances = {}

        for i in instances:
            self.market.instances[i.id] = i

        self.market.snapshot()

        with open(path.join(self.root, self.market._INSTANCES_FN), 'r') as f:
            params = safe_load(f)
            self.assertEqual(3, len(params))


if __name__ == '__main__':
    unittest.main()
