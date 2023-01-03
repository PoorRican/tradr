import datetime
import datetime as dt
from os import mkdir, path
import pandas as pd
from pytz import timezone
from shutil import rmtree
import unittest
from unittest.mock import patch, MagicMock, PropertyMock
from yaml import safe_dump, safe_load

from core import MarketAPI
from misc import TZ


class BaseMarketAPITests(unittest.TestCase):
    @patch("core.MarketAPI.__abstractmethods__", set())
    def setUp(self) -> None:
        self.args = {'api_key': 'key', 'api_secret': 'secret', 'update': False,
                     'auto_update': False}
        self.market = MarketAPI(**self.args)
        self.market.__class__._SECRET_FN = 'mock_secret.yml'
        self.market.__class__._INSTANCES_FN = 'mock_instances.yml'


class GeneralMarketAPITests(BaseMarketAPITests):
    def test_combine_candles(self):
        self.market._data = pd.DataFrame([2, 4, 6, 8], index=[2, 4, 6, 8])
        _df = pd.DataFrame([6, 7, 8, 9, 10], index=[6, 7, 8, 9, 10])

        expected = pd.DataFrame([2, 4, 6, 7, 8, 9, 10], index=[2, 4, 6, 7, 8, 9, 10])
        combined = self.market._combine_candles(_df)
        self.assertTrue(combined.equals(expected))

    def test_check_tz(self):
        valid_freqs = ('1h', '6h', '1d', '4h')
        self.market.valid_freqs = valid_freqs
        tz = timezone('US/Pacific')
        dates = []
        for freq in valid_freqs:
            dates.append(pd.date_range(dt.datetime.now(tz=tz), periods=10, freq=freq))

        # make `MultiIndex`
        index = pd.concat([pd.DataFrame(index=i) for i in dates],
                          keys=valid_freqs).index
        self.market._data = pd.DataFrame(index=index)

        # assert that index has correct tz
        for freq in valid_freqs:
            self.assertEqual(self.market.candles(freq).index.tz, tz)

        # assert that global timezone can be overwritten
        new_tz = timezone('MST')
        with patch('core.MarketAPI._global_tz',
                   new_callable=PropertyMock(return_value=new_tz)):
            self.market._check_tz()
            self.assertEqual(self.market.tz, new_tz)

            for freq in valid_freqs:
                self.assertIsNot(self.market.candles(freq).index.tz, tz)
                self.assertIs(self.market.candles(freq).index.tz, new_tz)


class InstanceTests(BaseMarketAPITests):
    def setUp(self) -> None:
        super().setUp()

        self._class = self.market.__class__
        self.root = f"/tmp/root_{dt.datetime.now()}"
        mkdir(self.root)

        self._class._check_symbol = MagicMock()
        self._class.root = self.root

    def tearDown(self):
        rmtree(self.root)

    def test_init(self):
        self.assertEqual(len(self.market.instances), 1)

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
