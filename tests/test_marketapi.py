import datetime as dt
import pandas as pd
from os import mkdir, path
from shutil import rmtree
import unittest
from unittest.mock import patch, MagicMock
from yaml import safe_dump, safe_load

from core import MarketAPI


class BaseMarketAPITests(unittest.TestCase):
    @patch("core.MarketAPI.__abstractmethods__", set())
    def setUp(self) -> None:
        self.args = {'api_key': 'key', 'api_secret': 'secret', 'update': False}
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
