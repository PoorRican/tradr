import datetime as dt
import pandas as pd
from os import mkdir, path
from shutil import rmtree
import unittest
from unittest.mock import patch
from yaml import safe_dump, safe_load

from core.MarketAPI import MarketAPI


class BaseMarketAPITests(unittest.TestCase):
    @patch("core.MarketAPI.MarketAPI.__abstractmethods__", set())
    def setUp(self) -> None:
        self.args = {'api_key': 'key', 'api_secret': 'secret', 'update': False}
        self.market = MarketAPI(**self.args)
        self.market.__class__._SECRET_FN = 'mock_secret.yml'
        self.market.__class__._INSTANCES_FN = 'mock_instances.yml'


class GeneralMarketAPITests(BaseMarketAPITests):
    def test_combine_candles(self):
        self.market.data = pd.DataFrame([2, 4, 6, 8], index=[2, 4, 6, 8])
        _df = pd.DataFrame([6, 7, 8, 9, 10], index=[6, 7, 8, 9, 10])

        expected = pd.DataFrame([2, 4, 6, 7, 8, 9, 10], index=[2, 4, 6, 7, 8, 9, 10])
        self.assertTrue(self.market._combine_candles(_df).equals(expected))


class InstanceTests(BaseMarketAPITests):
    def setUp(self) -> None:
        super().setUp()

        self._class = self.market.__class__
        self.root = f"/tmp/root_{dt.datetime.now()}"
        mkdir(self.root)

        self.market.root = self.root

    def tearDown(self):
        rmtree(self.root)

    def test_init(self):
        self.assertEqual(len(self.market.instances), 1)

    @patch("core.MarketAPI.MarketAPI.__abstractmethods__", set())
    def test_restore(self):
        params = [{'symbol': 'btcusd', 'freq': '15m'},
                  {'symbol': 'ethusd', 'freq': '15m'},
                  {'symbol': 'dogeusd', 'freq': '1m'}]
        _secrets = {'secret': 'blah', 'key': 'key'}

        with patch('core.MarketAPI.ROOT', self.root) as _root:
            with open(path.join(_root, self.market._SECRET_FN), 'w') as f:
                safe_dump(_secrets, f)
            with open(path.join(_root, self.market._INSTANCES_FN), 'w') as f:
                safe_dump(params, f)

            with patch.object(self.market.__class__, 'load') as _mock_load:
                self.market.restore()
                _mock_load.assert_called()

            for param in params:
                s = f"MarketAPI_{param['symbol']}_{param['freq']}"
                self.assertIn(s, self.market.instances.keys())
                val = self.market.instances[s]
                for attr in ('symbol', 'freq'):
                    self.assertEqual(getattr(val, attr), param[attr])

    @patch("core.MarketAPI.MarketAPI.__abstractmethods__", set())
    def test_snapshot(self):
        instances = [self._class(symbol='btcusd', freq='15m', **self.args),
                     self._class(symbol='ethusd', freq='15m', **self.args),
                     self._class(symbol='dogeusd', freq='1m', **self.args)]
        self.market.__class__.instances = {}

        for i in instances:
            self.market.instances[i.id] = i

        with patch('core.MarketAPI.ROOT', self.root) as _root:
            with patch.object(self.market.__class__, 'save') as _mock_save:
                self.market.snapshot()
                _mock_save.assert_called()

            with open(path.join(_root, self.market._INSTANCES_FN)) as f:
                params = safe_load(f)
                self.assertEqual(3, len(params))


if __name__ == '__main__':
    unittest.main()
