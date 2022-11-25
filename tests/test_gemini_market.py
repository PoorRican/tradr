import datetime as dt
from os import mkdir, path
import unittest
from unittest.mock import patch
from yaml import safe_dump, safe_load

from core.markets.GeminiMarket import GeminiMarket, _INSTANCES_FN, _SECRET_FN


class BaseGeminiMarketTests(unittest.TestCase):
    def setUp(self) -> None:
        self.args = {'api_key': 'key', 'api_secret': 'secret', 'update': False}
        self.market = GeminiMarket(**self.args)


class GeminiInstanceTests(BaseGeminiMarketTests):
    def setUp(self) -> None:
        super().setUp()

        self.root = f"/tmp/root_{dt.datetime.now()}"
        mkdir(self.root)

        self.market.root = self.root

    def test_init(self):
        self.assertEqual(len(self.market.instances), 1)

    def test_restore(self):
        params = [{'symbol': 'btcusd', 'freq': '15m'},
                  {'symbol': 'ethusd', 'freq': '15m'},
                  {'symbol': 'dogeusd', 'freq': '1m'}]
        _secrets = {'secret': 'blah', 'key': 'key'}
        with patch('core.markets.GeminiMarket.ROOT', self.root) as _root:
            with open(path.join(_root, _SECRET_FN), 'w') as f:
                safe_dump(_secrets, f)
            with open(path.join(_root, _INSTANCES_FN), 'w') as f:
                safe_dump(params, f)

            with patch.object(GeminiMarket, 'load') as _mock_load:
                self.market.restore()
                _mock_load.assert_called()

            for param in params:
                s = f"Gemini_{param['symbol']}_{param['freq']}"
                self.assertIn(s, self.market.instances.keys())
                val = self.market.instances[s]
                for attr in ('symbol', 'freq'):
                    self.assertEqual(getattr(val, attr), param[attr])

    def test_snapshot(self):
        instances = [GeminiMarket(symbol='btcusd', freq='15m', **self.args),
                     GeminiMarket(symbol='ethusd', freq='15m', **self.args),
                     GeminiMarket(symbol='dogeusd', freq='1m', **self.args)]
        self.market.instances = {}
        for i in instances:
            self.market.instances[i.id] = i

        with patch('core.markets.GeminiMarket.ROOT', self.root) as _root:
            with patch.object(GeminiMarket, 'save') as _mock_save:
                self.market.snapshot()
                _mock_save.assert_called()

            with open(path.join(_root, _INSTANCES_FN)) as f:
                params = safe_load(f)
                self.assertEqual(3, len(params))


if __name__ == '__main__':
    unittest.main()
