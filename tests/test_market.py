import pandas as pd
import unittest
from unittest.mock import MagicMock, patch

from core.market import Market


class BaseMarketTests(unittest.TestCase):
    @patch('core.market.Market.__abstractmethods__', set())
    def setUp(self):
        self.market = Market()


class GeneralMarketTests(BaseMarketTests):
    def test_combine_candles(self):
        self.market.data = pd.DataFrame([2, 4, 6, 8], index=[2, 4, 6, 8])
        _df = pd.DataFrame([6, 7, 8, 9, 10], index=[6, 7, 8, 9, 10])

        expected = pd.DataFrame([2, 4, 6, 7, 8, 9, 10], index=[2, 4, 6, 7, 8, 9, 10])
        self.assertTrue(self.market._combine_candles(_df).equals(expected))


if __name__ == '__main__':
    unittest.main()
