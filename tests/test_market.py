import pandas as pd
import unittest
from unittest.mock import MagicMock, patch

from core.market import Market


class BaseMarketTests(unittest.TestCase):
    @patch('core.market.Market.__abstractmethods__', set())
    def setUp(self):
        self.market = Market()


class MarketSaveLoadTests(BaseMarketTests):
    pass


if __name__ == '__main__':
    unittest.main()
