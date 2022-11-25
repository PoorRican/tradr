import datetime as dt
from os import mkdir, path
import unittest
from unittest.mock import patch
from yaml import safe_dump, safe_load

from core.markets.GeminiMarket import GeminiMarket


class BaseGeminiMarketTests(unittest.TestCase):
    def setUp(self) -> None:
        self.args = {'api_key': 'key', 'api_secret': 'secret', 'update': False}
        self.market = GeminiMarket(**self.args)


if __name__ == '__main__':
    unittest.main()
