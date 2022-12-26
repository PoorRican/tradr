import pandas as pd
from os import getcwd, path, mkdir, listdir
from shutil import rmtree
import unittest
from unittest.mock import MagicMock, patch
from yaml import safe_dump, safe_load

from core.market import Market


class BaseMarketTests(unittest.TestCase):
    @patch('core.market.Market.__abstractmethods__', set())
    def setUp(self):
        self.dir = path.join(getcwd(), '_data')
        self.market = Market(root=self.dir)

        # clear directory
        if path.isdir(self.dir):
            rmtree(self.dir)
        mkdir(self.dir)

    def tearDown(self):
        rmtree(self.dir)


class MarketPropertyTestCases(BaseMarketTests):
    def test_instance_dir(self):
        self.skipTest('')


if __name__ == '__main__':
    unittest.main()
