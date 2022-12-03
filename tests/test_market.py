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
        self.dir = path.join(getcwd(), 'data')
        self.market = Market(root=self.dir)

        # clear directory
        if path.isdir(self.dir):
            rmtree(self.dir)
        mkdir(self.dir)

    def tearDown(self):
        rmtree(self.dir)


class MarketSaveLoadTests(BaseMarketTests):
    def test_save(self):
        # test an arbitrary sequenced attribute
        _attr_name = 'mock_df'
        setattr(self.market, _attr_name, pd.DataFrame())
        # TODO: shouldn't an error be raised when adding a frame that didn't previously exist

        self.market.save()

        _dir = self.market._instance_dir
        dirs = ('literals.yml', 'data.yml', f"{_attr_name}.yml",)
        _files = listdir(self.market._instance_dir)
        for i in _files:
            self.assertIn(i, dirs)
        for i in dirs:
            self.assertIn(i, _files)

        # TODO: verify file contents

    def test_load_invalid(self):
        """ Test when an attribute that isn't part of instance attributes tries to get added via
        literal storage.
        """
        self.market.save()

        _fn = path.join(self.market._instance_dir, 'literals.yml')
        with open(_fn, 'r') as f:
            literals: dict = safe_load(f)

        self.assertIsInstance(literals, dict)
        literals['test'] = 'test'
        with open(_fn, 'w') as f:
            safe_dump(literals, f)

        with self.assertRaises(AssertionError):
            self.market.load()

    def test_load(self):
        # test an arbitrary sequenced attribute
        _attr_name = 'mock_df'
        setattr(self.market, _attr_name, pd.Series())

        self.market.save()
        _arbitrary_series_data = {1: 'test'}
        with open(path.join(self.market._instance_dir, f"{_attr_name}.yml"), 'w') as f:
            safe_dump(_arbitrary_series_data, f)

        _fn = path.join(self.market._instance_dir, 'literals.yml')
        with open(_fn, 'r') as f:
            literals: dict = safe_load(f)
        self.assertIsInstance(literals, dict)
        literals['root'] = 'test'
        with open(_fn, 'w') as f:
            safe_dump(literals, f)

        self.market.load()
        self.assertTrue(hasattr(self.market, 'root'))
        self.assertEqual(getattr(self.market, 'root'), 'test')

        # test arbitrary data
        self.assertEqual(_arbitrary_series_data, getattr(self.market, _attr_name).to_dict())

        # TODO: test that dataframes are properly loaded


if __name__ == '__main__':
    unittest.main()
