import unittest
from abc import ABC, abstractmethod
from typing import Union
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from primitives import TimeseriesWorker


class BaseTSWorkerTestCase(unittest.TestCase, ABC):
    @patch('primitives.TimeseriesWorker.__abstractmethods__', set())
    def setUp(self, ):
        self.obj = MagicMock()
        self.attr_name = 'mock_attr'
        self.ts = self._generate_ts()
        setattr(self.obj, self.attr_name, self.ts.copy())
        self.worker = TimeseriesWorker(self.obj, self.attr_name)

    @staticmethod
    @abstractmethod
    def _generate_ts() -> Union['pd.DataFrame', 'pd.Series']:
        pass

    @property
    def attr(self) -> Union['pd.DataFrame', 'pd.Series']:
        return getattr(self.obj, self.attr_name)


class DataFrameCases(BaseTSWorkerTestCase):
    @staticmethod
    def _generate_ts() -> pd.DataFrame:
        n_rows = 80
        n_cols = 5
        vals = np.random.randint(low=0, high=10, size=(n_rows, n_cols))
        index = pd.date_range(end=pd.Timestamp.today(), periods=n_rows)
        return pd.DataFrame(vals, index)

    def test_buffer_val(self):
        self.assertIsInstance(self.worker._buffer(), pd.DataFrame)

    def test_buffer(self):
        """ Assert that buffer returns correct values """
        expected = self.ts.iloc[len(self.ts) - self.worker._size:]
        _buffer = self.worker._buffer()
        self.assertTrue(_buffer.equals(expected))

    def test_buffer_point(self):
        """ Assert buffer behaves correctly when given a point. """
        idx = 77
        self.assertGreaterEqual(idx, self.worker._size)
        point = self.ts.index[idx]
        expected = self.ts.iloc[idx - self.worker._size + 1:idx + 1]     # include endpoint
        _buffer = self.worker._buffer(point)
        self.assertTrue(_buffer.equals(expected))

    def test_buffer_early_point(self):
        """ Assert buffer behaves correctly when given an early point. """
        idx = 5
        self.assertLess(idx, self.worker._size)
        point = self.ts.index[idx]
        expected = self.ts.iloc[:idx+1]     # include endpoint
        _buffer = self.worker._buffer(point)
        self.assertTrue(_buffer.equals(expected))


class SeriesCases(DataFrameCases):
    @staticmethod
    def _generate_ts() -> pd.Series:
        n_rows = 80
        vals = np.random.randint(low=0, high=10, size=n_rows)
        index = pd.date_range(end=pd.Timestamp.today(), periods=n_rows)
        return pd.Series(vals, index)

    def test_buffer_val(self):
        self.assertIsInstance(self.worker._buffer(), pd.Series)


if __name__ == '__main__':
    unittest.main()
