import pandas as pd
import unittest
from unittest.mock import MagicMock

from primitives.normalizer import Normalizer


class BaseNormalizerTestCases(unittest.TestCase):
    def setUp(self):
        self.object = MagicMock()
        self.attr_name = 'attr_name'
        self.ts = self._generate_ts()
        setattr(self.object, self.attr_name, self.ts)

    @staticmethod
    def _generate_ts() -> pd.DataFrame:
        n_rows = 80
        index = pd.date_range(end=pd.Timestamp.today(), periods=n_rows)
        df1 = pd.DataFrame([i + 1 for i in range(n_rows)], index=index, columns=[1])
        df2 = pd.DataFrame([i + 2 for i in range(n_rows)], index=index, columns=[2])
        df3 = pd.DataFrame([i + 3 for i in range(n_rows)], index=index, columns=[3])
        return pd.concat([df1, df2, df3], axis=1)


class GenericTests(BaseNormalizerTestCases):
    def test_init(self):
        self.skipTest('')

    def test_call(self):
        self.skipTest('')


class InitTests(BaseNormalizerTestCases):
    def setUp(self):
        super().setUp()

    def test_ts_w_columns(self):
        self.normalizer = Normalizer(self.object, self.attr_name, columns=[2, 3])

        expected = self.ts[[2, 3]]

        self.assertTrue(self.normalizer.ts.equals(expected))

    def test_buffer_diff(self):
        """ Assert that `_buffer()` operates correctly with `diff` flag """
        self.normalizer = Normalizer(self.object, self.attr_name, columns=[3, 2], diff=True)

        # a series of -1
        expected = self.ts[1] - self.ts[2]
        expected = expected.tail(self.normalizer._size)

        result = self.normalizer._buffer()
        self.assertTrue(result.equals(expected))

    def test_diff_buffer_abs(self):
        """ Assert that `_buffer()` returns absolute value with `diff` flag. """
        self.normalizer = Normalizer(self.object, self.attr_name, columns=[3, 2], diff=True, apply_abs=True)

        # a series of 1
        expected = self.ts[2] - self.ts[1]
        expected = expected.tail(self.normalizer._size)

        result = self.normalizer._buffer()
        self.assertTrue(result.equals(expected))


class BufferTests(BaseNormalizerTestCases):
    def setUp(self):
        super().setUp()

    def test_buffer(self):
        """ Assert that `_buffer()` operation is inherited. """
        self.normalizer = Normalizer(self.object, self.attr_name)

        expected = self.ts
        expected = expected.tail(self.normalizer._size)

        result = self.normalizer._buffer()
        self.assertTrue(result.equals(expected))

    def test_buffer_diff(self):
        """ Assert that `_buffer()` operates correctly with `diff` flag """
        self.normalizer = Normalizer(self.object, self.attr_name, columns=[3, 2])

        # a series of -1
        expected = self.ts[1] - self.ts[2]
        expected = expected.tail(self.normalizer._size)

        result = self.normalizer._buffer(diff=True)
        self.assertTrue(result.equals(expected))

    def test_diff_buffer_abs(self):
        """ Assert that `_buffer()` returns absolute value with `diff` flag. """
        self.normalizer = Normalizer(self.object, self.attr_name, columns=[3, 2])

        # a series of 1
        expected = self.ts[2] - self.ts[1]
        expected = expected.tail(self.normalizer._size)

        result = self.normalizer._buffer(diff=True, apply_abs=True)
        self.assertTrue(result.equals(expected))


if __name__ == '__main__':
    unittest.main()
