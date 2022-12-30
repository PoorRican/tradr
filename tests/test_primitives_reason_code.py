import unittest
from typing import Generator

from primitives import ReasonCode


class ReasonCodeTests(unittest.TestCase):
    @staticmethod
    def _iter_values():
        for i in range(len(ReasonCode)):
            yield ReasonCode(i)

    def test_bool(self):
        """ Test that object still will always equal false. """
        for val in self._iter_values():
            self.assertFalse(val)  # add assertion here

    def test_int(self):
        """ Test that object still acts like `int` """
        for i, val in enumerate(self._iter_values()):
            self.assertEqual(i, val)

    def test_gt_lt(self):
        """ Test that object still acts like `int` """
        for i, val in enumerate(self._iter_values()):
            self.assertTrue(i >= val)
            self.assertTrue(val >= i)
            self.assertTrue(i <= val)
            self.assertTrue(val <= i)

            self.assertTrue(val <= val)
            self.assertTrue(val >= val)

            self.assertFalse(i < val)
            self.assertFalse(i > val)
            self.assertFalse(val < i)
            self.assertFalse(val > i)

            self.assertFalse(val < val)
            self.assertFalse(val > val)


if __name__ == '__main__':
    unittest.main()
