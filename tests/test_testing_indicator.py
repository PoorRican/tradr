import pandas as pd
import unittest

from misc import TZ
from models.indicators import TestingIndicator


class TestingIndicatorTestCase(unittest.TestCase):
    def setUp(self):
        self.obj = TestingIndicator()
        self.index = pd.date_range(pd.Timestamp.now(), tz=TZ, freq='15m', periods=3)
        self.graph = pd.DataFrame({'first': [0, 1, 2], 'second': [3, 4, 5]},
                                  index=self.index)
        self.obj.graph = self.graph.copy()
        self.computed = pd.DataFrame({'signal': [0, 1, -1], 'strength': [-2, -3, -4]},
                                     index=self.index)
        self.obj.computed = self.computed.copy()

        self.assertTrue(self.graph.equals(self.obj.graph))
        self.assertTrue(self.computed.equals(self.obj.computed))

    def test_compute(self):
        self.obj.compute_decision(self.graph)
        self.assertTrue(self.computed.equals(self.obj.computed))

    def test_signal(self):
        for i in self.index:
            self.assertEqual(self.computed.loc[i, 'signal'], int(self.obj.signal(i, self.graph)))

    def test_strength(self):
        for i in self.index:
            self.assertEqual(self.computed.loc[i, 'strength'], int(self.obj.strength(i, self.graph)))


if __name__ == '__main__':
    unittest.main()
