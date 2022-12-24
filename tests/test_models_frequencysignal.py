from math import isnan
import pandas as pd
import unittest
from unittest.mock import MagicMock, create_autospec, patch

from core import GeminiMarket
from misc import TZ
from models import FrequencySignal
from models.indicators import TestingIndicator
from primitives import Signal


class BaseFrequencySignal(unittest.TestCase):
    @patch('models.indicator.Indicator.__abstractmethods__', set())
    def setUp(self):
        self.market = create_autospec(GeminiMarket, instance=True)
        self.market.valid_freqs = ['15m']
        self.market.translate_period = MagicMock(return_value=pd.Timedelta(minutes=15))
        indicators = []
        for i in range(3):
            indicators.append(TestingIndicator())
        self.obj: FrequencySignal = FrequencySignal(self.market, '15m', indicators, update=False)

        self.index = pd.date_range(pd.Timestamp.now(), tz=TZ, freq='15m', periods=3)
        self.graph = pd.DataFrame({'first': [0, 1, 2], 'second': [3, 4, 5]},
                                  index=self.index)
        self.computed = pd.DataFrame({'signal': [0, 1, -1], 'strength': [-2, -3, -4]},
                                     index=self.index)

        _last = self.index[-1]
        self.obj.last_update = _last

        for i in self.obj.indicators:
            i.graph = self.graph.copy()
            i.computed = self.computed.copy()

            self.assertTrue(i.graph.equals(self.graph))
            self.assertTrue(i.computed.equals(self.computed))

    def test_homogenous_signal(self):
        """ Homogenous values should always return true regardless of `unison` """
        # assert `signal` returns signal when all values are the same
        idx = self.index[2]
        self.assertEqual(Signal.SELL, self.obj.signal(idx))

    def test_homogenous_call(self):
        """ Homogenous values should always return true regardless of `unison` """
        # assert call returns `signal` when all values are the same
        # assert call returns correct `strength` when all values are the same
        idx = self.index[2]
        signal, strength = self.obj(idx)
        self.assertEqual(Signal.SELL, signal)
        self.assertEqual(-4, strength)

    def test_ambiguous_signal(self):
        """ Ambiguous values should always return `HOLD` regardless of `unison` """
        idx = self.index[2]
        self.assertEqual(Signal.SELL, self.obj.signal(idx))
        self.obj.indicators[1].computed.loc[idx, 'signal'] = 0
        self.obj.indicators[2].computed.loc[idx, 'signal'] = 1

        self.assertEqual(Signal.HOLD, self.obj.signal(idx))

    def test_ambiguous_call(self):
        """ Ambiguous values should always return `HOLD` regardless of `unison` """
        # assert call returns `HOLD` when one value is different
        # assert call returns correct `strength` when one value is different
        idx = self.index[2]
        self.assertEqual(Signal.SELL, self.obj.signal(idx))
        self.obj.indicators[1].computed.loc[idx, 'signal'] = 1
        self.obj.indicators[2].computed.loc[idx, 'signal'] = 0

        signal, strength = self.obj(idx)
        self.assertEqual(Signal.HOLD, signal)
        self.assertTrue(isnan(strength))


class TestUnisonTrue(BaseFrequencySignal):
    """ Assert that heterogeneous values return `CYCLE` when `unison=True`. """
    def setUp(self):
        super().setUp()

        self.obj.unison = True

    def test_heterogeneous_signal(self):
        # assert signal returns `HOLD` when one value is different
        idx = self.index[2]
        self.assertEqual(Signal.SELL, self.obj.signal(idx))
        self.obj.indicators[2].computed.loc[idx, 'signal'] = 1

        self.assertEqual(Signal.HOLD, self.obj.signal(idx))

    def test_heterogeneous_call(self):
        idx = self.index[2]
        self.assertEqual(Signal.SELL, self.obj.signal(idx))
        self.obj.indicators[2].computed.loc[idx, 'signal'] = 1

        signal, strength = self.obj(idx)
        self.assertEqual(Signal.HOLD, signal)
        self.assertTrue(isnan(strength))


class TestUnisonFalse(BaseFrequencySignal):
    """ Assert that heterogeneous values return True when `unison=False`. """
    def setUp(self):
        super().setUp()

        self.obj.unison = False
        self.idx = self.index[2]

    def test_heterogeneous_signal(self):
        # assert signal returns signal when one value is different
        self.assertEqual(Signal.SELL, self.obj.signal(self.idx))
        self.obj.indicators[2].computed.loc[self.idx, 'signal'] = 1

        signal, strength = self.obj(self.idx)
        self.assertEqual(Signal.SELL, signal)

    def test_heterogeneous_call(self):
        # assert call returns `HOLD` when one value is different
        # assert call returns correct averaged `strength` when one value is different
        self.obj.indicators[2].computed.loc[self.idx, 'signal'] = 1
        self.obj.indicators[1].computed.loc[self.idx, 'strength'] = 4

        signal, strength = self.obj(self.idx)
        self.assertEqual(Signal.SELL, signal)

        # assert that masking was applied `[-4, 4].mean()`
        self.assertEqual(0, strength)


if __name__ == '__main__':
    unittest.main()
