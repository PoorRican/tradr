from math import isnan, nan
from typing import List
from collections import namedtuple

import pandas as pd
import unittest
from unittest.mock import MagicMock, create_autospec, patch

from core import GeminiMarket
from misc import TZ
from models import IndicatorGroup
from models.indicators import TestingIndicator
from primitives import Signal

Expected = namedtuple('Expected', ['signal', 'strength'])


class IndicatorContext(object):
    signals: List[Signal]
    strengths: List[float]
    expected: Expected


class HOMOGENOUS(IndicatorContext):
    signals = [Signal.SELL, Signal.SELL, Signal.SELL]
    strengths = [-4, -4, -4]
    expected = Expected(Signal.SELL, -4)


class HETEROGENEOUS(IndicatorContext):
    signals = [Signal.SELL, Signal.HOLD, Signal.SELL]
    strengths = [-4, -4, -4]
    expected = Expected(Signal.SELL, -4)


class AMBIGUOUS(IndicatorContext):
    signals = [Signal.BUY, Signal.SELL, Signal.HOLD]
    strengths = [-4, -4, nan]
    expected = Expected(Signal.HOLD, nan)


class CONFLICTING(IndicatorContext):
    signals = [Signal.SELL, Signal.BUY, Signal.SELL]
    strengths = [-4, -4, -4]
    expected = Expected(Signal.HOLD, nan)


class BaseFrequencySignal(unittest.TestCase):
    @patch('models.indicator.Indicator.__abstractmethods__', set())
    def setUp(self):
        self.market = create_autospec(GeminiMarket, instance=True)
        self.market.valid_freqs = ['15m']
        self.market.translate_period = MagicMock(return_value=pd.Timedelta(minutes=15))
        indicators = []
        for i in range(3):
            indicators.append(TestingIndicator())
        self.obj: IndicatorGroup = IndicatorGroup(self.market, '15m', indicators, update=False)

        self.index = pd.date_range(pd.Timestamp.now(), tz=TZ, freq='15m', periods=3)
        self.idx = self.index[-1]
        self.market.process_point = MagicMock(return_value=self.idx)
        # reference graph and computed. Accurate returned values for `TestingIndicator`.
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

    def _set_indicators(self, context: type(IndicatorContext)):
        self._set_signals(context.signals)
        self._set_strengths(context.strengths)

    def _set_strengths(self, values: List[float]):
        assert len(values) == len(self.obj.indicators)
        for i, val in enumerate(values):
            assert isinstance(val, (float, int))
            self.obj.indicators[i].computed.loc[self.idx, 'strength'] = val

    def _set_signals(self, signals: List[Signal]):
        assert len(signals) == len(self.obj.indicators)
        for i, signal in enumerate(signals):
            assert type(signal) is Signal
            self.obj.indicators[i].computed.loc[self.idx, 'signal'] = signal


class BasicFrequencySignalTests(BaseFrequencySignal):
    def test_ambiguous_signal(self):
        """ Ambiguous values should always return `HOLD` regardless of `unison` """
        self._set_signals(AMBIGUOUS.signals)
        self.assertEqual(AMBIGUOUS.expected.signal, self.obj.signal(self.idx))

    def test_ambiguous_call(self):
        """ Ambiguous values should always return `HOLD` regardless of `unison` """

        self._set_indicators(AMBIGUOUS)
        signal, strength = self.obj(self.idx)

        self.assertEqual(AMBIGUOUS.expected.signal, signal)
        self.assertTrue(isnan(strength))

    def test_conflicting_call(self):
        """ Conflicting values should always return `HOLD` regardless of `unison` """
        self._set_indicators(CONFLICTING)

        signal, strength = self.obj(self.idx)
        self.assertEqual(CONFLICTING.expected.signal, signal)
        self.assertTrue(isnan(strength))

    def test_homogenous_signal(self):
        """ Homogenous values should always return true regardless of `unison` """
        self._set_indicators(HOMOGENOUS)
        # assert `signal` returns signal when all values are the same
        self.assertEqual(HOMOGENOUS.expected.signal, self.obj.signal(self.idx))

    def test_homogenous_call(self):
        """ Homogenous values should always return true regardless of `unison` """
        # assert call returns `signal` when all values are the same
        # assert call returns correct `strength` when all values are the same
        self._set_indicators(HOMOGENOUS)

        signal, strength = self.obj(self.idx)
        self.assertEqual(HOMOGENOUS.expected.signal, signal)
        self.assertEqual(HOMOGENOUS.expected.strength, strength)


class TestUnisonTrue(BaseFrequencySignal):
    """ Assert that heterogeneous values return `CYCLE` when `unison=True`. """
    def setUp(self):
        super().setUp()

        self.obj.unison = True

    def test_heterogeneous_signal(self):
        # assert signal returns `HOLD` when one value is different
        self.obj.indicators[2].computed.loc[self.idx, 'signal'] = Signal.BUY

        self.assertEqual(Signal.HOLD, self.obj.signal(self.idx))

    def test_heterogeneous_call(self):
        self.obj.indicators[2].computed.loc[self.idx, 'signal'] = Signal.BUY

        signal, strength = self.obj(self.idx)
        self.assertEqual(Signal.HOLD, signal)
        self.assertTrue(isnan(strength))


class TestUnisonFalse(BaseFrequencySignal):
    """ Assert that heterogeneous values return True when `unison=False`. """
    def setUp(self):
        super().setUp()

        self.obj.unison = False

    def test_heterogeneous_signal(self):
        # assert signal returns signal when one value is different
        self.obj.indicators[2].computed.loc[self.idx, 'signal'] = Signal.HOLD

        signal = self.obj.signal(self.idx)
        self.assertEqual(Signal.SELL, signal)

    def test_heterogeneous_call(self):
        """ assert call returns signal and correct averaged `strength` when one value is different"""
        self.obj.indicators[2].computed.loc[self.idx, 'signal'] = Signal.HOLD
        self.obj.indicators[1].computed.loc[self.idx, 'strength'] = 4

        signal, strength = self.obj(self.idx)
        self.assertEqual(Signal.SELL, signal)

        # assert that masking was applied `[-4, 4].mean()`
        self.assertEqual(0, strength)


if __name__ == '__main__':
    unittest.main()
