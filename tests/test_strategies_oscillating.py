import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pytz import timezone

from core.markets.GeminiMarket import GeminiMarket
from core.markets.SimulatedMarket import SimulatedMarket
from models import IndicatorContainer
from primitives import Signal, Side
from strategies.OscillationMixin import OscillationMixin


class MainOscillatingStrategyTests(unittest.TestCase):
    @patch("strategies.OscillationMixin.OscillationMixin.__abstractmethods__", set())
    def setUp(self) -> None:
        mark = GeminiMarket(update=False)
        self.market = SimulatedMarket(mark)
        self.strategy = OscillationMixin(market=self.market,
                                         indicators=(), threshold=0.1, capital=100)

    def test_init(self):
        self.assertIsInstance(self.strategy.timeout, str)
        self.assertIsInstance(self.strategy.indicators, IndicatorContainer)
        # TODO: assert `indicators` param gets passed to `IndicatorContainer`

    def test_check_timeout(self):
        import datetime as dt

        # check under timeout
        then = dt.datetime.now(tz=timezone('US/Pacific')) - dt.timedelta(hours=5)
        self.strategy.orders = pd.DataFrame(index=[then])
        self.assertFalse(self.strategy._check_timeout())

        # check over timeout
        then = dt.datetime.now() - dt.timedelta(hours=7)
        self.strategy.orders = pd.DataFrame(index=[then])
        self.assertTrue(self.strategy._check_timeout())

    def test_oscillation(self):
        # test first buy when empty
        self.assertTrue(self.strategy._oscillation(Signal.BUY))
        # test first sell is rejected when empty
        self.assertFalse(self.strategy._oscillation(Signal.SELL))

        # test Signal.HOLD returns false
        self.strategy.orders = pd.DataFrame([1])
        self.assertFalse(self.strategy._oscillation(Signal.HOLD))

        # test buy -> sell
        self.strategy.orders = pd.DataFrame([Side.BUY], columns=['side'])
        self.assertTrue(self.strategy._oscillation(Signal.SELL, timeout=False))
        self.assertFalse(self.strategy._oscillation(Signal.BUY, timeout=False))

        # test sell -> buy
        self.strategy.orders = pd.DataFrame([Side.SELL], columns=['side'])
        self.assertTrue(self.strategy._oscillation(Signal.BUY, timeout=False))
        self.assertFalse(self.strategy._oscillation(Signal.SELL, timeout=False))

        # test buy -> buy w/ timeout
        import datetime as dt

        then = dt.datetime.now() - dt.timedelta(hours=7)
        self.strategy.orders = pd.DataFrame({'side': [Signal.BUY], 'id': 'id', 'amt': 0, 'rate': 0}, index=[then])
        self.assertTrue(self.strategy._oscillation(Signal.BUY))


class DeterminePositionTests(unittest.TestCase):
    @patch("strategies.OscillationMixin.OscillationMixin.__abstractmethods__", set())
    def setUp(self) -> None:
        mark = GeminiMarket(update=False)
        self.market = SimulatedMarket(mark)
        self.strategy = OscillationMixin(market=self.market,
                                         indicators=(), threshold=0.1, capital=100)
                                            
    def test_point(self):
        """ Assert passed argument and default value are handled correctly """

        self.strategy._oscillation = MagicMock(return_value=True)
        self.strategy._is_profitable = MagicMock(return_value=True)
        self.strategy.indicators.signal = MagicMock(return_value=Signal.SELL)

        import datetime as dt
        index = [pd.Timestamp(dt.datetime.now() - dt.timedelta(hours=i)) for i in range(10)]
        index.reverse()
        self.market.data = pd.DataFrame({'blah': 'blah'}, index=index)
        self.assertEqual(self.strategy._determine_position(), (Signal.SELL, index[-1]))
        self.assertEqual(self.strategy._determine_position(point=index[5]), (Signal.SELL, index[5]))

    def test_false_on_hold(self):
        self.strategy.indicators.signal = MagicMock(return_value=Signal.HOLD)

        # assert false when `check()` returns `Signal.HOLD`
        self.assertFalse(self.strategy._determine_position(point=pd.Timestamp.now()))

    def test_oscillation(self):
        """ Test when out of sync with oscillation. """
        self.strategy._oscillation = MagicMock(return_value=False)
        self.strategy.indicators.signal = MagicMock(return_value=NotImplemented)

        # assert false when `Signal.BUY/SELL`, but `_oscillation()` is False
        self.assertFalse(self.strategy._determine_position(point=pd.Timestamp.now()))

    def test_not_profitable(self):
        """ Test when trade is not profitable """
        self.strategy._oscillation = MagicMock(return_value=True)
        self.strategy._is_profitable = MagicMock(return_value=False)
        self.strategy.indicators.signal = MagicMock(return_value=Signal.SELL)

        # assert false when `_oscillation()` is True, but `_is_profitable()` is False
        self.assertFalse(self.strategy._determine_position(point=pd.Timestamp.now()))

    def test_return_structure(self):
        """ Assert correct return structure """
        self.strategy._oscillation = MagicMock(return_value=True)
        self.strategy._is_profitable = MagicMock(return_value=True)
        self.strategy.indicators.signal = MagicMock(return_value=Signal.BUY)

        import datetime as dt
        now = pd.Timestamp(dt.datetime.now())
        then = now - dt.timedelta(hours=1)
        self.market.data = pd.DataFrame(index=[then, now])
        self.assertEqual(self.strategy._determine_position(then), (Signal.BUY, then))


if __name__ == '__main__':
    unittest.main()
