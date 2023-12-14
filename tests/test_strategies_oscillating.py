import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pytz import timezone

from core.markets.GeminiMarket import GeminiMarket
from core.markets.SimulatedMarket import SimulatedMarket
from models import IndicatorGroup, FailedTrade, FutureTrade
from primitives import Signal, Side, ReasonCode, TrendDirection
from strategies.IndicatorStrategy import IndicatorStrategy


class BaseOscillatingStratTests(unittest.TestCase):
    @patch("strategies.IndicatorStrategy.__abstractmethods__", set())
    def setUp(self) -> None:
        freq = '15m'
        mark = MagicMock(spec=GeminiMarket)
        self.market = SimulatedMarket(mark)
        self.market.translate_period = MagicMock(return_value=freq)
        self.strategy = IndicatorStrategy(market=self.market, freq=freq,
                                         indicators=[], threshold=0.1, capital=100)


class MainOscillatingStrategyTests(BaseOscillatingStratTests):
    def test_init(self):
        self.assertIsInstance(self.strategy.timeout, str)
        self.assertIsInstance(self.strategy.indicators, IndicatorGroup)
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
        self.assertTrue(self.strategy._allow_signal(Signal.BUY))
        # test first sell is rejected when empty
        self.assertFalse(self.strategy._allow_signal(Signal.SELL))

        # test Signal.HOLD returns false
        self.strategy.orders = pd.DataFrame([1])
        self.assertFalse(self.strategy._allow_signal(Signal.HOLD))

        # test buy -> sell
        self.strategy.orders = pd.DataFrame([Side.BUY], columns=['side'])
        self.assertTrue(self.strategy._allow_signal(Signal.SELL, timeout=False))
        self.assertFalse(self.strategy._allow_signal(Signal.BUY, timeout=False))

        # test sell -> buy
        self.strategy.orders = pd.DataFrame([Side.SELL], columns=['side'])
        self.assertTrue(self.strategy._allow_signal(Signal.BUY, timeout=False))
        self.assertFalse(self.strategy._allow_signal(Signal.SELL, timeout=False))

        # test buy -> buy w/ timeout
        import datetime as dt

        then = dt.datetime.now() - dt.timedelta(hours=7)
        self.strategy.orders = pd.DataFrame({'side': [Signal.BUY], 'id': 'id', 'amt': 0, 'rate': 0}, index=[then])
        self.assertTrue(self.strategy._allow_signal(Signal.BUY))


class DeterminePositionTests(BaseOscillatingStratTests):
    def test_point(self):
        """ Assert passed argument and default value are handled correctly """

        self.strategy._allow_signal = MagicMock(return_value=True)
        self.strategy._is_profitable = MagicMock(return_value=True)
        self.strategy.indicators.signal = MagicMock(return_value=Signal.SELL)
        self.strategy._calc_amount = MagicMock(return_value=1)
        self.strategy._calc_rate = MagicMock(return_value=1)

        import datetime as dt
        index = [pd.Timestamp(dt.datetime.now() - dt.timedelta(hours=i)) for i in range(10)]
        index.reverse()

        self.market._data = pd.DataFrame({'blah': 'blah'}, index=index)

        # Test when `point` is not passed
        result = self.strategy._determine_position()
        expected = index[-1]
        self.assertEqual(result.point, expected)

        # Test when `point` is passed
        expected = index[5]
        result = self.strategy._determine_position(point=expected)
        self.assertEqual(result.point, expected)

    def test_false_on_hold(self):
        self.strategy.indicators.signal = MagicMock(return_value=Signal.HOLD)

        # assert false when `check()` returns `Signal.HOLD`
        self.assertFalse(self.strategy._determine_position(point=pd.Timestamp.now()))

    def test_oscillation(self):
        """ Test when out of sync with oscillation. """
        self.strategy._allow_signal = MagicMock(return_value=False)
        self.strategy.indicators.signal = MagicMock(return_value=NotImplemented)

        # assert false when `Signal.BUY/SELL`, but `_oscillation()` is False
        self.assertFalse(self.strategy._determine_position(point=pd.Timestamp.now()))

    def test_not_profitable(self):
        """ Test when sale is not profitable """
        self.strategy._allow_signal = MagicMock(return_value=True)
        self.strategy._is_profitable = MagicMock(return_value=False)
        self.strategy.indicators.signal = MagicMock(return_value=Signal.SELL)
        self.strategy._calc_amount = MagicMock(return_value=1)
        self.strategy._calc_rate = MagicMock(return_value=1)

        # assert false when `_oscillation()` is True, but `_is_profitable()` is False
        result = self.strategy._determine_position(point=pd.Timestamp.now())
        self.assertFalse(result)
        self.assertIsInstance(result, FutureTrade)
        self.assertIs(result.load, ReasonCode.NOT_PROFITABLE)

    def test_return_value(self):
        """ Assert correct return structure """
        self.strategy._allow_signal = MagicMock(return_value=True)
        self.strategy._is_profitable = MagicMock(return_value=True)
        self.strategy.indicators.signal = MagicMock(return_value=Signal.BUY)
        self.strategy._calc_amount = MagicMock(return_value=1)
        self.strategy._calc_rate = MagicMock(return_value=2)

        import datetime as dt
        now = pd.Timestamp(dt.datetime.now())
        then = now - dt.timedelta(hours=1)
        self.market.data = pd.DataFrame(index=[then, now])

        result = self.strategy._determine_position(then)

        # check returned type
        self.assertIsInstance(result, FutureTrade)
        self.assertTrue(result.attempt)
        self.assertEqual(then, result.point)
        self.assertEqual(Side(Signal.BUY), result.side)
        self.assertEqual(1, result.amt)
        self.assertEqual(2, result.rate)


if __name__ == '__main__':
    unittest.main()
