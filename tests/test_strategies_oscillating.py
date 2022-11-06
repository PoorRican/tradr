import unittest
from unittest.mock import patch, MagicMock
import pandas as pd

from core.markets import GeminiMarket, SimulatedMarket
from models.signals import Signal, IndicatorContainer
from models.trades import Side, SuccessfulTrade
from strategies import OscillatingStrategy


class MainOscillatingStrategyTests(unittest.TestCase):
    @patch("strategies.OscillatingStrategy.__abstractmethods__", set())
    def setUp(self) -> None:
        mark = GeminiMarket(update=False)
        self.market = SimulatedMarket(mark)
        self.strategy = OscillatingStrategy(starting=100, market=self.market,
                                            indicators=())

    def test_init(self):
        self.assertIsInstance(self.strategy.incomplete, pd.Series)
        self.assertIsInstance(self.strategy.timeout, str)
        self.assertIsInstance(self.strategy.indicators, IndicatorContainer)
        # TODO: assert `indicators` param gets passed to `IndicatorContainer`

    def test_check_timeout(self):
        import datetime as dt

        # check under timeout
        then = dt.datetime.now() - dt.timedelta(hours=5)
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
        self.strategy.orders = pd.DataFrame({'side': [Signal.BUY], 'id': 'id'}, index=[then])
        self.assertTrue(self.strategy.incomplete.empty)
        self.assertTrue(self.strategy._oscillation(Signal.BUY))
        self.assertEqual(self.strategy.incomplete.iloc[0], 'id')

    def test_post_sale(self):
        # check that sold unpaired buys are removed
        self.strategy.orders = pd.DataFrame({'id': [1, 2, 3, 4], 'rate': [5, 6, 7, 8]})
        self.strategy.incomplete = pd.Series([1, 2, 3])
        trade = SuccessfulTrade(1, 6, Side.SELL, 9)
        self.strategy._post_sale(trade)
        self.assertEqual(self.strategy.incomplete.to_list(), [3])

        # check untouched for buy
        self.strategy.orders = pd.DataFrame({'id': [1, 2, 3, 4], 'rate': [5, 6, 7, 8]})
        self.strategy.incomplete = pd.Series([2, 3])
        trade = SuccessfulTrade(1, 6, Side.BUY, 9)
        self.strategy._post_sale(trade)
        self.assertEqual(self.strategy.incomplete.to_list(), [2, 3])

    def test_check_unpaired(self):
        self.strategy.orders = pd.DataFrame({'id': [1, 2, 3, 4], 'rate': [5, 6, 7, 8]})
        self.strategy.incomplete = pd.Series([1, 2])
        self.assertEqual(self.strategy._check_unpaired(6).to_dict(), {'id': {0: 1, 1: 2}, 'rate': {0: 5, 1: 6}})

    def test_get_unpaired_orders(self):
        self.strategy.orders = pd.DataFrame({'id': [1, 2, 3, 4], 'other': [5, 6, 7, 8]})
        self.strategy.incomplete = pd.Series([1, 2])
        self.assertEqual(self.strategy.get_unpaired_orders().to_dict(), {'id': {0: 1, 1: 2}, 'other': {0: 5, 1: 6}})


class DeterminePositionTests(unittest.TestCase):
    @patch("strategies.OscillatingStrategy.__abstractmethods__", set())
    def setUp(self) -> None:
        mark = GeminiMarket(update=False)
        self.market = SimulatedMarket(mark)
        self.strategy = OscillatingStrategy(starting=100, market=self.market,
                                            indicators=())

    def test_point(self):
        """ Assert passed argument and default value are handled correctly """

        self.strategy._oscillation = MagicMock(return_value=True)
        self.strategy._is_profitable = MagicMock(return_value=True)
        self.strategy.indicators.check = MagicMock(return_value=Signal.SELL)

        import datetime as dt
        index = [pd.Timestamp(dt.datetime.now() - dt.timedelta(hours=i)) for i in range(10)]
        index.reverse()
        self.market.data = pd.DataFrame({'blah': 'blah'}, index=index)
        self.assertEqual(self.strategy._determine_position(), (Signal.SELL, index[-1]))
        self.assertEqual(self.strategy._determine_position(point=index[5]), (Signal.SELL, index[5]))

    def test_false_on_hold(self):
        self.strategy.indicators.check = MagicMock(return_value=Signal.HOLD)

        # assert false when `check()` returns `Signal.HOLD`
        self.assertFalse(self.strategy._determine_position())

    def test_oscillation(self):
        """ Test when out of sync with oscillation. """
        self.strategy._oscillation = MagicMock(return_value=False)
        self.strategy.indicators.check = MagicMock(return_value=NotImplemented)

        # assert false when `Signal.BUY/SELL`, but `_oscillation()` is False
        self.assertFalse(self.strategy._determine_position())

    def test_not_profitable(self):
        """ Test when trade is not profitable """
        self.strategy._oscillation = MagicMock(return_value=True)
        self.strategy._is_profitable = MagicMock(return_value=False)
        self.strategy.indicators.check = MagicMock(return_value=Signal.SELL)

        # assert false when `_oscillation()` is True, but `_is_profitable()` is False
        self.assertFalse(self.strategy._determine_position())

    def test_return_structure(self):
        """ Assert correct return structure """
        self.strategy._oscillation = MagicMock(return_value=True)
        self.strategy._is_profitable = MagicMock(return_value=True)
        self.strategy.indicators.check = MagicMock(return_value=Signal.BUY)

        import datetime as dt
        now = pd.Timestamp(dt.datetime.now())
        then = now - dt.timedelta(hours=1)
        self.market.data = pd.DataFrame(index=[then, now])
        self.assertEqual(self.strategy._determine_position(then), (Signal.BUY, then))


if __name__ == '__main__':
    unittest.main()
