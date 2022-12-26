import datetime as dt
from os import mkdir, listdir
from os import path
import pandas as pd
from shutil import rmtree
import unittest
from unittest.mock import patch, MagicMock, create_autospec
from yaml import safe_load, safe_dump

from core.market import Market
from models import SuccessfulTrade, Trade
from primitives import Side
from strategies.strategy import Strategy


class BaseStrategyTestCase(unittest.TestCase):
    @patch('strategies.strategy.Strategy.__abstractmethods__', set())
    def setUp(self):
        self.market = create_autospec(Market, instance=True)
        self.strategy = Strategy(self.market, freq='')


class GenericStrategyTests(BaseStrategyTestCase):
    def test_init(self):
        # check initialized containers
        self.assertIsInstance(self.strategy.orders, pd.DataFrame,
                              "`orders` is not a DataFrame object")

        self.assertIsInstance(self.strategy.failed_orders, pd.DataFrame,
                              "`failed_orders` is not a DataFrame object")

        self.assertIsInstance(self.strategy.failed_orders, pd.DataFrame,
                              "`failed_orders` is not a DataFrame object")

        # check attributes
        self.assertIsInstance(self.strategy.market, Market, "Incorrect type for `market`")

        self.assertIsInstance(self.strategy.root, str)

    @patch('strategies.strategy.Strategy.__abstractmethods__', set())
    def test_init_load(self):
        """ Check that passing `load=True` calls load function. """
        with patch.object(Strategy, 'load', side_effect=RuntimeError) as _mock_load:
            Strategy(self.market, freq='', load=False)

        with patch.object(Strategy, 'load') as _mock_load:
            Strategy(self.market, freq='', load=True)
            _mock_load.assert_called_once()

    def test_instance_dir(self):
        """ Verify functionality of instance dir """
        self.skipTest('Not Implemented')

    def test_calc_profit(self):
        # mock order
        # mock market fee
        # ensure sub-cent value is truncated
        self.skipTest('Not Implemented')


class StrategyAddOrderTests(BaseStrategyTestCase):
    def test_add_successful_order(self):
        self.strategy._calc_amount = MagicMock(return_value=0)
        self.strategy._calc_rate = MagicMock(return_value=0)

        trade = SuccessfulTrade(0, 0, Side.BUY, 'id')
        self.market.post_order = MagicMock(return_value=trade)
        self.strategy._post_sale = MagicMock()

        with patch('strategies.strategy.add_to_df') as _mock_add_to_df:
            extrema = pd.Timestamp.now()
            result = self.strategy._add_order(extrema, Side.BUY)
            self.assertEqual(result, trade)

            self.strategy._calc_rate.assert_called_once()
            self.strategy._calc_amount.assert_called_once()
            self.strategy._post_sale.assert_called_once_with(extrema, trade)
            _mock_add_to_df.assert_called_once_with(self.strategy, 'orders', extrema, trade)

    def test_add_failed_order(self):
        self.strategy._calc_amount = MagicMock(return_value=0)
        self.strategy._calc_rate = MagicMock(return_value=0)

        self.market.post_order = MagicMock(return_value=False)
        self.strategy._post_sale = MagicMock(side_effect=RuntimeError('`_post_sale()` should not have been called'))

        with patch('strategies.strategy.add_to_df') as _mock_add_to_df:
            extrema = pd.Timestamp.now()
            trade = Trade(0, 0, Side.BUY)
            result = self.strategy._add_order(extrema, Side.BUY)
            self.assertEqual(result, False)

            self.strategy._calc_rate.assert_called_once()
            self.strategy._calc_amount.assert_called_once()
            _mock_add_to_df.assert_called_once_with(self.strategy, 'failed_orders', extrema, trade)


class StrategyProcessTests(BaseStrategyTestCase):
    """ Test `process()` in a variety of scenarios. """

    @patch.object(Strategy, '_buy', return_value='BUY')
    def test_buy_position(self, _mock_buy: MagicMock):
        """ Test when buy should be performed. """
        extrema = pd.Timestamp.now()
        with patch.object(Strategy, '_determine_position',
                          return_value=(Side.BUY, extrema)) as _mock_det_pos:
            result = self.strategy.process(extrema)

            self.assertEqual(result, 'BUY')
            _mock_det_pos.assert_called_once_with(extrema)
            _mock_buy.assert_called_once_with(extrema)

    @patch.object(Strategy, '_sell', return_value='SELL')
    def test_sell_position(self, _mock_sell: MagicMock):
        """ Test when sell should be performed. """
        extrema = pd.Timestamp.now()
        with patch.object(Strategy, '_determine_position',
                          return_value=(Side.SELL, extrema)) as _mock_det_pos:
            result = self.strategy.process(extrema)

            self.assertEqual(result, 'SELL')
            _mock_det_pos.assert_called_once_with(extrema)
            _mock_sell.assert_called_once_with(extrema)

    def test_false_position(self):
        """ Test when a trade should not be made. """
        extrema = pd.Timestamp.now()
        with patch.object(Strategy, '_determine_position',
                          return_value=False) as _mock_det_pos:
            result = self.strategy.process(extrema)

            self.assertEqual(result, False)
            _mock_det_pos.assert_called_once_with(extrema)

    def test_existing_order(self):
        """ Test that no action is taken when there is an existing order"""
        extrema = pd.Timestamp.now()

        self.strategy.orders = pd.DataFrame(index=[extrema])

        with patch.object(Strategy, '_determine_position',
                          return_value=(Side.SELL, extrema)) as _mock_det_pos:
            result = self.strategy.process(extrema)
            self.assertEqual(result, False)

        with patch.object(Strategy, '_determine_position',
                          return_value=(Side.BUY, extrema)) as _mock_det_pos:
            result = self.strategy.process(extrema)
            self.assertEqual(result, False)


class StrategyBuySellTests(BaseStrategyTestCase):
    @patch.object(Strategy, '_add_order', return_value=SuccessfulTrade(0, 0, Side.BUY, 'id'))
    def test_buy(self, _mock_add_order: MagicMock):
        extrema = pd.Timestamp.now()
        result = self.strategy._buy(extrema)

        # TODO: verify logging
        self.assertTrue(result)
        _mock_add_order.assert_called_once_with(extrema, Side.BUY)

    @patch.object(Strategy, '_add_order', return_value=False)
    def test_buy_rejected(self, _mock_add_order: MagicMock):
        extrema = pd.Timestamp.now()
        result = self.strategy._buy(extrema)

        self.assertFalse(result)

    @patch.object(Strategy, '_add_order', return_value=SuccessfulTrade(0, 0, Side.SELL, 'id'))
    def test_sell(self, _mock_add_order: MagicMock):
        extrema = pd.Timestamp.now()
        result = self.strategy._sell(extrema)

        # TODO: verify logging
        self.assertTrue(result)
        _mock_add_order.assert_called_once_with(extrema, Side.SELL)

    @patch.object(Strategy, '_add_order', return_value=False)
    def test_sell_rejected(self, _mock_add_order: MagicMock):
        extrema = pd.Timestamp.now()
        result = self.strategy._sell(extrema)

        self.assertFalse(result)


class StrategyCalcAllTests(BaseStrategyTestCase):
    def test_without_attributes(self):
        """ ensure that no error is raised without appropriate attributes """
        self.assertIsNone(self.strategy.calculate_all())

    def test_only_indicators(self):
        """ Test when `indicators` are the only available instance attribute. """
        self.strategy.indicators = MagicMock()
        self.market._data = MagicMock()

        self.assertIsNone(self.strategy.calculate_all())
        self.strategy.indicators.compute.assert_called_once()

    def test_only_detector(self):
        """ Test when `detector` are the only available instance attribute. """
        self.strategy.detector = MagicMock()
        self.market._data = MagicMock()

        self.assertIsNone(self.strategy.calculate_all())
        self.strategy.detector.update.assert_called_once()

    def test_high_level(self):
        """ Test when instance has both `indicators` and `detector` attributes. """
        self.strategy.detector = MagicMock()
        self.strategy.detector.update = MagicMock()

        self.strategy.indicators = MagicMock()
        self.strategy.indicators.compute = MagicMock()

        self.market._data = MagicMock()

        self.assertIsNone(self.strategy.calculate_all())
        self.strategy.indicators.compute.assert_called_once()
        self.strategy.detector.update.assert_called_once()


if __name__ == '__main__':
    unittest.main()
