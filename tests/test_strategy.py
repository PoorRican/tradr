import datetime as dt
from os import mkdir, listdir
from os import path
import pandas as pd
from shutil import rmtree
import unittest
from unittest.mock import patch, MagicMock, create_autospec
from yaml import safe_load, safe_dump

from core.market import Market
from models.trades import SuccessfulTrade, Trade, Side
from strategies.strategy import Strategy, DATA_ROOT


class BaseStrategyTestCase(unittest.TestCase):
    @patch('strategies.strategy.Strategy.__abstractmethods__', set())
    def setUp(self):
        self.market = create_autospec(Market, instance=True)
        self.strategy = Strategy(self.market)


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
        self.skipTest('Load argument passed to `Strategy.__init__()` is deprecated as of v0.2.0-alpha.')

        with patch.object(Strategy, 'load', side_effect=RuntimeError) as _mock_load:
            Strategy(self.market, load=False)

        with patch.object(Strategy, 'load') as _mock_load:
            Strategy(self.market, load=True)
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
        self.market.place_order = MagicMock(return_value=trade)
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

        self.market.place_order = MagicMock(return_value=False)
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
        self.market.data = MagicMock()

        with patch.object(self.strategy.indicators, 'develop') as _mock_develop:
            self.assertIsNone(self.strategy.calculate_all())
            _mock_develop.assert_called_once_with(self.market.data)

    def test_only_detector(self):
        """ Test when `detector` are the only available instance attribute. """
        self.strategy.detector = MagicMock()
        self.market.data = MagicMock()

        with patch.object(self.strategy.detector, 'develop') as _mock_develop:
            self.assertIsNone(self.strategy.calculate_all())
            _mock_develop.assert_called_once()

    def test_high_level(self):
        """ Test when instance has both `indicators` and `detector` attributes. """
        self.strategy.detector = MagicMock()
        self.strategy.detector.develop = MagicMock()

        self.strategy.indicators = MagicMock()
        self.strategy.indicators.develop = MagicMock()

        self.market.data = MagicMock()

        self.assertIsNone(self.strategy.calculate_all())
        self.strategy.indicators.develop.assert_called_once_with(self.market.data)
        self.strategy.detector.develop.assert_called_once()


class StrategySerializationTests(BaseStrategyTestCase):
    """ Ensure `save()` and `load()` methods work as expected. """
    # data should be verified in temp dir
    def setUp(self):
        super().setUp()
        self.root = f"/tmp/data_{dt.datetime.now()}"
        with patch(f"strategies.strategy.DATA_ROOT", self.root) as _root:
            mkdir(_root)

            self.strategy.root = _root

        self.market.__name__ = 'MockMarket'
        self.market.symbol = 'MockSymbol'

    def tearDown(self):
        super().setUp()

        rmtree(self.root)

    def test_save(self):
        # test an arbitrary sequenced attribute
        _attr_name = 'mock_df'
        setattr(self.strategy, _attr_name, pd.DataFrame())
        # TODO: shouldn't an error be raised when adding a frame that didn't previously exist

        self.strategy.save()

        _dir = self.strategy._instance_dir
        dirs = ('literals.yml', 'orders.yml', 'failed_orders.yml',
                f"{_attr_name}.yml",)
        _files = listdir(self.strategy._instance_dir)
        for i in _files:
            self.assertIn(i, dirs)
        for i in dirs:
            self.assertIn(i, _files)

        # TODO: verify file contents

    def test_load_invalid(self):
        """ Test when an attribute that isn't part of instance attributes tries to get added via
        literal storage. """
        self.strategy.save()

        _fn = path.join(self.strategy._instance_dir, 'literals.yml')
        with open(_fn, 'r') as f:
            literals: dict = safe_load(f)

        self.assertIsInstance(literals, dict)
        literals['test'] = 'test'
        with open(_fn, 'w') as f:
            safe_dump(literals, f)

        with self.assertRaises(AssertionError):
            self.strategy.load()

    def test_load(self):
        # test an arbitrary sequenced attribute
        _attr_name = 'mock_series'
        setattr(self.strategy, _attr_name, pd.Series())
        # TODO: shouldn't an error be raised when adding a frame that didn't previously exist

        self.strategy.save()
        _arbitrary_series_data = {1: 'test'}
        with open(path.join(self.strategy._instance_dir, f"{_attr_name}.yml"), 'w') as f:
            safe_dump(_arbitrary_series_data, f)

        _fn = path.join(self.strategy._instance_dir, 'literals.yml')
        with open(_fn, 'r') as f:
            literals: dict = safe_load(f)
        self.assertIsInstance(literals, dict)
        literals['root'] = 'test'
        with open(_fn, 'w') as f:
            safe_dump(literals, f)

        self.strategy.load()
        self.assertTrue(hasattr(self.strategy, 'root'))
        self.assertEqual(getattr(self.strategy, 'root'), 'test')

        # test arbitrary data
        self.assertEqual(_arbitrary_series_data, getattr(self.strategy, _attr_name).to_dict())

        # TODO: test that dataframes are properly loaded


if __name__ == '__main__':
    unittest.main()
