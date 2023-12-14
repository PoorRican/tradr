import pandas as pd
import unittest
from unittest.mock import patch, MagicMock, create_autospec

from core.market import Market
from models import SuccessfulTrade, Trade, FutureTrade, FailedTrade
from primitives import Side, ReasonCode
from strategies.strategy import Strategy


class BaseStrategyTestCase(unittest.TestCase):
    @patch('strategies.strategy.Strategy.__abstractmethods__', set())
    def setUp(self):
        self.market = create_autospec(Market, instance=True)
        self.strategy = Strategy(self.market, freq='')
        self.strategy.build_order_handler(threshold=0.1)

    @staticmethod
    def prevent_call(obj, func: str):
        setattr(obj, func, MagicMock(side_effect=RuntimeError(f'`{func}()` should not have been called')))


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
        extrema = pd.Timestamp.now()
        trade = FutureTrade(0, 0, Side.BUY, True, extrema)
        self.market.post_order = MagicMock(return_value=trade.convert(0))

        self.strategy._post_sale = MagicMock()
        self.prevent_call(self.strategy, '_calc_amount')
        self.prevent_call(self.strategy, '_calc_rate')

        with patch('strategies.strategy.add_to_df') as _mock_add_to_df:
            result = self.strategy._add_order(trade)
            self.assertTrue(result)
            self.assertIsInstance(result, SuccessfulTrade)

            self.strategy._post_sale.assert_called_once()
            _mock_add_to_df.assert_called_once_with(self.strategy, 'orders', extrema, result)

    def test_post_sale(self):
        self.strategy.order_handler = MagicMock()
        self.strategy.order_handler._clean_incomplete = MagicMock()
        self.strategy.order_handler._adjust_capital = MagicMock()
        self.strategy.order_handler._adjust_assets = MagicMock()

        trade = SuccessfulTrade(0, 0, Side.BUY, 0)
        now = pd.Timestamp.now()
        self.strategy._post_sale(now, trade)

        self.strategy.order_handler._clean_incomplete.assert_called_with(trade)
        self.strategy.order_handler._adjust_assets.assert_called_with(trade, now)
        self.strategy.order_handler._adjust_capital.assert_called_with(trade, now)


    def test_add_failed_order(self):
        extrema = pd.Timestamp.now()
        trade = FutureTrade(0, 0, Side.BUY, False, extrema)
        self.market.post_order = MagicMock(return_value=trade.convert(0))

        self.prevent_call(self.strategy, '_post_sale')
        self.prevent_call(self.strategy, '_calc_amount')
        self.prevent_call(self.strategy, '_calc_rate')

        with patch('strategies.strategy.add_to_df') as _mock_add_to_df:
            result = self.strategy._add_order(trade)
            self.assertFalse(result)
            self.assertIsInstance(result, FailedTrade)

            _mock_add_to_df.assert_called_once_with(self.strategy, 'failed_orders', extrema, result)


class StrategyProcessTests(BaseStrategyTestCase):
    """ Test `process()` in a variety of scenarios. """

    @patch.object(Strategy, '_buy', return_value='BUY')
    def test_buy_position(self, _mock_buy: MagicMock):
        """ Test when buy should be performed. """
        extrema = pd.Timestamp.now()
        trade = FutureTrade(0, 0, Side.BUY, True, extrema)
        with patch.object(Strategy, '_determine_position',
                          return_value=trade) as _mock_det_pos:
            result = self.strategy.process(extrema)

            self.assertEqual(result, 'BUY')
            _mock_det_pos.assert_called_once_with(extrema)
            _mock_buy.assert_called_once_with(trade)

    @patch.object(Strategy, '_sell', return_value='SELL')
    def test_sell_position(self, _mock_sell: MagicMock):
        """ Test when sell should be performed. """
        extrema = pd.Timestamp.now()
        trade = FutureTrade(0, 0, Side.SELL, True, extrema)
        with patch.object(Strategy, '_determine_position',
                          return_value=trade) as _mock_det_pos:
            result = self.strategy.process(extrema)

            self.assertEqual(result, 'SELL')
            _mock_det_pos.assert_called_once_with(extrema)
            _mock_sell.assert_called_once_with(trade)

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

        trade = FutureTrade(0, 0, Side.SELL, True, extrema)
        self.strategy.orders = pd.DataFrame(index=[trade.point])

        with patch.object(Strategy, '_determine_position',
                          return_value=trade) as _mock_det_pos:
            result = self.strategy.process(trade.point)
            self.assertEqual(result, False)

        trade.side = Side.BUY
        with patch.object(Strategy, '_determine_position',
                          return_value=trade) as _mock_det_pos:
            result = self.strategy.process(trade.point)
            self.assertEqual(result, False)


class StrategyBuySellTests(BaseStrategyTestCase):
    def test_buy(self):
        extrema = pd.Timestamp.now()
        trade = FutureTrade(0, 0, Side.BUY, True, extrema)
        load = 'id'
        with patch.object(Strategy, '_add_order',
                          return_value=trade.convert(load)) as _mock_add_order:

            result = self.strategy._buy(trade)

            # TODO: verify logging
            self.assertTrue(result)
            _mock_add_order.assert_called_once_with(trade)

    def test_buy_rejected(self):
        extrema = pd.Timestamp.now()
        trade = FutureTrade(0, 0, Side.BUY, True, extrema)
        with patch.object(Strategy, '_add_order',
                          return_value=trade.convert(ReasonCode.MARKET_REJECTED, False)) as _mock_add_order:
            result = self.strategy._buy(trade)

            self.assertFalse(result)
            _mock_add_order.assert_called_once_with(trade)

    def test_sell(self):
        extrema = pd.Timestamp.now()
        trade = FutureTrade(0, 0, Side.SELL, True, extrema)
        load = 'id'
        with patch.object(Strategy, '_add_order',
                          return_value=trade.convert(load)) as _mock_add_order:

            result = self.strategy._sell(trade)

            # TODO: verify logging
            self.assertTrue(result)
            _mock_add_order.assert_called_once_with(trade)

    def test_sell_rejected(self):
        extrema = pd.Timestamp.now()
        trade = FutureTrade(0, 0, Side.SELL, True, extrema)
        with patch.object(Strategy, '_add_order',
                          return_value=trade.convert(ReasonCode.MARKET_REJECTED, False)) as _mock_add_order:
            result = self.strategy._buy(trade)

            self.assertFalse(result)
            _mock_add_order.assert_called_once_with(trade)


class StrategyCalcAllTests(BaseStrategyTestCase):
    def test_without_attributes(self):
        """ ensure that no error is raised without appropriate attributes """
        self.assertIsNone(self.strategy.calculate_all())

    def test_only_indicators(self):
        """ Test when `indicators` are the only available instance attribute. """
        self.strategy.indicators = MagicMock()
        self.market._data = MagicMock()

        self.assertIsNone(self.strategy.calculate_all())
        self.strategy.indicators.update.assert_called_once()

    def test_only_detector(self):
        """ Test when `detector` are the only available instance attribute. """
        self.strategy.detector = MagicMock()
        self.market._data = MagicMock()

        self.assertIsNone(self.strategy.calculate_all())
        self.strategy.detector.update.assert_called_once()

    def test_high_level(self):
        """ Test when instance has both `indicators` and `detector` attributes. """
        self.strategy.detector = MagicMock()
        self.strategy.indicators = MagicMock()

        self.market._data = MagicMock()

        self.assertIsNone(self.strategy.calculate_all())
        self.strategy.indicators.update.assert_called_once()
        self.strategy.detector.update.assert_called_once()


if __name__ == '__main__':
    unittest.main()
