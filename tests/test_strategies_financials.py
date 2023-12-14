import pandas as pd
import unittest
from unittest.mock import patch, MagicMock

from strategies.OrderHandler import OrderHandler
from models import SuccessfulTrade
from primitives import Side
from strategies.strategy import Strategy


class BaseFinancialsMixinTestCase(unittest.TestCase):
    def setUp(self) -> None:
        with patch('core.market.Market') as cls:
            self.market = cls()

        self.capital = 500
        self.threshold = .1
        self.assets = 1
        self.order_count = 4
        self.order_handler = OrderHandler(threshold=self.threshold,
                                          capital=self.capital,
                                          assets=self.assets,
                                          order_limit=self.order_count)


class FinancialsMixinTestCase(BaseFinancialsMixinTestCase):

    def test_init(self):
        self.assertEqual(self.order_handler.threshold, self.threshold)
        self.assertEqual(self.order_handler.capital, self.capital)
        self.assertEqual(self.order_handler.assets, self.assets)
        self.assertEqual(self.order_handler.order_limit, self.order_count)

    def test_check_unpaired(self):
        """ Assert that orders with rates lower than given value are returned """
        self.order_handler.orders = pd.DataFrame({'id': [1, 2, 3, 4], 'rate': [5, 6, 7, 8]})
        self.order_handler.incomplete = pd.DataFrame({'id': [1, 2], 'rate': [5, 6], 'amt': [10, 10]})

        expected = pd.DataFrame({'id': [1, 2], 'rate': [5, 6]})
        self.assertTrue(self.order_handler._check_unpaired(6).equals(expected))

        # TODO: check of `original` flag

    def test_unpaired(self):
        """ Assert that """
        self.order_handler.orders = pd.DataFrame({'id': [1, 2, 3, 4], 'other': [5, 6, 7, 8]})
        self.order_handler.incomplete = pd.DataFrame({'id': [1, 2]})

        expected = pd.DataFrame({'id': [1, 2], 'other': [5, 6]})
        self.assertTrue(self.order_handler.unpaired().equals(expected))

    def test_clean_incomplete(self):
        # check that completely sold unpaired buys are removed
        self.order_handler.orders = pd.DataFrame({'id': [1, 2, 3, 4], 'rate': [5, 6, 7, 8]})
        self.order_handler.incomplete = pd.DataFrame({'id': [1, 2, 3], 'amt': [10, 10, 10], 'rate': [5, 6, 7]})
        trade = SuccessfulTrade(21, 6, Side.SELL, 9)
        self.order_handler._clean_incomplete(trade)
        self.assertEqual([3], self.order_handler.incomplete['id'].to_list())

        # check untouched for buy
        self.order_handler.orders = pd.DataFrame({'id': [1, 2, 3, 4], 'rate': [5, 6, 7, 8]})
        self.order_handler.incomplete = pd.DataFrame({'id': [2, 3], 'rate': [6, 7], 'amt': [12, 14]})
        trade = SuccessfulTrade(1, 10, Side.BUY, 9)
        self.order_handler._clean_incomplete(trade)
        self.assertEqual([2, 3], self.order_handler.incomplete['id'].to_list())

    def test_remaining(self):
        self.assertTrue(self.order_handler.incomplete.empty)

        self.order_handler.order_limit = 5
        self.assertEqual(self.order_handler._remaining, 5)

        self.order_handler.incomplete = [i for i in range(5)]
        self.assertEqual(self.order_handler._remaining, 0)

    def test_handle_inactive(self):
        self.assertTrue(self.order_handler.incomplete.empty)

        row = pd.Series({'amt': 5, 'rate': 5, 'id': 5, 'side': Side.BUY})

        self.order_handler._handle_inactive(row)

        self.assertTrue(row['id'] in self.order_handler.incomplete['id'].values)

        # check that values remain after second addition
        row2 = pd.Series({'amt': 2, 'rate': 2, 'id': 2, 'side': Side.BUY})
        self.order_handler._handle_inactive(row2)

        self.assertTrue(row['id'] in self.order_handler.incomplete['id'].values)
        self.assertTrue(row2['id'] in self.order_handler.incomplete['id'].values)

        self.assertEqual(len(self.order_handler.incomplete), 2)

        # ============================== #
        # assert exceptions and warnings #

        # side must be `BUY
        with self.assertRaises(AssertionError):
            invalid_row = pd.Series({'amt': 5, 'rate': 5, 'id': 5, 'side': Side.SELL})
            self.order_handler._handle_inactive(invalid_row)

        # row must be `Series`
        with self.assertRaises(AssertionError):
            # noinspection PyTypeChecker
            self.order_handler._handle_inactive(pd.DataFrame())

        # warn upon adding duplicates
        with self.assertWarns(Warning):
            self.order_handler._handle_inactive(row)

    def test_deduct_sold(self):
        # check that second row is dropped, and difference deducted from the third
        trade = SuccessfulTrade(6, 50, Side.SELL, None)
        self.order_handler.incomplete = pd.DataFrame({'id': [1, 2, 3], 'amt': [5, 4, 3], 'rate': [60, 50, 40]})
        self.order_handler.orders = pd.DataFrame({'id': [4], 'rate': ['50'], 'amt': [1], 'side': [Side.BUY]})
        self.order_handler._deduct_sold(trade, self.order_handler._check_unpaired(trade.rate, False))

        # assert that first value is kept and second has been dropped
        self.assertTrue(1 in self.order_handler.incomplete['id'].values)
        self.assertFalse(2 in self.order_handler.incomplete['id'].values)
        self.assertEqual(2, self.order_handler.incomplete.iloc[1].amt)

    def test_starting(self):
        self.order_handler.capital = 1000
        self.order_handler.order_limit = 10
        self.order_handler.incomplete = []
        self.assertEqual(self.order_handler.available_capital, 100)

        self.order_handler.incomplete = [1] * 10
        with self.assertWarns(Warning):
            self.assertEqual(self.order_handler.available_capital, 100)


class AssetsCapitalTests(BaseFinancialsMixinTestCase):
    def test_init(self):
        self.assertTrue(hasattr(self.order_handler, '_assets'))
        self.assertIsInstance(self.order_handler._assets, pd.Series)

        self.assertTrue(hasattr(self.order_handler, '_capital'))
        self.assertIsInstance(self.order_handler._capital, pd.Series)
        self.assertEqual(self.capital, self.order_handler.capital)

    def protected_attribute_getter_test(self, interface: str, attr: str):
        _val = 3
        _data = {1: 5, 2: _val}
        setattr(self.order_handler, attr, pd.Series(_data))
        self.assertEqual(_val, getattr(self.order_handler, interface))

    def protected_attribute_setter_test(self, interface: str, attr: str):
        _val = 50
        setattr(self.order_handler, interface, _val)
        self.assertEqual(_val, getattr(self.order_handler, attr).iloc[-1],
                         f"{attr} not appended to private sequence")
        self.assertIsInstance(getattr(self.order_handler, attr).index[-1], pd.Timestamp,
                              f"{attr} was not indexed with a `Timestamp`")

        _val += 1
        now = pd.Timestamp.now()
        setattr(self.order_handler, interface, (now, _val))
        self.assertEqual(_val, getattr(self.order_handler, attr).iloc[-1],
                         f"{attr} not appended to private sequence")
        self.assertEqual(now, getattr(self.order_handler, attr).index[-1],
                         f"Timestamp passed to {attr}.setter was not used as index "
                         "or was not appended.")

        # test that init value, and both sets are stored in sequence
        self.assertEqual(3, len(getattr(self.order_handler, attr)))

    def test_assets_functionality(self):
        _args = ('assets', '_assets')
        self.protected_attribute_setter_test(*_args)
        self.protected_attribute_getter_test(*_args)

    def test_capital_functionality(self):
        _args = ('capital', '_capital')
        self.protected_attribute_setter_test(*_args)
        self.protected_attribute_getter_test(*_args)

    def test_adjust_assets(self):
        self.order_handler.assets = 5

        trade = SuccessfulTrade(5, 5, Side.BUY, None)
        self.order_handler._adjust_assets(trade)
        self.assertEqual(self.order_handler.assets, 10)

        trade = SuccessfulTrade(3, 5, Side.SELL, None)
        self.order_handler._adjust_assets(trade)
        self.assertEqual(self.order_handler.assets, 7)

        # assert warning is raised when set to negative value
        with self.assertWarns(Warning):
            trade = SuccessfulTrade(8, 5, Side.SELL, None)
            self.order_handler._adjust_assets(trade)
            self.assertEqual(self.order_handler.assets, -1)

    def test_adjust_capitol(self):
        now = pd.Timestamp.now()
        self.order_handler.capital = (now, 25)

        trade = SuccessfulTrade(5, 5, Side.BUY, None)
        self.order_handler._adjust_capital(trade)
        self.assertEqual(self.order_handler.capital, 0)

        trade = SuccessfulTrade(3, 10, Side.SELL, None)
        self.order_handler._adjust_capital(trade)
        self.assertEqual(self.order_handler.capital, 30)

        # assert warning is raised when set to negative value
        with self.assertWarns(Warning):
            trade = SuccessfulTrade(8, 5, Side.BUY, None)
            self.order_handler._adjust_capital(trade)
            self.assertEqual(-10, self.order_handler.capital)


class PNLTestCases(BaseFinancialsMixinTestCase):
    def test_pnl(self):
        self.skipTest('')

    def test_unrealized_gain(self):
        incomplete_order_data = {
            'rate': [100, 200, 300, 400],
            'amt': [1, 1, 1, 1],
            'id': [1, 2, 3, 4],
        }
        order_data = {
            'rate': [100, 200, 300, 400, 500],
            'amt': [1, 1, 1, 1, 1],
            'side': [Side.BUY, Side.BUY, Side.BUY, Side.BUY, Side.BUY],
            'id': [1, 2, 3, 4, 5],
        }
        # fill incomplete
        incomplete = pd.DataFrame(incomplete_order_data)
        self.order_handler.unpaired = MagicMock(return_value=incomplete)

        # fill orders
        orders = pd.DataFrame(order_data)
        self.order_handler.orders = orders

        last_order = self.order_handler.orders.tail(1)

        # assert sum of incomplete + last order
        unpaired = orders.copy()
        _max = max(unpaired['rate'])
        _sum = unpaired['amt'].sum() * _max

        self.assertEqual(_sum, 5 * 500, 'Expected calculation is incorrect')
        self.assertEqual(_sum, self.order_handler.unrealized_gain(), 'Unrealized gain did not return correct sum')


if __name__ == '__main__':
    unittest.main()
