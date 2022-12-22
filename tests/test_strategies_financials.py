import pandas as pd
import unittest
from unittest.mock import patch, MagicMock

from strategies.financials import FinancialsMixin
from models import SuccessfulTrade
from primitives import Side
from strategies.strategy import Strategy


class BaseFinancialsMixinTestCase(unittest.TestCase):
    @patch("analysis.financials.FinancialsMixin.__abstractmethods__", set())
    def setUp(self) -> None:
        with patch('core.market.Market') as cls:
            self.market = cls()

        self.capital = 500
        self.threshold = .1
        self.assets = 1
        self.order_count = 4
        self.strategy = FinancialsMixin(market=self.market, threshold=self.threshold,
                                        capital=self.capital, assets=self.assets, order_count=self.order_count)


class FinancialsMixinTestCase(BaseFinancialsMixinTestCase):

    def test_init(self):
        self.assertIsInstance(self.strategy, Strategy)

        self.assertEqual(self.strategy.threshold, self.threshold)
        self.assertEqual(self.strategy.capital, self.capital)
        self.assertEqual(self.strategy.assets, self.assets)
        self.assertEqual(self.strategy.order_count, self.order_count)

    def test_check_unpaired(self):
        """ Assert that orders with rates lower than given value are returned """
        self.strategy.orders = pd.DataFrame({'id': [1, 2, 3, 4], 'rate': [5, 6, 7, 8]})
        self.strategy.incomplete = pd.DataFrame({'id': [1, 2], 'rate': [5, 6], 'amt': [10, 10]})

        expected = pd.DataFrame({'id': [1, 2], 'rate': [5, 6]})
        self.assertTrue(self.strategy._check_unpaired(6).equals(expected))

        # TODO: check of `original` flag

    def test_unpaired(self):
        """ Assert that """
        self.strategy.orders = pd.DataFrame({'id': [1, 2, 3, 4], 'other': [5, 6, 7, 8]})
        self.strategy.incomplete = pd.DataFrame({'id': [1, 2]})

        expected = pd.DataFrame({'id': [1, 2], 'other': [5, 6]})
        self.assertTrue(self.strategy.unpaired().equals(expected))

    def test_clean_incomplete(self):
        # check that completely sold unpaired buys are removed
        self.strategy.orders = pd.DataFrame({'id': [1, 2, 3, 4], 'rate': [5, 6, 7, 8]})
        self.strategy.incomplete = pd.DataFrame({'id': [1, 2, 3], 'amt': [10, 10, 10], 'rate': [5, 6, 7]})
        trade = SuccessfulTrade(21, 6, Side.SELL, 9)
        self.strategy._clean_incomplete(trade)
        self.assertEqual([3], self.strategy.incomplete['id'].to_list())

        # check untouched for buy
        self.strategy.orders = pd.DataFrame({'id': [1, 2, 3, 4], 'rate': [5, 6, 7, 8]})
        self.strategy.incomplete = pd.DataFrame({'id': [2, 3], 'rate': [6, 7], 'amt': [12, 14]})
        trade = SuccessfulTrade(1, 10, Side.BUY, 9)
        self.strategy._clean_incomplete(trade)
        self.assertEqual([2, 3], self.strategy.incomplete['id'].to_list())

    def test_remaining(self):
        self.assertTrue(self.strategy.incomplete.empty)

        self.strategy.order_count = 5
        self.assertEqual(self.strategy._remaining, 5)

        self.strategy.incomplete = [i for i in range(5)]
        self.assertEqual(self.strategy._remaining, 0)

    def test_post_sale(self):
        self.strategy._clean_incomplete = MagicMock()
        self.strategy._adjust_capital = MagicMock()
        self.strategy._adjust_assets = MagicMock()

        trade = SuccessfulTrade(0, 0, Side.BUY, 0)
        now = pd.Timestamp.now()
        self.strategy._post_sale(now, trade)

        self.strategy._clean_incomplete.assert_called_with(trade)
        self.strategy._adjust_assets.assert_called_with(trade, now)
        self.strategy._adjust_capital.assert_called_with(trade, now)

    def test_handle_inactive(self):
        self.assertTrue(self.strategy.incomplete.empty)

        row = pd.Series({'amt': 5, 'rate': 5, 'id': 5, 'side': Side.BUY})

        self.strategy._handle_inactive(row)

        self.assertTrue(row['id'] in self.strategy.incomplete['id'].values)

        # check that values remain after second addition
        row2 = pd.Series({'amt': 2, 'rate': 2, 'id': 2, 'side': Side.BUY})
        self.strategy._handle_inactive(row2)

        self.assertTrue(row['id'] in self.strategy.incomplete['id'].values)
        self.assertTrue(row2['id'] in self.strategy.incomplete['id'].values)

        self.assertEqual(len(self.strategy.incomplete), 2)

        # ============================== #
        # assert exceptions and warnings #

        # side must be `BUY
        with self.assertRaises(AssertionError):
            invalid_row = pd.Series({'amt': 5, 'rate': 5, 'id': 5, 'side': Side.SELL})
            self.strategy._handle_inactive(invalid_row)

        # row must be `Series`
        with self.assertRaises(AssertionError):
            # noinspection PyTypeChecker
            self.strategy._handle_inactive(pd.DataFrame())

        # warn upon adding duplicates
        with self.assertWarns(Warning):
            self.strategy._handle_inactive(row)

    def test_deduct_sold(self):
        # check that second row is dropped, and difference deducted from the third
        trade = SuccessfulTrade(6, 50, Side.SELL, None)
        self.strategy.incomplete = pd.DataFrame({'id': [1, 2, 3], 'amt': [5, 4, 3], 'rate': [60, 50, 40]})
        self.strategy.orders = pd.DataFrame({'id': [4], 'rate': ['50'], 'amt': [1], 'side': [Side.BUY]})
        self.strategy._deduct_sold(trade, self.strategy._check_unpaired(trade.rate, False))

        # assert that first value is kept and second has been dropped
        self.assertTrue(1 in self.strategy.incomplete['id'].values)
        self.assertFalse(2 in self.strategy.incomplete['id'].values)
        self.assertEqual(2, self.strategy.incomplete.iloc[1].amt)

    def test_starting(self):
        self.strategy.capital = 1000
        self.strategy.order_count = 10
        self.strategy.incomplete = []
        self.assertEqual(self.strategy.starting, 100)

        self.strategy.incomplete = [1] * 10
        with self.assertWarns(Warning):
            self.assertEqual(self.strategy.starting, 100)


class AssetsCapitalTests(BaseFinancialsMixinTestCase):
    def test_init(self):
        self.assertTrue(hasattr(self.strategy, '_assets'))
        self.assertIsInstance(self.strategy._assets, pd.Series)

        self.assertTrue(hasattr(self.strategy, '_capital'))
        self.assertIsInstance(self.strategy._capital, pd.Series)
        self.assertEqual(self.capital, self.strategy.capital)

    def protected_attribute_getter_test(self, interface: str, attr: str):
        _val = 3
        _data = {1: 5, 2: _val}
        setattr(self.strategy, attr, pd.Series(_data))
        self.assertEqual(_val, getattr(self.strategy, interface))

    def protected_attribute_setter_test(self, interface: str, attr: str):
        _val = 50
        setattr(self.strategy, interface, _val)
        self.assertEqual(_val, getattr(self.strategy, attr).iloc[-1],
                         f"{attr} not appended to private sequence")
        self.assertIsInstance(getattr(self.strategy, attr).index[-1], pd.Timestamp,
                              f"{attr} was not indexed with a `Timestamp`")

        _val += 1
        now = pd.Timestamp.now()
        setattr(self.strategy, interface, (now, _val))
        self.assertEqual(_val, getattr(self.strategy, attr).iloc[-1],
                         f"{attr} not appended to private sequence")
        self.assertEqual(now, getattr(self.strategy, attr).index[-1],
                         f"Timestamp passed to {attr}.setter was not used as index "
                         "or was not appended.")

        # test that init value, and both sets are stored in sequence
        self.assertEqual(3, len(getattr(self.strategy, attr)))

    def test_assets_functionality(self):
        _args = ('assets', '_assets')
        self.protected_attribute_setter_test(*_args)
        self.protected_attribute_getter_test(*_args)

    def test_capital_functionality(self):
        _args = ('capital', '_capital')
        self.protected_attribute_setter_test(*_args)
        self.protected_attribute_getter_test(*_args)

    def test_adjust_assets(self):
        self.strategy.assets = 5

        trade = SuccessfulTrade(5, 5, Side.BUY, None)
        self.strategy._adjust_assets(trade)
        self.assertEqual(self.strategy.assets, 10)

        trade = SuccessfulTrade(3, 5, Side.SELL, None)
        self.strategy._adjust_assets(trade)
        self.assertEqual(self.strategy.assets, 7)

        # assert warning is raised when set to negative value
        with self.assertWarns(Warning):
            trade = SuccessfulTrade(8, 5, Side.SELL, None)
            self.strategy._adjust_assets(trade)
            self.assertEqual(self.strategy.assets, -1)

    def test_adjust_capitol(self):
        now = pd.Timestamp.now()
        self.strategy.capital = (now, 25)

        trade = SuccessfulTrade(5, 5, Side.BUY, None)
        self.strategy._adjust_capital(trade)
        self.assertEqual(self.strategy.capital, 0)

        trade = SuccessfulTrade(3, 10, Side.SELL, None)
        self.strategy._adjust_capital(trade)
        self.assertEqual(self.strategy.capital, 30)

        # assert warning is raised when set to negative value
        with self.assertWarns(Warning):
            trade = SuccessfulTrade(8, 5, Side.BUY, None)
            self.strategy._adjust_capital(trade)
            self.assertEqual(-10, self.strategy.capital)


class PNLTestCases(BaseFinancialsMixinTestCase):
    def test_pnl(self):
        self.skipTest('')

    def test_unrealized_gain(self):
        # fill incomplete
        _incomplete_rates = [200, 150, 225]
        _incomplete_amt = [3, 2, 1]
        _incomplete_ids = [1, 2, 3]
        incomplete = pd.DataFrame({'rate': _incomplete_rates, 'amt': _incomplete_amt, 'id': _incomplete_ids})
        self.strategy.unpaired = MagicMock(return_value=incomplete)

        # fill orders
        _order_rates = [150, 250]
        _order_amt = [2, 1]
        _order_side = [Side.BUY, Side.BUY]
        _order_ids = [2, 4]
        orders = pd.DataFrame({'rate': _order_rates, 'amt': _order_amt, 'id': _order_ids, 'side': _order_side})
        self.strategy.orders = orders

        last_order = self.strategy.orders.tail(1)

        # assert sum of incomplete + last order
        unpaired = pd.concat([incomplete, last_order], ignore_index=True)
        _max = max(unpaired['rate'])
        _sum = unpaired['amt'].sum() * _max

        self.assertEqual(_sum, 7 * 250, 'Expected calculation is incorrect')
        self.assertEqual(_sum, self.strategy.unrealized_gain(), 'Unrealized gain did not return correct sum')


if __name__ == '__main__':
    unittest.main()
