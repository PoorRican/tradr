import pandas as pd
import unittest
from unittest.mock import patch, MagicMock

from analysis.financials import FinancialsMixin
from models.trades import SuccessfulTrade, Side
from strategies.strategy import Strategy


class FinancialsMixinTestCase(unittest.TestCase):
    @patch("analysis.financials.FinancialsMixin.__abstractmethods__", set())
    def setUp(self) -> None:
        with patch('core.market.Market') as cls:
            self.market = cls()

        self.strategy = FinancialsMixin(market=self.market, threshold=.1, capital=500, assets=0)

    def test_init(self):
        self.assertIsInstance(self.strategy, Strategy)

        self.assertEqual(self.strategy.threshold, .1)
        self.assertEqual(self.strategy.capital, 500)
        self.assertEqual(self.strategy.assets, 0)
        self.assertEqual(self.strategy.order_count, 4)

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
        self.strategy.capital = 25

        trade = SuccessfulTrade(5, 5, Side.BUY, None)
        self.strategy._adjust_capitol(trade)
        self.assertEqual(self.strategy.capital, 0)

        trade = SuccessfulTrade(3, 10, Side.SELL, None)
        self.strategy._adjust_capitol(trade)
        self.assertEqual(self.strategy.capital, 30)

        # assert warning is raised when set to negative value
        with self.assertWarns(Warning):
            trade = SuccessfulTrade(8, 5, Side.BUY, None)
            self.strategy._adjust_capitol(trade)
            self.assertEqual(-10, self.strategy.capital)

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
        self.strategy._adjust_capitol = MagicMock()
        self.strategy._adjust_assets = MagicMock()

        trade = SuccessfulTrade(0, 0, Side.BUY, 0)
        self.strategy._post_sale(trade)

        self.strategy._clean_incomplete.assert_called_with(trade)
        self.strategy._adjust_assets.assert_called_with(trade)
        self.strategy._adjust_capitol.assert_called_with(trade)

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


if __name__ == '__main__':
    unittest.main()
