import unittest

from models.trades import Trade, SuccessfulTrade
from core.markets import SimulatedMarket


class MySimulatedMarket(unittest.TestCase):
    def test_convert(self):
        """ Test that `_convert` returns a `SuccessfulTrade` object """
        trade = Trade(1, 2, 'buy')
        market = SimulatedMarket()
        self.assertIsInstance(market._convert(trade), SuccessfulTrade,
                              'SimulatedMarket not returned')

        self.assertEqual(market, 1, 'orders counter not increased')

    def test_place_order(self):
        """ Test that `place_order` always succeeds"""
        market = SimulatedMarket()
        trade = Trade(1, 2, 'buy')
        for i in range(10):
            self.assertTrue(market.place_order(trade), 'trade not accepted')
        self.assertEqual(market.orders, 10)


if __name__ == '__main__':
    unittest.main()
