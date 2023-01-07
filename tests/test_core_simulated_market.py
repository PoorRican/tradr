import unittest

from models import Trade, SuccessfulTrade
from primitives import Side
from core.markets.SimulatedMarket import SimulatedMarket


class SimulatedMarketTestCases(unittest.TestCase):
    def test_translate(self):
        """ Test that `_convert` returns a `SuccessfulTrade` object """
        trade = Trade(1, 2, Side.BUY)
        market = SimulatedMarket()
        self.assertIsInstance(market._translate(trade), SuccessfulTrade,
                              'SimulatedMarket not returned')

        self.assertEqual(market.orders, 1, 'orders counter not increased')

    def test_place_order(self):
        """ Test that `place_order` always succeeds"""
        market = SimulatedMarket()
        trade = Trade(1, 2, Side.BUY)
        for i in range(10):
            self.assertTrue(market.post_order(trade), 'trade not accepted')
        self.assertEqual(market.orders, 10)


if __name__ == '__main__':
    unittest.main()
