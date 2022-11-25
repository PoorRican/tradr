import unittest


class CalcAmountTestCases(unittest.TestCase):
    """ Test `_calc_amount()` in a variety of scenarios """

    def test_buy_during_uptrend(self):
        """ assert proper scalar is applied when buying during an uptrend """
        self.skipTest('')

    def test_buy_during_downtrend(self):
        """ assert proper scalar is applied when buying during a downtrend """
        self.skipTest('')

    def test_sell_during_uptrend(self):
        """ assert proper scalar is applied when selling during a uptrend """
        self.skipTest('')

    def test_sell_during_downtrend(self):
        """ assert proper scalar is applied when selling during a downtrend """
        self.skipTest('')

    def test_sell_exceeds_assets(self):
        """ assert amount returned does not exceed accumulated asset amount """
        # assert warning is raised when calculated amount does exceed assets
        self.skipTest('')

    def test_buy_exceeds_capitol(self):
        """ assert amount returned does not exceed amount of capital """
        # assert warning is raised when calculated amount exceeds capital
        self.skipTest('')


class IsProfitableTestCases(unittest.TestCase):
    """ Test `is_profitable()` in a variety of scenarios """

    def test_extreme_trend(self):
        """ Test returned value during extreme trend (`scalar > 3`) """
        # should not buy during steep uptrend
        # should not sell during steep downtrend
        self.skipTest('')

    def test_always_buy(self):
        """ Assert that all buy orders should pass """
        # pass invalid `rate` (any calculations should fail)
        self.skipTest('')

    def test_uptrend_increases_threshold(self):
        """ Assert that minimum acceptable profit increases during uptrend """
        self.skipTest('')
