import unittest
from unittest.mock import patch
from math import floor

import pandas as pd

from models.trades import Trade, SuccessfulTrade, add_to_df


class TestTrade(unittest.TestCase):
    def test_attr(self):
        """ Test that attributes are properly calculated """
        amt = 2
        rate = 7
        trade = Trade(amt, rate, 'buy')

        self.assertEqual(trade.amt, amt, 'trade.amt incorrect')
        self.assertEqual(trade.rate, rate, 'trade.rate incorrect')
        self.assertEqual(amt * rate, trade.cost, 'trade.cost incorrect')

    def test_attr_cost_init(self):
        """ Test that cost cannot be passed to `__init__()`"""
        with self.assertRaises(TypeError):
            Trade(5, 5, 'buy', cost=5)

        with self.assertRaises(TypeError):
            Trade(5, 5, 'buy', 5)

    def test_containerize_columns(self):
        """ Test that containerized produced the right column names and order """
        df = Trade.container()
        quantized = floor(df.columns.isin(('amt', 'rate', 'side', 'cost')).mean())
        self.assertTrue(bool(quantized))


class TestSuccessfulTrade(unittest.TestCase):
    def test_inheritance(self):
        """ Verify valid parent inheritance
        """
        df = Trade.container()
        quantized = floor(df.columns.isin(('amt', 'rate', 'side', 'cost')).mean())
        self.assertTrue(bool(quantized))

    def test_arg_id(self):
        """ Test that id initialized correctly """
        t = SuccessfulTrade(5, 5, 'buy', 7)
        self.assertEqual(t.id, 7)

    def test_containerize_columns(self):
        """ Test that containerize produced the right column names and order """
        df = Trade.container()
        quantized = floor(df.columns.isin(('amt', 'rate', 'side', 'cost', 'id')).mean())
        self.assertTrue(bool(quantized))


class TestAddToDf(unittest.TestCase):
    def test_inplace_insertion(self):
        """ Verify that insertion is done in-place"""
        with patch('strategies.strategy.Strategy') as cls:
            instance = cls()
            instance.orders = pd.DataFrame([5], index=[1])
        add_to_df(instance, container='orders', extrema=2, instance=10)
        self.assertEqual(len(instance.orders), 2)

    def test_no_duplicate(self):
        """ Assert that adding duplicate index raises an error """
        extrema = 1
        with patch('strategies.strategy.Strategy') as cls:
            obj = cls()
            obj.orders = pd.DataFrame([5], index=[extrema])
        with self.assertRaises(IndexError):
            add_to_df(obj, container='orders', extrema=extrema, instance=1)

    def test_arg_force(self):
        """ Test that `force` argument allows data for duplicate index """
        extrema = '10/20/2030'
        with patch('strategies.strategy.Strategy') as cls:
            obj = cls()
            obj.orders = pd.DataFrame([5], index=[extrema])
        add_to_df(obj, container='orders', extrema=extrema, instance=1, force=True)
        self.assertEqual(len(obj.orders), 2)

    def test_arg_container_dne(self):
        """ Assert an error is raised when container does not exist"""
        with self.assertRaises(AttributeError):
            obj = dict()
            add_to_df(obj, "dne", "10/20/2030", 'object')


if __name__ == '__main__':
    unittest.main()
