import unittest
from unittest.mock import patch, MagicMock
from math import floor

import pandas as pd

from models import Trade, SuccessfulTrade, add_to_df
from primitives import Side


class TestTrade(unittest.TestCase):
    def test_attr(self):
        """ Test that attributes are properly calculated """
        amt = 2
        rate = 7
        trade = Trade(amt, rate, Side.BUY)

        self.assertEqual(trade.amt, amt, 'trade.amt incorrect')
        self.assertEqual(trade.rate, rate, 'trade.rate incorrect')
        self.assertEqual(amt * rate, trade.cost, 'trade.cost incorrect')

    def test_attr_cost_init(self):
        """ Test that cost cannot be passed to `__init__()`"""
        with self.assertRaises(TypeError):
            Trade(5, 5, Side.BUY, cost=5)

        with self.assertRaises(TypeError):
            Trade(5, 5, Side.BUY, 5)

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
        t = SuccessfulTrade(5, 5, Side.BUY, 7)
        self.assertEqual(t.id, 7)

    def test_containerize_columns(self):
        """ Test that containerize produced the right column names and order """
        df = Trade.container()
        quantized = floor(df.columns.isin(('amt', 'rate', 'side', 'cost', 'id')).mean())
        self.assertTrue(bool(quantized))


class TestAddToDf(unittest.TestCase):
    def setUp(self):
        self.index = [1]
        self.container = 'df'
        self.df = pd.DataFrame([5], index=self.index)
        self.object = MagicMock()
        self.object.df = self.df

    def test_inplace_insertion(self):
        """ Verify that insertion is done in-place"""
        add_to_df(self.object, container=self.container, extrema=2, instance=10)
        self.assertEqual(len(getattr(self.object, self.container)), 2)

    def test_no_duplicate(self):
        """ Assert that adding duplicate index raises an error """
        extrema = 1
        with self.assertRaises(IndexError):
            add_to_df(self.object, container=self.container, extrema=extrema, instance=1)

    def test_arg_force(self):
        """ Test that `force` argument allows data for duplicate index """
        extrema = self.index[0]
        add_to_df(self.object, container=self.container, extrema=extrema, instance=1, force=True)
        self.assertEqual(len(getattr(self.object, self.container)), 2)

    def test_arg_container_dne(self):
        """ Assert an error is raised when container does not exist"""
        with self.assertRaises(AttributeError):
            obj = dict()
            add_to_df(obj, "dne", "10/20/2030", self.container)


if __name__ == '__main__':
    unittest.main()
