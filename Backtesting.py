""" Backtesting.py

Used for backtesting, improving and debugging strategies and models
"""

from strategies.Strategy import Strategy
import pandas as pd
import matplotlib.pyplot as plt
from typing import Union
import logging


class Backtesting(object):
    """ Class to test strategies on existing ticker data.

    This plots market data and shows when a given strategy will
    decide to buy, hold, or sell.
    """
    def __init__(self, strategy: Strategy):
        self.strategy = strategy

    @property
    def market(self):
        return self.strategy.market

    def process_timeframes(self, start: Union[pd.Timestamp, str] = None, end: Union[pd.Timestamp, str] = None):
        """
        Process market data between given dates/timeframes

        Args:
            start: timestamp or date. If `None`, go to beginning of ticker data.
            end: timestamp or date. If `None`, go to end of ticker data.
        """
        if start is None:
            start = self.market.data.iloc[0].name
        if end is None:
            end = self.market.data.iloc[-1].name

        logging.info("Beginning to process data")

        freq = self.market.data.attrs['freq']
        freq = self.market.convert_freq(freq)

        frames = pd.date_range(start, end, freq=freq)
        for frame in frames:
            # TODO: log updates on data processing
            self.strategy.process(frame)

        logging.info("Finished processing data")

    def plot(self, start: Union[pd.Timestamp, str] = None, end: Union[pd.Timestamp, str] = None):
        """
        Plot strategy decisions between given dates/timestamps

        Args:
            start: date or timestamp to begin plot
            end: date or timestamp to end plot
        """
        if start is None:
            start = self.market.data.iloc[0].name
        if end is None:
            end = self.market.data.iloc[-1].name

        self.market.data.loc[start:end]['close'].plot(color='blue')

        orders = self.strategy.orders.loc[start:end]
        buys = orders[orders['side'] == 'buy']
        sells = orders[orders['side'] == 'sell']
        if not buys.empty:
            plt.scatter([pd.to_datetime(i.name) for i in buys], buys['rate'], marker='^', s=100, color='orange')
        if not sells.empty:
            plt.scatter([pd.to_datetime(i.name) for i in sells], sells['rate'], marker='v', s=100, color='red')

        plt.show()


def increment_minute(timestamp):
    return pd.Timestamp(timestamp) + pd.offsets.Minute(1)


def decrement_minute(timestamp):
    return pd.Timestamp(timestamp) - pd.offsets.Minute(1)
