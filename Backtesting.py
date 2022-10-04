""" Backtesting.py

Used for backtesting, improving and debugging strategies and models
"""

from MarketAPI import Market
from Strategy import Strategy
import pandas as pd
import matplotlib.pyplot as plt
from typing import Union
import logging


class SimulatedMarket(Market):
    """
    Mock class of `Market`.
    Has flat fee, always accepts an order.
    """
    def __init__(self):
        super().__init__()

    def place_order(self, amount, rate, side) -> Union[dict, bool]:
        return {'amount': amount,
                'price': rate,
                'side': side,
                'is_cancelled': False,
                'order_id': 0}

    @property
    def filename(self) -> str:
        return 'data/15m.pkl'

    def calc_fee(self):
        return 0.35


class Backtesting(object):
    """Class to test strategies on existing ticker data.

    This plots market data and shows when a given strategy will
    decide to buy, hold, or sell.
    """
    def __init__(self, market: Market, strategy: Strategy):
        self.market = market
        self.strategy = strategy

    def process_timeframes(self, start: Union[pd.Timestamp, str, None], end: Union[pd.Timestamp, str, None]):
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
        frames = pd.date_range(start, end, freq="15min")
        for frame in frames:
            # TODO: log updates on data processing
            self.strategy.process(self.market.data, frame)
        logging.info("Finished processing data")

    def plot(self, start: Union[pd.Timestamp, str], end: Union[pd.Timestamp, str]):
        """
        Plot strategy decisions between given dates/timestamps

        Args:
            start: date or timestamp to begin plot
            end: date or timestamp to end plot
        """
        self.market.data.loc[start:end]['close'].plot(color='blue')
        orders = self.strategy.orders.loc[start:end]
        buys = orders[orders['side'] == 'buy']
        sells = orders[orders['side'] == 'sell']
        plt.scatter([pd.to_datetime(i) for i in buys.index], buys['rate'], marker='^', s=100, color='orange')
        plt.scatter([pd.to_datetime(i) for i in sells.index], sells['rate'], marker='v', s=100, color='red')
        plt.show()
