""" Evaluate strategy performance, improve and debug strategies and models.

Notes:
    TODO:
        - Replace `Market.BASE_URL` values with test URLs
"""

import pandas as pd
import matplotlib.pyplot as plt
from typing import Union
import logging

from misc import TZ
from strategies.IndicatorStrategy import IndicatorStrategy


class Simulation(object):
    """ Class to test strategies on existing candle data.

    Plots market data and shows when a given strategy will decide to buy, hold, or sell.

    Notes:
        TODO:
            - Implement a way to store and access historical values.
    """

    current_progress: int
    strategy: IndicatorStrategy

    def __init__(self, strategy: IndicatorStrategy):
        self.current_progress = 0
        self.strategy = strategy

    @property
    def market(self):
        return self.strategy.market

    def print_progress(self, current: int, interval: int = 10) -> None:
        """ Print current progress

        Args:
            current: current timeframe being processed
            interval: percentage as a whole number; interval at which to show progress. Defaults to `10`, showing
                completion every 10%.
        """
        progress = self._progress(current)
        if progress > 0 and progress % interval == 0 and progress > self.current_progress:
            self.current_progress = progress
            print("%d completed" % progress)

    def _progress(self, current: int) -> int:
        """ Show percentage complete as an integer """
        total = len(self.strategy.candles)
        return (100 * current) // total

    def process_timeframes(self, start: Union[pd.Timestamp, str] = None, end: Union[pd.Timestamp, str] = None):
        """
        Process market data between given dates/timeframes

        Args:
            start: timestamp or date. If `None`, go to beginning of ticker data.
            end: timestamp or date. If `None`, go to end of ticker data.
        """
        if start is None:
            start = self.strategy.candles.iloc[0].name
        if end is None:
            end = self.strategy.candles.iloc[-1].name

        msg = "Starting simulation"
        logging.info(msg)
        print(msg)

        freq = self.market.translate_period(self.strategy.freq)

        msg = "Pre-processing data"
        logging.info(msg)
        print(msg)
        self.strategy.calculate_all()

        msg = "Beginning to process decision data"
        logging.info(msg)
        print(msg)
        points = pd.date_range(start, end, freq=freq, tz=TZ)
        for i, _point in enumerate(points):

            # remove `freq` attribute
            point = pd.Timestamp.fromtimestamp(_point.timestamp(), tz=TZ)

            # TODO: enable multithreading
            self.print_progress(i)
            self.strategy.process(point)

        msg = "Finished processing data"
        logging.info(msg)
        print(msg)

    def plot(self, start: Union[pd.Timestamp, str] = None, end: Union[pd.Timestamp, str] = None):
        """
        Plot strategy decisions between given dates/timestamps

        Args:
            start: date or timestamp to begin plot
            end: date or timestamp to end plot
        """
        if start is None:
            start = self.strategy.candles.iloc[0].name
        if end is None:
            end = self.strategy.candles.iloc[-1].name

        self.strategy.candles.loc[start:end]['close'].plot(color='blue')

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
