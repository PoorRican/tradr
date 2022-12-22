from enum import IntEnum
from matplotlib.colors import to_rgba
from matplotlib.pyplot import Figure
import matplotlib.pyplot as plt
import pandas as pd
from typing import Mapping, Sequence, Union, NoReturn

from models import IndicatorContainer
from models.indicators import BBANDSRow, MACDRow, STOCHRSIRow
from strategies import ThreeProngAlt


class Location(IntEnum):
    """ Abstracts sub-plot indexes """
    PRIMARY = 0
    ASSETS = 1


# TODO: create parent object that serves as base wrapper for ThreeProngAlt models
class Plotter(object):
    """ Wrapper that plots `ThreeProngAlt` models """
    rows = 4

    def __init__(self, model: ThreeProngAlt):
        self.model = model
        self.subplots: Sequence[Figure] = self._init_figure(self.rows)

    @classmethod
    def _init_figure(cls, rows: int) -> Sequence[Figure]:
        fig, ax = plt.subplots(nrows=rows, figsize=[16, 9*4], dpi=250)
        return ax

    @property
    def candles(self):
        return self.model.candles

    @property
    def indicators(self):
        return self.model.indicators

    def _plot_indicators(self, endpoints: Mapping[str, 'pd.Timestamp']):
        macd = 2
        stoch = 3
        container: IndicatorContainer = self.indicators

        container[BBANDSRow].plot(self.subplots, **endpoints, render=False)
        container[MACDRow].plot(self.subplots, macd, **endpoints, render=False)
        container[STOCHRSIRow].plot(self.subplots, stoch, **endpoints, render=False)

    def graph(self, ):
        """ Plot graph of market price, """
        pass

    def performance(self):
        pass

    def plot(self, start: Union['pd.Timestamp', str] = None, width: str = '1d',
             render: bool = True) -> NoReturn:
        """ Plot trade enter and exit points as an overlay to market data.
        """
        if start:
            if type(start) is str:
                point = pd.Timestamp(start)
            else:
                point = start
            delta = pd.Timedelta(width)
            start = point - delta
            stop = point + delta
            candles = self.candles.loc[start:stop]
            assets = self.model.assets_ts.loc[start:stop]
            capital = self.model.capital_ts.loc[start:stop]
            endpoints = {'start': start, 'stop': stop}  # dict to be passed as kwargs
            orders = self.model.orders[start:stop]
        else:
            candles = self.candles
            assets = self.model.assets_ts
            capital = self.model.capital_ts
            endpoints = {}
            orders = self.model.orders

        pri = self.subplots[Location.PRIMARY]
        pri.plot(candles.index, candles['close'], color=to_rgba('blue', 0.8))

        self._plot_decisions(orders)
        self._plot_money(index=candles.index, assets=assets, capital=capital)
        self._plot_indicators(endpoints)

        if render:
            plt.show()

    def _plot_money(self, index: pd.Index, assets: pd.Series, capital: pd.Series):
        """ Plot acquired capital and assets """
        sec = self.subplots[1]
        _last = assets.iloc[-1]
        _assets: pd.Series = assets.copy()
        _assets = _assets.reindex(index, fill_value=None)
        _assets.iloc[-1] = _last
        _assets = _assets.interpolate()
        sec.plot(_assets.index, _assets, color="purple")

        sec2 = sec.twinx()
        _last = capital.iloc[-1]
        _capital = capital.copy()
        _capital = _capital.reindex(index, fill_value=None)
        _capital.iloc[-1] = _last
        _capital = _capital.interpolate()
        sec2.plot(_capital.index, _capital.values, color="green")

    def _plot_decisions(self, orders: pd.DataFrame):
        """ Plot trade enter and exit locations. """
        figure = self.subplots[Location.PRIMARY]
        size = 10
        scalar = 4

        sells = orders[orders['side'] == -1]
        buys = orders[orders['side'] == 1]

        # normalize amt column so that the smallest value is 1
        _min_buys = buys['amt'].min()
        _max_buys = buys['amt'].max()
        if _min_buys == _max_buys:
            _min_buys = 0

        _min_sells = sells['amt'].min()
        _max_sells = sells['amt'].max()
        if _min_sells == _max_sells:
            _min_sells = 0

        _buys = ((buys['amt'] - _min_buys) / (_max_buys - _min_buys)) + size
        _sells = ((sells['amt'] - _min_sells) / (_max_sells - _min_sells)) + size

        # plot decision outcomes
        if len(buys) > 0:
            figure.scatter(buys.index, buys['rate'], _buys * scalar, marker="^", color='red')
        if len(sells) > 0:
            figure.scatter(sells.index, sells['rate'], _sells * scalar, marker="v", color='green')
        if len(self.model.failed_orders) > 0:
            figure.scatter(self.model.failed_orders.index, self.model.failed_orders['rate'], color="black")
