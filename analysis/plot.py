from enum import IntEnum
from matplotlib.colors import to_rgba
from matplotlib.pyplot import Figure
import matplotlib.pyplot as plt
import pandas as pd
from typing import Mapping, Sequence, Union, NoReturn, Tuple

from misc import TZ
from models import IndicatorGroup
from strategies import IndicatorStrategy


class Location(IntEnum):
    """ Abstracts sub-plot indexes """
    PRIMARY = 0
    ASSETS = 1


class Plotter(object):
    """ Wrapper that plots `HighFreqStrategy` models """

    def __init__(self, model: IndicatorStrategy):
        self.model = model

    @classmethod
    def _init_figure(cls, rows: int) -> Sequence[Figure]:
        fig, ax = plt.subplots(nrows=rows, figsize=[16, 9*4], dpi=350)
        return ax

    @property
    def candles(self):
        return self.model.candles

    @property
    def indicators(self):
        return self.model.indicators

    def _plot_candle_indicators(self, figure: Figure, endpoints: Mapping[str, 'pd.Timestamp']):
        container: IndicatorGroup = self.indicators

        for indicator in container:
            indicator.plot(figure, **endpoints, render=False)

    def graph(self, ):
        """ Plot graph of market price, """
        pass

    def performance(self):
        pass

    @staticmethod
    def _determine_start_stop(start: Union['pd.Timestamp', str] = None, width: str = '1d') -> Tuple[pd.Timestamp, pd.Timestamp]:
        """ Determine start and stop points for plotting

        Args:
            start: center point of window
            width: width of window
        """
        if type(start) is str:
            point = pd.Timestamp(start, tz=TZ)
        else:
            point = start

        delta = pd.Timedelta(width) / 2

        start = point - delta
        stop = point + delta

        return start, stop

    def plot(self, start: Union['pd.Timestamp', str] = None, width: str = '1d', render_failed: bool = False) -> None:
        """ Plot trade enter and exit points as an overlay to market data.

        Primary method of assessing `Strategy` performance is done visually by observing enter and exit points
        in relation to candle data.

        Args:
            start:
                Center of window when observing a specific point in time.
            width:
                Width of window for observing windows in time.
            render_failed:
                Optional flag to render failed orders on chart to reduce obfuscation from too many trades.
                Typically, rendering of failed trades should be done when focusing on a window of time
                and is not necessary for judging strategy performance.
        """
        if start:
            start, stop = self._determine_start_stop(start, width)

            candles = self.candles.loc[start:stop]
            assets = self.model.order_handler.assets_ts.loc[start:stop]
            capital = self.model.order_handler.capital_ts.loc[start:stop]
            endpoints = {'start': start, 'stop': stop}
            orders = self.model.orders[start:stop]
        else:
            candles = self.candles
            assets = self.model.order_handler.assets_ts
            capital = self.model.order_handler.capital_ts
            endpoints = {}
            orders = self.model.order_handler.orders

        plots = self._init_figure(rows=2)

        pri = plots[Location.PRIMARY]

        self._plot_candle_indicators(pri, endpoints)
        pri.plot(candles.index, candles['close'], color=to_rgba('blue', 0.8))
        self._plot_decisions(pri, orders, render_failed)

        sec = plots[Location.ASSETS]
        self._plot_money(sec, index=candles.index, assets=assets, capital=capital)

        plt.show()

    @staticmethod
    def _plot_money(figure: Figure, index: pd.Index, assets: pd.Series, capital: pd.Series) -> None:
        """ Plot acquired capital and assets """
        if assets.empty:
            return

        _last = assets.iloc[-1]
        _assets: pd.Series = assets.copy()
        _assets = _assets.reindex(index, fill_value=None)
        _assets.iloc[-1] = _last
        _assets = _assets.interpolate()
        figure.plot(_assets.index, _assets, color="purple")

        sec2 = figure.twinx()
        _last = capital.iloc[-1]
        _capital = capital.copy()
        _capital = _capital.reindex(index, fill_value=None)
        _capital.iloc[-1] = _last
        _capital = _capital.interpolate()
        sec2.plot(_capital.index, _capital.values, color="green")

    def _plot_decisions(self, figure: Figure, orders: pd.DataFrame, render_failed: bool = False) -> None:
        """ Plot trade enter and exit locations. """
        if orders.empty:
            return
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
        if render_failed and len(self.model.failed_orders) > 0:
            figure.scatter(self.model.failed_orders.index, self.model.failed_orders['rate'],
                           color=to_rgba("gray", 0.4))
