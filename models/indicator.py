from abc import ABC, abstractmethod
from math import isnan, floor
from warnings import warn

import matplotlib as plt
from matplotlib.colors import to_rgba
from matplotlib.pyplot import Figure
import pandas as pd
from typing import Sequence, ClassVar, Callable, Dict, NoReturn, Tuple, Union

from primitives import Signal


MAX_STRENGTH = 4
""" Defines a cap for maximum scalar value returned by strength """


class Indicator(ABC):
    """
    An abstract class for a statistical indicator.

    The indicator function, defined by `_function`, is called in `process()`, which updates `graph`. `graph` is a
    DataFrame that stores the output of the indicator function on a time series that matches the supplied candle data.
    The values of `graph` are qualified by `_row_decision()` and `_row_strength()`, which return a `Signal` or float
    respectively.

    During backtesting, `process()` should be called with all available market data. During a live implementation,
    `process()` should be called with buffered candle data and `graph` will be updated atomically.

    During backtesting, computation of graph can be vectorized by calling `calculate_all()`, which uses `computed` to
    store results of `Signal` and strength. During a live implementation, `signal()` and `strength()` can both be given
    a point to derive their respective values from `graph`.
    """
    name: ClassVar[str]
    _function: ClassVar[Callable]
    """ indicator function that is passed a single column of candle data, and ambiguous keyword arguments. """

    _parameters: ClassVar[Dict] = {}
    """ Ambiguous parameters for `_function` """

    _source: ClassVar[Dict] = 'close'
    """ Stores which column of input candle data to use.
    """

    columns = ClassVar[Tuple[str, ...]]

    graph: pd.DataFrame
    """ Stores output of `_function` on a time series that matches candle data. """
    computed: pd.DataFrame
    """ Stores output of `_row_decision()` and `_row_strength()` on `graph`. """

    def __init__(self, index: pd.Index = None, lookback: int = 0):
        self._lookback = lookback

        self.graph = self.container(index)
        self.computed = self.container(index, columns=('signal', 'strength'))

    @classmethod
    def container(cls, index: Union['pd.Index', Sequence] = None, data: Union[Tuple[pd.Series, ...], pd.Series] = None,
                  columns: Sequence[str] = None) -> pd.DataFrame:
        """ A helper for creating a DataFrame

        Args:
            index:
                Index to use for DataFrame. If `None`, `data` must be given.
            data:
                Data to populate DataFrame with. If `None`, `columns` must be given.
            columns:
                Columns to use for DataFrame. If `None`, `cls.columns` is used.
        """
        if data is not None:
            if isinstance(data, pd.Series):
                _data = (data,)
            else:
                _data = [i.values.T for i in data]
            _dict = {}
            for name, _col in zip(cls.columns, _data):
                _dict[name] = _col
            return pd.DataFrame(_dict, index=index, dtype=float)

        if columns is None:
            columns = cls.columns

        return pd.DataFrame(index=index, columns=list(columns), dtype=float)

    def generate_indicator_graph(self, candles: pd.DataFrame, **kwargs) -> NoReturn:
        """ Processes incoming `data` and populates `graph`.

        This function is used for computing indicator functions with existing or new data.

        In the future, data should be atomically updated with incoming data.

        Args:
            candles:
                New candle data to process.
            **kwargs:
                Arbitrary keyword-arguments to pass to indicator function `_function()`. These arguments override the
                class property `_parameters`. Overriding indicator arguments is used for tuning and optimizing signal or
                trend detection on a per-instance basis.

        """
        assert not candles.empty

        # assert that index contains no gaps not empty
        if len(self.graph.index.notna()) != len(self.graph.index):
            raise ValueError('Resulting index contains a date-gap')

        # _not_empty = self.graph.notna()
        # updates = _not_empty.index.isin(_index)

        # setup and run indicator function
        params = self._parameters
        params.update(kwargs)

        _output = self.__class__._function(candles[self._source], **params)
        buffer = self.container(candles.index, _output)

        # TODO: atomically update graph
        # self.graph = pd.concat([self.graph, buffer.loc[updates.values]])
        self.graph = buffer

    def compute_decision(self, candles: pd.DataFrame) -> NoReturn:
        """ Compute `Signal` and strength for each row in `graph`.

        This function is used for computing `Signal` and strength for each row in `graph`.

        This function should only be called after `process()` has been called.
        """
        assert not self.graph.empty
        assert not candles.empty

        # TODO: vectorizing computation across columns should provide greater speed increase
        # TODO: setting `raw` flag to true should increase speed according to docs, however,
        #   DataFrame gets passed as ndarray, and it is not clear how it is converted to an `ndarray`

        # NOTE: in order to debug `apply()`, place breakpoint at the nested function `f()` in `Apply.__init__()`.
        # This can be found at "pandas/core/apply.py:139"
        self.computed['signal'] = self.graph.apply(self._row_decision, axis='columns', candles=candles)
        self.computed['strength'] = self.graph.apply(self._row_strength, axis='columns', candles=candles)

    @abstractmethod
    def _row_decision(self, row: Union['pd.Series', 'pd.DataFrame'], candles: pd.DataFrame) -> Signal:
        """ Determine a trade signal.

        Notes:
            For the sake of speed, no assertions or other superfluous value checks should be called in this function.
            Any verification should be performed before and outside of this function. Also, verification should only
            occur once, and not be repeated unless candle data is updated.

        TODO:
            - Convert this function to only accept a point in time, since `candles` is now accessible from `Indicator`
                object.
        """
        pass

    def signal(self, point: pd.Timestamp, candles: pd.DataFrame) -> Signal:
        """ Return computed `Signal` at `point`.

        First, `computed` is checked to see if a value has been calculated. If not, decision is calculated
        from `graph`, added to `computed`, then returned.

        A look-back has been implemented which checks the previous row, to see both signals are equal. This is to
        prevent both false-positives and add a delay to "catch" optimal price movements.

        Notes:
            This function should not be used for iteration and should only be used as a wrapper for `_row_strength()`
            for given timestamp.

        Args:
            point:
                Point in time to get `Signal` from.
            candles:
                Available market candle data. Is needed for certain implementations of `_row_decision()`.

        Returns:
            `Signal` derived from `_function` at the given `point`.
        """
        if point in self.computed.index:
            signal = self.computed.loc[point, 'signal']

            # TODO: hack when `signal` is `Series` (occurs when there is duplicate data for daily candles)
            if type(signal) is pd.Series:
                signal = signal[0]

            if not isnan(signal):
                signal = int(signal)
                idx: Union[int, slice] = self.computed.index.get_loc(point)
                if isinstance(idx, slice):
                    start = idx.start
                    stop = idx.stop
                else:
                    start = idx
                    stop = idx + 1

                if start > 0:
                    lookback = self._lookback
                    if start < self._lookback:
                        lookback = idx
                    prev_points = slice(start - lookback, stop)
                    prev = self.computed[prev_points]
                    prev = prev['signal']
                    avg = int(prev.mean())
                    if floor(abs(avg)):
                        return Signal(avg)
                else:
                    return Signal(signal)
            return Signal.HOLD

        assert len(self.graph)

        try:
            row = self.graph.loc[point]
        except KeyError:
            warn("`Indicator.signal()` has defaulted to returning `HOLD` because `point` is not in `graph`.")
            return Signal.HOLD
        decision = self._row_decision(row, candles)
        self.computed.loc[point, 'signal'] = decision
        return decision

    def strength(self, point: pd.Timestamp, candles: pd.DataFrame) -> float:
        """ Return strength from `point`.

        First, `computed` is checked to see if a value has been calculated. If not, strength is calculated
        from `graph`.

        Notes:
            This function should not be used for iteration and should only be used as a wrapper for `_row_strength()`
            for given timestamp.

        Args:
            point:
                Point in time to get `Signal` strength from.
            candles:
                Available market candle data. Is needed for certain instances of `_row_strength()`.

        Returns:
            `Signal` strength derived from `_function` at the given `point`.
        """
        if point in self.computed.index:
            strength = self.computed.loc[point, 'strength']
            if not isnan(strength):
                return float(strength)

        assert len(self.graph)

        if point:
            row = self.graph.loc[point]
        else:
            row = self.graph.iloc[-1]

        return self._row_strength(row, candles)

    @abstractmethod
    def _row_strength(self, row: Union['pd.Series', 'pd.DataFrame'], candles: pd.DataFrame) -> float:
        """ Determine strength of "trend".

        Returns:
            When corresponding signal is either `BUY` or `SELL`, returned value should be 1 through 4, corresponding
            to perceived strength of trade signal. Value should never be 0. Instead, `nan` is returned in the absence
            of a trade signal.

        Notes:
            For the sake of speed, no assertions or other superfluous value checks should be called in this function.
            Any verification should be performed before and outside of this function. Also, verification should only
            occur once, and not be repeated unless candle data is updated.

        TODO:
            - Convert this function to only accept a point in time, since `candles` is now accessible from `Indicator`
                object.
        """
        pass

    def plot(self, figure: Figure, reindex: pd.Index = None, color=to_rgba('cyan', .7),
             start: pd.Timestamp = None, stop: pd.Timestamp = None,
             render: bool = True) -> Union[NoReturn, 'Figure']:
        """ Plot onto given figure

        Args:
            figure:
                Figure to plot onto.
            reindex:
                Index to reindex graph with. If `None`, `graph` is plotted as-is.
            color:
                Color to plot with.
            start:
                Start date to plot from. If `None`, `stop` must be given.
            stop:
                Stop date to plot to. If `None`, `start` must be given.
            render:
                Whether to render figure. If `False`, figure is returned.
        """
        if start:
            assert stop is not None
            graph = self.graph.loc[start:stop]
        else:
            graph = self.graph

        for label in graph.columns:
            col = graph[label]
            if reindex:
                _col = col.reindex(reindex)
                _index = _col.index
                _values = _col.values
            else:
                _index = col.index
                _values = col.values
            figure.plot(_index, _values, color=color)

        if not render:
            return figure
        plt.show()
