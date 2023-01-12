from typing import Union

import pandas as pd
from sklearn.preprocessing import StandardScaler

from primitives.ts_worker import TimeseriesWorker


class Normalizer(TimeseriesWorker):
    """ Functor that accesses a timeseries object and normalizes a window of values. """
    def __init__(self, *args, diff: bool = False, apply_abs: bool = False, **kwargs):
        super().__init__(*args, **kwargs)

        self.diff = diff
        self.apply_abs = apply_abs 
        self._scalar = StandardScaler()

    def __call__(self, point: pd.Timestamp = None, *args, **kwargs) -> pd.Series:
        _buffer = self._buffer(point).copy()
        if type(_buffer) is pd.Series:
            _buffer = _buffer.to_frame()
        return pd.Series(self._scalar.fit_transform(_buffer))
    
    def _buffer(self, point: pd.Timestamp = None,
                diff: bool = None, apply_abs: bool = None) -> Union['pd.Series', 'pd.DataFrame']:
        # apply defaults
        if apply_abs is None:
            apply_abs = self.apply_abs
        if diff is None:
            diff = self.diff

        if diff:
            return self._diff(point, apply_abs)
        return super()._buffer(point)

    def _diff(self, point: pd.Timestamp = None, apply_abs: bool = False) -> pd.Series:
        # noinspection PyUnresolvedReferences
        """ Calculate the difference between two columns returned by `_buffer()`.
         
        Notes:
            The first column (as supplied `columns` initialization argument) is subtracted from the second column.

            `apply_abs` is implemented here.

        Args:
            point:
                Timestamp representing point in time to pass to `_buffer()`
            apply_abs:
                Optional flag to override instance value for `apply_abs`

        Examples:
            Assuming an object `obj` has a timeseries named `attr`, with columns "1" and "2":
                >>> normalizer = Normalizer(object, str, columns=[2, 1], diff=True)
                >>> normalizer._diff()
            Would return the equivalent to:
                >>> df[1] - df[2]
            *Note the reversal of column order between initialization and the result.*
        """
        _buffer: pd.DataFrame = super()._buffer(point)
        assert type(_buffer) is pd.DataFrame
        assert len(_buffer.columns) == 2

        _buffer: pd.Series = _buffer.diff(axis=1).iloc[:, 1]
        if apply_abs:
            return _buffer.abs()
        return _buffer
