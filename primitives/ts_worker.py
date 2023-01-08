from abc import ABC, abstractmethod
from typing import Any, List, Union

import pandas as pd


class TimeseriesWorker(ABC):
    """ Functor that accesses a timeseries object and performs windowed operations. """

    def __init__(self, obj: Any, attr: str, columns: List[str] = None, buffer_size: int = 75, verify: bool = True):
        assert hasattr(obj, attr)

        self._obj = obj
        self._attr = attr
        self.columns = columns

        # verify instance and parameter types
        if verify:
            if columns is not None:
                _attr: Union['pd.DataFrame', 'pd.Series'] = getattr(obj, attr)
                for col in self.columns:
                    assert col in _attr
            assert type(self.ts) in (pd.Series, pd.DataFrame)
            assert type(self.ts.index) is pd.DatetimeIndex

        self._size = buffer_size

    @property
    def ts(self) -> Union['pd.Series', 'pd.DataFrame']:
        """ Access timeseries
        """
        attr = getattr(self._obj, self._attr)
        if self.columns is None:
            return attr
        return attr[self.columns]

    @abstractmethod
    def __call__(self, point: pd.Timestamp = None, *args, **kwargs) -> Any:
        pass

    def _buffer(self, point: pd.Timestamp = None) -> pd.Series:
        """ Get buffer from `ts`.

        Args:
            point:
                Point in time. If None, then last index is used in `ts`. Value is inclusive
                and row is returned alongside returned.

        Returns:
            `Series` of indexed data *before* `point` of length `_size`.
        """
        if point is None:
            point = self.ts.index[-1]
        assert point in self.ts.index

        if len(self.ts) < self._size:
            return self.ts

        indexed = self.ts.loc[:point]
        if len(indexed) < self._size:
            return indexed
        return indexed.tail(self._size)
