from abc import ABC, abstractmethod
from typing import Any, List, Union

import pandas as pd


class TimeseriesWorker(ABC):
    """ Generic base functor that accesses a timeseries object and performs windowed operations.

    Timeseries object is directly accessed via `getattr()`. This class only accesses and manipulates timeseries data
    and does not perform any real computation by itself. There is the functionality to fetch data from multiple columns
    via the `columns` argument.

    Attributes:
        _obj:
            Object which has a timeseries as an attribute
        _attr:
            Attribute containing timeseries data in `obj`
        _columns:
            Specific columns to access candle data from. Columns should only be passed if either `__call__()` uses
            the specified columns.
    """

    def __init__(self, obj: Any, attr: str, columns: List[str] = None, buffer_size: int = 750, verify: bool = True):
        """
        Args:
            obj:
                Object which has a timeseries as an attribute
            attr:
                Attribute containing timeseries data in `obj`
            columns:
                Specific columns to access candle data from. Columns should only be passed if either `__call__()` uses
                the specified columns.
            buffer_size:
                Length of timeseries returned by `_buffer()`. Buffer length is used to reduce computation necessary to
                complete `__call__()` since old data should not need to be processed during runtime. However, value
                should be large enough to characterize extreme values while not adding any noticeable calculation time.
            verify:
                Verify that `obj` has the attribute `_attr` and that attribute is a timeseries during runtime.

                Verification is to ensure proper operation and disabling verification is only implemented for unit
                testing.
        """
        assert hasattr(obj, attr)

        self._obj = obj
        self._attr = attr
        self._columns = columns

        # verify instance and parameter types
        if verify:
            if columns is not None:
                _attr: Union['pd.DataFrame', 'pd.Series'] = getattr(obj, attr)
                for col in self._columns:
                    assert col in _attr
            assert type(self.ts) in (pd.Series, pd.DataFrame)
            if not self.ts.index.empty:
                assert type(self.ts.index) is pd.DatetimeIndex

        self._size = buffer_size

    @property
    def ts(self) -> Union['pd.Series', 'pd.DataFrame']:
        """ Access timeseries.

        If instance was initialized with `columns`, then those columns are returned.
        """
        attr = getattr(self._obj, self._attr)
        if self._columns is None:
            return attr
        return attr[self._columns]

    @abstractmethod
    def __call__(self, point: pd.Timestamp = None, *args, **kwargs) -> Any:
        pass

    def _buffer(self, point: pd.Timestamp = None, **kwargs) -> Union['pd.Series', 'pd.DataFrame']:
        """ Shorten length of `ts`.

        If original timeseries is shorter than `_size`, than all existing values are returned.

        Args:
            point:
                Point in time. If None, then last index is used. Specified `point` wil be included in returned
                timeseries.
            kwargs:
                Reserved for when child classes override this function.

        Returns:
            `Series` of indexed data *before* `point` of length `_size`.
        """
        if point is None:
            point = self.ts.index[-1]

        if len(self.ts) < self._size:
            return self.ts

        indexed = self.ts.loc[:point]
        if len(indexed) < self._size:
            return indexed
        return indexed.tail(self._size)
