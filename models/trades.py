""" Define trade models to use as `pd.DataFrame` rows and argument values.

Notes:
    By using dataclasses inside `DataFrame`, data structure has more rigidity and boilerplate
    can be reduced. Columns are standardized and error is reduced when setting values (eg: positional arguments out of
    order, or mistyped values), and allows the use of properties as columns.
"""

from dataclasses import dataclass, field, fields
import pandas as pd
from typing import Union, Any, Tuple

from misc import TZ
from primitives import Side, ReasonCode


@dataclass
class Trade:
    """ Abstraction for theoretical trade.

    Contains minimal data for representing a trade and is intended to be
    used as function arguments and in `Strategy.failed_orders`.

    Notes:
        Don't pass a value to `cost` when initializing.
    """
    amt: float
    rate: float
    side: Side
    cost: float = field(init=False)
    """NOTE: this is not to be given as an argument to `__init__()` """

    def __post_init__(self):
        """ Check values and calculate `cost`.

        Notes:
            This occurs after `__init__()` has been run.
        """
        assert self.side.name in ('BUY', 'SELL')
        self.cost = truncate(self.amt * self.rate, 2)

    @classmethod
    def container(cls) -> pd.DataFrame:
        """ Factory function for `Trade` (or `Trade`-derived dataclass) containers.

        Produces a DataFrame containing instances or inherited instances of `Trade`.

        See Also:
            ```containerize()```
        """
        return containerize(cls)


@dataclass
class FutureTrade(Trade):
    attempt: bool
    point: pd.Timestamp
    load: Any = None

    def __bool__(self) -> bool:
        return bool(self.attempt)

    @classmethod
    def factory(cls, trade: Trade, attempt: bool, point: pd.Timestamp, load: Any = None) -> 'FutureTrade':
        """ Transform `Trade` into `FutureTrade`. """
        return cls(trade.amt, trade.rate, trade.side, attempt, point, load)

    def _trade(self):
        return Trade(self.amt, self.rate, self.side)

    def convert(self, load: Any = None, attempt: bool = None) -> Union['SuccessfulTrade', 'FailedTrade']:
        if load is None:
            load = self.load
        if attempt is None:
            attempt = self.attempt

        if attempt:
            return SuccessfulTrade(self.amt, self.rate, self.side, str(load))
        return FailedTrade(self.amt, self.rate, self.side, ReasonCode(load))

    def separate(self) -> Tuple['Trade', 'pd.Timestamp']:
        return self._trade(), self.point


@dataclass
class SuccessfulTrade(Trade):
    id: field(default_factory=str)

    @classmethod
    def __bool__(cls):
        return True


@dataclass
class FailedTrade(Trade):
    reason: ReasonCode

    @classmethod
    def __bool__(cls):
        return False

    @classmethod
    def convert(cls, trade: Trade, reason: ReasonCode) -> 'FailedTrade':
        return cls(trade.amt, trade.rate, trade.side, reason)


def containerize(class_or_instance: Any) -> pd.DataFrame:
    """ Create a `pd.DataFrame` using `fields` as column names.

    Using a dataclass adds rigidity to data-structure and reduces required boilerplate.

    Notes:
        Using a `dataclass` with `DataFrame` standardizes columns and reduces error when
        setting values (eg: positional arguments out of order, or mistyped values), and allows the
        use of properties as columns.

    Args:
        class_or_instance (Any):
            May be a dataclass or a dataclass instance. Must have `_FIELDS` attribute.

    Returns:
        `DataFrame` with dataclass attributes as column names. Column order is identical to
            order of attributes defined in dataclass.
    """
    return pd.DataFrame(columns=[i.name for i in fields(class_or_instance)])


def add_to_df(__object: object, container: str, extrema: Union['pd.Timestamp', str, int],
              instance: Any, force: bool = False):
    """ Insert dataclass instance into time-series.

    This is the recommended interface for inserting into `Trades.container()`.

    Notes:
        TODO:
            -   Test that insertion occurs 'in-place'
            -   Make `Containerized` type

    Args:
        __object:
            Object whose container we're inserting into.

        container (str):
            name of container to insert

        extrema:
            Time-series or numeric index to insert `instance`. If index already exists, `force`
            performs the insertion regardless, causing a duplicate. Otherwise, an error is made.

        instance:
            Dataclass instance to insert into `container`. Can be anytype as long as it is compatible
            with container columns.

        force:
            Flag to duplicate index. Data would not be replaced, but a duplicate index would exist
            pointing to two discrete values.
    """
    assert type(extrema) in (pd.Timestamp, str, int)
    if type(extrema) == str:
        extrema = pd.Timestamp(extrema, tz=TZ)

    df = getattr(__object, container)
    if extrema in df.index and not force:
        raise IndexError('duplicate index')
    else:
        row = pd.DataFrame([instance], index=[extrema])
        setattr(__object, container, pd.concat([df, row]))


def truncate(f, n) -> float:
    """ Truncates/pads a float f to n decimal places without rounding. """
    s = '{}'.format(f)
    if 'e' in s or 'E' in s:
        return float('{0:.{1}f}'.format(f, n))
    i, p, d = s.partition('.')
    return float('.'.join([i, (d+'0'*n)[:n]]))
