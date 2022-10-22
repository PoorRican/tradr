""" Define trade models to use as `pd.DataFrame` rows and argument values.
"""
from dataclasses import dataclass, field, fields
import pandas as pd
from typing import Union


@dataclass
class Trade:
    """ Minimum data for theoretical trade.

    Intended to be used as function arguments and in `Strategy.failed_orders`.
    """
    amt: float
    rate: float
    side: str
    cost: float = field(init=False)

    def __post_init__(self):
        assert self.side in ('buy', 'sell')
        self.cost = truncate(self.amt * self.rate, 2)

    @classmethod
    def container(cls) -> 'pd.DataFrame':
        return trade_container(cls)


@dataclass
class SuccessfulTrade(Trade):
    id: field(default_factory=str)
    related: field(default_factory=list) = None


def trade_container(cls: type(Trade)) -> 'pd.DataFrame':
    return pd.DataFrame(columns=[i.name for i in fields(cls)])


def add_to_df(__object: object, container: str, extrema: Union['pd.Timestamp', str],
              trade: Trade, force: bool = False):

    assert type(extrema) in (pd.Timestamp, str)
    if type(extrema) == str:
        extrema = pd.Timestamp(extrema)

    df = getattr(__object, container)
    if extrema not in df.index or force:
        row = pd.DataFrame([trade], index=[extrema])
        setattr(__object, container, pd.concat([df, row]))


def truncate(f, n) -> float:
    """Truncates/pads a float f to n decimal places without rounding"""
    s = '{}'.format(f)
    if 'e' in s or 'E' in s:
        return float('{0:.{1}f}'.format(f, n))
    i, p, d = s.partition('.')
    return float('.'.join([i, (d+'0'*n)[:n]]))
