from dataclasses import dataclass, fields
import pandas as pd
from datetime import datetime


@dataclass
class Ticker:
    open: float
    high: float
    low: float
    close: float
    volume: float

    @classmethod
    def container(cls):
        return pd.DataFrame(columns=[i.name for i in fields(cls)])

    @classmethod
    def from_json(cls, data) -> pd.DataFrame:
        _data = data
        for i in _data:
            i[0] = datetime.fromtimestamp(i[0] / 1000)    # convert milliseconds to seconds
        cols = ['dt', *[i.name for i in fields(cls)]]
        df = pd.DataFrame(_data, columns=cols)
        df.index = pd.DatetimeIndex(df['dt'])
        del df['dt']
        del _data

        return df
