from abc import abstractmethod, ABC
from models.data import timestamp_representer, timestamp_constructor, TIMESTAMP_REPR_STR
import numpy as np
from os import path, mkdir
from typing import Tuple, List
from warnings import warn
import yaml

import pandas as pd


_FN_EXT = ".yml"
_LITERALS_FN = f"literals{_FN_EXT}"

yaml.add_representer(pd.Timestamp, timestamp_representer)
yaml.add_constructor(TIMESTAMP_REPR_STR, timestamp_constructor)


class Market(ABC):
    """ Core infrastructure which abstracts communication with exchange.

    Holds minimal market data.

    Todo:
        - Add a layer of risk management:
            - "runaway" trade decisions or unfavorable outcomes
    """

    __name__ = 'Base'
    valid_freqs: Tuple[str, ...]
    asset_pairs: Tuple[str, ...]
    columns = ('open', 'high', 'low', 'close', 'volume')

    def __init__(self, symbol: str = None, root: str = None):
        """
        Args:
            root:
                root directory to store candle data
            symbol:
                Asset pair symbol to use for trading for this instance.
        """
        self._data = pd.DataFrame(columns=list(self.columns))
        """DataFrame: container for candle data.
        
        Container gets populated by `get_candles` and should otherwise be read-only.
        
        Notes:
            Should have `source` and `freq` set via the `DataFrame.attrs` convention.
        """

        if symbol:
            assert symbol in self.asset_pairs
        self.symbol = symbol

        self.root = root

    @property
    def id(self) -> str:
        return f"{self.__name__}_{self.symbol}"

    @property
    def most_recent_timestamp(self) -> pd.Timestamp:
        return self._data.iloc[-1].name

    @abstractmethod
    def candles(self, freq: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def update(self):
        """ Update ticker data """
        pass

    def load(self):
        """ Load stored attributes and sequence data from instance directory onto memory.

        Notes
            All data on memory is overwritten.
        """
        # TODO: load linked/stored indicator and market data/parameters

        _dir = self._instance_dir
        _literals_fn = path.join(_dir, _LITERALS_FN)
        if not path.exists(_dir):
            mkdir(_dir)
            return None
        elif not path.exists(_literals_fn):
            return None

        # load `_literals`. Literals should be verified (ie: not be a function)
        with open(_literals_fn, 'r') as f:
            _literals: dict = yaml.safe_load(f)
            for k, v in _literals.items():
                # verify data
                assert k in self.__dict__.keys()
                assert type(v) in (str, int, float)

                setattr(self, k, v)

        for k, v in self.__dict__.items():
            _t = type(v)
            if _t not in (pd.DataFrame, pd.Series):
                continue
            try:
                with open(path.join(_dir, f"{k}.yml"), 'r') as f:
                    container = yaml.safe_load(f)
                    if _t == pd.DataFrame:
                        _seq = pd.DataFrame.from_dict(container)
                    elif _t == pd.Series:
                        _seq = pd.Series(container)
                    else:
                        warn('non-pandas object passed through check...')
                        continue
                    setattr(self, k, _seq)
            except FileNotFoundError:
                continue

            # TODO: verify data checksum

    def save(self):
        # aggregate attributes
        _literals = {}
        _df_keys: List[str, ...] = []
        _sequence_keys: List[str, ...] = []
        for k, v in self.__dict__.items():
            _t = type(v)
            if _t == str or _t == int or _t == float:
                _literals[k] = v
            elif _t == np.float64:              # `assets` sometimes get stored as `np.float64`
                _literals[k] = float(v)
            elif _t == pd.DataFrame:
                _df_keys.append(k)
            elif _t == pd.Series:
                _sequence_keys.append(k)

        # TODO: implement data checksum

        _dir = self._instance_dir
        if not path.exists(_dir):
            # TODO: implement mode for read/write access controls
            mkdir(_dir)

        # store literal parameters
        with open(path.join(_dir, _LITERALS_FN), 'w') as f:
            yaml.safe_dump(_literals, f)

        # store sequence data
        for attr in _df_keys:
            with open(path.join(_dir, f"{attr}.yml"), 'w') as f:
                yaml.safe_dump(getattr(self, attr).to_dict(orient='index'), f)

        for attr in _sequence_keys:
            with open(path.join(_dir, f"{attr}.yml"), 'w') as f:
                yaml.safe_dump(getattr(self, attr).to_list(), f)

    @property
    def _instance_dir(self) -> str:
        return path.join(self.root, f"{self.id}")

    @abstractmethod
    def translate_period(self, freq):
        """ Convert given market string interval into valid Pandas `DateOffset` value

        References:
            View Pandas documentation for a list of valid values
            https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timeseries-offset-aliases
        """
        pass
