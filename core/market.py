from abc import abstractmethod, ABC
import numpy as np
from os import path, mkdir
from typing import Tuple, List
from warnings import warn
from yaml import safe_dump, safe_load

import pandas as pd


_FN_EXT = ".yml"
_LITERALS_FN = f"literals{_FN_EXT}"


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

    def __init__(self, symbol: str = None, freq: str = None, root: str = None):
        """
        Args:
            freq:
                candle frequency
            root:
                root directory to store candle data
            symbol:
                Asset pair symbol to use for trading for this instance.
        """
        self.data = pd.DataFrame(columns=list(self.columns))
        """DataFrame: container for candle data.
        
        Container gets populated by `get_candles` and should otherwise be read-only.
        
        Notes:
            Should have `source` and `freq` set via the `DataFrame.attrs` convention.
        """
        if symbol and hasattr(self, 'asset_pairs'):
            assert symbol in self.asset_pairs
        if freq and hasattr(self, 'valid_freqs'):
            assert freq in self.valid_freqs

        self.symbol = symbol
        self.freq = freq

        self.root = root

    @property
    def id(self) -> str:
        return f"{self.__name__}_{self.symbol}_{self.freq}"

    @abstractmethod
    def update(self):
        """ Update ticker data """
        pass

    def load(self, ignore: bool = True):
        """ Load stored attributes and sequence data from instance directory onto memory.

        Notes
            All data on memory is overwritten.
        """
        # TODO: load linked/stored indicator and market data/parameters

        _dir = self._instance_dir

        # load `_literals`. Literals should be verified (ie: not be a function)
        with open(path.join(_dir, _LITERALS_FN), 'r') as f:
            _literals: dict = safe_load(f)
            for k, v in _literals.items():
                # verify data
                assert k in self.__dict__.keys()
                assert type(v) in (str, int, float)

                setattr(self, k, v)

        for k, v in self.__dict__.items():
            _t = type(v)
            if _t not in (pd.DataFrame, pd.Series):
                continue
            with open(path.join(_dir, f"{k}.yml"), 'r') as f:
                container = safe_load(f)
                if _t == pd.DataFrame:
                    _seq = pd.DataFrame.from_records(container)
                elif _t == pd.Series:
                    _seq = pd.Series(container)
                else:
                    warn('non-pandas object passed through check...')
                    continue
                setattr(self, k, _seq)

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
            safe_dump(_literals, f)

        # store sequence data
        for attr in _df_keys:
            with open(path.join(_dir, f"{attr}.yml"), 'w') as f:
                safe_dump(getattr(self, attr).to_dict(orient='records'), f)

        for attr in _sequence_keys:
            with open(path.join(_dir, f"{attr}.yml"), 'w') as f:
                safe_dump(getattr(self, attr).to_list(), f)

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
