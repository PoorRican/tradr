from abc import ABC, abstractmethod
from warnings import warn

import numpy as np
from os import path, mkdir
import pandas as pd
from typing import List, NoReturn
from warnings import warn
import yaml

from misc import TZ, DATA_ROOT

_FN_EXT = ".yml"
_LITERALS_FN = f"literals{_FN_EXT}"

# Store Timestamp in YAML
TIMESTAMP_REPR_STR = '!timestamp'


def timestamp_representer(dumper, data):
    return dumper.represent_scalar(TIMESTAMP_REPR_STR, str(data))


def timestamp_constructor(loader, node):
    return pd.Timestamp(node.value, tz=TZ)


yaml.add_representer(pd.Timestamp, timestamp_representer)
yaml.add_constructor(TIMESTAMP_REPR_STR, timestamp_constructor)


class StoredObject(ABC):
    def __init__(self, *args, root: str = DATA_ROOT, **kwargs):
        super().__init__(*args, **kwargs)
        self.root = root

    @property
    @abstractmethod
    def _instance_dir(self):
        pass

    @staticmethod
    def _create_dir(_dir: str) -> NoReturn:
        if not path.exists(_dir):
            # TODO: implement mode for read/write access controls
            mkdir(_dir)

    def save(self):
        print(f"Beginning save for {self.__name__}")

        # aggregate attributes
        _literals = {}
        _df_keys: List[str, ...] = []
        _sequence_keys: List[str, ...] = []
        for k, v in self.__dict__.items():
            _t = type(v)
            if _t == str or _t == int or _t == float:
                _literals[k] = v
            elif _t == np.float64:  # `assets` sometimes get stored as `np.float64`
                _literals[k] = float(v)
            elif _t == pd.DataFrame:
                _df_keys.append(k)
            elif _t == pd.Series:
                _sequence_keys.append(k)

        # TODO: implement data checksum
        _dir = self._instance_dir
        self._create_dir(_dir)

        # store literal parameters
        with open(path.join(_dir, _LITERALS_FN), 'w') as f:
            yaml.dump(_literals, f)

        # store sequence data
        for attr in _df_keys:
            with open(path.join(_dir, f"{attr}.yml"), 'w') as f:
                yaml.dump(getattr(self, attr).to_dict(orient='index'), f)

        for attr in _sequence_keys:
            with open(path.join(_dir, f"{attr}.yml"), 'w') as f:
                yaml.dump(getattr(self, attr).to_list(), f)

        print(f"Finished saving {self.__name__}")

    def load(self):
        """ Load stored attributes and sequence data from instance directory onto memory.

        Notes
            All data on memory is overwritten.
        """
        # TODO: load linked/stored indicator and market data/parameters
        
        print(f"Loading data for {self.__name__}")

        _dir = self._instance_dir
        _literals_fn = path.join(_dir, _LITERALS_FN)
        if not path.exists(_dir):
            mkdir(_dir)
            return None
        elif not path.exists(_literals_fn):
            return None

        # load `_literals`. Literals should be verified (ie: not be a function)
        with open(_literals_fn, 'r') as f:
            _literals: dict = yaml.full_load(f)
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
                    container = yaml.full_load(f)
                    if _t == pd.DataFrame:
                        _seq = pd.DataFrame.from_dict(container, orient="index")
                    elif _t == pd.Series:
                        _seq = pd.Series(container)
                    else:
                        warn('non-pandas object passed through check...')
                        continue
                    setattr(self, k, _seq)
            except FileNotFoundError:
                continue

            # TODO: verify data checksum
            
        print(f"Load complete for {self.__name__}")