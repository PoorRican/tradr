from abc import ABC, abstractmethod
import numpy as np
from os import path, mkdir
from pathlib import Path
import pandas as pd
from typing import List, Iterable, NoReturn, ClassVar
from warnings import warn
import yaml

from misc import DATA_ROOT

_FN_EXT = ".yml"
_LITERALS_FN = f"literals{_FN_EXT}"

# Store Timestamp in YAML
TIMESTAMP_REPR_STR = '!timestamp'


def timestamp_representer(dumper, data):
    return dumper.represent_scalar(TIMESTAMP_REPR_STR, str(data))


def timestamp_constructor(loader, node):
    return pd.Timestamp(node.value)


yaml.add_representer(pd.Timestamp, timestamp_representer)
yaml.add_constructor(TIMESTAMP_REPR_STR, timestamp_constructor)


class StoredObject(ABC):
    root: ClassVar[str] = DATA_ROOT
    exclude: Iterable[str]
    __name__ = 'StoredObject'

    def __init__(self, *args, load: bool = False, exclude: Iterable[str] = None):
        super().__init__()

        self.exclude: Iterable[str] = exclude

        if load:
            self.load()

    @property
    @abstractmethod
    def _instance_dir(self) -> Path:
        pass

    @staticmethod
    def _create_dir(_dir: Path) -> NoReturn:
        if not path.exists(_dir):
            # TODO: implement mode for read/write access controls
            mkdir(_dir)

    def save(self, ignore_exclude: bool = False) -> NoReturn:
        """
        Args:
            ignore_exclude:
                When True, values in instance `exclude` are filtered; otherwise, `exclude` attributes
                are returned. Default value is to exclude values.
        """
        print(f"Beginning save for {self.__name__}")

        # aggregate attributes
        _literals = {}
        _df_keys: List[str, ...] = []
        _sequence_keys: List[str, ...] = []
        for k, v in self.__dict__.items():
            _t = type(v)
            if not ignore_exclude and self.exclude and k in self.exclude:
                continue
            elif _t == str or _t == int or _t == float:
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

    def load(self, ignore_exclude: bool = False) -> NoReturn:
        """ Load stored attributes and sequence data from instance directory onto memory.

        Notes
            All data on memory is overwritten.

        Args:
            ignore_exclude:
                When True, values in instance `exclude` are filtered; otherwise, `exclude` attributes
                are returned. Default value is to exclude values.
        """
        # TODO: load linked/stored indicator and market data/parameters
        
        print(f"Loading data for {self.__name__}")

        _dir = self._instance_dir
        _literals_fn = Path(_dir, _LITERALS_FN)
        if not _dir.exists():
            _dir.mkdir(parents=True)
            return None
        elif not _literals_fn.exists():
            return None

        # load `_literals`. Literals should be verified (ie: not be a function)
        with open(_literals_fn, 'r') as f:
            _literals: dict = yaml.full_load(f)
            for k, v in _literals.items():
                # verify data
                # excluded values could be filtered here, but is handled below instead.
                assert hasattr(self, k)
                assert type(v) in (str, int, float)

                setattr(self, k, v)

        for k, v in self.__dict__.items():
            _t = type(v)
            # skip excluded
            excluded = not ignore_exclude and self.exclude and k in self.exclude
            if excluded or _t not in (pd.DataFrame, pd.Series):
                continue
            try:
                with open(Path(_dir, f"{k}.yml"), 'r') as f:
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
