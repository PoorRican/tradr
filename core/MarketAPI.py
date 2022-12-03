from abc import ABC, abstractmethod
import logging
import pandas as pd
from os import path
from typing import Dict, List, NoReturn, Union
import urllib3
from yaml import safe_dump, safe_load
import warnings

from core.market import Market
from models.data import json_to_df, DATA_ROOT, ROOT
from models.trades import Trade, SuccessfulTrade


class MarketAPI(Market, ABC):
    """ Intermediate class which abstracts a Market API.

    Posts sell and buy orders, records historical candle data.

    For specific platforms, functions need to be defined to post and process orders, and request other data.
    However, all derived class instances are stored in the class var `instances`, and can be accessed by instance
    `key`. During program start-up and shutdown, `snapshot()`/`restore()` functionality saves and restores all
    running instances.

    Fields:
        __name__ (str):
            Platform name. Used for setting flag attributes and filenames.

        valid_freqs (tuple[str, ...]):
            iterable with valid frequency/interval values. This will be changed into an `enum` in the near
            future.

        asset_pairs (tuple[str, ...]):
            List of valid asset pairs

        BASE_URL (str):
            Base URL for accessing all API endpoints.
    """
    __name__ = 'MarketAPI'
    BASE_URL: str
    _INSTANCES_FN: str
    _SECRET_FN: str

    instances: Dict[str, 'MarketAPI'] = {}

    def __init__(self, api_key: str = None, api_secret: str = None, freq: str = None,
                 root: str = DATA_ROOT, update=True, symbol: str = None):
        """
        Args:
            api_key:
                API key
            api_secret:
                API secret
            freq:
                candle frequency
            root:
                root directory to store candle data
            update:
                flag to disable fetching active market data. Reads any cached file by default.
            symbol:
                Asset pair symbol to use for trading for this instance.
        """
        super().__init__(symbol, freq, root)

        if api_key and api_secret:
            self.api_key = api_key
            self.api_secret = api_secret.encode()

        if update:
            self.update()
            self.save()

        self.instances[self.id] = self

    @abstractmethod
    def post(self, endpoint: str, data: dict = None) -> dict:
        pass

    @classmethod
    def restore(cls, fn: str = None) -> NoReturn:
        """ Generates several instances of `GeminiMarket` based on config file.

        Paired with `snapshot()`.
        """
        if fn is None:
            fn = cls._INSTANCES_FN

        with open(path.join(ROOT, cls._SECRET_FN), 'r') as f:
            secrets = safe_load(f)

        with open(path.join(ROOT, fn), 'r') as f:
            params = safe_load(f)

        for i in params:
            instance = cls(secrets['key'], secrets['secret'], **i, update=False)
            instance.load()
            cls.instances[instance.id] = instance

    @classmethod
    def snapshot(cls, fn: str = None):
        if fn is None:
            fn = cls._INSTANCES_FN

        lines: List[Dict] = []
        for i in cls.instances.values():
            lines.append({'symbol': i.symbol, 'freq': i.freq})
            i.save()

        with open(path.join(ROOT, fn), 'w') as f:
            safe_dump(lines, f)

    @property
    def filename(self) -> str:
        """ Generate filename representing candle data.

        Notes:
            Each different candle interval is stored separately from other intervals from the same source.

        Returns:
            Relative filename with candle source and interval
        """
        return path.join(self.root, self.__name__ + '_' + self.freq + ".pkl")

    def update(self) -> None:
        """ Updates `data` with recent candle data.
        Notes
            Because this function takes time. It should not be called in `__init__()`
            and should be run asynchronously.
        """
        try:
            self.load()
            data = self.get_candles()
            data = self._combine_candles(data)
            self.data = data
        except urllib3.HTTPSConnectionPool as e:
            logging.error(f'Connection error during `MarketAPI.update(): {e}')
            warnings.warn("Connection error")
            self.load(ignore=False)

    @abstractmethod
    def _convert(self, trade: Trade, response: dict) -> 'SuccessfulTrade':
        """ Generate `SuccessfulTrade` """
        pass

    @abstractmethod
    def place_order(self, trade: Trade) -> Union['SuccessfulTrade', 'False']:
        """ Post order to market.

        Args:
            trade:
                Potential trade data

        Returns:
            If the market accepted trade and the order was executed, `SuccessfulTrade` is returned. This is
            necessary because the `rate` of trade might be better than requested.
        """
        pass

    @abstractmethod
    def get_fee(self, *args, **kwargs) -> float:
        """ Calculate cost of a transaction

        """
        pass

    @abstractmethod
    def get_candles(self, *args, **kwargs):
        pass

    def _combine_candles(self, incoming: pd.DataFrame) -> pd.DataFrame:
        combined = pd.concat([self.data, incoming])
        combined.drop_duplicates(inplace=True)
        combined.attrs = incoming.attrs
        return combined

    def _repair_candles(self, data: pd.DataFrame) -> pd.DataFrame:
        """ Fill in missing values for candle data via interpolation. """
        buffer = pd.DataFrame(data, copy=True, dtype=float)

        start = data.iloc[0].name
        end = data.iloc[-1].name
        attrs = data.attrs
        _freq = self.translate_period(attrs['freq'])

        # drop invalid rows
        index = data.index
        # data.drop(data.loc[data['volume'] == 0], inplace=True)        # drop rows w/ 0 volume
        buffer.drop_duplicates(keep="first", inplace=True)
        buffer.drop(index=index[index.duplicated()], inplace=True)

        index = pd.date_range(start=start, end=end, freq=_freq, tz=data.index.tz)
        buffer.attrs = attrs

        buffer = buffer.reindex(index)
        buffer.interpolate(inplace=True)

        assert buffer.index.is_monotonic_increasing

        return buffer
