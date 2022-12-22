import datetime
from abc import ABC, abstractmethod
import logging
import pandas as pd
from os import path
from typing import Dict, List, NoReturn, Union, Optional, Tuple
from yaml import safe_dump, safe_load
import warnings

from core.market import Market
from core.misc import TZ
from models import DATA_ROOT, ROOT, Trade, SuccessfulTrade
from primitives.cache import CachedValue


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

    def __init__(self, api_key: str = None, api_secret: str = None,
                 root: str = DATA_ROOT, update=True, auto_update=True, symbol: str = None):
        """
        Args:
            api_key:
                API key
            api_secret:
                API secret
            root:
                root directory to store candle data
            update:
                flag to disable fetching active market data. Reads any cached file by default.
            auto_update:
                Flag to disable auto-updating. Prevents checking of stale candle data.
            symbol:
                Asset pair symbol to use for trading for this instance.
        """
        super().__init__(symbol, root)

        if api_key and api_secret:
            self.api_key = api_key
            self.api_secret = api_secret.encode()

        if update:
            self.update()
        else:
            self.load()

        self.auto_update = auto_update

        self.instances[self.id] = self

        # Setup special values
        self._fee = CachedValue(self._get_fee)

    @property
    def fee(self):
        return self._fee()

    @property
    def _stale_candles(self) -> Tuple[str]:
        """ Check all candles for stale data

        Returns
            A list of frequencies that need to be updated.
        """
        now = datetime.datetime.now(tz=TZ)
        stale = []
        for freq in self.valid_freqs:
            _stale = self._check_candle_age(freq, now)
            if _stale:
                stale.append(freq)

        return tuple(stale)

    def _check_candle_age(self, frequency: str = None, now: datetime.datetime = None) -> bool:
        """ Check if candle data is expired.

        Candle data is considered expired the difference between last index and the current
        time are greater than the index frequency.

        Args:
            frequency:
                Candle data to check

            now:
                Pre-calculated `datetime` for current time.

        Returns
            True if candle data is stale and needs to be updated. Otherwise, returns False.
        """
        if now is None:
            now = datetime.datetime.now(tz=TZ)
        data = self.candles(frequency)
        last_point = data.iloc[-1].name
        delta: pd.Timedelta = now - last_point

        return delta > pd.Timedelta(frequency)

    @abstractmethod
    def _fetch_candles(self, freq: Optional[str] = None) -> pd.DataFrame:
        """ Low-level function to retrieve candle data. """
        pass

    @abstractmethod
    def _translate(self, trade: Trade, response: dict) -> 'SuccessfulTrade':
        """ Generate `SuccessfulTrade` using data returned from """
        pass

    def _update_frequency(self, frequency: str) -> NoReturn:
        assert frequency in self.valid_freqs
        assert frequency in self._data.index.levels[0]

        self._data.loc[frequency] = self.fetch_candles(frequency)

    def candles(self, freq: str) -> pd.DataFrame:
        """ Retrieve specified candle data.

        `_data` contains candle data for all frequencies, but index contains keys which correspond to
        different frequencies. This function checks that `freq` is valid.

        Also, when `auto_update` is True, this function checks that data is still valid by calling
        `_check_candle_age()` which returns True if candle data is stale.
        """
        assert freq in self.valid_freqs
        assert freq in self._data.index.levels[0]

        if self.auto_update and self._check_candle_age(freq):
            self._update_frequency(freq)

        return self._data.loc[freq]

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
            lines.append({'symbol': i.symbol})
            i.save()

        with open(path.join(ROOT, fn), 'w') as f:
            safe_dump(lines, f)

    def update(self) -> None:
        """ Updates `data` with recent candle data.
        Notes
            Because this function takes time. It should not be called in `__init__()`
            and should be run asynchronously.
        """
        self.load()

        try:
            _data = []
            for freq in self.valid_freqs:
                _data.append(self.fetch_candles(freq))

            data = pd.concat(_data, axis='index', keys=self.valid_freqs)
            data = self._combine_candles(data)

            data.index = pd.MultiIndex.from_tuples(data.index)      # convert to MultiIndex
            self._data = data
            self.save()
        except ConnectionError as e:
            msg = f'Connection Error. Deferring to cached data.'
            logging.error(e)
            warnings.warn(msg)
            raise e

    @abstractmethod
    def post_order(self, trade: Trade) -> Union['SuccessfulTrade', 'False']:
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
    def _get_fee(self, *args, **kwargs) -> float:
        """ Calculate cost of a transaction
        """
        pass

    def _combine_candles(self, incoming: pd.DataFrame) -> pd.DataFrame:
        combined = pd.concat([self._data, incoming])
        combined = combined[~combined.index.duplicated(keep="first")]         # drop rows w/ duplicated index
        return combined

    def _repair_candles(self, data: pd.DataFrame, freq: str) -> pd.DataFrame:
        """ Fill in missing values for candle data via interpolation. """
        assert freq in self.valid_freqs

        buffer = pd.DataFrame(data, copy=True, dtype=float)

        start = data.iloc[0].name
        end = data.iloc[-1].name
        _freq = self.translate_period(freq)

        # drop invalid rows
        # drop duplicated rows w/ 0 in columns
        # data.drop(data.loc[data['volume'] == 0], inplace=True)        # drop rows w/ 0 volume
        buffer = buffer[~buffer.index.duplicated(keep="first")]         # drop rows w/ duplicated index

        index = pd.date_range(start=start, end=end, freq=_freq, tz=data.index.tz)

        buffer = buffer.reindex(index)
        buffer.interpolate(inplace=True)

        assert buffer.index.is_monotonic_increasing

        return buffer

    def fetch_candles(self, freq: Optional[str] = None) -> pd.DataFrame:
        """ Fetch and clean candle data """
        print(f"Fetching candle data for {freq}...")
        data = self._fetch_candles(freq)
        data = self._repair_candles(data, freq)

        return data

    @abstractmethod
    def process_point(self, point: pd.Timestamp, freq: str) -> Union['pd.Timestamp', str]:
        """ Quantize and shift timestamp `point` for parsing market specific data.

        Used for indexing higher level frequencies. Frequency needs to be modified before accessing indicator graphs. A
        pandas unit of frequency to select indicator data from greater time frequencies needs to be passed as `point`.
        Used by `TrendDetector` to prevent `KeyError` from being raised during simulation arising from larger timeframes
        of stored candle data (ie: 1day, 6hr, or 1hr) and smaller timeframes used by strategy/backtesting functions
        (ie: '15min'). If passed, `point` is rounded down to the largest frequency less than `point` (eg: 14:45 becomes
        14:00). If not passed, `point` is untouched.

        Notes:
            Specific times and offsets are market dependent. Function is currently set for the Gemini platform. In the
            future, this function will be migrated and declared `MarketAPI` but defined in specific platform instances.

        Args:
            point:
                timestamp to use to index `_indicators`
            freq:
                Unit of time to quantize to. Should be written

        Returns:
            Quantized and shifted pd.Timestamp (or str if `freq='1D'`
        """
        pass
