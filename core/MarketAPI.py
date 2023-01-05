from abc import ABC, abstractmethod
import datetime
import logging
from os import path
from warnings import warn

import pandas as pd
from pytz import timezone
from typing import Dict, List, NoReturn, Union, Optional, Tuple
from yaml import safe_dump, safe_load
import warnings

from core.market import Market
from misc import TZ
from models.trades import Trade, SuccessfulTrade, FailedTrade
from primitives import CachedValue


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
                 update: bool = True, load: bool = True, auto_update: bool = True,
                 symbol: str = None, fee: float = None, **kwargs):
        """
        Args:
            api_key:
                API key
            api_secret:
                API secret
            update:
                flag to disable fetching active market data. Reads any cached file by default.
            load:
                Flag to disable loading from disk
            auto_update:
                Flag to disable auto-updating. Prevents checking of stale candle data.
            symbol:
                Asset pair symbol to use for trading for this instance.
            fee:
                Default value for fee if `CachedValue` is unable to fetch data.
            kwargs:
                Remaining keyword arguments are passed to `MarketAPI.__init__()`
        """
        super().__init__(symbol, **kwargs)

        self._tzname: str = self._global_tz

        self.api_key = api_key
        self.api_secret = api_secret
        if api_secret is not None:
            self.api_secret = api_secret.encode()

        self.auto_update = auto_update
        if update:
            self.update()
        elif load:
            self.load()

        self.instances[self.id] = self

        # Setup special values
        self._fee = CachedValue(self._get_fee, default=fee)

    @property
    def fee(self):
        return self._fee()

    @property
    def _stale_candles(self) -> Tuple[str]:
        """ Check all candles for stale data

        Returns
            A list of frequencies that need to be updated.
        """
        now = datetime.datetime.now(tz=timezone(self._global_tz))
        stale = []
        for freq in self.valid_freqs:
            _stale = self._check_candle_age(freq, now)
            if _stale:
                stale.append(freq)

        return tuple(stale)

    @property
    def tz(self) -> timezone:
        return timezone(self._tzname)

    @tz.setter
    def tz(self, val: timezone) -> NoReturn:
        self._tzname = str(val)

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
            True if candle data is empty or is stale and therefore needs to be updated. Otherwise, returns False.
        """
        if self._data.empty:
            return True
        if now is None:
            now = datetime.datetime.now(tz=timezone(self._global_tz))
        data = self._data.loc[frequency]
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
    def restore(cls, fn: str = None, **kwargs) -> NoReturn:
        """ Generates several instances of `GeminiMarket` based on config file.

        Paired with `snapshot()`.

        Args:
            fn:

            kwargs:
                Passed down to `cls.__init__()`
        """
        if fn is None:
            fn = cls._INSTANCES_FN

        with open(path.join(cls.root, cls._SECRET_FN), 'r') as f:
            secrets = safe_load(f)

        with open(path.join(cls.root, fn), 'r') as f:
            params = safe_load(f)

        for i in params:
            instance = cls(secrets['key'], secrets['secret'], **i, update=False, **kwargs)
            instance.load()
            cls.instances[instance.id] = instance

    @classmethod
    def snapshot(cls, fn: str = None) -> NoReturn:
        if fn is None:
            fn = cls._INSTANCES_FN

        lines: List[Dict] = []
        for i in cls.instances.values():
            lines.append({'symbol': i.symbol})
            i.save()

        with open(path.join(cls.root, fn), 'w') as f:
            safe_dump(lines, f)

    def update(self) -> None:
        """ Updates `data` with recent candle data.

        Notes
            Because this function takes time. It should not be called in `__init__()`
            and should be run asynchronously.
        """
        print("Beginning update")
        self.load()
        self._check_tz()

        try:
            _data = []
            for freq in self.valid_freqs:
                if self._check_candle_age(freq):
                    _candles = self.fetch_candles(freq)
                else:
                    print(f"Using cached candle data for {freq}")
                    _candles = self._data.loc[freq]
                _data.append(_candles)

            data = pd.concat(_data, axis='index', keys=self.valid_freqs)    # add keys
            self._data = self._combine_candles(data)

            self.save()
            print(f"Update complete for {self.__name__}")
        except ConnectionError as e:
            msg = f'Connection Error. Deferring to cached data.'
            logging.error(e)
            warnings.warn(msg)
            raise e

    @abstractmethod
    def post_order(self, trade: Trade) -> Union['SuccessfulTrade', 'FailedTrade']:
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
        assert self._data.index.get_level_values(1).tz == incoming.index.get_level_values(1).tz
        combined = pd.concat([self._data, incoming])
        combined = combined[~combined.index.duplicated(keep="first")]         # drop rows w/ duplicated index

        _sorted = combined.sort_index()
        del combined
        _sorted.index = pd.MultiIndex.from_tuples(_sorted.index)
        return _sorted

    def _repair_candles(self, data: pd.DataFrame, freq: str) -> pd.DataFrame:
        """ Fill in missing values for candle data via interpolation. """
        assert freq in self.valid_freqs

        buffer = data.copy(deep=True)

        start = data.index[0]
        end = data.index[-1]
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

    def _set_index_tz(self, tz: timezone = None) -> NoReturn:
        if tz is None:
            tz = self.tz

        try:
            dt_idx = pd.to_datetime(self._data.index.get_level_values(1), utc=True).tz_convert(tz)
            self._data.index = self._data.index.set_levels(dt_idx, level=1, verify_integrity=False)
        except IndexError:
            warn("Error converting embedded index to DatetimeIndex")

    def load(self):
        super().load()

        # fix `DateTimeIndex`
        self._set_index_tz()

    @classmethod
    @property
    def _global_tz(cls):
        """ Get global timezone.

        Implemented during testing to simulate global timezone.
        """
        return str(TZ)

    def _check_tz(self) -> NoReturn:
        """ Convert current timezone of existing candle data to new timezone.

        Notes:
            Last timezone is stored via the `tz` instance attribute. Last timezone should be stored on an instance level
            since all candle data is stored per instance, and not on a class level. Since the global variable `TZ`
            captures system timezone, system timezone is checked against instance timezone. If timezones differ, then
            `tz_convert()` is called on each individual frequency. Iterating over multiple frequencies avoids an error
            that is raised due to duplicated datetime objects (intersecting times is a function of multi-frequency OHLC
            data).

            Maybe a discreet `CandleData` class could lower boilerplate code in the future.
        """
        _tzname = self._global_tz
        _tz = timezone(_tzname)
        print(f"Checking tz. Detected global tz to be {_tzname}.")
        if self.tz != _tz:
            print(f"Instance tz ({self.tz}) differs from global tz...")
            self._set_index_tz(_tz)
            self.tz = _tz
            print(f"Localized candle data to {_tzname}")
        else:
            print(f"Instance tz matches global tz")

    @classmethod
    def fetch_all(cls, api_secret: str, api_key: str, assets: List[str] = None):
        if assets is None:
            assets = cls.asset_pairs
        for symbol in assets:
            print(f"Starting to update candle data for {symbol}")
            cls(api_secret=api_secret, api_key=api_key, symbol=symbol)
