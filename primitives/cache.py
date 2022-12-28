from datetime import datetime, timedelta
import logging
from typing import Any, Callable, Tuple, Dict

from misc import TZ


class CachedValue(object):
    def __init__(self, func: Callable = None, *args, timeout: timedelta = timedelta(hours=1),
                 default: Any = None, **kwargs):
        assert issubclass(type(timeout), timedelta)

        self.func: Callable = func
        self.args: Tuple = args
        self.kwargs: Dict = kwargs

        self._value: Any = None
        self.default = None

        self.timeout: timedelta = timeout
        self.last_updated = None

        self.update()

    def __call__(self, point: datetime = datetime.now(tz=TZ)):
        if self.timeout < point - self.last_updated:
            self.last_updated = point
            return self.update()
        return self._value

    def _call(self):
        return self.func(*self.args, **self.kwargs)

    def update(self) -> Any:
        try:
            _value = self._call()
            self._value = _value
            self.last_updated = datetime.now(tz=TZ)
        except ConnectionError as e:
            logging.warning("Deferring to cached value")
            if self._value is not None:
                return self._value
            elif self.default is not None:
                return self.default
            raise e
        return self._value
