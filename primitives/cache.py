import logging

from core.misc import TZ
from datetime import datetime, timedelta
from typing import Any, Callable, Tuple, Dict


class CachedValue(object):
    def __init__(self, func: Callable, *args, **kwargs):
        self.func: Callable = func
        self.args: Tuple = args
        self.kwargs: Dict = kwargs

        self._value: Any = None

        self.timeout = timedelta(hours=1)
        self.last_updated = None

        self.update()

    def __call__(self):
        now = datetime.now(tz=TZ)
        if self.timeout < now - self.last_updated:
            self.last_updated = now
            return self.update()
        return self._value

    def _call(self):
        return self.func(*self.args, **self.kwargs)

    def update(self) -> Any:
        self.last_updated = datetime.now(tz=TZ)
        try:
            _value = self._call()
            self._value = _value
        except ConnectionError as e:
            logging.warning("Deferring to cached value")
            if self._value is None:
                raise e
        return self._value
