from os import path

from pytz import timezone


TZ_NAME = 'US/Pacific'
TZ = timezone(TZ_NAME)


def _project_root() -> str:
    _root = path.join(__file__, path.pardir)
    return path.abspath(_root)


ROOT = _project_root()
DATA_ROOT = f'{_project_root()}/data/'
