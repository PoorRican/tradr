import time
from os import path
from pytz import timezone


# capture and manipulate system timezone
_tzname = time.tzname[0]
if _tzname == 'PST':
    _tzname = 'US/Pacific'
elif _tzname == 'EST':
    _tzname = 'US/Eastern'
TZ = timezone(_tzname)


def _project_root() -> str:
    _root = path.join(__file__, path.pardir)
    return path.abspath(_root)


ROOT = _project_root()
DATA_ROOT = f'{_project_root()}/data/'
