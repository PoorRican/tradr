from pathlib import Path
from pytz import timezone
from os import path
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import time
from yaml import safe_load


# capture and manipulate system timezone
_tzname = time.tzname[0]
if _tzname == 'PST':
    _tzname = 'US/Pacific'
elif _tzname == 'EST':
    _tzname = 'US/Eastern'
TZ = timezone(_tzname)


# manage root directories
def _project_root() -> str:
    _root = path.join(__file__, path.pardir)
    return path.abspath(_root)


ROOT = _project_root()
DATA_ROOT = f'{_project_root()}/data/'


# setup db connection
def setup_engine() -> Engine:
    with open(CREDS_F, 'r') as f:
        data = safe_load(f)
        for key in ('dialect', 'user', 'pw', 'host', 'port', 'schema'):
            assert key in data.keys()
        dialect = data['dialect']
        user = data['user']
        pw = data['pw']
        host = data['host']
        port = data['port']
        schema = data['schema']

        url = f'postgresql+{dialect}://{user}:{pw}@{host}:{port}/{schema}'
        return create_engine(url, echo=False)


CREDS_FN = 'db_creds.yml'
CREDS_F = Path(ROOT, CREDS_FN)
ENGINE = setup_engine()
