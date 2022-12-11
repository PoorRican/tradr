import pandas as pd
import requests
from os import path
from datetime import datetime
import yaml


TIMESTAMP_REPR_STR = '!timestamp'


# Store Timestamp in YAML
def timestamp_representer(dumper, data):
    return dumper.represent_scalar(TIMESTAMP_REPR_STR, str(data))


def timestamp_constructor(loader, node):
    return pd.Timestamp(node.value)


yaml.add_representer(pd.Timestamp, timestamp_representer)
yaml.add_constructor(TIMESTAMP_REPR_STR, timestamp_constructor)


# Manage Root

def _project_root() -> str:
    _root = path.join(__file__, path.pardir, path.pardir)
    return path.abspath(_root)


ROOT = _project_root()
DATA_ROOT = f'{_project_root()}/data/'


def json_to_df(data) -> pd.DataFrame:
    """ Convert timestamp and add columns to raw data.
    """
    _data = data
    for i in _data:
        i[0] = datetime.fromtimestamp(i[0] / 1000)    # convert milliseconds to seconds
    df = pd.DataFrame(_data, columns=['dt', 'open', 'high', 'low', 'close', 'volume'])
    df.index = pd.DatetimeIndex(df['dt'])
    del df['dt']
    del _data

    return df


def get_candles(t='1m') -> pd.DataFrame:
    """ Get ticker data from Gemini
    Args:
        t: Ticker interval. Can be 1m, 5m, 15m, 30m, 1hr, 6hr, 1day

    Raises:
        ValueError if `t` is not valid.

    Returns:
        DataFrame of parsed ticker data.
    """
    if t in ('1m', '5m', '15m', '30m', '1hr', '6hr', '1day'):
        base_url = "https://api.gemini.com/v2"
        response = requests.get(base_url + "/candles/btcusd/" + t)
        btc_candle_data = response.json()

        data = json_to_df(btc_candle_data)
        return data
    else:
        raise ValueError(t + " is not a valid argument.")


def combine_data(existing, new):
    """ Combines, cleans and sorts DataFrames """
    joined = existing.append(new)
    joined = joined[~joined.index.duplicated(keep='last')]
    joined.sort_values('dt', inplace=True)
    return joined[pd.notnull(joined.index)]


def read_data(fn) -> pd.DataFrame:
    """ Unpickle data """
    return pd.read_pickle(fn)


def write_data(data, fn) -> None:
    """ Write dataframe to file """
    data.to_pickle(fn)


def update_candles(root='../data/'):
    """ Get and update ALL candle data from Gemini and write to disk. """
    for t in ('1m', '5m', '15m', '30m', '1hr', '6hr', '1day'):
        data = get_candles(t)
        fn = path.join(root, t + '.pkl')
        try:
            data = combine_data(read_data(fn), data)
        except FileNotFoundError:
            pass
        write_data(data, fn)
