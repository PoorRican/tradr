""" The `models` contains abstracts market interactions as data-types, containers, and functors.

The contents of this module are meant to aid in the interactions between both the `core` and `strategies` modules.
"""
from models.data import json_to_df, get_candles,\
     combine_data, read_data, write_data, update_candles
from models.indicator import Indicator, MAX_STRENGTH
from models.trades import Trade, SuccessfulTrade, add_to_df, truncate, FailedTrade, FutureTrade
from models.IndicatorGroup import IndicatorGroup
