from typing import Union
import pandas as pd

from core.api_proto import APIPrototype
from core import Exchange, Market
from models.trades import Trade, SuccessfulTrade


class SimulatedPlatform(Market, Exchange, APIPrototype):
    """ Mock class of `Platform`.

    Uses flat fee, and always accepts an order.

    Todo:
        - Only accept trade 50% of the time. This adds realism to simulation.
    """

    @classmethod
    def _process_secrets(cls, *args):
        pass

    @classmethod
    def post(cls, endpoint: str, data: dict = None) -> dict:
        pass

    def __init__(self, model):
        self.valid_freqs = model.valid_freqs
        super().__init__()

        self.model = model
        self.update()
        self.orders = 0

    def _convert(self, trade: Trade, response: dict = None,) -> 'SuccessfulTrade':
        trade = SuccessfulTrade(trade.amt, trade.rate, trade.side, self.orders)
        self.orders += 1
        return trade

    def place_order(self, trade: Trade) -> Union['SuccessfulTrade', bool]:
        return self._convert(trade)

    @property
    def filename(self) -> str:
        return self.model.filename

    def calc_fee(self):
        return self.model.calc_fee()

    def update(self):
        self.model.update()
        self.data = self.model.data

    def convert_freq(self, freq: str):
        return self.model.convert_freq(freq)

    def get_candles(self, freq: str) -> pd.DataFrame:
        return self.model.get_candles(freq)
