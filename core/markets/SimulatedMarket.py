import pandas as pd
from typing import Union

from core.market import Market
from core.MarketAPI import MarketAPI
from models import Trade, SuccessfulTrade


class SimulatedMarket(Market):
    """ A simulated market that uses the data from a real market.

    Has flat fee, always accepts an order.

    Todo:
        - Only accept trade 50% of the time. This adds realism to simulation.
    """

    def __init__(self, model: MarketAPI = None):
        self.asset_pairs = model.asset_pairs
        super().__init__(model.symbol)

        self.model = model

        self.orders = 0

    @property
    def __name__(self):
        return f"Simulated_{self.model.__name__}"

    def _translate(self, trade: Trade, response: dict = None,) -> 'SuccessfulTrade':
        _trade = SuccessfulTrade(trade.amt, trade.rate, trade.side, self.orders)
        self.orders += 1
        return _trade

    def post_order(self, trade: Trade) -> Union['SuccessfulTrade', bool]:
        return self._translate(trade)

    @property
    def fee(self):
        return self.model.fee

    def update(self):
        self.model.update()

    def translate_period(self, freq: str):
        return self.model.translate_period(freq)

    def candles(self, freq: str):
        return self.model.candles(freq)

    def process_point(self, point: pd.Timestamp, freq: str) -> Union['pd.Timestamp', str]:
        return self.model.process_point(point, freq)
