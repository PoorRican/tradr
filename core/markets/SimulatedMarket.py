from typing import Union

from core.market import Market
from core.MarketAPI import MarketAPI
from models.trades import Trade, SuccessfulTrade


class SimulatedMarket(Market):
    """
    Mock class of `Market`.
    Has flat fee, always accepts an order.
    Todo:
        - Only accept trade 50% of the time. This adds realism to simulation.
    """

    def __init__(self, model: MarketAPI):
        super().__init__()

        self.model = model

        self.orders = 0

    @property
    def __name__(self):
        return f"Simulated_{self.model.__name__}"

    def _convert(self, trade: Trade, response: dict = None,) -> 'SuccessfulTrade':
        trade = SuccessfulTrade(trade.amt, trade.rate, trade.side, self.orders)
        self.orders += 1
        return trade

    def post_order(self, trade: Trade) -> Union['SuccessfulTrade', bool]:
        return self._convert(trade)

    def get_fee(self):
        return self.model.get_fee()

    def update(self):
        self.model.update()

    def translate_period(self, freq: str):
        return self.model.translate_period(freq)

    def candles(self, freq: str):
        return self.model.candles(freq)
