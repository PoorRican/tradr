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

    def __init__(self, model: MarketAPI = None, update: bool = False):
        super().__init__()

        if model:
            self.model = model
            if update:
                self.update()
            else:
                self.model.load()
                self.data = self.model.data
        self.orders = 0

    @property
    def __name__(self):
        return f"Simulated_{self.model.__name__}"

    def _convert(self, trade: Trade, response: dict = None,) -> 'SuccessfulTrade':
        trade = SuccessfulTrade(trade.amt, trade.rate, trade.side, self.orders)
        self.orders += 1
        return trade

    def place_order(self, trade: Trade) -> Union['SuccessfulTrade', bool]:
        return self._convert(trade)

    @property
    def filename(self) -> str:
        return self.model.filename

    def get_fee(self):
        return self.model.get_fee()

    def update(self):
        self.model.update()
        self.data = self.model.data

    def translate_period(self, freq: str):
        return self.model.translate_period(freq)

    def get_candles(self, *args, **kwargs):
        return self.model.get_candles(*args, **kwargs)
