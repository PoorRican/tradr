from abc import ABC
from typing import Union

from markets.Market import Market
from models.trades import Trade, SuccessfulTrade


class SimulatedMarket(Market):
    """
    Mock class of `Market`.
    Has flat fee, always accepts an order.
    Todo:
        - Only accept trade 50% of the time. This adds realism to simulation.
    """

    def __init__(self, model: Market):
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
