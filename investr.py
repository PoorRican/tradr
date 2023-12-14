import pandas as pd
from core import market, Market
from strategies.strategy import Strategy


class Investr(object):
    """ Mediator object which joins `MarketAPI` and `Strategy` """
    def __init__(self, strategy: Strategy):
        self.strategy = strategy

    @property
    def market(self) -> Market:
        return self.strategy.market

    def run(self):
        self.market.update()
        self.strategy.process()

    def orders(self) -> pd.DataFrame:
        return self.strategy.order_handler.orders

    def amount(self) -> float:
        return self.strategy.order_handler.assets

    def fiat(self) -> float:
        return self.strategy.order_handler.capital

    def save(self):
        self.market.save()
        self.strategy.save()
