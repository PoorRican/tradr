import pandas as pd
from MarketAPI import Market
from strategies.Strategy import Strategy


class Investr(object):
    """ Mediator object which joins `MarketAPI` and `Strategy` """
    def __init__(self, strategy: Strategy):
        self.strategy = strategy

    @property
    def market(self) -> Market:
        return self.strategy.market

    def run(self):
        self.market.update()
        extrema = self.strategy.process(self.market.data)

    def orders(self):
        return self.strategy.orders

    def amount(self):
        return self.strategy.total_amt()

    def fiat(self):
        return self.strategy.total_fiat()

    def save(self):
        self.market.save()
        self.strategy.save()
