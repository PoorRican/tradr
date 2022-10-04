import pandas as pd
from MarketAPI import GeminiAPI
from StochasticMACD import StochasticMACD


class Investr(object):
    """ Mediator object which joins `MarketAPI` and `Strategy` """
    def __init__(self, market: GeminiAPI, strategy: StochasticMACD):
        self.market = market
        self.strategy = strategy

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

    def calc_market_increase(self, rate):
        """ Calculates potential increase of market value """
        buy_orders = self.strategy.orders[self.strategy.orders['side'] == 'buy']
        return buy_orders['price'].sum() - buy_orders['amt'].sum() * rate

    def calc_profit(self, rate):
        """ Calculates how much profit was generated against the market volatility """
        sell_orders = self.strategy.orders[self.strategy.orders['side'] == 'sell']
        return sell_orders['price'].sum() - self.calc_market_increase(rate)
