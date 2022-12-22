""" Strategies encapsulate the computational and qualitative infrastructure.

    Performs computation necessary to determine when and how much to trade. Inherited instances shall
    define a profitable trade, when and how many trades of a certain type should be executed,
    and the amount of an asset to trade at a single time. Strategies should also determine a minimum amount of
    an asset to hold. In essence, `Strategy` controls the trading process which is ~currently~ synchronous.

    To determine the fitness and performance of the trading strategy, reporting functions can the total amount of
    assets and fiat accrued. This can be used in active implementations as well as during backtesting.
"""
from strategies.strategy import Strategy
from strategies.OscillationMixin import OscillationMixin
from strategies.StaticAlternatingStrategy import StaticAlternatingStrategy
from strategies.ThreeProngAlt import ThreeProngAlt
