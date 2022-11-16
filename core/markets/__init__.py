""" Specialized `Market` objects go here.

Each file inside this module should only have one class that abstracts interacting with one platform.

Notes:
    `SimulatedMarket` is an ideal market (accepts all trades) that is used for general backtesting. It should be
    improved *by not accepting all orders* to more accurately model real world `Strategy` performance.
"""

from core.market import Market
from core.markets.GeminiMarket import GeminiMarket
from core.markets.SimulatedMarket import SimulatedMarket
