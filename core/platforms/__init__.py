""" Represents the core infrastructure when one single API is being used for both market queries and exchange
    interaction.

    Typically, at least one of these objects will need to be implemented in a strategy.
"""
from core.platforms.GeminiPlatform import *
from core.platforms.SimulatedPlatform import *
